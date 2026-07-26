mod config;
mod storage;

use clap::{Parser, Subcommand};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::signal;

const PID_FILE: &str = "/tmp/slimhub.pid";
const SOCKET_PATH: &str = "/tmp/slimhub.sock";

#[derive(Parser)]
#[command(name = "slimhub", version)]
struct Cli {
    #[arg(short, long)]
    config: Option<String>,

    /// Daemon mode (fork to background)
    #[arg(short = 'd', long)]
    daemon: bool,

    /// Sled database path (overrides config)
    #[arg(short = 's', long)]
    db_path: Option<String>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Show daemon status
    Status,
    /// Reload configuration
    Reload,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct IpcRequest {
    cmd: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct IpcResponse {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

fn main() {
    let cli = Cli::parse();

    if cli.daemon {
        let daemonize = daemonize::Daemonize::new()
            .pid_file(PID_FILE)
            .working_directory("/")
            .umask(0o027);
        daemonize.start().expect("failed to daemonize");
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(run_daemon(cli));
        return;
    }

    if let Some(cmd) = &cli.command {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(handle_client_command(cmd));
        return;
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(run_foreground(cli));
}

async fn run_foreground(cli: Cli) {
    tracing_subscriber::fmt::init();
    let cfg = config::Config::load(cli.config.clone()).expect("failed to load config");
    let cfg = apply_cli_overrides(cli, cfg);

    let hub_id = cfg.hub_id.clone();
    let store = Arc::new(storage::Storage::open(&cfg.db_path).expect("failed to open storage"));
    tracing::info!("storage ready, pending items: {}", store.pending_len());

    let session = match zenoh::open(zenoh::Config::default()).await {
        Ok(s) => Arc::new(s),
        Err(e) => {
            tracing::error!("zenoh: failed to connect: {}", e);
            return;
        }
    };

    run_hub(store, session, hub_id, cfg).await;
}

async fn run_daemon(cli: Cli) {
    tracing_subscriber::fmt::init();
    let _ = tokio::fs::remove_file(SOCKET_PATH).await;

    let listener = UnixListener::bind(SOCKET_PATH).expect("failed to bind Unix socket");
    let start = std::time::Instant::now();

    let cfg = config::Config::load(cli.config.clone()).expect("failed to load config");
    let cfg = apply_cli_overrides(cli, cfg);

    let hub_id = cfg.hub_id.clone();
    let store = Arc::new(storage::Storage::open(&cfg.db_path).expect("failed to open storage"));
    tracing::info!("storage ready, pending items: {}", store.pending_len());

    let session = match zenoh::open(zenoh::Config::default()).await {
        Ok(s) => Arc::new(s),
        Err(e) => {
            tracing::error!("zenoh: failed to connect: {}", e);
            return;
        }
    };

    let hub_handle = tokio::spawn(async move {
        run_hub(store, session, hub_id, cfg).await;
    });

    let ipc_handle = tokio::spawn(async move {
        serve_ipc(listener, start).await;
    });

    signal::ctrl_c().await.expect("failed to listen for ctrl+c");
    tracing::info!("shutting down...");
    hub_handle.abort();
    ipc_handle.abort();
    let _ = tokio::fs::remove_file(SOCKET_PATH).await;
    let _ = std::fs::remove_file(PID_FILE);
}

async fn run_hub(
    store: Arc<storage::Storage>,
    session: Arc<zenoh::Session>,
    hub_id: String,
    cfg: config::Config,
) {
    // 1. Subscribe to chunks
    {
        let store = store.clone();
        let session = session.clone();
        let sub_topic = format!("{}/**", slim_common::topics::CHUNK_PREFIX);
        tokio::spawn(async move {
            match session.declare_subscriber(&sub_topic).await {
                Ok(subscriber) => {
                    tracing::info!("subscribed: {}", sub_topic);
                    while let Ok(sample) = subscriber.recv_async().await {
                        let key_expr = sample.key_expr().to_string();
                        let blind_id_hex = match key_expr.rsplit('/').next() {
                            Some(seg) => seg,
                            None => continue,
                        };
                        let blind_id = match hex::decode(blind_id_hex) {
                            Ok(bytes) if bytes.len() == 16 => {
                                let mut arr = [0u8; 16];
                                arr.copy_from_slice(&bytes);
                                arr
                            }
                            _ => continue,
                        };
                        let payload: Vec<u8> = sample.payload().to_bytes().into();
                        match store.insert_pending(&blind_id, &payload) {
                            Ok((_is_new, _key)) => {
                                if _is_new {
                                    tracing::debug!("inserted pending: {}", blind_id_hex);
                                }
                                let ack_topic =
                                    format!("{}/{}", slim_common::topics::ACK_PREFIX, blind_id_hex);
                                if let Err(e) = session.put(&ack_topic, "OK").await {
                                    tracing::error!("ack send failed: {:?}", e);
                                }
                            }
                            Err(e) => {
                                tracing::error!("insert_pending error: {:?}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("failed to declare subscriber: {:?}", e);
                }
            }
        });
    }

    // 2. Watermark monitor
    {
        let store = store.clone();
        let session = session.clone();
        let hub_id = hub_id.clone();
        let disk_cap = cfg.disk_capacity_gb * 1024 * 1024 * 1024;
        let high = cfg.high_watermark_pct;
        let low = cfg.low_watermark_pct;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                match store.trim_to_watermark(disk_cap, high, low) {
                    Ok(true) => {
                        let topic = format!(
                            "{}/{}",
                            slim_common::topics::BACKPRESSURE_PREFIX,
                            hub_id
                        );
                        let frame = serde_json::json!({
                            "hub_id": hub_id,
                            "level": "CRITICAL",
                            "disk_usage_pct": high,
                        });
                        if let Err(e) = session.put(&topic, frame.to_string()).await {
                            tracing::error!("backpressure broadcast failed: {:?}", e);
                        }
                        tracing::warn!("backpressure broadcast: disk at {}%", high);
                    }
                    Ok(false) => {}
                    Err(e) => tracing::error!("watermark error: {:?}", e),
                }
            }
        });
    }

    // 3. Pull-push to slimRagSvr
    {
        let store = store.clone();
        let session = session.clone();
        let hub_id = hub_id.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                if store.pending_is_empty() {
                    continue;
                }
                let batch = store.pop_pending_batch(64);
                for (key, value) in &batch {
                    let topic = format!("slim/hub/{}/data", hub_id);
                    if let Err(e) = session.put(&topic, value.clone()).await {
                        tracing::warn!("pull push error: {:?}", e);
                        continue;
                    }
                    if let Err(e) = store.confirm_consumed(key) {
                        tracing::error!("confirm consumed error: {:?}", e);
                    }
                }
            }
        });
    }

    // 4. Wait for shutdown
    signal::ctrl_c().await.expect("failed to listen for ctrl+c");
    tracing::info!("shutting down...");
}

fn apply_cli_overrides(cli: Cli, mut cfg: config::Config) -> config::Config {
    if let Some(db_path) = cli.db_path {
        cfg.db_path = db_path;
    }
    cfg
}

async fn serve_ipc(listener: UnixListener, start: std::time::Instant) {
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("accept: {}", e);
                continue;
            }
        };

        let start = start;
        tokio::spawn(async move {
            let (rd, mut wr) = tokio::io::split(stream);
            let mut reader = BufReader::new(rd);
            let mut line = String::new();

            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) => break,
                    Err(e) => {
                        let resp = IpcResponse {
                            ok: false,
                            data: None,
                            error: Some(format!("read error: {}", e)),
                        };
                        let _ = wr
                            .write_all(
                                format!("{}\n", serde_json::to_string(&resp).unwrap()).as_bytes(),
                            )
                            .await;
                        break;
                    }
                    _ => {}
                }

                let req: IpcRequest = match serde_json::from_str(line.trim()) {
                    Ok(r) => r,
                    Err(e) => {
                        let resp = IpcResponse {
                            ok: false,
                            data: None,
                            error: Some(format!("invalid JSON: {}", e)),
                        };
                        let _ = wr
                            .write_all(
                                format!("{}\n", serde_json::to_string(&resp).unwrap()).as_bytes(),
                            )
                            .await;
                        continue;
                    }
                };

                let response = match req.cmd.as_str() {
                    "status" => {
                        let uptime = start.elapsed().as_secs();
                        IpcResponse {
                            ok: true,
                            data: Some(serde_json::json!({
                                "uptime_secs": uptime,
                                "version": env!("CARGO_PKG_VERSION"),
                                "service": "slimhub",
                            })),
                            error: None,
                        }
                    }
                    "reload" => {
                        tracing::info!("reload requested via IPC");
                        IpcResponse {
                            ok: true,
                            data: None,
                            error: None,
                        }
                    }
                    _ => IpcResponse {
                        ok: false,
                        data: None,
                        error: Some(format!("unknown cmd: {}", req.cmd)),
                    },
                };

                let resp_json = serde_json::to_string(&response).unwrap_or_default();
                if let Err(e) = wr.write_all(format!("{}\n", resp_json).as_bytes()).await {
                    tracing::error!("write response: {}", e);
                    break;
                }
            }
        });
    }
}

async fn handle_client_command(cmd: &Commands) {
    let stream = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio::net::UnixStream::connect(SOCKET_PATH),
    )
    .await;

    let mut stream = match stream {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            eprintln!("error: cannot connect to daemon ({}): {}", SOCKET_PATH, e);
            std::process::exit(1);
        }
        Err(_) => {
            eprintln!(
                "error: daemon not running ({} connection timeout)",
                SOCKET_PATH
            );
            std::process::exit(1);
        }
    };

    let req = match cmd {
        Commands::Status => IpcRequest {
            cmd: "status".into(),
        },
        Commands::Reload => IpcRequest {
            cmd: "reload".into(),
        },
    };

    let req_json = serde_json::to_string(&req).unwrap();
    stream
        .write_all(format!("{}\n", req_json).as_bytes())
        .await
        .unwrap();

    let (rd, _wr) = stream.split();
    let mut reader = BufReader::new(rd);
    let mut resp_line = String::new();
    reader.read_line(&mut resp_line).await.unwrap();

    if let Ok(resp) = serde_json::from_str::<IpcResponse>(&resp_line) {
        if resp.ok {
            if let Some(data) = resp.data {
                println!("{}", serde_json::to_string_pretty(&data).unwrap());
            } else {
                println!("ok");
            }
        } else {
            eprintln!(
                "error: {}",
                resp.error.unwrap_or_else(|| "unknown".into())
            );
            std::process::exit(1);
        }
    } else {
        eprintln!("error: invalid response from daemon");
        std::process::exit(1);
    }
}
