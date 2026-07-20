# slimHub — 无脑密文环形缓冲区

> 彻底、纯粹、无脑的密文环形缓存区。不参与任何业务逻辑，不感知文件，不管理 Session。

---

## 一、设计哲学

slimHub 的唯一底层定位：**密文环形缓存区（Ring Buffer）**。

它只认两件事：
- **Blind-ID**（什么数据）
- **Timestamp**（什么时候来的）

slimHub 不参与任何文件级、Session 级的业务逻辑，不需要知道什么是"文件"，不需要关心切片是否来自同一个通道。

三驾马车清晰的分工边界：

| 模块 | 定位 | 业务感知 |
|------|------|----------|
| **slimSync** | 端侧自治，有网就发 | 感知文件、切片、Checkpoint |
| **slimHub** | **无脑密文缓存** | 不感知任何业务 |
| **slimRagSvr** | 后端唯一业务收拢点 | 感知语义、图谱、去重 |

---

## 二、拓扑架构

```
                          ┌──────────────────┐
         Zenoh Pub        │    slimHub        │       Zenoh Sub/Pull
slimSync ────────────────►│  (纯 Rust 进程)    ├────────────────► slimRagSvr
  (无脑漂移, 不关心       │                    │  (后端唯一业务收拢)
   当前连了哪个Hub)       │  ┌──────────────┐  │  (通过 Blind-ID 拼装)
                          │  │  sled 引擎    │  │
                          │  │ (环形缓冲)    │  │
                          │  └──────────────┘  │
                          └────────────────────┘

多 Hub 拓扑：
slimSync ────► slimHub-A ────► slimRagSvr (主)
         \──► slimHub-B ────► slimRagSvr (备)
  (无脑切换,       (各自独立 namespace, 不做数据同步)
   不粘滞)
```

### 2.1 进程形态

**独立 Rust 进程**，使用 `zenoh::open()` 嵌入式会话。

| 方案 | 优点 | 缺点 |
|------|------|------|
| **独立进程** | 可自控资源、独立部署、编译体积极小 | 需自行管理 Zenoh 会话 |
| zenohd 插件 | 复用官方路由能力 | C++ 依赖重、配置复杂、体积暴增 |

### 2.2 网络拓扑

| 模式 | 说明 | 适用场景 |
|------|------|----------|
| **Peer** | slimHub 作为 Zenoh Peer 接入网络 | 标准部署 |
| **Router** | slimHub 作为 Zenoh Router 运行 | 多 Hub 边缘网关 |

---

## 三、存储引擎：sled 环形缓冲

### 3.1 为什么选择 sled

| 维度 | sled | RocksDB |
|------|------|---------|
| 语言 | 纯 Rust | C++ 绑定 |
| 交叉编译 | ✔ 零痛点 | ❌ 极困难 |
| 单文件 DB | ✔ | ❌ 多文件 |
| 无锁并发 | ✔ lock-free | ❌ |
| 二进制体积 | ~3MB | ~20MB+ |

### 3.2 数据模型

sled Tree 设计：

| Tree | 用途 | Key 模式 | Value | 淘汰策略 |
|------|------|----------|-------|----------|
| `pending` | 待消费密文 | `blind_id(16B) + timestamp(8B)` | `encrypted_payload` | 双水位截断 + 7天 TTL |
| `acknowledged` | 已消费去重缓存 | `blind_id(16B)` | `timestamp(8B)` | FIFO 环形覆盖，24h 淘汰 |

```rust
// pending Key 结构
//   blind_id(16B) + timestamp_be(8B)  ← timestamp 大端序保证自然有序
//   前缀扫描 blind_id 即可快速定位

// acknowledged Key 结构
//   blind_id(16B)
//   仅作为去重回查的二级缓存
```

### 3.3 TTL 与水位线

```
磁盘使用量
    │
    ├── 高水位 (80%)  ──► 启动 pending 最旧数据淘汰（LRU），发出告警
    ├── 低水位 (60%)  ──► 停止淘汰，恢复正常
    ├── 软限制 (90%)  ──► 拒绝新 Pub 写入，向 slimSync 广播背压信号
    └── 硬限制 (95%)  ──► 进程安全退出，防止磁盘写满

pending 树：
  - 双水位截断（80%~60% 之间的最旧数据 LRU 淘汰）
  - 强 TTL：超过 7 天的数据自动淘汰

acknowledged 树：
  - 纯 FIFO 环形覆盖，固定 Slot 大小
  - 最老记录被新记录自然挤出
  - 不需要定时器轮询清理，开销极低
```

---

## 四、核心数据流

### 4.1 正常流（含异步 ACK）

```
slimSync                    slimHub                        slimRagSvr
    │                          │                              │
    ├─ zenoh.put(chunk) ──────►│                              │
    │                          ├─ 写入 sled::pending          │
    │                          ├─ fsync → 磁盘安全落盘        │
    │                          ├─ 立即转发 ──────────────────►│
    │                          │                              ├─ 解密 → 入库
    │                          │                              ├─ zenoh.put(ack) ──►
    │                          ◄──────────────────────────────┤
    │                          ├─ pending → acknowledged       │
    ◄─ async ack ──────────────┤                              │
    │  (blind_id confirmed=1)  │                              │
    │                          │                              │
```

**ACK 时序**：slimHub 将密文写入 sled 并 `fsync` 落盘后，立即向主题 `slim/sync/ack/{blind_id}` 发布一个轻量 ACK 帧。slimSync 常驻监听该主题，收到后将本地 `sent_hashes.confirmed` 置为 1。

### 4.2 slimRagSvr 离线流

```
slimSync                    slimHub (RAG 离线)              slimRagSvr
    │                          │                              │
    ├─ zenoh.put(chunk) ──────►│                              │
    │                          ├─ 写入 sled::pending          │
    │                          ├─ fsync → 落盘                │
    └─ async ack ◄─────────────┤  (写入即 ACK，不等待 RAG)    │
    │  (confirmed=1)           │                              │
    │                          │                              │
    │                          │          ... 一段时间后 ...  │
    │                          │                              │
    │                          ◄────── slimRagSvr 上线 ──────┤
    │                          ├─ 扫描 pending 中未消费数据   │
    │                          ├─ 批量回放 ──────────────────►│
    │                          │                              ├─ 解密 → 入库
    │                          │                              ├─ 批量 ack ──────►
    │                          ◄──────────────────────────────┤
    │                          └─ pending → acknowledged       │
```

**关键设计**：slimHub 写入 sled 后就 ACK，不等待 slimRagSvr 消费。slimSync 收到 ACK 即推进本地 `confirmed=1`，无需关心 RAG 是否在线。

### 4.3 slimSync 离线 → 恢复流

```
slimSync (离线)              slimHub                       slimRagSvr
    │                          │                              │
    │   本地 SQLite 累积       │                              │
    │   PUB 队列暂存           │                              │
    │   sent_hashes.confirmed=0│                              │
    │                          │                              │
    ├─ 恢复在线 ──────────────►│                              │
    │  (自动连上最近的 Hub)    ├─ 写入 sled::pending           │
    │                          ├─ fsync → 落盘                │
    ├─ async ack ◄─────────────┤                              │
    │  confirmed=1             │  转发 ──────────────────────►│
    │                          │                              └─ 正常消费
```

**无粘滞**：slimSync 不关心连的是 Hub-A 还是 Hub-B，只管发。未确认的 `confirmed=0` 数据会自动重传。

---

## 五、多 Hub 拓扑

### 5.1 设计原则

**slimSync 无脑漂移，不引入任何粘滞逻辑。**

| 原则 | 说明 |
|------|------|
| 端侧无状态 | slimSync 不关心当前连哪个 Hub，不绑定 Session |
| Hub 间不同步 | 各自独立 sled，不做分布式共识 |
| RAG 端拼装 | slimRagSvr 以 Blind-ID 为唯一索引，跨通道乱序组合 |

### 5.2 多 Hub 部署与命名空间隔离

```
slimSync ────► Hub-A (namespace: slim/hub-A/sync/...)
         \──► Hub-B (namespace: slim/hub-B/sync/...)
```

- 每个 Hub 的 Zenoh 主题空间带上自身 ID
- `slimRagSvr` 同时消费 `slim/hub-A/sync/**` 和 `slim/hub-B/sync/**`
- 跨通道数据通过 **Blind-ID** 在 RAG 端做乱序拼装

### 5.3 丢包恢复：Checkpoint + confirmed 兜底

```
场景：slimSync 发送切片 1~10 到 Hub-A
     切片 9~10 未落盘时网络断开
     自动漂移到 Hub-B

恢复流程：
  1. 切片 9~10 未收到 Hub-A 的 ACK
  2. 本地 sent_hashes 中 9~10 的 confirmed 仍为 0
  3. slimSync 漂移到 Hub-B 后，自动重传 confirmed=0 的切片
  4. Hub-B 收到后写入 pending，发布 ACK
  5. slimRagSvr 从两个 Hub 消费，Blind-ID 全局去重
```

**这就是 CHECKPOINT 的威力** — 端侧通过未确认的指纹链精准重传，不需要任何粘滞或 Session 绑定。

---

## 六、背压信号

### 6.1 触发机制

slimHub 内部独立监控线程，每隔 **500ms** 检查一次磁盘水位：

```
[监控线程] 每 500ms
       │
       ▼
检查 sled 磁盘使用率
       │
       ├── < 80% → 不动作
       ├── 80%~89% → 广播 WARNING 背压帧
       ├── 90%~94% → 广播 CRITICAL 背压帧，拒绝新 Pub 写入
       └── ≥ 95% → 进程安全退出
```

### 6.2 通信方式

**Zenoh Pub 异步广播推送**（非 Query 拉取）。

```
slimHub ── zenoh.put("slim/hub/backpressure/{hub_id}", BackpressureFrame)

slimSync 订阅 "slim/hub/backpressure/**"
  → 收到 CRITICAL: 本地防抖窗口拉长，PUB 队列步进挂起
  → 收到 NORMAL: 恢复正常发送
```

```protobuf
message BackpressureFrame {
    string hub_id = 1;
    uint32 disk_usage_pct = 2;
    BackpressureLevel level = 3;    // NORMAL / WARNING / CRITICAL
    uint64 suggested_interval_ms = 4;
}

enum BackpressureLevel {
    NORMAL = 0;
    WARNING = 1;
    CRITICAL = 2;
}
```

---

## 七、无信任安全模型

| 安全层 | 说明 |
|--------|------|
| 数据加密 | slimHub 仅存储 ChaCha20 密文，永不解密 |
| 去重凭证 | Blind-ID 为 HMAC 盲哈希，不可反推原文 |
| ACL | Zenoh 访问控制限制 Pub/Sub 主题范围 |
| 存储加密 | 可选：sled 所在磁盘启用 LUKS/dm-crypt |

**slimHub 管理员即使拿到完整磁盘，也读不到任何明文内容。**

---

## 八、资源约束与部署

| 指标 | 目标 |
|------|------|
| 二进制体积 | < 8MB（静态编译） |
| 常驻内存 | < 30MB RSS（不含 OS Page Cache） |
| 磁盘 | 按需配置，推荐 10GB+ SSD |
| 吞吐 | 单实例 > 100MB/s |
| 平台 | x86_64 / ARM64 |
| 部署 | 单文件可执行，解压即用 |

---

## 九、定位总结

> **slimHub 就是一个无脑的密文水池。给（密文）就存，存完就 ACK，被拉就给，绝不过问业务。**

| 它不做什么 | 它做什么 |
|-----------|---------|
| 不感知文件 | 写入 sled、fsync、ACK |
| 不管理 Session | 转发给 slimRagSvr |
| 不做粘滞绑定 | 广播背压 |
| 不参与去重 | 维护 acknowledged 去重缓存 |
| 不做共识 | 按 TTL/水位淘汰 |

---

## 十、行为红线（不可违反的架构边界）

> 以下红线的目的是防止 slimHub 从"无脑缓存"退化回"有状态业务节点"。
> **任何 contributor 违反下列任意一条，必须否决该 PR。**

```
slimHub 行为红线（编码前定死）：

  ✔ 收到数据 → 写入 sled → fsync → 发出 ACK
  ✔ 被拉取 → 从 pending 读出 → 推给 slimRagSvr
  ✔ 水位超限 → 广播背压帧

  ❌ 不查询、不关联、不排序、不聚合、不维护 Session
  ❌ 不感知文件、不感知 Session、不感知哪个 sync 发的
  ❌ 不缓存明文、不解密、不审计内容
  ❌ 不参与去重决策（去重是 slimRagSvr 的事）
  ❌ 不引入任何形式的分布式共识（Paxos/Raft）
```

**一旦 slimHub 有了业务感知，整个系统的"无脑缓存"定位就破了。任何"只加一点点"的冲动，都要用这条红线压回去。**
