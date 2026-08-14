# slimHub — Zero-Knowledge Ciphertext Ring Buffer

> A thorough, pure, mindless ciphertext ring buffer. No business logic, no file awareness, no Session management.

**[English](README.en.md) | [中文](README.md)**

---

## 1. Design Philosophy

slimHub's single bottom-line positioning: **ciphertext ring buffer (Ring Buffer)**.

It only recognizes two things:
- **Blind-ID** (what data)
- **Timestamp** (when it arrived)

slimHub does not participate in any file-level or Session-level business logic. It does not need to know what a "file" is, nor does it care whether slices come from the same channel.

Clear division of labor across the troika:

| Module | Role | Business awareness |
|--------|------|--------------------|
| **slimSync** | edge autonomy, send whenever online | aware of files, slices, Checkpoints |
| **slimHub** | **mindless ciphertext cache** | unaware of any business |
| **slimRagSvr** | the single backend business focal point | aware of semantics, graphs, dedup |

---

## 2. Topology

```
                          ┌──────────────────┐
         Zenoh Pub        │    slimHub        │       Zenoh Sub/Pull
slimSync ────────────────►│  (pure Rust proc)  ├────────────────► slimRagSvr
  (mindless drift,        │                    │  (sole backend focal point)
   doesn't care which     │  ┌──────────────┐  │  (assembles via Blind-ID)
   Hub it's on)           │  │  sled engine │  │
                          │  │ (ring buffer)│  │
                          │  └──────────────┘  │
                          └────────────────────┘

Multi-Hub topology:
slimSync ────► slimHub-A ────► slimRagSvr (primary)
         \──► slimHub-B ────► slimRagSvr (standby)
  (mindless switch,      (each independent namespace, no data sync)
   no stickiness)
```

### 2.1 Process Form

**Standalone Rust process**, using an embedded `zenoh::open()` session.

| Option | Pros | Cons |
|--------|------|------|
| **Standalone process** | self-controlled resources, independent deployment, tiny binary | must manage the Zenoh session yourself |
| zenohd plugin | reuses official routing capability | heavy C++ dependency, complex config, bloated binary |

### 2.2 Network Topology

| Mode | Description | Use case |
|------|-------------|----------|
| **Peer** | slimHub joins the network as a Zenoh Peer | standard deployment |
| **Router** | slimHub runs as a Zenoh Router | multi-Hub edge gateway |

---

## 3. Storage Engine: sled Ring Buffer

### 3.1 Why sled

| Dimension | sled | RocksDB |
|-----------|------|---------|
| Language | pure Rust | C++ bindings |
| Cross-compilation | ✔ zero pain | ❌ extremely difficult |
| Single-file DB | ✔ | ❌ multiple files |
| Lock-free concurrency | ✔ lock-free | ❌ |
| Binary size | ~3MB | ~20MB+ |

### 3.2 Data Model

sled Tree design:

| Tree | Purpose | Key pattern | Value | Eviction policy |
|------|---------|-------------|-------|-----------------|
| `pending` | ciphertext awaiting consumption | `blind_id(16B) + timestamp(8B)` | `encrypted_payload` | dual-watermark truncation + 7-day TTL |
| `acknowledged` | consumed dedup cache | `blind_id(16B)` | `timestamp(8B)` | FIFO ring overwrite, 24h eviction |

```rust
// pending Key structure
//   blind_id(16B) + timestamp_be(8B)  ← big-endian timestamp for natural ordering
//   prefix scan on blind_id for fast lookup

// acknowledged Key structure
//   blind_id(16B)
//   secondary cache for dedup lookups only
```

### 3.3 TTL & Watermarks

```
Disk usage
    │
    ├── high watermark (80%)  ──► evict oldest pending data (LRU), emit alert
    ├── low watermark (60%)   ──► stop eviction, return to normal
    ├── soft limit (90%)      ──► reject new Pub writes, broadcast backpressure to slimSync
    └── hard limit (95%)      ──► process exits safely to prevent disk full

pending tree:
  - dual-watermark truncation (LRU eviction of oldest data between 80% and 60%)
  - hard TTL: data older than 7 days auto-evicted

acknowledged tree:
  - pure FIFO ring overwrite, fixed slot size
  - oldest records naturally squeezed out by new ones
  - no timer polling needed, extremely low overhead
```

---

## 4. Core Data Flow

### 4.1 Normal Flow (with async ACK)

```
slimSync                    slimHub                        slimRagSvr
    │                          │                              │
    ├─ zenoh.put(chunk) ──────►│                              │
    │                          ├─ write to sled::pending      │
    │                          ├─ fsync → safely on disk      │
    │                          ├─ forward immediately ───────►│
    │                          │                              ├─ decrypt → ingest
    │                          │                              ├─ zenoh.put(ack) ──►
    │                          ◄──────────────────────────────┤
    │                          ├─ pending → acknowledged       │
    ◄─ async ack ──────────────┤                              │
    │  (blind_id confirmed=1)  │                              │
    │                          │                              │
```

**ACK timing**: after slimHub writes the ciphertext to sled and `fsync`-persists it, it immediately publishes a lightweight ACK frame to topic `slim/sync/ack/{blind_id}`. slimSync listens on this topic persistently and sets its local `sent_hashes.confirmed` to 1 upon receipt.

### 4.2 slimRagSvr Offline Flow

```
slimSync                    slimHub (RAG offline)           slimRagSvr
    │                          │                              │
    ├─ zenoh.put(chunk) ──────►│                              │
    │                          ├─ write to sled::pending      │
    │                          ├─ fsync → on disk             │
    └─ async ack ◄─────────────┤  (ACK on write, no wait for RAG)│
    │  (confirmed=1)           │                              │
    │                          │                              │
    │                          │          ... later ...       │
    │                          │                              │
    │                          ◄────── slimRagSvr online ─────┤
    │                          ├─ scan unconsumed pending data│
    │                          ├─ batch replay ──────────────►│
    │                          │                              ├─ decrypt → ingest
    │                          │                              ├─ batch ack ──────►
    │                          ◄──────────────────────────────┤
    │                          └─ pending → acknowledged       │
```

**Key design**: slimHub ACKs right after writing to sled, without waiting for slimRagSvr consumption. slimSync advances its local `confirmed=1` upon ACK, without caring whether RAG is online.

### 4.3 slimSync Offline → Recovery Flow

```
slimSync (offline)          slimHub                       slimRagSvr
    │                          │                              │
    │   local SQLite accrual   │                              │
    │   PUB queue buffering    │                              │
    │   sent_hashes.confirmed=0│                              │
    │                          │                              │
    ├─ back online ───────────►│                              │
    │  (auto-connect nearest Hub) ├─ write to sled::pending    │
    │                          ├─ fsync → on disk             │
    ├─ async ack ◄─────────────┤                              │
    │  confirmed=1             │  forward ───────────────────►│
    │                          │                              └─ normal consumption
```

**No stickiness**: slimSync does not care whether it connects to Hub-A or Hub-B — it just sends. Unconfirmed `confirmed=0` data is automatically retransmitted.

---

## 5. Multi-Hub Topology

### 5.1 Design Principles

**slimSync drifts mindlessly — no stickiness logic is introduced.**

| Principle | Description |
|-----------|-------------|
| Edge is stateless | slimSync does not care which Hub it is connected to, no Session binding |
| No inter-Hub sync | each has an independent sled, no distributed consensus |
| RAG-side assembly | slimRagSvr uses Blind-ID as the sole index, reassembling out-of-order across channels |

### 5.2 Multi-Hub Deployment & Namespace Isolation

```
slimSync ────► Hub-A (namespace: slim/hub-A/sync/...)
         \──► Hub-B (namespace: slim/hub-B/sync/...)
```

- Each Hub's Zenoh topic space carries its own ID
- `slimRagSvr` consumes both `slim/hub-A/sync/**` and `slim/hub-B/sync/**`
- Cross-channel data is reassembled out-of-order at the RAG end via **Blind-ID**

### 5.3 Packet Loss Recovery: Checkpoint + confirmed Fallback

```
Scenario: slimSync sends slices 1~10 to Hub-A
     slices 9~10 not yet persisted when the network drops
     automatically drifts to Hub-B

Recovery flow:
  1. slices 9~10 received no ACK from Hub-A
  2. their sent_hashes.confirmed is still 0 locally
  3. after drifting to Hub-B, slimSync auto-retransmits confirmed=0 slices
  4. Hub-B writes to pending and publishes ACK
  5. slimRagSvr consumes from both Hubs, Blind-ID global dedup
```

**This is the power of CHECKPOINT** — the edge precisely retransmits via the unconfirmed fingerprint chain, requiring no stickiness or Session binding.

---

## 6. Backpressure Signaling

### 6.1 Trigger Mechanism

slimHub runs an internal dedicated monitoring thread checking disk water level every **500ms**:

```
[monitor thread] every 500ms
       │
       ▼
check sled disk usage
       │
       ├── < 80% → no action
       ├── 80%~89% → broadcast WARNING backpressure frame
       ├── 90%~94% → broadcast CRITICAL backpressure frame, reject new Pub writes
       └── ≥ 95% → process exits safely
```

### 6.2 Communication Method

**Zenoh Pub async broadcast push** (not Query pull).

```
slimHub ── zenoh.put("slim/hub/backpressure/{hub_id}", BackpressureFrame)

slimSync subscribes to "slim/hub/backpressure/**"
  → on CRITICAL: lengthen local debounce window, pause PUB queue stepping
  → on NORMAL: resume normal sending
```

```rust
// actual definition in slim-common/src/types.rs
struct BackpressureFrame {
    hub_id: String,
    disk_usage_pct: u32,
    level: BackpressureLevel,   // Normal / Warning / Critical
    suggested_interval_ms: u64,
}
```

---

## 7. Zero-Trust Security Model

| Security layer | Description |
|----------------|-------------|
| Data encryption | slimHub stores only ChaCha20 ciphertext, never decrypts |
| Dedup credential | Blind-ID is an HMAC blind hash, cannot be reversed to plaintext |
| ACL | Zenoh access control restricts Pub/Sub topic scope |
| Storage encryption | optional: enable LUKS/dm-crypt on the sled disk |

**Even with the full disk in hand, a slimHub administrator can read no plaintext.**

---

## 8. Resource Constraints & Deployment

| Metric | Target |
|--------|--------|
| Binary size | < 8MB (statically compiled) |
| Resident memory | < 30MB RSS (excluding OS Page Cache) |
| Disk | on demand; 10GB+ SSD recommended |
| Throughput | > 100MB/s per instance |
| Platforms | x86_64 / ARM64 |
| Deployment | single executable, unzip-and-run |

---

## 9. Positioning Summary

> **slimHub is a mindless ciphertext pond. Give it (ciphertext) and it stores, stores then ACKs, pulls and it hands over — never asks about business.**

| What it does NOT do | What it DOES do |
|---------------------|-----------------|
| does not perceive files | writes to sled, fsync, ACK |
| does not manage Sessions | forwards to slimRagSvr |
| no sticky binding | broadcasts backpressure |
| does not participate in dedup | maintains the acknowledged dedup cache |
| no consensus | evicts by TTL/watermark |

---

## 10. Behavior Red Lines (Inviolable Architecture Boundaries)

> The purpose of these red lines is to prevent slimHub from degrading from a "mindless cache" back into a "stateful business node".
> **Any contributor violating any of the following must have their PR rejected.**

```
slimHub behavior red lines (fixed before coding):

  ✔ receive data → write sled → fsync → emit ACK
  ✔ on pull → read from pending → push to slimRagSvr
  ✔ watermark exceeded → broadcast backpressure frame

  ❌ no query, no join, no sort, no aggregation, no Session maintenance
  ❌ no file awareness, no Session awareness, no knowledge of which sync sent it
  ❌ no plaintext caching, no decryption, no content auditing
  ❌ no participation in dedup decisions (dedup is slimRagSvr's job)
  ❌ no distributed consensus of any kind (Paxos/Raft)
```

**The moment slimHub gains business awareness, the system's "mindless cache" positioning is broken. Any impulse to add "just a tiny bit" must be pushed back with these red lines.**
