# 17. Future Distributed Architecture

## 17.1 前提：分散は「同じ IR の別配置」

Rivus は既に DAG IR を単一の真実として持つ。分散化とは、その DAG を **stage に
分割し、複数 worker に配置し、edge を network shuffle に置き換える**ことに帰着する
（新しいプログラミングモデルを足さない）。Flink 的なストリーム分散を、Rivus の
flow / chunk / mode / observability の枠内で行う。

## 17.2 構成

```
            ┌──────────────── Coordinator (control plane) ───────────────┐
            │  IR 受領 → stage 分割 → 配置計画 → mode 集約 → 可視化集約    │
            └───────▲───────────────────────────────────────────┬────────┘
                    │ telemetry / error stream                   │ 制御
        ┌───────────┴───────┐   ┌───────────────────┐   ┌────────▼──────────┐
        │ Worker A           │   │ Worker B           │   │ Worker C           │
        │ stage0: source     │──▶│ stage1: filter/proj │──▶│ stage2: group/join │
        │ (local chunk exec) │ shuffle (chunk over net) │ (keyed partition)  │
        └────────────────────┘   └───────────────────┘   └────────────────────┘
```

各 worker は MVP と同じ chunk 実行エンジンを走らせる（`rivus-runtime` を再利用）。

## 17.3 stage 分割と shuffle

- **stage 境界**: stateful（group/join）と branch/merge を境界候補にする。stateless
  fusion 済みカーネルは1 stage 内に閉じる（08）。
- **partitioning**: keyed operator（`|#` group / `&` join）は key の hash で chunk を
  partition し、同 key を同 worker に集める（shuffle）。
- **shuffle フォーマット**: Arrow IPC（03/12 と同じ）。zero-copy 寄り・言語非依存・
  hidden serialization を避ける。
- **ordering**: 必要な flow には chunk meta の id/seq で順序復元。

## 17.4 chunk-native が効く点

chunk は元々 splittable / metadata 付き / checkpointable（03）。分散でもそのまま
転送単位になり、`ChunkMeta`（id / mode / corrupt / warnings）がノード跨ぎの観測と
回復に使える。1 行単位の分散より遥かに効率的。

## 17.5 backpressure とフロー制御（分散）

ローカルの credit（05 §5.4）を network に延伸：下流 worker が credit を上流へ返し、
shuffle channel を bounded に保つ。詰まりは Coordinator が観測し、mode を
escalate（degraded）して buffer/priority を調整する。

## 17.6 continue-first の分散版

- **部分障害**: 1 worker / 1 branch の失敗で全体を止めない。該当 partition を
  isolation mode に落とし、error stream を Coordinator に集約（13）。
- **recovery**: checkpoint（chunk id 境界）から replay（`stream Label` の分散版）。
  破損 chunk は reroute。
- fatal のみ全停止。

## 17.7 observability の分散版

各 worker の `RuntimeSnapshot`（11/14）を Coordinator が集約し、単一の execution
graph として可視化する。`visualize Runtime;` はクラスタ全体のライブグラフを返す。
OpenTelemetry へのトレース連携も同じ snapshot から。

## 17.8 一貫性とセマンティクス

- **delivery**: at-least-once を既定、checkpoint + idempotent sink で
  effectively-once を選択可能に。
- **state**: keyed state は partition local。再配置時は checkpoint 経由で移送。
- **時間**: event-time / watermark は Phase 3 で導入（Flink 準拠の窓処理）。

## 17.9 段階導入

```
Phase 3a  single-node 並列の延長として 2-node shuffle（Arrow IPC over TCP）
Phase 3b  Coordinator による stage 配置 + telemetry/error 集約 + 分散可視化
Phase 3c  checkpoint/replay・effectively-once・watermark・autoscale
```

中核（IR / chunk / operator / telemetry）を変えずに「配置とトランスポート」を
足していく。ローカル MVP がそのまま分散の1 worker になる、が設計の要。

### 段階表

| | Distributed |
|---|---|
| MVP | なし（single-node）。ただし IR/chunk/telemetry が分散の前提を満たす |
| 次 | （Phase 1-2 の並列化が分散の足場） |
| 将来 | stage 分割 / Arrow shuffle / Coordinator / checkpoint replay / watermark |
