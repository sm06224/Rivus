# 34. Transport architecture — CPU-budgeted, channel-separated, host-shared

> 状態：**一部実装（チャネル分離＋イベント中心の可観測性＝landed）／設計（CPU 予算・
> ホスト共有 Transport Service・DPU オフロード＝批准待ち）。** 統括の意見具申
> （2026 トランスポート層検討メモ）を取り込み、§33（保護チャネル分散実行）の上に据える。
> §00 ピラー3/4・§17・§0.15 と整合。**設計先行・批准必須・自己マージ禁止**（CPU 予算以降）。

## 34.0 中心命題：通信は「速くする」より「CPU 消費を制御する」

Rivus は SIMD でデータ処理を行い、理想は **CPU を使い切る**こと。分散では通信・制御・
テレメトリ・データ処理が**同一 CPU 資源を奪い合う**。特に WireGuard/QUIC/TLS の暗号は
SIMD を使うため **Rivus SIMD vs 通信 SIMD の競合**が起きる。よって一般 Web と異なり、
**通信速度の最大化でなく、通信の CPU 消費を予測可能に制御する**ことが要件。

二つの先行事例の教訓を組み合わせる：
- **PMCN（通信責務の集約）**：通信を賢くするのでなく、**通信の存在をアプリから隠蔽**し、
  複雑な通信制御を**基盤側へ集約**する。
- **QUIC（チャネルの論理分離）**：1 接続上で Telemetry/Control/Data を**論理的に分離**。
  Rivus が学ぶのは QUIC の通信機能でなく、この**論理分離**。

## 34.1 論理チャネル分離（**landed**・§33 wire に実装）

`crates/rivus-runtime/src/distributed.rs` のフレームに**先頭チャネルバイト**を追加：
`[channel:u8][kind:u8][len:u32][payload]`。チャネルは：

```
Transport (one connection)
 ├ Control   (CTRL)  : HELLO / JOB / CREDIT / END / ERR — ライフサイクル・背圧
 ├ Data      (DATA)  : CHUNK — 実データ（結果・将来は shuffle/中間成果物）
 └ Telemetry (TELE)  : EVENT — 構造化イベント（下記）
```

消費側はチャネルで demux し、**Data を止めずに Telemetry を surface**できる（`run_remote_observed`）。
QUIC のストリーム分離を、物理 N 接続でなく**フレームのチャネルタグ**で実現（QUIC backend では
本物のストリームに 1:1 で載る）。

## 34.2 イベント中心の可観測性（**landed**）

「パケットを監視する（tcpdump）」から「**イベントを監視する**」へ。ワーカが Telemetry
チャネルで構造化イベントを narrate：

```
flow.started   job_bytes=<n>
flow.completed result_bytes=<n> ms=<t>
flow.failed
transfer.done  frames=<n> bytes=<n>
```

将来追加：`node.joined` / `node.lost` / `transfer.retry` / `transfer.throughput`
（§17.7 coordinator 集約・既存 `RuntimeSnapshot`/`--json` と同系）。CLI `--on` は
イベントを stderr に出し（`[rivus @addr] …`）、結果（Data）は stdout に流す。

## 34.3 CPU 予算の明示管理（**設計・批准待ち**）

OS 任せでなく **CPU 利用率自体を設計対象**にする。例：

```
1.0 core  Transport (暗号・I/O)
0.5 core  Telemetry
0.5 core  Control
残り       Data Processing (Rivus SIMD)
```

- **CPU affinity**：暗号/通信を限定コアに隔離し、Rivus SIMD と競合させない。Linux は
  `sched_setaffinity`（libc・`unsafe`・Linux 限定）、他 OS は no-op。**off-by-default
  feature `cpubudget`** 裏に隔離（依存ゼロ既定を保つ）。env `RIVUS_NET_TRANSPORT_CORES` 等。
- byte-identity 契約**不変**：affinity は性能ノブであってデータに影響しない（§0.14 の
  「環境設定であってデータでない」運用ノブ＝`watch` の queue budget と同類）。

## 34.4 ホスト共有 Transport Service（s1 UDS フロント＝**プレ実装 landed**／以降 設計）

1 台に Rivus A/B/C が同居すると、各々が QUIC/TLS/WG を持つと**通信だけで複数コア消費**＋
SIMD 競合。PMCN の集約思想で、**ホスト単位の通信専用サービス**へ責務を集約：

```
Machine
 ├ Rivus Transport Service   (QUIC / TLS / WireGuard / Telemetry / Routing)
 ├ Rivus A ─┐
 ├ Rivus B ─┼─ IPC / SHM / Unix Domain Socket
 └ Rivus C ─┘
```

- 各 Rivus は **UDS/SHM** 経由で Transport Service を使う（ネットワーク endpoint は
  サービスが一手に持つ）。プロトコルは §34.1 のチャネル分離フレームを UDS に流すだけ
  （transport が TCP/UDS/QUIC でも論理は不変＝直交）。
- **利点**：CPU 固定化（Core0-1 Transport / Core2- Rivus）・**SIMD 競合削減**（暗号を限定
  コアへ隔離）・**セッション共有**（TLS/QUIC/WG を複数 Rivus で共用）・**Telemetry 集約**。
- **capability（§28.12.4）不変**：allowlist・identity はサービスが境界として強制。秘匿
  資格情報（wg 秘密鍵）はサービス内に留め、IR/テレメトリに写さない。
- スライス案：**s1 UDS フロント＋s2 フォワーディング・ゲートウェイ＝プレ実装 landed**
  （`rivus serve --uds PATH [--upstream rivus://host:port]`／`--on uds://PATH`・
  `distributed::{serve_uds, run_remote_uds, forwarding_handler}`）→ s2' セッション共有/プール
  （永続上流接続・#176 と連動）→ s3 ルーティング/集約。
- **s2 フォワーディング・ゲートウェイ（PMCN 集約の実体）**：ハンドラ抽象のおかげで「上流へ
  転送して結果を返すハンドラ」（`forwarding_handler(upstream, cfg) = |ir| run_remote(upstream, …)`）を
  `serve_uds` に渡すだけで、**同居 Rivus が1つのローカルサービス経由で上流ワーカに到達**する
  （サービスがネットワーク egress を一手に持つ）。topology＝UDS client → UDS service → TCP worker
  → 戻り。byte-identical（`tests/net.rs::distributed_uds_forwarding_gateway`・CLI 実演済）。
- **s2' 永続セッション＝プレ実装 landed**：プロトコルを **1 接続=複数ジョブ**に拡張
  （worker の `serve_protocol` がジョブループ・stray credit を読み飛ばす）。クライアント
  `Session`（`connect` で HELLO 一度・`run` を何度でも）で **connect/handshake を償却**。
  ゲートウェイは `forwarding_session_handler`（`Mutex<Session>` で**1 つの上流接続を全 downstream
  で共有**＝真のセッション共有）。測定：std で per-call 0.633ms→session 0.441ms（**1.4×**・
  QUIC では handshake 支配ゆえ劇的＝#176）。test `distributed_session_reuses_one_connection_for_many_jobs`。
  副次効果：ジョブループの read-until-EOF が旧 single-job drain を**構造的に包含**（大転送 RST 解消）。
- **s2' を QUIC にも適用＝プレ実装 landed（#176 の仮説を実測検証）**：`QuicSession` を
  std `Session` と対称に実装——`connect` で **handshake＋静的鍵ピン留めを一度**だけ行い、以降の
  `run` は **QUIC bidi ストリームをジョブ毎に開く**（QUIC ネイティブのストリーム多重化＝
  「1 セキュア接続・多ジョブ」に最適、ジョブ間 head-of-line ブロッキングなし）。worker `process_conn`
  は handshake＋ピンを一度行い `accept_bi` ループで `serve_stream` を回す。**実測：per-call
  7.891ms/job（新規接続＋TLS＋証明書）→ session 1.815ms/job（接続再利用・新ストリーム）＝4.3× 高速**
  （20 ジョブ）。#176 の「session reuse は QUIC の per-call を std 値へ収束させる」を裏取り——
  予算化すべきは *handshake*（一度払い）であって per-job のセキュア通信ではない。test
  `quic_protected_channel_round_trip_and_pinning`(c)・bench `bench_quic_distributed_latency`。
- **s1 プレ実装で実証されたこと（§34.1 の主張の裏取り）**：TCP の worker/client プロトコル中核を
  `serve_protocol` / `client_protocol` として **transport 非依存に抽出**し、**全く同じチャネル付き
  フレーム**（Control/Data/Telemetry＋credit 背圧＋`flow.*` イベント）を **UDS 上でそのまま**流して
  byte-identical なラウンドトリップを得た（`tests/net.rs::distributed_uds_transport_service_round_trips`）。
  UDS はローカル限定・ファイル権限ゲートなので IP allowlist は不要——capability 境界はソケットファイルの
  パス/権限（§28.12.4 と整合）。**残り（ネットワーク endpoint をサービスに集約・セッション共有・
  CPU 固定）は批准制で s2 以降**。`unix` のみ（`#[cfg(unix)]`）。

## 34.5 将来：DPU / SmartNIC オフロード（**設計**）

Transport Service を一枚噛ませたので、将来 **SmartNIC / DPU / QUIC offload / WireGuard
offload** を入れる際は **Transport Service のみ差し替え**ればよい（Rivus 本体・IR 不変）。
これは §0.1「エッジ＝同一の直交基盤」「transport を差し替える」の具現。

## MVP / 次 / 将来

- **landed**：§34.1 論理チャネル分離（Control/Data/Telemetry）＋§34.2 イベント中心可観測性
  （`distributed.rs`・`run_remote_observed`・CLI `--on` の stderr イベント・test
  `distributed_emits_telemetry_events`）＋**§34.4 s1 UDS フロント＋s2 フォワーディング・ゲートウェイ（プレ実装）**
  （`serve_uds`/`run_remote_uds`/`forwarding_handler`・CLI `serve --uds [--upstream]`／`--on uds://`・
  transport 非依存プロトコル抽出・test `distributed_uds_transport_service_round_trips`／
  `distributed_uds_forwarding_gateway`）。byte-identity 不変。
  §34.4 s2' 永続セッション（std `Session` + QUIC `QuicSession`、**4.3× QUIC reuse 実測**）。
- **次（批准後）**：§34.3 CPU 予算/affinity（feature `cpubudget`・Linux）→ §34.4 セッション
  集約/プール → QUIC backend をチャネルフレームに 1:1 マップ。
- **将来**：§34.5 DPU/SmartNIC オフロード・§17 stage 分割 shuffle・制御プレーン（ピラー4）。
