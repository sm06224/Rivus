# 33. Networking execution — protected-channel distributed execution (Pillar 3)

> **実装状況（main）**：本設計は **#173 で批准済**。コードは段階着地中 —
> **B1 `net`**（`open http://` / `subscribe tcp://` / `serve` / `run --on`・**std/依存ゼロ**）
> → **B2 `quic`**（quinn/rustls/rcgen/ring・**ring 0.17 ライセンス裁定後**）
> → **B3 `cpubudget`**（§34・ベンチで効果実証後）。本文中の「landed/完動/実装済」は
> 元 #173 ブランチの状態を指す（main では順次着地する）。
>
> 状態：実装（feature `net` ＝ std-only・依存ゼロの本命／feature `quic` ＝ 重い
> 依存の代替）。§00 ピラー3（分散＝ネットワーク transport）と §28.12.5（socket/http
> の方向性・#149 裁定附記）を実体化する。**本命はネットワーク越しのフロー実行**であり、
> 単なるリモート CSV 取得ではない。

## 33.0 狙い（#149 裁定附記に忠実）

North Star §0.1：**IR を唯一の通貨とし、実行は「解釈 or コンパイル or 分散」でバイト
同一**。分散とは「同じ IR の別配置」（§17.1）＝**IR を配備成果物として遠隔ワーカへ運び、
そこで実行して結果を返す**こと。#149 裁定附記（socket/http 後続スライスの方向性）：

1. **素のリスナーは存在しない** — 「保護されたチャネルか、無しか」。例外は loopback のみ・
   capability 明示許可制。
2. **既定態＝カーネル WireGuard に乗る（埋め込まない）** — capability で「信頼インター
   フェース（wg 等）にしかバインドしない」を強制。**Rivus 本体に暗号コード・依存は入らない**。
3. **feature-gated 代替＝QUIC（候補 quinn）** — 1 接続にコントロール＋データのストリーム
   多重・ストリーム毎流量制御が背圧（§28.12.2 ④）の bounded pull と整合。
4. **身元＝静的公開鍵。allowlist＝許可ピア公開鍵リスト。配備成果物は IR そのもの。**
5. userspace WG 埋め込み・TLS+CA は非推奨。

## 33.1 二層のトランスポート

### (A) 保護チャネル分散実行 ＝ 本命（`crates/rivus-runtime/src/distributed.rs`）

`feature = "net"`・**std のみ・依存ゼロ**。`Discovery→Transport→Codec` の Transport を
ネットワークへ広げる中核：

- **ワーカ** `rivus serve [--bind ADDR]`：信頼インターフェース（wg）または loopback に
  だけ bind し（`may_bind`＝#149-1）、**allowlist のピアのみ**受け付け（`peer_allowed`）、
  受領した **IR（正準ソース＝配備成果物）**を既存エンジンで実行し、結果を client の
  **credit（bounded pull）**でストリーム返却する。**素のリスナーではない**——loopback /
  wg-iface ＋ peer allowlist で締めた保護チャネル。
- **コーディネータ** `rivus run flow.riv --on rivus://host:port`：ローカル IR を遠隔
  ワーカへ送り、結果を受け取って表示。
- **暗号は委譲（#149-2）**：confidentiality/authentication は**カーネル WireGuard** の仕事。
  Rivus は wg インターフェースへのバインドと peer allowlist（静的公開鍵 ↔ wg-IP の対応）を
  **強制するだけ**で、暗号コード・依存を持たない。
- **プロトコル**（コントロール＋データ多重・1 接続）：`HELLO`（静的鍵 identity 交換）→
  `JOB`（IR ソース）→ `CHUNK`×n（credit で律速）→ `END`（or `ERR`）。長さ前置フレーム。
- **capability（§28.12.4）**：`RIVUS_CAP_NET_IFACE`（bind 可能な wg アドレス）・
  `RIVUS_CAP_NET_PEERS`（許可ピア）・`RIVUS_NET_IDENTITY`（自分の静的鍵 identity）・
  `RIVUS_NET_CREDIT`（窓）。allowlist は**境界であって秘密ではない**——拒否イベントは対象
  のみを載せ allowlist 全体を漏らさない。資格情報（wg 秘密鍵）は Rivus に一切写らない。
- **byte-identity（§0.5）**：ワーカの結果は**ローカル実行と同一バイト**（interpret==
  distribute）。`tests/net.rs::distributed_*` で固定。

### (B) QUIC ＝ feature-gated 代替（`distributed_quic.rs`・#149-3）

`feature = "quic"`。カーネル wg が無い環境向け。**同一プロトコルを 1 本の双方向 QUIC
ストリーム上**で。身元＝自己署名証明書の **公開鍵フィンガープリント（SHA-256/DER）**、
allowlist＝許可ピアのフィンガープリント（`RIVUS_CAP_NET_PEER_KEYS`）。TLS は accept-any＋
**アプリ層でフィンガープリント pin**（境界・秘匿に依らない）。秘密鍵はプロセス外に出ない。
依存：`quinn`/`rustls`+`ring`/`rcgen`/`tokio`（off-by-default・小型 multi-thread runtime を
`block_on` で同期 API に橋渡し・bounded idle timeout＋keepalive）。**完動・テスト済み**：相互認証
ハンドシェイク・静的鍵 pin・**credit ストリームの結果ラウンドトリップが byte-identical**
（`tests/quic.rs`・CLI `serve --quic`／`run --on quic://…` で e2e 実演）。
**根本原因の教訓**：当初ストリームが停止したのは `QuicConfig` の `#[derive(Default)]` が
**window=0** を生み、クライアントが credit 0 を送ってワーカが永久に credit 待ちになっていたため
（手書き Default で window=8 に修正・`window.max(1)` で防御）。`block_on` 二重ランタイムは無実
（最小エコーで実証）。**チャネル分離（§34）は QUIC では本物のストリームに 1:1 で載せられる**（後続）。

### (C) loopback 例外層 ＝ クライアント取得（HTTP/subscribe）

#149-1 の **loopback 例外**として、保護チャネル不要の単純なクライアント取得も持つ
（`net.rs`・std HTTP/1.1）：

- **`open "http://host/path"`** — 有界 GET（CSV+JSON・content-length/chunked/close・
  redirect5・単一パス・chunk-size 非依存）。`https`/TLS は範囲外。
- **`subscribe "tcp://host:port"`** — 非有界 TCP クライアント購読（CSV+JSON・ロスレス背圧・
  `watch` と同じ非有界/決定性タグ）。
- capability：loopback 既定＋`RIVUS_CAP_NET_HOSTS` allowlist。`RIVUS_NET_TIMEOUT_MS`。

## 33.2 決定性境界（§0.14）

- 保護チャネル分散実行：ワーカの実行は**有界・決定的サブ DAG**なら byte-identity 契約内
  （interpret==distribute）。源が非有界（subscribe）なら従来どおり契約外。
- HTTP GET は有界（到着順非依存＝行順）で契約内。subscribe は非有界・到着順依存で契約外。

## 33.3 feature ゲートと never-silent

parse / `to_source` / `rivus explain` は**常時 std**（IR 可逆）。評価のみ feature ゲート：
`net` 無しでネットワークに触れる flow は実行前 `RivusError::Build` で明示拒否（`regex`/
`gzip`/`unbounded` と同型）。`serve`/`--on` は `net` 無しで明示エラー。

## MVP / 次 / 将来

- **MVP（本書）＝完動・テスト済**：保護チャネル分散実行（IR 配備・peer allowlist・wg バインド
  capability・credit 背圧・byte-identity）／**QUIC 代替も完動**（ハンドシェイク/認証/pin/
  byte-identical ストリーム）／loopback 例外層（http/subscribe）。CLI `serve [--quic]`／
  `run --on rivus://｜quic://`・英日ガイド・demo・ベンチ（`docs/BENCHMARKS.md`）。
- **次**：`quic` を `full` 収載（cargo deny 通過後）／§17.3 stage 分割＋shuffle（DAG を複数
  ワーカへ）／Arrow IPC shuffle／コーディネータの telemetry 集約（§17.7）／checkpoint/replay
  （§17.8）／§34 チャネルを QUIC ストリームに 1:1 マップ。
- **将来**：制御プレーン（§0.7・ピラー4）からの allowlist 署名更新・無停止スケール。
