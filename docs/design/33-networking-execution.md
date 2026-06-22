# 33. Networking execution — `http://` pull source ＋ `tcp://` subscribe stream

> 状態：実装（feature `net`・std-only・依存ゼロ）。§28.10 slice 5 の後続＝§28.12.1
> 「含まない（後続スライス）：socket/http transport」を、§28.12.5 の方向性
> （統括裁可済）に忠実な形で着地させる。本書は §28 I/O substrate の直交4層
> （Discovery → Transport → Codec → Provenance）の上に **Transport を
> ネットワークへ** 広げる最初のスライス。

## 33.0 狙い

North Star ピラー3（分散＝transport をネットワークに）の最小実体。既存の有界・
決定的経路のバイトを 1 ビットも変えずに、**ネットワーク越しの実行**を一級にする：

- **`open "http://host[:port]/path.csv"`** — 有界 HTTP GET。リモートの CSV/JSON を
  そのまま既存の codec が decode（transport だけが変わる＝§28.2 の直交性の実証）。
- **`subscribe "tcp://host:port"`** — 非有界 TCP クライアント・ストリーム。行区切り
  レコードを購読し、`watch` と同じ非有界・決定性タグ規律に乗る（§28.12 / §0.14）。

両者とも **off-by-default feature `net`** の裏。feature off のビルドはネットワーク
コードを一切コンパイルせず、有界フローのバイト・テスト・依存グラフは完全不変
（slice 1〜5 の stress / optimizer_equiv はそのまま緑）。parse / `to_source` /
`rivus explain` は **常時 std**（IR 可逆）— 評価のみ feature ゲート。feature-off で
ネットワーク源を実行＝実行前 `RivusError::Build` で明示拒否（`regex`/`gzip`/
`unbounded` と同型・never-silent）。

## 33.1 依存ゼロの HTTP/1.1 クライアント（std のみ）

統括方針「依存ゼロは原則・依存なしで実装可能になるまでは依存ありで可」（§28.12.1a）に
従い、**HTTP は std で実装可能**なので依存を入れない。`std::net::TcpStream` 上に
最小の HTTP/1.1 GET クライアントを実装（`crates/rivus-runtime/src/net.rs`）：

- `GET path HTTP/1.1` ＋ `Host` ＋ `Connection: close` ＋ `User-Agent: rivus/<ver>`。
- ステータス行＋ヘッダをパース。2xx 以外は明示エラー（`3xx` の `Location` を
  最大 5 回まで追従＝同一スキーム・capability 再チェック）。
- ボディは `Transfer-Encoding: chunked`／`Content-Length`／close-delimited の3形を
  扱う `BodyReader`（`BufRead`）として返す。codec はこれを通常のバイトストリーム
  として読む（CSV は単一パス・サンプル推論＝非シーク経路は圧縮リーダーと共有）。
- **TLS は入れない**（§28.12.5-5：証明書ライフサイクルの運用負荷で非推奨）。`https://`
  は明示エラー（誘導：保護は WireGuard/QUIC レーン＝後続）。

## 33.2 capability — loopback 既定・allowlist は境界（§28.12.4/5）

§28.12.5-1「素のリスナーは存在しない（保護されたチャネルか、無しか・例外は loopback）」
に忠実に、本スライスは **クライアント接続のみ**（`open`=GET、`subscribe`=dial）＝
リスナーを一切 bind しない。到達先は capability で締める：

- 既定で **loopback のみ**到達可（`127.0.0.0/8` / `::1` / `localhost`）。
- 非 loopback ホストは環境変数 **`RIVUS_CAP_NET_HOSTS`**（カンマ区切りの
  `host` または `host:port`）の allowlist にある時のみ到達可。
- 違反は **拒否イベント**で surface（§28.12.4：拒否対象のみを載せ allowlist 全体は
  漏らさない・never-silent）。源が開けない＝そのフローはデータ無し＝Fatal（ファイル
  not-found と同型）。
- allowlist は秘匿ではなく **境界**。秘匿すべき資格情報（トークン等）は本レーンに
  一切写さない（IR・to_source・テレメトリ・エラーに出さない＝§28.12.4）。

## 33.3 IR / 構文（可逆・既存ノード非再形）

- `Discovery::Subscribe(addr)`（非有界）を `Watch` の隣に追加（**新 Op なし**＝
  §28.12.2 と同じ slot 追加方式）。`is_unbounded()` は `Watch | Subscribe`。
  `subscribe "tcp://…"` で desugar、`to_source` 復元（可逆）。
- HTTP は `open "http://…"` のまま＝`Discovery::Fixed(url)`＋拡張子/明示 codec。
  transport は **path スキームから導出**（`Scheme::Http`）＝既存の `Local`/`Stdin`/
  `Compressed` と同じ「IR は Local・runtime が scheme で選択」方式（§28.2）。
- `PlanGraph::uses_net()`（http 源 or `Subscribe`）／`uses_watch()`（`Watch`）を分離。
  engine は前者で `net` feature、後者で `unbounded` feature を別々にゲート。共通の
  `uses_unbounded()`（`Watch | Subscribe`）は窓無しブロッキング op の拒否と直列実行を
  従来どおり駆動。

## 33.4 決定性境界（§0.14）

- **HTTP GET は有界**だが、ネットワークは外部要因（再試行・部分応答）。本スライスは
  「1 回 GET して全ボディを decode」＝**到着順に依存しない**（行順＝バイト順）ので
  byte-identity 契約の内側に置ける（serial==chunk-size）。並列 byte-range は
  非シークなので不可＝直列固定（圧縮源と同型）。
- **`subscribe` は非有界・到着順依存**＝決定性 op 集合の外側。`watch` と同じく
  決定性タグで最適化・並列再結合から外す。終了は下流飽和（`take N`）かピア close。

## 33.5 背圧・メモリ有界（§28.12.0）

- HTTP：codec が逐次 pull＝有界メモリ（全ボディを抱えない・行ストリーム）。
- subscribe：ソケット読みは `read_line` の逐次 pull＝有界。下流が遅ければ TCP の
  受信ウィンドウが詰まり生成側が自然に待つ（ロスレス・drop しない）。

## MVP / 次 / 将来

- **MVP（本スライス）**：`open http://`（CSV＋JSON）／`subscribe tcp://`（CSV 行）・
  loopback capability・std HTTP クライアント・feature `net`・英日ガイド・demo・test。
- **次**：JSONL の単一パス購読／`read` が Resource 列の `http://` を開く（多ファイル
  ネット連結）／HTTP POST sink（出力の鏡像・§28.7）。
- **将来（§28.12.5）**：保護チャネル＝カーネル WireGuard に乗る（埋め込まない）／
  feature-gated QUIC（quinn）＝1 接続多重＋ストリーム毎流量制御が bounded pull と整合／
  身元＝静的公開鍵 allowlist。本スライスの capability・決定性境界がその前提。
