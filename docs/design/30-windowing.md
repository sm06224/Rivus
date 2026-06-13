# 30. 窓スライス — event-time / arrival 窓と決定性境界

> **本書は設計先行（doc-first・phase-0）。批准前に実装に入らない（§25.10）。
> 自己マージ禁止。** 窓の**二分原則**（裁定 A）・**段階順 6a→6d**（裁定 B）・**窓を
> socket/http より先行**（裁定 C）は **issue #154 の裁定コメント（issuecomment-4698281991・
> 統括裁可済）**で確定済み。本書はその裁定を忠実に反映し、残る決定分岐（構文・`Window`
> enum の形・タグ粒度・late 既定・メモリノブ）を §30.8 に集約して批准 issue（#143/#149
> 形式）へ送る。
>
> 既存の正しさ機械（byte-identity・continue-first・never-silent・IR 可逆・zero-dep・
> null モデル）は **保存して載せ替える**。窓は新 `Op` を作らず、既存 `Op::GroupBy`
> （`crates/rivus-ir/src/graph.rs:724`）に直交スロットを足す方式（Transport/Route/
> Discovery と同型）。

関連：§0.13（有界/非有界＋時間＋状態）・§0.14（決定性の境界）・§23（datetime レーン＝
時間軸の土台）・§28.12（slice 5＝非有界 transport 骨組み・`watch`／決定性タグ
`unbounded_nodes`／bounded-block 背圧 `RIVUS_WATCH_QUEUE`）・#41（f64 集約の非結合性）。

---

## 30.0 狙いと位置づけ

窓（windowing）は「時間（または到着）で区切った範囲での集約」である。Rivus にとって
窓は2つの異なる要請を**同時に**満たす：

1. **有界の時系列集計を一級にする**（即実用・watch 非依存）。「1時間ごとの売上」「日次の
   エラー率」のような **event-time バケット集計**は、ファイル等の**有界**入力に対しても
   日常的に必要で、現状の Rivus には綴りが無い（`|# key agg:col` は全体集約のみ）。これは
   §0.13 の「時間：窓を IR の段に」の具体化であり、未着手の ts Epic #56・#60〜#67 に
   直接効く。
2. **非有界集約のメモリ有界化**（§28.12 の積み残し）。slice 5 は非有界 `watch` 骨組みを
   据えたが、**窓無し集約（`|#`）は実行前に拒否**した——非有界ストリームを全部貯めずに
   集約するには「いつ締めるか」＝窓が要るからである（§28.12.0 のメモリ有界節）。窓は
   その締めの規則を与え、パイプラインブレーカを区切ってメモリを有界化する。

この2要請が**段階順**を決める（§30.2）：決定的核（有界 event-time 窓）を先に landed させ、
非有界＋watermark は後段に置く。**「窓 ＝ 非有界専用」ではない**——むしろ有界の時系列
集計が最初の実用価値であり、`watch` を一切要しない。

**スコープ外（本書では設計しない）**：socket/http transport（§28.12.5・分散ピラー寄り）／
分散シャッフルでの窓再分配（Phase 3）／GPU 窓集約（§22）。窓そのものの IR・構文・決定性
契約・メモリ規律に絞る。

---

## 30.1 二分原則（裁定 A・本書の背骨）

窓は **締める基準（どの行がどの窓に入るか・いつ窓を閉じるか）**で2系統に**分かれる**。
この分岐が決定性契約（§0.14）に乗るか外れるかを一意に決める。

- **event-time 窓**（`tumbling`/`sliding`/`session`）：締める基準＝**タイムスタンプ列＝データの
  値**。窓割り当ては「その行の ts 値」の純粋関数なので、**同じ入力には常に同じ窓割り当て**が
  起きる。到着順・実時刻・並列分割に依存しない → **byte-identity を保持**し、§0.14 の決定的
  op 集合に乗る（有界源の場合・§30.4）。

- **arrival / processing 窓**（`arrival` ＝ 到着 N 件ごと等）：締める基準＝**到着順・実時刻**。
  どの行がどの窓に入るかが**読み取り順・並列分割・実行タイミング**で変わる → **本質的に
  非決定**。これが #154 (c) ＝ 6d。決定性タグは貫通したまま（§30.4）、`--exact` でも回復
  しない。

二分の根拠は #154 (a) 却下の一般化：`take N` は非有界ストリームに**終了性**を回復するが
**決定性**は回復しない（どの N 件が来るかは到着順依存）。同様に **arrival 窓は終了性
（窓の区切り）を与えるが決定性は与えない**。よって両者を型で分け、event-time 窓だけを
決定的契約に乗せ、arrival 窓は never-silent な非決定として明示扱いする。f64→decimal の
厳格路線（黙って正しさを緩めない）に揃える。

| | event-time 窓 | arrival / processing 窓 |
|---|---|---|
| 締める基準 | ts 列（データの値） | 到着順・実時刻 |
| 窓割り当て | 入力の純粋関数 | 実行ごとに変わり得る |
| 決定性（§0.14） | 契約内（有界源） | 契約外（常に） |
| byte-identity | 保持 | 非保証 |
| 段階 | 6a/6b（有界）・6c（非有界） | 6d（opt-in＋never-silent warn） |
| `Window` 変種 | `Tumbling`/`Sliding`/`Session` | `Arrival` |

---

## 30.2 段階スライス 6a→6d（裁定 B・決定的核を先に）

`watch` を s2〜s5 で段階に割ったのと同じく、窓も**決定的核を先に landed させる**順で割る。
各段は前段の上に乗り、前段の byte-identity / continue-first を不変に保つ。

- **6a — 有界 event-time tumbling**（最初に landed・watch 非依存・完全決定・即実用）
  固定幅・非重複の event-time 窓。`|# country sum:amount over tumbling(ts, 1h)`。入力は
  **有界**（ファイル等）。`watch` も watermark も不要——全データが揃っているので、窓境界＝
  `floor(ts.ticks / size.ticks)`（§30.5 で詳説）で各行を厳密に1窓へ割り、窓を出力キーに
  足すだけ。**完全決定・byte-identical**（決定性タグは付かない＝§30.4）。ts Epic #56・
  #60〜#67 の「N時間ごとの集計」がここで実用になる。

- **6b — 有界 sliding / session**（有界・決定的）
  重複窓（`sliding(ts, size, hop)`・`hop<size` で重複）と動的境界窓（`session(ts, gap)`＝
  連続 ts の間隔が `gap` 以下の間ひと続き）。いずれも入力は有界・締める基準は ts 列なので
  **決定的**。sliding は1行が複数窓に入りうる（重複分の集約状態を同時に持つ＝§30.6 のメモリ
  規律対象）。session はキーごとに窓が動的に伸縮する。

- **6c — 非有界＋watermark＋late-data**（§28.12 の積み残しを解く・非決定が残る）
  非有界源（`watch`/将来の subscribe/socket）上の event-time 窓。「全データが揃う」が成り
  立たないため、**watermark**（event-time の進行推定）で窓を締め、watermark 通過後に来た
  **late row** を扱う。late の既定＝**counted＋surface**（黙って drop しない・§30.7）。窓割り
  当て自体は ts 値の純粋関数だが、**どの行が watermark 前に間に合うか**は到着順に依存する
  ため、**非有界源では決定性タグが残る**（§30.4・タグは打ち切れない）。これが「窓で
  メモリ有界化」の本丸：窓境界／watermark が集約状態の解放トリガになる（§30.6）。

- **6d — arrival / processing 窓**（#154 (c)・明示 opt-in・never-silent）
  到着 N 件ごと等、締める基準が**到着順・実時刻**の窓（`arrival(count)`）。**本質的に非決定**
  （§30.1）。よって：①**明示 opt-in**（既定では選べない・誤用回避）、②パース／実行時に
  **「到着順依存で実行ごとに変わり得る」**を **never-silent warn** で surface、③決定性タグは
  **非決定のまま伝播**（`--exact` でも打ち切れない・§30.4）。有界源の上でも arrival 窓は
  非決定タグを**新たに種付け**する（窓の基準がデータ値でないため）。

**段階の独立性**：6a/6b は `unbounded` feature にも `watch` にも依存しない（有界フローの
純機能）。6c は §28.12 の `unbounded` feature の裏（非有界経路の評価は feature-gate）。6d は
opt-in 構文＋warn。**6a だけでも単独で価値があり**、後段を待たずに landed できる。

---

## 30.3 IR・構文（新 `Op` なし・GroupBy へ直交スロット・to_source 可逆）

### IR：`Op::GroupBy` に `window` スロットを足す
`watch` が `Op` を再形せず `Discovery::Watch` slot を足したのと同型——窓も**新ノードを
作らず**、既存 `Op::GroupBy`（`crates/rivus-ir/src/graph.rs:724`）に `Option<Window>` を
足す：

```rust
// crates/rivus-ir/src/graph.rs
GroupBy {
    keys: Vec<String>,
    aggs: Vec<(AggFunc, String)>,
    window: Option<Window>,   // None ＝ 現状の全体集約（挙動不変）
}

/// 窓の種類（§30.1 の二分が型に出る）。
pub enum Window {
    /// 固定幅・非重複の event-time 窓。境界 = floor(ticks / size) ＝厳密 i64（§30.5）。
    /// 有界源では決定的。
    Tumbling { time_col: String, size: Duration },
    /// 幅 `size`・前進 `hop` の重複 event-time 窓（`hop<size` で重複・`hop==size`≡tumbling）。
    Sliding { time_col: String, size: Duration, hop: Duration },
    /// event-time セッション：連続 ts の間隔が `gap` 以下の間ひと続き、超えたら締める。
    Session { time_col: String, gap: Duration },
    /// **arrival / processing 窓**（6d・#154 (c)）：到着 `count` 件ごと。締める基準が
    /// データ値でない＝非決定（決定性タグを種付け・never-silent warn）。明示 opt-in。
    Arrival { count: u64 },
}
```

- **`window: None` は挙動不変**：現状の全体集約（`|# key agg:col`）は `None` を出力し、IR も
  実行も**1ビットも変わらない**。既存 stress / optimizer_equiv はそのまま緑。
- **`size`/`hop`/`gap` は core の `Duration` 値型**（std・`crates/rivus-core`：`DurColumn{ticks,
  unit}` の単票）。`1h`/`15m`/`30s` のような Duration リテラルで綴る。**parse / to_source は
  常時 std**（IR 可逆・feature 不問）。評価のみ、非有界 6c は `unbounded` feature ゲート。
- `time_col` は datetime レーン（`DtColumn{ticks:Vec<i64>,unit}`・§23）を指す。date（`Vec<i32>`
  日数）・time（`Vec<i64>` 午前0時からの tick）レーンへの拡張は自然（いずれも厳密整数）。
  MVP の主対象は datetime。

### 構文：`over` 句（提案・③ で批准）
集約に窓を付ける綴りは **`over` 句**を提案する（SQL 既視感・`|#` の後置）：

```
events.csv |> open |# country sum:amount over tumbling(ts, 1h)
events.csv |> open |# host count over sliding(ts, 1h, 15m)
events.csv |> open |# user count over session(ts, 30m)
stream     |> ...  |# host count over arrival(1000)   # 6d・opt-in・warn
```

- 窓指定は **関数呼び `(...)` ＝式**（§29 の記号原則：`()`＝式）。`tumbling(ts, 1h)` /
  `sliding(ts, size, hop)` / `session(ts, gap)` / `arrival(count)`。
- **to_source 可逆**：`window=None` は従来どおり ` over` を付けず復元。`Some(w)` は
  ` over <spec>` を付けて round-trip（`tumbling`/`sliding`/`session`/`arrival` の正規形へ収束）。
- `over` キーワード採用・session の gap 表記・sliding の `size, hop` 順は **③（§30.8）で批准**
  （別表記の余地：`every`/`by`、`window:` 接頭など。記号原則と to_source 正規形の一意性が基準）。

### explain での可視化
`Op::GroupBy{window:Some(..)}` は `rivus explain` で窓種・時間列・幅を表示（observable-first）。
決定性タグ（§30.4）が付くノード（6c の非有界・6d の arrival）は explain 上で**非決定マーク**を
付け、最適化・並列が触れない領域を可視化する。

---

## 30.4 決定性タグの打ち切り規則（固定点1・有界 event-time に限定）

slice 5 は `PlanGraph::unbounded_nodes()`（`graph.rs:1524`）を据えた：**非有界 discovery
（`Discovery::is_unbounded()`）か、その（推移的）下流であるノードに true を立てる導出式タグ
（保存しない＝陳腐化不能・`to_source` は可逆のまま）**。最適化・並列はこの集合の内側を
並べ替え・再結合・書き換えてはならず、byte-identity は外側（有界補集合）でのみ主張する。

窓を入れるとき、**窓がこのタグをどう動かすか**を粒度を絞って明文化する。**結論：窓は
タグを打ち切らない。** 「決定的窓はタグを打ち切る」という直観は**“有界源上の event-time
窓”という退化ケースに限定**してのみ正しい——その場合は**元々タグが無い**ので「打ち切る」
対象が存在しないだけである。3段で整理する：

- **6a/6b（有界源上の event-time 窓）＝決定的・タグ無し**：源が有界なのでそもそもタグが
  立たない。窓割り当ては ts 値の純粋関数（§30.1）＝順序非依存なので、窓付き `GroupBy` は
  **有界・決定的補集合に留まり byte-identical**。タグの追加も打ち切りも起きない。

- **6c（非有界源上の event-time 窓）＝非決定維持・タグは打ち切れない**：源が非有界なので
  `unbounded_nodes()` のタグが窓付き `GroupBy` まで推移伝播している。窓割り当て自体は
  決定的でも、**どの行が watermark に間に合うか**（late 判定）は到着順に依存する（§30.7）
  ため、**窓はタグを閉じない**。＝「窓が非有界を有界に畳むからタグを打ち切れる」を**明示的に
  却下**する（late-data の非決定が残る）。実装上は現行の推移伝播が自然にタグを保つので、
  **「窓でタグを打ち切る」特例を足さない**ことがそのまま正しい挙動になる。

- **6d（arrival 窓・源の有界性を問わず）＝非決定・タグを新たに種付け**：締める基準が
  データ値でない（到着順）ため、**有界源の上でも**非決定を導入する。よってタグの**種**を
  非有界 discovery だけでなく **arrival 窓ノードにも広げる**。

### `unbounded_nodes` の一般化（実装契約・批准後）
タグの意味を「非有界由来」から「**非決定由来**」へ一般化する（名は批准時に確定・⑤）。
**種集合 = 非有界 discovery ∪ arrival 窓 `GroupBy`**、そこから下流へ推移伝播。導出式・
保存しない・`to_source` 可逆は不変：

```text
seed(id) := Op::Source{discovery} かつ discovery.is_unbounded()
          ∨ Op::GroupBy{window: Some(Window::Arrival{..})}
tag(id)  := id ∈ seed ∨ 上流のいずれかが tag         # 推移伝播（現行と同じ walk）
```

event-time 窓（`Tumbling`/`Sliding`/`Session`）は**種に入れない**＝有界源では決定的、
非有界源では上流のタグがそのまま流れる。**窓が独自にタグを打ち切る経路は作らない**——
これが固定点1の明文。

---

## 30.5 #41 f64 集約制約の継承（固定点2・窓は新たな境界ハザードを足さない）

窓は集約の**範囲**を変えるだけで、集約の**結合性**は変えない。よって #41（f64 加算の
非結合性）の制約をそのまま継承する：

- **f64 レーンの `sum`/`avg`/`std`**：窓内であっても、並列で窓を分割集約してからマージすると
  partition-then-merge の加算順が変わり ULP がずれる＝**byte-identity が壊れる**（#41 と
  同根）。よって窓付き f64 集約は **直列維持**か **decimal レーン（`--exact`・i128 scaled・
  §21）**へ。`avg` は和／件数なので和の非結合性をそのまま受ける。
- **順序非依存＝並列安全な exact reduction**：`min`/`max`/`count`（COUNT(*) と COUNT(col)）/
  `count_distinct`/`first`/`last`/`percentile(pct)`、および**整数・decimal の `sum`/`avg`**。
  これらは窓内でも順序非依存なので、窓を並列分割・再結合しても**結果不変＝byte-identical**。
  `first`/`last` は源順に依存するが、源順は有界経路で serial==parallel に安定（#41 で確定）
  なので安全。`percentile` は窓×群基数ぶんの値を貯める pipeline-breaker（現行の全体集約 Pct と
  同じ・§30.6 のメモリ対象）だが、結果は多重集合の関数＝順序非依存。
- **AggFunc 一覧**（`graph.rs:132`・窓でも同一集合）：`Sum`/`Avg`/`Min`/`Max`/`Std`/`Count`/
  `CountDistinct`/`First`/`Last`/`Pct(u8)`。並列安全＝`Min`/`Max`/`Count`/`CountDistinct`/
  `First`/`Last`/`Pct`＋整数/decimal の `Sum`/`Avg`。f64 の `Sum`/`Avg`/`Std` のみ直列/decimal。

### 窓**境界**は新ハザードを足さない（厳密整数比較）
datetime レーンは **exact i64 ticks**（`DtColumn{ticks:Vec<i64>,unit}`・`graph` ではなく
`crates/rivus-core/src/chunk.rs:175`）。tumbling 境界は `floor(ticks / size_ticks)`、session の
gap 判定は `ticks[i] - ticks[i-1] <= gap_ticks`、いずれも **i64 の厳密整数演算**＝浮動小数を
通らない。よって**どの行がどの窓に入るかの判定に f64 のような境界ハザードは無い**。唯一の
注意は**単位整合**：`size`/`gap`（`Duration`）と `time_col`（`DtColumn.unit`）の tick 単位を
揃える（より細かい単位へ正規化＝無切捨て・cast レーンと同じ widening 方針）。境界が窓の
端ちょうど（`ticks == k*size`）の行は **左閉右開 `[start, end)`** に正規化（to_source 可逆・
explain で明示）。これも整数比較なので決定的。

---

## 30.6 メモリ有界化（bounded-block 協調・同時窓数ノブ）

窓は集約状態を「開いている窓ぶん」だけ保持する。**同時に開く窓の数**を有界に保つことが
非有界フローでメモリを潰す鍵であり（§28.12.0）、有界フローでも巨大入力で効く。

- **窓ごとの状態量**：exact reduction（`min`/`max`/`count`/`sum`(int/dec)/`first`/`last`）は
  群×開窓ぶんの**定数サイズ**アキュムレータ。`count_distinct`/`percentile` は群×開窓ぶんに
  **値集合／値列**を貯める（現行の全体集約と同じ pipeline-breaker・窓で範囲が切れるぶん
  むしろ有界化に効く）。
- **解放トリガ（窓が閉じる契機）**：
  - **tumbling**：時間が窓境界 `k*size` を跨いだら、その窓を出力して状態を解放。event-time
    が単調に進むなら同時開窓は実質1〜数個。
  - **sliding**：重複ぶん同時に開く（`ceil(size/hop)` 個オーダ）。`hop` が小さいほど多い。
  - **session**：キーごとに1セッション。`gap` 超過 or watermark 通過で締める。
  - **event-time（6c）**：**watermark** が窓末を越えたら締める（§30.7）。late row は
    allowed-lateness ぶん状態を延命。
- **slice 5 の bounded-block 背圧と協調**：`watch` は満杯でブロック＝ロスレスな bounded
  queue（`SourceWatch`／`RIVUS_WATCH_QUEUE`・§28.12.2 裁定④）を据えた。窓バッファは
  その下流で**同様に有界**にする：開窓数が上限に達したら**上流をブロック**（drop しない＝
  never-silent）。有界経路の直列ループ不変（§28.12.0 の pin）は窓を入れても保つ。
- **同時窓数ノブ（運用・⑦で批准）**：`route` の fd budget（LRU handle pool・#147）と**同型**の
  運用ノブ＝**同時開窓数の上限**（env、例：`RIVUS_WINDOW_BUDGET`）。決定性契約の**外側の
  運用ノブ**（データではない・§0.14・`watch` queue や fd budget と同類＝結果バイトに影響
  しない）。**超過時挙動は never-silent**：候補は ①上流ブロック（既定・ロスレス）、②最古窓を
  早期出力＋surface、③拒否イベント。既定とノブ形は §30.8 ⑦ で批准。
  drop-oldest / sampling は **既定では不可**（never-silent 違反・将来の明示 opt-in のみ・
  §28.12.2 と同方針）。

---

## 30.7 watermark・late-data（6c・counted＋surface）

非有界源では「全データが揃う」が成り立たないため、**いつ窓を締めるか**を決める基準＝
**watermark**（event-time の進行推定：「これより古い行はもう来ない、と見なす時刻」）が要る。

- **決定性契約の粒度**：
  - **有界源（6a/6b）**：watermark は不要——入力末で全窓を締めればよい（あるいは
    最大 ts を watermark とみなす）。late は構造的に発生しない → **完全決定・byte-identical**。
  - **非有界源（6c）**：watermark は到着の進行に対する**ヒューリスティック**。どの行が
    watermark 前に間に合うかが到着順に依存 → **決定性は late ポリシー次第で限定的**にしか
    保証できない（§30.4 でタグが残る理由）。byte-identity は主張しない。
- **late-data 既定 ＝ counted＋surface（never-silent）**：watermark を越えてから来た late row
  （`ts < watermark - allowed_lateness`）を**黙って drop しない**。既定は **counted（late 件数を
  メトリクスに計上）＋ surface（error/telemetry ストリームへイベントとして出す）**。これは
  continue-first（落とさず流す）と never-silent（黙らない）の合流点であり、f64→decimal の
  厳格路線（黙って正しさを緩めない）と同じ精神。
- **allowed-lateness（猶予）**：watermark を越えてもこの猶予ぶんは窓状態を延命し late row を
  受け入れる（その後に最終締め）。指定形（`over tumbling(ts, 1h) allow late 5m` 等）と既定値
  （0＝即締め？ 明示必須？）は **§30.8 ⑥で批准**。
- **watermark の導出**：MVP 候補＝**観測した最大 ts − 固定遅延**（bounded out-of-orderness）。
  source 由来の明示 watermark（将来の subscribe/socket メタ）は後段。導出式は explain で
  可視化（observable-first）。

---

## 30.8 決定分岐（→ 批准 issue・#143/#149 形式）

①② は **issue #154 で裁定済み**（本書はその確認）。③以降は本書で詰めた案を提示し、
統括の裁可で分岐を閉じる。批准 issue から本書（§30）と #154 裁定コメント
（issuecomment-4698281991）を相互リンクする。

1. **二分原則（裁定 A）＝確認**：event-time 窓は決定的契約に乗せ、arrival/processing 窓は
   非決定として分離（§30.1）。— *ratified #154、確認のみ*。
2. **段階順 6a→6d ＋ 窓を socket/http より先行（裁定 B/C）＝確認**：決定的核（6a 有界
   event-time tumbling）を先に landed（§30.2）。— *ratified #154、確認のみ*。
3. **構文（未確定）**：`over` 句採用の可否（`|# … over tumbling(ts, 1h)`・§30.3）。`tumbling`/
   `sliding(size, hop)`/`session(gap)`/`arrival(count)` の関数表記と引数順。代替表記
   （`every`/`by`/`window:` 接頭等）。基準＝記号原則（§29）と to_source 正規形の一意性。
4. **`Window` enum の形（未確定）**：`Tumbling{time_col,size}` / `Sliding{time_col,size,hop}` /
   `Session{time_col,gap}` / `Arrival{count}`（§30.3）。`size`/`hop`/`gap`＝`Duration`、
   `count`＝`u64`。フィールド名・将来の Date/Time レーン対応の置き方。
5. **タグ打ち切り粒度（未確定）**：固定点1（§30.4）＝**窓はタグを打ち切らない**／arrival 窓を
   タグ種に加える／`unbounded_nodes` を「非決定由来」へ一般化（改名の是非・新名）。
6. **late 既定（未確定）**：late-data＝**counted＋surface**（§30.7）で確定方向。allowed-lateness
   の指定形（`allow late 5m` 等）と既定値（0／明示必須）。
7. **メモリノブ（未確定）**：同時開窓数の上限ノブ（`route` fd budget 同型・§30.6）の env 名と
   超過時の既定挙動（上流ブロック／早期出力＋surface／拒否イベント）。drop は既定不可。

**裁可後の実装順（doc 批准が前提・§25.10）**：6a（有界 event-time tumbling）→ 6b（sliding/
session）→ 6c（非有界＋watermark＋late）→ 6d（arrival・opt-in＋warn）。各スライスは前段の
byte-identity を不変に保ち、ローカルゲート（fmt / clippy `--all-features -D warnings` 0 /
全テスト / stress serial==parallel==chunk-size / optimizer_equiv / 依存ゼロ）を全緑で push。

---

## MVP / 次 / 将来

- **MVP（本書批准の対象）**：窓の二分原則・段階スライス（6a〜6d）・IR スロット
  （`Op::GroupBy.window`）・`Window` enum・決定性タグ規則（§30.4）・#41 継承（§30.5）・
  メモリ規律（§30.6）・watermark/late 既定（§30.7）の**設計確定**。
- **次**：批准後 **6a（有界 event-time tumbling）**＝最初の実装スライス（watch 非依存・完全
  決定・即実用・ts Epic #56/#60〜#67 に直結）→ 6b → 6c → 6d。
- **将来**：session の高度な締め（句読点イベント）／source 由来の明示 watermark
  （subscribe/socket・§28.12.5）／分散シャッフルでの窓再分配（Phase 3）／GPU 窓集約
  （§22）。いずれも本書の二分原則・決定性タグ・メモリ規律を前提に載せる。

