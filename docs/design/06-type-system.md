# 06. Type System

## 6.1 哲学

型システムは Rust ほど厳密すぎず、PowerShell ほど緩すぎない。理想は gradual
typing + runtime specialization + observed type optimization + structural typing +
stream-aware typing（初期は柔軟、hot path で特化）。HotSpot / Truffle 的アプローチ。

そして Rivus 固有の核：**型 = 実行戦略（execution lane）**（原則7）。型は「メモリ
表現」ではなく「どの実行経路に乗るか」を選ぶ。

## 6.2 三層の型

```
1. 構造（structural）  : Schema = [Field{name, DataType}]  ← chunk が運ぶ
2. lane（execution）  : DataType がどの実行レーンを意味するか
3. observed（runtime） : 実行中に観測した実型（hot path 特化の根拠）
```

### 構造的・gradual

スキーマは構造的：同じ field 名と lane を持つ chunk は出自を問わず互換。型注釈は
任意で、未注釈なら source/データから推論する（CSV は列ごとに int→float→bool→str の
順で推論。`rivus-runtime/src/csv.rs::infer`）。

## 6.3 Numeric lane（実行戦略としての数値型）

数値は「どう速く計算するか」のレーン選択（原則7 / Master §8）：

| lane | 型 | 実装戦略 |
|---|---|---|
| SIMD（既定） | `i32 i64 f32 f64` | 連続バッファ + ベクトル化。最速 |
| Decimal | `d32 d64 d128` | scaled integer（固定小数点）。金額など |
| Big number | `bigint bigdecimal` | 任意精度。最遅・最安全 |

MVP は SIMD lane を `i64`/`f64` に集約して実装。`DataType` enum は lane を表現
できる形にしてあり、`Decimal`/`Big` variant の追加で拡張する（`Column` に対応
バッファを足すだけ。operator は dtype 分岐を増やす）。

> **Decimal lane の実装可能詳細は [21-exact-decimal](21-exact-decimal.md) を参照。**
> scaled integer（i128）で加算が結合的・厳密になるため、並列集計（#41）が
> byte-identical になる。`--exact` / `:decimal[(n)]` でユーザーオプトイン
> （既定は従来 f64・速度最優先）。GPU 経路（[22](22-gpu-backend.md)）の数値正確性も
> この decimal lane で担保できる。

```
        ┌─ 既定 ─────────────┐
 数値 ──┤  SIMD lane (i64/f64) │  ← まずここ。観測で必要なら昇格
        ├─ d128 (scaled int)  │  ← 精度要求が観測されたら
        └─ bigint/bigdecimal  │  ← overflow 観測 / 明示注釈で
```

## 6.4 Text lane（stream-based / 原則8）

string は完成物ではなく **bytes stream + encoding-aware decode continuation**。

| encoding | path | 不正バイト |
|---|---|---|
| ascii | SIMD fast path | warning + 継続 |
| utf8 | fast path | degraded decoding（置換）+ warning + 継続 |
| shift-jis / iso-2022-jp | stateful decoder | warning + 継続 |

不正文字でも停止しない（continue-first の文字列版）。MVP は UTF-8 文字列で実装し、
不正行は CSV 層で warning にしている。Phase 1 で `Column::Str` を「offset + bytes +
encoding tag」に変え、decode を遅延・継続可能にする。

## 6.5 observed type optimization（Phase 2 / JIT 連携）

```
interpreter で実行
   │  各 operator で「実際に流れた型/分布」を観測（telemetry に併設）
   ▼
hot path 判定（呼ばれ回数 × chunk 量）
   │
   ▼
specialization : 観測型に特化したカーネルを生成（例: i64 専用 filter）
   │  観測が外れたら guard で deopt → generic path へ
   ▼
JIT（Cranelift, 09 参照）
```

stream-aware typing：型は「1 chunk 内で均質」という前提を観測で確かめ、均質なら
列まるごとに特化カーネルを適用、不均質なら per-row generic にフォールバック。

## 6.6 access 戦略と型

`$_.x`（Fast）は構造アクセスで静的に解決でき特化可能。`$_..x`（Deep）/`item("x")`
（Dynamic）は slow path で、JIT 対象外（または polymorphic inline cache）。IR の
`Access` タグがこの分岐の根拠になる（04 参照）。

### 段階表

| | 型システム |
|---|---|
| MVP | structural / gradual / CSV 推論 / SIMD lane を i64,f64 に集約 |
| 次 | decimal・bignum lane / encoding lane / 明示型注釈・スキーマ宣言 |
| 将来 | observed-type specialization + deopt guard / 型推論器の本格化 |
