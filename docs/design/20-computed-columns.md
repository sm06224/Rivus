# 20. 計算列（derived columns）と式モード字句解析

> 本ドキュメントは **次に実装する** 機能の設計。`|>` projection に算術式と別名
> を持ち込み、「フィルタ・選択・整列・集約・重複除去」に続いて **新しい列を
> 計算する** 能力を Rivus に与える。これは pandas / DuckDB を日常のデータ加工
> から卒業するための本丸機能の一つ。

## 20.1 目標構文

```
# 既存（純粋な列選択）はそのまま
|> name age

# 計算列 + 別名（追加分）
|> name (age * 12) as months (price * tax) as gross
```

- projection item = `IDENT`（裸の列選択）| `'(' expr ')' 'as' IDENT`（計算列）。
- 算術: `+ - * / %`、優先順位は `* / %` > `+ -`、括弧で grouping。
- 文字列連結など型別演算は将来拡張（まずは数値レーン）。

## 20.2 なぜ今すぐ入れないか — 字句解析の衝突

現在の lexer は **パス親和**（`users.csv` `/tmp/x` `a-b` を 1 トークンにする
ため、`. / -` を word 継続文字に含む）。このため中置算術 `age-1` `age/2`
`a.b` は 1 つの word に飲み込まれ、`-` `/` 単独トークンも path 開始の `/` と
衝突する。安易に演算子化すると **パス字句が壊れる**（回帰リスク大）。

→ 解決は **モード付き字句解析**：パス文脈（`open`/`save`/`readbin` の引数）
では今の貪欲 word を維持し、**式文脈**（`|?` / `(...)` の内側）では
`+ - * / %` `(` `)` を独立トークンとして切る。lexer をステートフルにするか、
parser 駆動の「式モード再字句化」を入れる。これは独立した慎重な変更単位。

## 20.3 IR / 実行モデルへの影響

- `Expr` に `Arith { left, op: ArithOp, right }`（`ArithOp = Add|Sub|Mul|Div|Mod`）
  を追加。`Display` は決定的に括弧付けして可逆性を保つ。
- projection は **計算列を含むときだけ** 新 op `Op::ProjectExpr { items:
  Vec<(Expr, String)> }` に lower（純粋選択は既存 `Op::Project { fields }` の
  まま＝既存の fusion / pushdown / ベンチに無影響）。
- 実行は **列指向**で評価する `eval_column(expr, &chunk) -> Column`：
  - `Field` は列を clone、`Literal` は定数列、`Arith` は左右を数値レーン
    （I64/F64/Bool、Str は best-effort で f64 parse・失敗は NaN）に落として
    要素ごとに計算。
  - 型昇格: 両辺が整数レーンかつ op ∈ {+,-,*,%} なら I64、それ以外と `/` は
    F64（chunk 内で列型は一定なので行ごとに型がブレない）。
  - 0 除算は continue-first（F64=NaN / I64 mod0=0 + warn）。
- optimizer: `Op::ProjectExpr` は不透明（fusion 対象外、pushdown の consumer と
  しては安全側で `safe=false`）。後で「計算列が参照する列だけを pushdown」する
  最適化を足せる。
- engine: `ProjectExpr` は **stateless** なので並列パス適格（Take/Sort/Distinct
  のような直列強制は不要）。

## 20.4 段階

| | 内容 |
|---|---|
| 次 | 式モード字句解析 → `Expr::Arith` → `Op::ProjectExpr` → `eval_column` → oracle テスト（算術の chunk-size 非依存） |
| その後 | 文字列関数（`upper`/`lower`/`len`/`substr`）・`case when`・計算列の pushdown 最適化・計算述語の SIMD kernel 化 |
