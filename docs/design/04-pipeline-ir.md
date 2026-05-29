# 04. Pipeline IR（DAG IR）

## 4.1 役割

IR は Rivus の単一の真実である。source は IR に lower され、optimizer は IR を
書き換え、runtime は IR を実行し、`to_source()` は IR を source に戻す。

## 4.2 AST と IR の関係

概念的には `source → AST → IR` だが、MVP は parse 中に直接 `PlanGraph` へ lower
する（IR がグラフ形の AST を兼ねる）。将来 optimizer の都合で中間 AST を分離する
場合も、IR が正本である点は変えない。

```
AST（概念）                          IR（実体: PlanGraph）
 Scope("Users")            ───▶      Node#2 { label:"Users", op:Project }
   Source(open "u.csv")    ───▶      Node#0 { op:OpenCsv }      ─edge→ #1
   Filter(age>=20)         ───▶      Node#1 { op:Filter }       ─edge→ #2
   Project([name])         ───▶      Node#2 { op:Project }
```

## 4.3 PlanGraph の構造

`rivus-ir/src/graph.rs`：

```rust
struct PlanGraph {
    nodes: Vec<Node>,                 // id == index
    edges: Vec<Edge>,                 // from -> to, kind ∈ {Stream, Error}
    labels: HashMap<String, NodeId>,  // scope label -> 生成ノード
}
struct Node { id, label: Option<String>, op: Op, hooks: Vec<Hook> }
struct Edge { from: NodeId, to: NodeId, kind: EdgeKind }
```

`Op` は source / transform / 分岐合流 / sink を1つの enum に統一（原則1）：

```rust
enum Op {
    OpenCsv { path }            // open
    StreamRef { name }          // stream（replay）
    Filter { pred: Expr }       // |?
    Project { fields: Vec<String> } // |>
    GroupBy { key }             // |#
    Branch                      // ->（fan-out; MVP は構造的に表現）
    Merge                       // +
    Join { left_key, right_key }// &
    SinkPrint                   // print
    SinkCsv { path }            // save
}
```

### 構造の例（branch.riv）

```
#0 open(users.csv)
   ├──▶ #1 filter(age>=20)  [Adults]
   │         └──▶ #3 merge  [Merged]
   └──▶ #2 filter(age<20)   [Minors]
             └──▶ #3 merge  [Merged]
```

`->` の fan-out は「1ノードから複数 out-edge」で表現するため、専用 Branch
ノードを作らずに済む（`rivus-parser` の branch 処理）。Merge は多入力1出力。

## 4.4 Expr — 式と access 戦略

`rivus-ir/src/expr.rs`。filter 述語と projection で使う。アクセス戦略を型に
持たせ、optimizer/JIT が fast path を特化できるようにする（原則7）。

```rust
enum Expr {
    Field { name, access: Access }   // Fast: $_.x / Deep: $_..x / Dynamic: item("x")
    Literal(Value)
    Compare { left, op: CmpOp, right }
    And(Box<Expr>, Box<Expr>)
    Or (Box<Expr>, Box<Expr>)
}
enum Access { Fast, Deep, Dynamic }
```

`$_:N`（scope stack）も parse され、MVP では level を無視して Fast 解決する
（nested chunk 実装後に親 scope 参照を有効化）。

## 4.5 可逆性（原則5）

`PlanGraph::to_source()` は IR から読める source を再生成する。`explain` で確認できる：

```
$ rivus explain examples/branch.riv
▒ regenerated source (IR -> source, reversibility)
  Users:
      open examples/users.csv
      -> Adults: ... ;
      -> Minors: ... ;
  ;
  Adults:
      |? $_.age >= 20
  ;
  ...
```

再生成は「best-effort canonical」：意味は保存するが、整形は正規形になる
（bare field `age` は正規形 `$_.age` に展開される等）。これが「リファクタリング =
graph transformation」を支える土台：最適化で DAG を書き換えても、常に人間可読な
source を提示できる（Observability §18）。

### 可逆性の検証戦略

`parse(to_source(parse(src)))` の IR が `parse(src)` の IR と **構造同型**である
ことを property test で保証する（Phase 1）。整形差は許容し、ノード種別・edge・
述語の意味の一致を検査する。

## 4.6 不変条件

- `nodes[i].id == i`（index = id）。
- DAG（Stream edge に循環なし）。`topo_order()` が `None` を返したらビルドエラー。
- label は一意。merge/join の参照先 label は定義済みであること（MVP は前方参照
  のみ。Phase 1 で2パス解決）。

### 段階表

| | IR |
|---|---|
| MVP | PlanGraph / Op / Expr / to_source / topo_order |
| 次 | 中間 AST 分離・2パス label 解決・IR バージョニング・property test |
| 将来 | DataFusion LogicalPlan との相互変換・分散用の stage 分割注釈 |
