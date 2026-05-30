# 10. Shell Syntax（Unified Flow Syntax）

## 10.1 思想

すべては Scope と Flow。`:` で scope 開始、`;` で scope 終了。プログラムは「逐次
命令」ではなく「データ流の定義」。停止ではなく Continuity を優先する。

## 10.2 演算子早見表

| 記号 | 意味 | IR |
|---|---|---|
| `Label: ... ;` | scope（execution graph node）宣言 | label 付きノード |
| `: ... ; Label` | 無名 scope + 結果への label 付与 | 同上 |
| `\|?` | filter | `Op::Filter` |
| `\|>` | map / projection（裸の列） | `Op::Project` |
| `\|> (expr) as name` | 計算列（`+ - * / %`・別名） | `Op::ProjectExpr` |
| `\|#` | group / partition by | `Op::GroupBy` |
| `take N` / `limit N` / `head N` | 先頭 N 行で打ち切り（chunk-size 非依存） | `Op::Take` |
| `sort KEY [asc\|desc]` | キー列で安定ソート（blocking・chunk-size 非依存） | `Op::Sort` |
| `distinct [KEY ...]` | 重複行を除去（全列 or キー列・先勝ち） | `Op::Distinct` |
| `\| map { ... }` | map block（要素変換） | （MVP: 解析のみ） |
| `->` | branch（tee, 多分岐） | fan-out edge |
| `+` | merge（union） | `Op::Merge` |
| `&` | synchronized join | `Op::Join` |
| `Label!` | force materialize | （MVP: 構造的 no-op） |
| `stream Label` | replay | `Op::StreamRef` |
| `stop flow;` | 明示停止（例外的） | （MVP: directive） |
| `$_` / `$_.x` / `$_..x` / `$_:N` | current object / field / deep / scope stack | `Expr::Field` |
| `item("x")` | dynamic lookup（slow path） | `Expr::Field{Dynamic}` |

## 10.3 文法（実装済み MVP サブセット）

`rivus-parser`（lexer + recursive descent）が受理する範囲：

```ebnf
program    = item* ;
item       = scope | anon-scope | directive ;
scope      = IDENT ':' body ';' ;
anon-scope = ':' body ';' IDENT? ;
directive  = ('monitor'|'watch'|'visualize'|'stop') ... ';' ;  (* MVP: no-op *)

body       = head transform* ;
head       = 'open' PATH ('as' FMT)?                       (* 既定は拡張子, asで上書き *)
           | 'readcsv' PATH | 'readjson' PATH              (* 明示エイリアス *)
           | 'readbin' PATH ('le'|'be')? ('packed'|'aligned')? '(' (IDENT ':' BINTYPE)+ ')'
           | 'stream' IDENT
           | ref-expr
           | (* branch 子では空: 親 flow を継承 *) ;
FMT        = 'csv'|'tsv'|'json'|'jsonl'|'ndjson' ;
BINTYPE    = 'i8'|'i16'|'i32'|'i64'|'u8'|'u16'|'u32'|'u64'|'f32'|'f64'|'bool' ;

(* フォーマット判別は拡張子に依存しすぎない:                                   *)
(*   - open f.csv / f.jsonl        … 拡張子で自動（最短手数の既定）          *)
(*   - open f.dat as json          … 拡張子が無い/嘘の時は as で明示上書き    *)
(*   - readcsv f / readjson f / readbin f (...) … 動詞エイリアスで一目瞭然    *)

(* sink は source と対称（write what you can read）:                          *)
(*   save PATH [as FMT] | writecsv PATH | writejson PATH                       *)

(* stdin/stdout 連携（他シェルのパイプに挟める）:                              *)
(*   open stdin [as FMT]   … 標準入力を読む（既定 csv）                       *)
(*   save stdout [as FMT]  … 標準出力へ書く（`rivus run` の可視化は stderr）  *)
(*   ( `stdin`/`stdout`/`-` はすべて同じ標準ストリームを指す )                 *)
ref-expr   = IDENT ( ('+' IDENT)+ | ('&' IDENT) )? ;   (* merge / join *)

transform  = '|?' expr
           | '|>' proj+
           | '|#' field
           | ('take'|'limit'|'head') INT
           | 'sort' IDENT ('asc'|'desc')?
           | 'distinct' IDENT*
           | '|' 'map' block
           | branch
           | sink
           | hook ;
branch     = '->' IDENT ':' body ';' ;
sink       = 'save' PATH | 'print' ;
hook       = 'on' EVENT ('severity' '>=' SEV)? ':' action ';' ;
action     = 'transition' MODE | 'log' STRING | ['route'|'reroute'] IDENT | IDENT ;

proj       = IDENT ('as' IDENT)?           (* bare field / rename *)
           | '(' expr ')' 'as' IDENT ;     (* computed column *)
expr       = or ;
or         = and ('or' and)* ;
and        = cmp ('and' cmp)* ;
cmp        = add (CMP add)? ;
add        = mul (('+'|'-') mul)* ;         (* arithmetic: 括弧内のみ字句化 *)
mul        = primary (('*'|'/'|'%') primary)* ;
primary    = INT | FLOAT | STRING | 'true' | 'false'
           | '(' expr ')'                  (* grouping + expression mode *)
           | '$_' field-tail | '$_:'N field-tail
           | 'item' '(' STRING ')'
           | IDENT ;                       (* bare field of current object *)
field-tail = '.' IDENT | '..' IDENT ;
CMP        = '==' | '!=' | '<' | '<=' | '>' | '>=' ;
```

字句規則：`#` は行コメント（ただし `|#` は演算子）。空白・改行は非有意。
ファイルパスは `users.csv` `/tmp/x.csv` `data/out` が1トークンになるよう、
word に `. / -` を許容（先頭は英字/`_`/`/`）。

## 10.4 例（すべて `examples/` にあり、動く）

```
# adults.riv — 線形 flow
Users:
    open examples/users.csv
    |? age >= 20
    |> name age
;
```

```
# branch.riv — 多分岐 + 合流（DAG）
Users:
    open examples/users.csv
    -> Adults: |? age >= 20 ;
    -> Minors: |? age <  20 ;
;
Merged:
    Adults + Minors
;
```

```
# recover.riv — continue-first + mode escalation
Import:
    open examples/messy.csv
    |? age >= 20
    on error severity >= warning:
        transition degraded
    ;
;
```

## 10.5 scope stack（$_:N）の意味

```
Orders:
    | map {
        $_.items
        |? $_:1.country == "JP"   # $_:0 = current(item), $_:1 = parent(order)
    }
;
```

`$_:0` 現在 / `$_:1` 親 scope / `$_:2` その上。MVP は parse して level を保持し、
評価では flat schema 解決（nested chunk 実装後に親参照を有効化）。

## 10.6 未実装（設計済み・パーサは将来拡張）

`mode <name>: ...;` 定義、`on chunk_begin/recovery` の本体実行、`| map {}` 本体の
評価、`item(..)`/`$_..` の slow path 実行、materialize/replay の実体。いずれも
IR・実行モデル側の受け皿（`Op` / `Hook` / `Access`）は用意済み。

### 段階表

| | 構文 |
|---|---|
| MVP | scope / 無名 scope / `\|? \|> \|#` / `-> + &` / hook(on error) / sink |
| 次 | `mode` 定義 / map block 評価 / 全 hook 実行 / scope stack 評価 |
| 将来 | マクロ・ユーザ定義 operator 構文 / 型注釈構文 / `rivus live` Markdown |
