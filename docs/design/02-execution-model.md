# 02. 実行モデル

## 2.1 Flow = Scope = Graph Node

Rivus のプログラムは「逐次命令」ではなく「データ流の定義」である。`Label: ... ;`
は変数束縛ではなく **execution graph node の宣言**であり、値ではなく flow
reference を生む。function / filter / scriptblock / pipeline / exception handler
はすべて同じ「Flow Scope」に統一される（原則1）。

- **Node** = Scope / Transform / Event（hook）
- **Edge** = Stream / Dependency / Branch / Error path

実行は常に DAG の評価であり、線形パイプラインはその退化形にすぎない（原則3）。

## 2.2 push 型・chunk 粒度・単一スレッド（MVP）

MVP の実行エンジン（`rivus-runtime/src/engine.rs`）は、DAG 上を chunk が流れる
**push 型スケジューラ**である。pull 型（Iterator）ではなく push 型を採るのは、
branch（1→多）と merge/join（多→1）を自然に表現するため。

各ノードに入力キュー `in_q[node]` を持たせ、トポロジカル順に1ラウンドずつ駆動する。

```rust
// 擬似コード（実装と対応）
loop {
    let mut active = false;
    for nid in topo_order {            // 上流→下流
        if done[nid] { continue; }
        if op[nid].is_source() {
            match op[nid].pull(ctx) {
                Some(chunk) => distribute(nid, [chunk]),   // 後続へ push
                None        => finish(nid),                // ソース枯渇
            }
        } else if let Some((from, chunk)) = in_q[nid].pop_front() {
            let out = op[nid].process(from, chunk, ctx);    // 1 chunk 変換
            distribute(nid, out);
        } else if upstream_remaining[nid] == 0 {
            let out = op[nid].finish(ctx);                  // flush（group/join）
            distribute(nid, out); finish(nid);
        } else {
            continue;                                       // 上流待ち
        }
        active = true;
    }
    if !active { break; }
}
```

`distribute(nid, chunks)` は後続が複数なら **clone して全 edge に push**（branch =
fan-out）、後続が無ければ leaf として `results[nid]` に捕捉する。

### なぜ chunk 粒度か（原則6）

1 item ずつ動かすとオーバーヘッドが支配的になり SIMD も効かない。chunk
（数千行の columnar batch）を最小単位にすることで、ベクトル化・キャッシュ効率・
スケジューラのコストを同時に解決する。`process` は「1 chunk を受けて 0 個以上の
chunk を返す」インターフェースに統一されている。

## 2.3 ライフサイクルとイベント（hooks are scopes）

各ノードはライフサイクルイベントを持ち、それぞれに hook scope を結び付けられる
（Observability §10）。

```
begin → first → (chunk_begin → process → chunk_end)* → last → end
                                     │
                                     ├─▶ error      （任意時点）
                                     ├─▶ retry / timeout
                                     ├─▶ mode_change
                                     └─▶ recovery
```

hook も Scope なので、本体に任意の flow を書ける（`on error: Errors` で error
stream を別 flow へ流す等）。MVP では `on error`（+ severity guard）→ `transition`
が実行され、その他の hook は IR に保持され `explain` で再生成 source に現れる。

## 2.4 continue-first の実行的意味

例外送出やスタックアンワインドを行わない（原則2）。エラーは side-channel の
**error stream** に積まれ、main flow は走り続ける。停止するのは
`Severity::Fatal` のみ（`engine.rs` で `fatal` フラグを立て、mode を `Halted` に）。

```
main flow :  ──chunk──▶──chunk──▶──(bad chunk skip)──▶──chunk──▶ ...
error flow:                    └──[recoverable] chunk 42 ─────▶ (hook / 集約)
```

これは「文字列のデコード失敗でも止めない」「壊れた行はスキップして警告」という
細部まで一貫する（原則8、`csv.rs` の `bad_rows`）。

## 2.5 materialization と replay

- `Users!` … **force materialize**。境界を確定し全実体化する。MVP では sink/group
  が暗黙の materialization 境界（group は `finish` で1 chunk を出す）。
- `stream Users` … **replay**。記録済みグラフを再実行する。MVP は checkpoint store
  未実装のため info を error stream に出して空を返す（設計は §17/§14 で記述）。

### 段階表

| | 実行モデル |
|---|---|
| MVP | 単一スレッド・push・chunk 粒度・round 駆動・leaf 捕捉 |
| 次 | 並列ワーカ + backpressure（credit）+ adaptive chunk sizing |
| 将来 | feedback edge（循環）/ checkpoint & replay / 分散実行 |
