# 03. Stream / Chunk モデル

## 3.1 基本概念

```
Stream<Chunk<T>>
```

Rivus の基本は byte stream ではなく **構造化された chunk stream**。Chunk は
bounded / binary native / metadata attached / zero-copy oriented / parallelizable /
splittable / backpressure aware であること。

## 3.2 Chunk のメモリレイアウト

MVP の `Chunk`（`rivus-core/src/chunk.rs`）は columnar：

```
Chunk
├─ meta : ChunkMeta { id, created_at, warnings[], corrupt, mode }
├─ schema : Arc<Schema>          ← 共有（複製しない）
├─ columns : Vec<Column>
└─ len : usize

Column = enum {                  ← lane ごとに連続バッファ（SIMD-friendly）
    Bool(Vec<bool>)
    I64 (Vec<i64>)
    F64 (Vec<f64>)
    Str (Vec<String>)            ← MVP。将来は offset+bytes の Arrow Utf8 へ
}
```

論理レイアウト（columnar）:

```
            row0   row1   row2   row3
 name(Str)  "aki"  "ben"  "cho"  "dee"
 age (I64)   30     15     40     10
 ctry(Str)  "JP"   "US"   "JP"   "US"
            └──────────── len = 4 ───────────┘
```

columnar である理由：
- フィルタ/集計が列単位でベクトル化できる（SIMD lane に直結、原則6/7）。
- projection（`|>`）が「列の選択」= ポインタの選択で済み、zero-copy に近い。
- 圧縮・dictionary encoding・null bitmap を列ごとに最適化できる。

## 3.3 ChunkMeta — 観測可能なメタデータ（Observability §9）

```rust
struct ChunkMeta {
    id: u64,              // chunk id（生成順）
    created_at: Instant,  // latency 計測の起点
    warnings: Vec<String>,// degraded decoding 等の非致命警告
    corrupt: bool,        // 破損フラグ（recovery 経路へ）
    mode: Mode,           // 生成時点の runtime mode
}
```

chunk は自分が「いつ・どのモードで・どんな警告つきで」生まれたかを運ぶ。これに
より downstream とテレメトリが per-chunk に観測でき、checkpointable になる。

## 3.4 splittable / parallelizable

chunk は行範囲で分割可能。並列化（Phase 1）では 1 chunk を N サブ chunk に
split → 複数ワーカで処理 → 結果を merge する。`Chunk::gather(&indices)` が
分割・フィルタの共通プリミティブ（列ごとに `Column::gather`）。

```
        split                 parallel map               merge
 chunk ──────▶ [c0 c1 c2 c3] ───────────▶ [c0' c1' c2' c3'] ─────▶ chunk'
                 (worker x4)
```

順序保証が必要な場合は sub-chunk に sequence 番号を付け、merge 時に整列する
（MVP は単一スレッドなので不要）。

## 3.5 zero-copy 伝播の方針（優先順位2）

MVP は `Vec` で素直に実装しているが、API は zero-copy を見据えている：

- `schema` は `Arc<Schema>` で共有。変換で複製しない。
- projection は列の **選択**（`Vec<Column>` の組み替え）であり、列バッファ自体は
  複製しない設計に移行できる（現状は `clone()` だが Arrow 化で `ArrayRef` の
  refcount コピーになる）。
- filter は `gather` で新バッファを作るが、選択率が高い（ほぼ全通過）の場合は
  入力 chunk をそのまま転送する最適化を既に実装（`Filter::process` の
  `keep.len() == chunk.len` 分岐）。

### Arrow への移行計画

`Column` を Arrow の `ArrayRef` に置き換える。`value_at` / `gather` / `project`
の3メソッドさえ Arrow compute kernels（`take`, `filter`）に差し替えれば、
上位（operator / engine）は無改修で zero-copy・SIMD・FFI を得る。

```
MVP:   Column::I64(Vec<i64>)
Arrow: Arc<arrow::array::Int64Array>     // バッファ共有・null bitmap・SIMD kernel
```

## 3.6 backpressure-aware（設計）

push 型では下流が詰まると `in_q` が膨らむ。これを無制限にすると原則違反
（implicit unbounded buffering 禁止）。Phase 1 で各 edge に **bounded queue +
credit** を導入：下流が空き credit を上流に返し、credit が 0 なら上流の `pull`/
`process` を一時停止する。MVP は単一スレッド round 駆動なので 1 chunk ずつしか
進まず、自然に bounded（最大滞留 = ノード数）。

### 段階表

| | Chunk モデル |
|---|---|
| MVP | columnar `Vec` / `Arc<Schema>` / meta 付き / gather・project |
| 次 | Arrow `ArrayRef` backing / null bitmap / dictionary / bounded queue + credit |
| 将来 | mmap / GPU buffer / off-heap arena / cross-node 転送フォーマット |
