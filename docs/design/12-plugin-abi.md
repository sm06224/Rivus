# 12. Plugin ABI

## 12.1 目的

source / sink / transform をサードパーティが追加できるようにする。Rivus の
中核思想（chunk-native, observable, continue-first）をプラグインにも強制する。

## 12.2 二層のプラグイン

```
1. Native plugin (Rust, dynamic)  : cdylib を dlopen。最速。trust 必要
2. Sandboxed plugin (Wasm)        : Wasmtime 等で隔離。安全・言語非依存
```

両者とも同じ論理 ABI（下記）を満たす。Wasm 側は chunk を Arrow IPC で受け渡す
（言語非依存・zero-copy 寄り）。

## 12.3 論理 ABI（operator の契約）

中核は MVP の `Operator` trait と同型：

```rust
// 安定 ABI 版（C ABI / Wasm import として公開）
trait RivusOperator {
    fn kind(&self) -> OpKind;          // Source / Transform / Sink
    fn schema_out(&self, schema_in: &Schema) -> Schema;  // 型伝播
    fn pull(&mut self, ctx) -> Option<Chunk>;            // Source のみ
    fn process(&mut self, from: NodeId, chunk: Chunk, ctx) -> Vec<Chunk>;
    fn finish(&mut self, ctx) -> Vec<Chunk>;
    fn capabilities(&self) -> Capabilities;  // pushdown 可否 / 並列可否 / 決定性
}
```

`capabilities` で「filter pushdown を受け付ける」「chunk split で並列化してよい」
「副作用なし（再実行安全）」を宣言させ、optimizer/scheduler が安全に扱う。

## 12.4 C ABI 境界（native plugin）

dynamic library は次の最小 C 関数を export する：

```c
// プラグイン登録: operator 名と vtable を返す
const RivusPluginManifest* rivus_plugin_register(void);

// chunk は Arrow C Data Interface (ArrowArray/ArrowSchema) で受け渡す（zero-copy）
typedef struct {
    int  (*process)(void* state, const ArrowArray* in, ArrowArray* out, RivusErrorSink*);
    int  (*finish) (void* state, ArrowArray* out, RivusErrorSink*);
    void (*drop)   (void* state);
} RivusOpVTable;
```

Arrow C Data Interface を使うことで、言語・バージョン跨ぎでも chunk を安定・
zero-copy に渡せる（自前 serialize を持たない＝アンチパターン hidden serialization
回避）。

## 12.5 error stream への参加（continue-first 強制）

プラグインは例外で落ちてはならない。`RivusErrorSink` 経由で `ErrorEvent` を
出し、`Severity` を申告する。fatal 以外は main flow を止めない。panic/trap は
runtime が捕捉して `Critical` に変換し（Wasm は trap を隔離）、該当ノードを
isolation mode に落とす（13）。

## 12.6 telemetry への参加

プラグイン operator も `NodeTelemetry` の対象。計測は runtime 側で行う（plugin に
計測責務を持たせない）ため、観測の一貫性が保たれる（observable-first をプラグイン
境界でも維持）。

## 12.7 登録と名前解決

```
rivus.toml:
  [[plugin]]
  name = "parquet"
  kind = "native"          # or "wasm"
  path = "./plugins/libparquet_rivus.so"
  provides = ["open-parquet", "save-parquet"]
```

source `open foo.parquet` は拡張子/明示宣言から plugin を解決する。未知拡張子は
error stream に `Recoverable` を出して継続（停止しない）。

### 段階表

| | Plugin ABI |
|---|---|
| MVP | なし（operator は組込みのみ。`Operator` trait は ABI の原型） |
| 次 | native cdylib + Arrow C Data Interface / capabilities / rivus.toml 登録 |
| 将来 | Wasm sandbox plugin / バージョニング / plugin marketplace |
