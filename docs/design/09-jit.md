# 09. JIT Strategy

## 9.1 目標と非目標

目標：hot path（よく流れる filter/projection/map のカーネル）を機械語化し、
Rust/C 級の性能に近づける。非目標：全コードの AOT コンパイル。Rivus は
interpreter ファーストで、**観測に基づき hot な部分だけ**を JIT する
（HotSpot / Truffle 流、原則7）。

## 9.2 三段ティア

```
Tier 0  Interpreter        : eval.rs の row-wise 評価。常に正しい基準実装
   │  hot 判定（呼出回数 × chunk 量 × 観測型の安定性）
   ▼
Tier 1  Vectorized kernel  : 観測型に特化した列単位ループ（まだ Rust、SIMD intrinsics）
   │  さらに hot & 安定
   ▼
Tier 2  JIT (Cranelift)    : fused operator を機械語化。guard で deopt
```

Tier 0 は「正しさの真実」。Tier 1/2 は Tier 0 と **必ず同一結果**を出さねば
ならない（差分テストで保証、15 参照）。

## 9.3 何を JIT するか

最も効くのは **fused predicate/projection カーネル**（08 の fusion 結果）。

```
入力: fused(filter(age>=20 and country=="JP"), project[name,age])
観測: age:i64（列均質）, country:str（dictionary 化可）
生成: fn kernel(age: &[i64], country: &DictArray, out_idx: &mut Vec<usize>)
        → SIMD 比較 + ビットマスク + selection
```

## 9.4 Cranelift を選ぶ理由と差し替え余地

- **Cranelift**: コンパイルが速く（JIT に好適）、Rust ネイティブ、依存が軽い。
  まずここ。
- **LLVM**: ピーク性能が要る場合の上位ティア（コンパイル遅・依存重）。
- **MLIR**: 列演算を高水準方言で表現し最適化したくなった段階で検討。

`rivus-jit` crate を `Operator` 実装として追加する。`build()` が「この op は
JIT 可能か」を判定し、可能なら `JitOperator`（Cranelift で生成した関数ポインタを
保持）を返す。中核 API（`process`）は不変。

## 9.5 deoptimization（observed type が外れたら）

```
JIT kernel 実行
   │  guard: 列が想定 lane(i64) か / null 無しか / dictionary 同一か
   ├─ OK   → 機械語パスで処理
   └─ NG   → deopt: その chunk だけ Tier 0 interpreter へフォールバック
              （観測を更新し、必要なら再 JIT）
```

これにより「特化の賭けが外れても止まらない」（continue-first を JIT 層でも維持）。

## 9.6 安全性とサンドボックス

- 生成コードはメモリ安全な範囲（境界チェックを生成時に保証、または安全な
  selection API 経由）に限定。
- JIT 失敗（コンパイルエラー）は Tier 1/0 へフォールバックし、error stream に
  info を出す（fatal にしない）。
- plugin（12）由来の式は JIT 対象外に隔離可能（信頼境界）。

## 9.7 ベンチで駆動する

JIT は「速くなった」を必ず計測で示す（優先順位6 だが観測必須）。15 のベンチに
Tier 0/1/2 の比較を組み込み、回帰したら自動で警告する。

### 段階表

| | JIT |
|---|---|
| MVP | なし（Tier 0 interpreter のみ。これが基準実装） |
| 次 | Tier 1 vectorized kernel（SIMD intrinsics）+ 観測カウンタ |
| 将来 | Tier 2 Cranelift JIT + deopt guard +（必要なら）LLVM/MLIR 上位ティア |
