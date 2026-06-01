# 22. GPU backend — feature-gated 任意アクセラレータ（既定は GPU なし・依存ゼロ）

> 統括方針（2026-06-01）: **GPU 利用オプションを計画する。** 設計ノート＋
> feature-gate の骨組み（trait 境界と CPU fallback）の両方を用意する。
> これは `09-jit.md`（Tier 2 任意 backend）と同じ規律 — **既定ビルドは GPU なし・
> 依存ゼロを死守**し、重い backend は off-by-default・feature-gate・operator/eval
> 境界の裏・transitive まで vet（`docs/SUPPLY-CHAIN.md`）。

## 22.1 目標と非目標

- **目標**: 大規模な列スキャン（数値 filter / projection / 集計の縮約）を GPU の
  広帯域メモリと数千レーンで加速する。#39 の測定が示した通り、述語比較は
  *memory-bandwidth-bound* で CPU の AVX2 では帯域が頭打ち。GPU は帯域が一桁上で、
  ここが効く可能性がある（**要測定**）。
- **非目標**: 全実行の GPU 化。GPU は**特定の重いカーネルだけ**を担い、それ以外は
  CPU。小さな chunk / 分岐の多い処理 / 文字列の不規則処理は CPU が速い。

## 22.2 死守する堀（CLAUDE.md / 原則）

1. **既定ビルドは GPU 無し・依存ゼロ**。GPU は `--features gpu`（さらに backend 別
   `gpu-wgpu` / `gpu-cuda`）でのみ有効化。`cargo tree -p rivus-cli` は既定で
   rivus-* のみ。`cargo deny check --all-features` 緑。
2. **byte-identical**: GPU カーネルは CPU 基準（`eval.rs` / `kernel.rs`）と
   **必ず同一結果**。浮動小数の縮約順序差が出る集計（sum/avg/std）は、GPU でも
   **決定的縮約順序**を規定するか、§22.6 のとおり **decimal lane（21）= 整数演算**
   に倒して厳密化する。差分テスト（`gpu_matches_cpu`）で gate。
3. **continue-first**: GPU 不在 / OOM / ドライバ異常は **panic させず CPU fallback**
   + warning（telemetry に `gpu_fallback` 計上・原則4）。GPU は「速くなれば使う」
   最適化であって、可用性の前提にしない。
4. **operator boundary を厚くしない**: GPU は `process(from,chunk,ctx)->Vec<Chunk>`
   / eval 境界の裏に差し込む（原則「Operator boundary stays thin」）。エンジンは
   GPU を知らない。

## 22.3 backend 選定（pure-Rust 優先・vet 記録）

| backend | 長所 | 短所 | 位置づけ |
|---|---|---|---|
| **wgpu**（WebGPU, Rust） | pure-Rust 寄り・クロスGPU（Vulkan/Metal/DX12）・C ツールチェーン不要 | 演算特化でなく汎用GPU、compute shader を書く | **第一候補**（`gpu-wgpu`） |
| **CUDA**（`cudarc` 等） | NVIDIA でピーク性能 | NVIDIA 限定・C/ドライバ依存・supply-chain 重 | 上位 opt（`gpu-cuda`、要強い動機＋vet） |
| OpenCL | 広互換 | エコシステム停滞 | 不採用寄り |

選定は `docs/SUPPLY-CHAIN.md` チェックリスト（成熟/主要/stable/trusted/feature-gate/
transitive vet/permissive license）を通してから。まず **wgpu** を骨組みの参照に
置く（クロスプラットフォーム・C 不要が依存ゼロ理念に最も近い）。

## 22.4 何を GPU に載せるか（測定で決める）

候補（効果が出やすい順、いずれも **bench で勝ちを証明してから**採用）:

1. **数値述語 filter の大規模スキャン**: 連続 `&[i64]`/`&[f64]` を GPU に転送し、
   比較 → ビットマスク → selection。#39 の「比較は帯域律速」を GPU 帯域で破れるか。
   ただし **PCIe 転送コスト**が支配し得る（§22.5）。
2. **集計の縮約**: group-by の per-key 縮約を GPU で。decimal lane なら整数縮約で
   厳密（§22.6）。
3. **arith projection**（`price * qty` 等）: 列同士の要素演算は GPU 向き。

**測定の鉄則**（CLAUDE.md「測定なき高速化を主張しない」）: 各カーネルで
**転送込み**の end-to-end を CPU と比較し、`docs/BENCHMARKS.md` に before/after。
転送で負ければ採用しない（#39 の AVX2 と同じ正直さ）。

## 22.5 転送コストという現実

GPU の弱点は **CPU↔GPU 転送（PCIe）**。Rivus は streaming（chunk 単位・有界メモリ）
なので、chunk ごとに転送すると転送が支配しがち。緩和策（測定対象）:

- **大 chunk バッチ**: GPU 経路では chunk を束ねて転送粒度を上げる（`--memory` 戦略
  と連携、Pillar C）。
- **しきい値ゲート**: 入力サイズ・選択率が GPU 有利圏のときだけ GPU（オートチューナ
  #33 に `gpu` 戦略を足す）。小入力は常に CPU。
- **kernel fusion**: filter+project+agg を 1 回の転送で GPU 上に連鎖（08 fusion を
  GPU 経路へ）。

## 22.6 数値正確性 — decimal lane（21）との連携

GPU の f64 縮約は**並列ツリー縮約**ゆえ CPU の逐次和と縮約順序が違い、最終 ULP が
ズレ得る（#41 と同じ非結合性の問題が GPU でも起きる）。対策は 2 択、いずれも
ユーザーオプトイン:

- **`--exact`（decimal lane, 21）**: 値を i128 scaled integer にして GPU でも
  **整数縮約** → 結合的・厳密・byte-identical。「速度を犠牲にした正確性」を GPU でも
  そのまま実現。
- **既定 f64**: GPU 経路は「数値的に等価（~1e-15）」を許容し、厳密一致が要るなら
  CPU/decimal に倒す、と **明示ドキュメント**。

min/max/count/first/last/percentile は GPU でも順序非依存に厳密。

## 22.7 段階実装（骨組み → 測定 → 採用）

1. **骨組み（このノートとセット）**: `rivus-gpu` crate（feature `gpu`）と
   `trait PredicateBackend { fn filter_mask(&self, col, op, rhs) -> Mask; }` を定義。
   既定提供は **CPU 実装**（`kernel.rs` を呼ぶ）で、GPU 無効時はこれだけがリンク。
   `cargo build`（既定）= GPU コード一切なし・依存ゼロを `cargo tree` で固定。
2. **wgpu 最小カーネル**: `gpu-wgpu` で f64 比較 → mask の compute shader を実装。
   `gpu_matches_cpu` 差分テスト（seed 列、CPU == GPU、NaN/境界含む）。
3. **転送込みベンチ**: §22.4-1 を CPU/GPU/転送分解で測定。勝てば採用、負ければ
   #39 同様「測定済みネガティブ」として記録し非採用。
4. **オートチューナ連携**: 勝ち圏（サイズ・選択率・GPU 有無）を `analytics`(#33) に
   足し、`--memory`/`--accel gpu|cpu|auto` で選択。既定 `auto` は GPU 有利圏のみ。
5. **decimal × GPU**: §22.6 の整数縮約で厳密 GPU 集計（優先度低、金額は CPU で足りる）。

## 22.8 CLI / UX

```
rivus run flow.riv --accel gpu        # GPU を試みる（不可なら CPU fallback + warning）
rivus run flow.riv --accel auto       # オートチューナ（既定）。有利圏のみ GPU
rivus run flow.riv --accel cpu        # 明示 CPU（再現性/デバッグ）
```

`--accel` は無指定（= GPU feature 無効ビルド）では存在しないか no-op。telemetry に
「どのカーネルが GPU で走ったか / fallback したか」を出し（原則4）、ダッシュボード
（Pillar B）で可視化する。

## 22.9 まとめ

GPU は **#39 が示した帯域の壁を物理的に超えうる**唯一のレバーだが、転送コストと
正確性（縮約順序）という二つの現実があり、**両方とも測定と decimal lane で対処**
する。実装は trait 境界＋ feature-gate ＋ CPU fallback の骨組みから始め、
**転送込みで勝ちを測定**してからカーネルを増やす。既定ビルドの依存ゼロと
byte-identical の堀は一切緩めない。
