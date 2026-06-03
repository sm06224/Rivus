# 24. Validation layer — 宣言契約 + 強制 observability + disposition policy

> 統括方針（2026-06-03, バグ報告の翻訳）: **validator は filter とは別物。** filter は
> 「黙って整形」、validator は「宣言した契約を**強制**し、不適合を*明示的に*排除/警告
> し、その事実を必ず surface する（**never silent**）」。`#80`（parse 失敗の surface）・
> `#81`（parse-error policy）は「最初の**行内 ingress validator**」であり、本層で
> 一般化して one-off を収斂させる（Epic `#82` / 基盤 `#83`）。server/relay は egress
> validation を前提とする別 Epic（本書 §24.4）。

## 24.1 なぜ必要か / filter との違い

|  | filter (`\|?`) | validator (`\|!`) |
|---|---|---|
| 目的 | 関心ある行を**選ぶ** | 契約に反する行を**検出して処分する** |
| 不適合行 | 黙って落ちる（それが正常） | 落とす/警告するが **必ず観測可能** |
| 既定 | — | disposition を**明示**。silent drop は禁止 |
| 観測 | selectivity のみ | 件数・理由・列・サンプルを error stream へ |

Rivus 哲学との整合: **Observable First**（観測が一級・§14）、**continue-first**
（Fatal 以外は流れ続ける・§13）、**stream-native / bounded**（行間・窓状態は有界）、
**依存ゼロ**（既定ビルド）。validator は §13 error model の上に「宣言契約」を載せた層。

## 24.2 disposition（処分）— never silent

| disposition | 行 | surface | severity (§13) |
|---|---|---|---|
| `warn` | 残す（素通し） | 件数+列+理由+サンプル | `Warn`/`Recoverable` |
| `reject` | **落とす** | 同上 | `Recoverable` |
| `quarantine(<sink>)` | 落とす + **dead-letter sink へ退避** | 同上 | `Recoverable` |
| `halt` (strict) | — | 同上 | `Fatal`（mode=Halted） |

全 disposition で「何件・どの列・なぜ」を §14 observability に流す。**silent は無い**
のが本層の不変条件。既定は `warn`（never-silent の最小形）。`reject`/`quarantine`/
`halt` は明示。`quarantine` は egress 契約（§24.4）で「次段に渡すデータをクリーンに」。

## 24.3 粒度（3 段）

1. **行内 (intra-row)**: 型 / 範囲 / regex / 必須(non-null) / enum / 長さ。状態不要・
   完全ストリーミング・並列安全。**`#80`/`#81` の parse 検証はここ。**
2. **行間 (inter-row)**: 単調増加 / 一意 / 連番 / 参照整合。**有界ストリーム状態**
   （直前値・seen-set・参照表）で実装。seen-set/参照表は **cardinality 有界**が前提
   （無界なら `#50` unbounded opt-in 下のみ、または近似 + 明示）。
3. **窓内 (intra-window)**: 窓を閉じる前の集計検証・網羅性（欠損キー検出）。
   **`#56` 時系列の窓**と一体（同じ窓状態を共有）。

## 24.4 入出力両面（ingress / egress）

- **ingress**: source 直後で入力を清掃（= 現 parse validator）。先に着手する。
- **egress**: sink / relay の**直前**で**下流契約**を保証し、不適合は dead-letter
  （`quarantine`）。「次段連結をクリーンに」。**egress ゲートは将来の server/relay
  Epic（常駐デーモン・ソケット source・リモート sink・中継・back-pressure）の前提**で、
  そちらは独立 Epic として後置（本層が整ってから起票）。

## 24.5 構文（提案）

既存演算子族（`\|?` filter / `\|>` project / `\|#` group）に合わせ **`\|!`**（assert/validate）。
`validate …` キーワード別名も可（可読性）:

```
\|! <rule> [reject|warn|quarantine(<sink>)|halt]      # 既定 disposition = warn
```
```
\|! age in 0..120 reject
\|! email ~ "^[^@]+@[^@]+$" warn
\|! id required halt
\|! status in {active, closed} quarantine(bad.csv)
\|! ts monotonic               # 行間（直前値のみ＝O(1) 状態）
\|! id unique                  # 行間（有界 seen-set）
```

複数規則はカンマ（filter の AND と同じ流儀）か連続 `\|!` で連ねる。`to_source` 可逆。

## 24.6 IR / engine への落とし込み

- `rivus_ir::Op::Validate { rules: Vec<ValidationRule>, disposition }` を追加（`to_source`
  可逆、`08-optimization` の equiv ゲート対象）。
- `ValidationRule { target: Col(name)|Row|Window, kind, disposition }`、`kind ∈ {Type,
  Range, Regex, Required, Enum, Len, Monotonic, Unique, Sequential, Reference}`。
- operator boundary は薄いまま `process(from, chunk, ctx) -> Vec<Chunk>`。行内は per-chunk
  で stateless、行間は operator が**有界状態**を持つ（`sort`/`distinct` と同類、必要時
  pipeline-breaker か bounded）。
- 不適合は `ctx.raise(ErrorEvent { severity, scope: Item, message, node, chunk_id })`。
  disposition→severity は §24.2 の表。§13 continue-first / mode ladder に完全準拠
  （`halt` のみ `Fatal` で停止）。
- `reject`/`quarantine` の drop は selection-vector gather（`#40`）で行除去。

## 24.7 byte-identity / 並列（§invariants）

- **検証は決定的**で、追加は drop と telemetry のみ → 出力は規則が同じなら同一バイト。
- **行内・窓内（結合的検証）は分割安全**（並列パーティション間で同一規則→同一結果）。
- **行間は規則ごとに分ける（`#41` と同じ判断）**:
  - **加算的に出せるもの**: `count_distinct`（集合和＝結合的）等「集約値の検証」は
    seen-set のマージが結合的 → 有界なら並列安全（per-partition 集合の和を取るだけ）。
  - **加算的に*出せない*もの**: `unique` の **行単位の跨ぎ重複検出 / keep-first**
    （後続の重複行を reject）や `reference`（参照整合）、`monotonic`/`sequential` は
    **partition を跨ぐ source-order に依存**するため per-partition 結果の加算マージは
    *正しくない*。`#41` と同様に **source-order 決定的マージ（直列、または協調マージ）**
    が要る。phase-3 実装時はこの 2 系統を**明確に分けて**設計する（集約検証＝並列加算 /
    跨ぎ重複・整合＝順序マージ）。
- 件数・サンプルは観測（§14）であり結果不変。stress(chunk-size sweep) + optimizer_equiv +
  件数オラクルでゲート。

## 24.8 `#80`/`#81` の収斂（one-off → 一般化）

- **`#80`** = 行内 ingress validator の `disposition = warn`（parse 失敗を surface、既定で
  行保持）。**本層 phase 0（実装済み）**。
- **`#81`** `--on-parse-error warn|reject|halt` = parse validator の **disposition 選択**。
  本層の disposition policy にそのまま統一（`--on-parse-error` は `\|!` reject/halt の糖衣）。
- `#bugreport ①⑤`（null/空/0 の区別・`dropna` が defaulted blank を見られない）は
  **nullable-column モデル**（`06-type-system`）が前提で、`Required`(non-null) validator の
  土台。null モデルは別途設計（本層と並行、`#81`/`#82` で追跡）。

## 24.9 bounded / 依存ゼロ

- 行間状態（seen-set / 参照表 / 直前値）は **有界**前提。無界 `unique`/`reference` は
  `#50` unbounded opt-in 下のみ、または近似（明示）。窓状態は `#56` と共有し窓を閉じたら解放。
- regex は既存 `like`/`glob`（std・§`#71` 方針）で大半を賄い、本格 regex は feature-gate
  （既定ビルドはゼロ依存を維持）。

## 24.10 段階計画

| phase | 内容 | 状態 |
|---|---|---|
| 0 | parse 失敗 surface（行内 ingress・`warn`） | **済（`#80`）** |
| 1 | parse-error disposition（`warn`/`reject`/`halt`・`#81`） | 次 |
| 2 | 宣言的行内 validator（type/range/regex/required/enum）＋ `\|!` 構文・`Op::Validate` | |
| 3 | 行間 validator（有界状態）。**2 系統に分離**: 集約検証(`count_distinct`)=並列加算 / 跨ぎ重複・整合(`unique`/`reference`/`monotonic`)=source-order 決定的マージ（§24.7・`#41`） | |
| 4 | 窓内 validator（`#56` と一体）＋ egress(`quarantine`/dead-letter) | |

各 phase は byte-identity（stress / optimizer_equiv）と件数オラクルでゲートし、
`docs/BENCHMARKS.md` に観測オーバーヘッド（ゼロが目標）を記録する。
