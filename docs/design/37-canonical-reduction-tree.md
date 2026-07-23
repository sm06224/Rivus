# 37. 正準縮約木 — f64 並列 sum/avg/std の byte-identity 化（#45 / #41 の残り半分・批准依頼）

> **状態：Q1 許容済（#240 キュー1）→ 方式 (b) 裁定（issuecomment-5039548220）→
> 実装済み（file-major 正準・serial mirror = P=1 同一機械・条件 5 点対応、
> BENCHMARKS「#45 canonical reduction trees」節）。単一ファイル byte-range
> 経路（§37.4-37.5 のプリパス＋carry）は対象外のまま＝将来スライス。**
> #41（f64 非結合ゆえ並列 partition→merge の sum/avg/std は ULP ズレ）に対する現行裁定は
> 「f64 は serial 据え置き・exactness は decimal レーン迂回」（BENCHMARKS「f64 parallel
> aggregation」節）。本メモは**新しい精度計測が判断材料を変えうる**ことを示し、正準縮約木を
> 採用するか否かの批准を仰ぐ。**値を変える縮約を批准なしにエンジンへ配線しない**（CLAUDE.md
> byte-identity 節「Never silently relax byte-identity for f64」を厳守）。

## 37.0 結論（先出し）

正準固定ブロック縮約木は、**並列でも serial と bit 一致する f64 sum/avg/std** を可能にする。
現行裁定（deferred）を維持する道もあるが、**先行研究で新たに測った 2 つの事実**が採用側に
傾ける材料になる：

1. **partition 独立は達成できる**（実測：P=2/4/8 で bit 一致）。素朴 partition→merge が
   partition 数依存でズレる（同・実測）のとは対照的。
2. **正準ブロック和は素朴左 fold より数値的に正確**（実測：n=1M・40 シードで **39/40 が
   高精度・平均 70.5× 高精度**）。ブロック/pairwise 和の誤差は O(log n)、素朴左 fold は
   O(n)（教科書的事実の実測確認）。

→ **批准の問いは「~1 ULP のバージョン間シフトを一度受け入れれば、並列 byte-identity ＋
恒常的な精度向上が同時に得られる。受け入れるか？」** に収束する。値の変化は**一度きり**
（採用バージョンの境界）で、以後は正準木が新しい安定基準になる。

## 37.1 なぜ f64 が問題か（#41 の再掲）

f64 加算は非結合：`(a+b)+c ≠ a+(b+c)`（丸めのため）。並列は入力を partition に割って各々を
畳んでから merge するので、**partition の切り方が積算順を変え、値が動く**。現行の
`group_parallel_safe`（`aggregate.rs`）は f64 の `sum`/`avg` を弾き（Decimal/Duration レーン
のみ許可）、`std` は無条件で弾く。結果、f64 集計を含むフローは**並列経路に乗らず直列
フォールバック**する（`engine.rs` の `try_parallel_group` が `None` を返す）。

現状の serial が chunk-size 不変なのは「賢いから」ではなく「**単一の source 順しか存在
しないから常に同じ**」。並列を入れた瞬間にこの前提が崩れる。

## 37.2 正準固定ブロック縮約木

**定義**：値列を絶対行インデックスに固定した幅 `BLOCK`（プロトタイプでは 128）のブロックに
分け、各ブロックを畳み、ブロック和の列を**再帰的に同じ規則で**畳む。これは `(values, BLOCK)`
の**純関数**であり、**どう partition しても同じ木**になる。

**並列で serial-canonical と bit 一致させる鍵**（プロトタイプで発見・実証）：各ワーカは自分の
BLOCK 整列レンジの**「ブロック和のベクトル」を返す**（単一スカラではない）。全ワーカのブロック
和をグローバル順に連結し、その完全なベクトルを畳む＝`canonical(全体)` の level-0 と厳密に
同一。単一スカラに畳んでから merge すると**別の木**になり一致しない（プロトタイプで最初に
踏んだ罠）。

```
canonical(v):
  if |v| <= BLOCK: return naive_fold(v)
  block_sums = [ naive_fold(v[i:i+BLOCK]) for i in 0,BLOCK,2·BLOCK,… ]
  return canonical(block_sums)          # 再帰的に正準

parallel_canonical(v, P):
  各ワーカ w: block_sums_of(その BLOCK 整列レンジ)  # スカラでなくベクトルを返す
  all = ワーカ順に連結
  return canonical(all)                  # canonical(v) と bit 一致
```

## 37.3 計測（独立プロトタイプ・出荷コード非配線）

`f64_wide`（仮数 [−0.5,0.5)×10^(−4..7)＝ULP に厳しい広ダイナミックレンジ）で測定：

| 項目 | n=100k | n=1M | n=10M |
|---|---|---|---|
| 正準 vs 素朴 serial 相対差 | 1.6e-14 | 8.9e-14 | 4.8e-16 |
| **P={2,4,8} 並列正準が serial-canonical と bit 一致** | ✅ | ✅ | ✅ |
| 素朴 partition→merge の P=2 vs P=4 相対差（壊れている） | 9.8e-15 | 5.8e-15 | 1.3e-13 |
| 並列正準 P=8 の速度（対 serial-fold） | 0.24× | 1.5× | **3.0×** |

**精度（Kahan-Babuška-Neumaier 補償和＝真値基準）**：

| | n=100k | n=1M |
|---|---|---|
| 素朴 serial fold 誤差 | 1.4e-14 | 8.7e-14 |
| 正準ブロック和 誤差 | 2.2e-15 | 1.5e-15 |
| **正準が高精度** | 6.5× | 60× |

**40 シード集計（n=1M）**：正準が厳密に高精度 **39/40**・素朴が高精度 1/40・平均
`naive_err/canonical_err` = **70.5×**。

**要点**：(a) 正準は素朴 serial と ~1 ULP 異なる（採用コスト）が、(b) その差の向きは**ほぼ常に
真値に近い側**。速度は小 n では糸のオーバーヘッド負けだが、大 n（1M 超）で 1.5–3× 得。

## 37.4 有界メモリの要件（設計の肝）

グループ集計で**有界かつ並列**に正準木を回すには「global-row coordination」が要る（BENCHMARKS
既述）：
- **行数プリパス**：各 byte-range ワーカに自分のグローバル開始行を与える（BLOCK 整列の
  基準）。CSV は行数を数える軽い一次走査、Parquet は row-group メタで既知。
- **境界 carry**：ワーカ境界を跨ぐ端数ブロック（≤BLOCK 要素）を merge で再結合。
これを怠ると「グループ毎の全行バッファ」に退化＝有界メモリ不変条件を破る。だから**素朴に
`group_parallel_safe` を開けるだけでは不十分**で、`AggAcc` を「部分ブロック和ツリー状態」に
持ち替え、`merge` に境界位置を渡すシグネチャ拡張が要る（実装は別 PR）。

## 37.5 実装スケッチ（批准後・別 PR）

1. `AggAcc`（`aggregate.rs`）の f64 `sum`/`sum_sq` を**ブロック和ツリー状態**へ（std も同じ木＝
   Σx と Σx² 両方）。std は現行の素朴二モーメント（`(Σx² − Σx·mean)/(n−1)`）を**ブロック和で
   計算**する形が最小（Welford/Chan 版に替えると serial std も別の値になり、変化幅が大きく
   なる — 二モーメント維持を推奨）。
2. `AggAcc::merge` に global-row 位置＋境界 carry を渡すシグネチャ拡張。
3. `group_parallel_safe` の f64 Sum/Avg/Std を（正準木があるので）許可。
4. `engine.rs` の並列グループ経路にワーカ開始行の配布を追加。
5. **stress**：正準の serial==parallel==chunk-size を bit で pin、素朴 partition が壊れる
   ことを対照 pin（`byte_identity.rs` の既存 `canonical()` テスト資産を拡張）。

## 37.6 批准を仰ぐ問い（統括の専権）

**Q1. f64 sum/avg/std の値が採用バージョンで一度 ~1 ULP シフトすることを許容するか？**
- 許容する → 37.5 を別 PR で実装（並列 byte-identity ＋ 精度向上を獲得）。
- 許容しない → 現行裁定（serial 据え置き・decimal 迂回）を維持。本メモは「将来 decimal を
  使えない実 f64 並列集計ワークロードが現れた時の設計」として棚上げ。

**Q2. 採用する場合、`BLOCK` 幅の確定**（プロトタイプ 128。速度/精度/carry コストのトレード
オフ — 別 PR で掃引して数値決定）。

**推奨（研究員私見）**：Rivus は「Byte-identical across execution strategies」を旗印に掲げる
プロジェクトなので、**f64 だけ「並列にできない例外」であり続けるのは北極星に対する負債**。
精度も上がる以上、**Q1=許容し、次の実装スライスで正準木を入れる**のが筋と考える。ただし
値シフトはユーザ可視の契約変更なので、**タグ境界（例 v1.4.0-dev.N）で CHANGELOG に明記**し、
`--exact`/decimal を使っていたユーザには無影響（そちらは i128 で不変）である点を添える。

> 批准は統括の専権。本メモ＋プロトタイプは研究成果＝設計提案であり、自己マージしない。
> プロトタイプの計測コードは出荷ツリーに入れない（scratch のみ）。BENCHMARKS.md「f64
> parallel aggregation」節に本メモへの相互参照を追加した。
