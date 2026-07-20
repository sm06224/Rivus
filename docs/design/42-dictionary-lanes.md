# 42 — Dictionary-encoded Str lanes（提案・批准待ち）

Status: **批准済み（2026-07-19、#180 記録 → 現運用は #240）・第 (a) 段実装中**。
批准条件 4 点＝①§2 不変条件の property test（dict vs plain の value()/gather/
write_cell バイト一致）②高カード escape hatch 必須 ③(a)→(b)→(c) の段階ごとに
既存ガード＋計測 ④発動可観測性を最初から（WPROF/strategy）。

## 1. なぜ今これか（実測の帰結）

#239 の研究サイクルで、fused worker の行毎コストは次の床に到達した
（BENCHMARKS の各節・負の結果 4 件が根拠）:

- decode ~72-108ms/file（S1-S3・mmap・構造マーク走査すべて計測で out）
- reconcile 0ms（narrow-keep）
- 述語 ~0（kernel マスク）
- feed ~75-113ms/file — 内訳は join キー構築・group キー probe・`Value` 往復・
  `AggAcc` 更新の行毎仕事。**キー表現の改良（prefix 事前計算=+5%、固定幅
  パック=wash）では probe 側はもう動かない**ことを実測で確認済み。

残る 2 倍は「行毎に文字列とハッシュを扱う」構造そのものの除去 — つまり
**Str セルを decode 時に辞書 id 化**し、join/group/filter が整数 id で走る
形にしかない。DuckDB/Polars が dictionary/categorical で得ている当のもの。

## 2. 提案の核

`ColumnData::StrDict { dict: Arc<StrColumn>, codes: Vec<u32> }` を追加する
（名称仮）。不変条件:

1. **観測等価**: `value(row)` は今日の `Str` と同じ `Value::Str` を返す。
   辞書化は表現であって型ではない — スキーマ上の dtype は `Str` のまま、
   IR・構文・explain に一切現れない（Execution-aware typing の範疇）。
2. **byte-identity**: 出力・キー符号化・write_cell は id→bytes 参照で従来と
   同一バイト。serial==parallel==chunk-size の全ガードが従来どおり通ること。
3. **選択式**: 生成するのは reader の判断（カーディナリティ検出）。低カード
   列のみ辞書化し、高カード列は今日の `Str` のまま。全 operator は `Str` と
   `StrDict` の両方を受ける（`StrDict` 未対応の operator は `value()` 経由で
   自動的に正しく動く — 遅くなるだけ）。

## 3. 期待できる勝ち（見積り・要実測）

- **group キー**: 複合キー = (右行 id, 左セル id) の整数組 → 小配列直引き
  （probe ~25ns → ~3ns/row）。feed の ~40-50%。
- **join probe**: 右表構築時に左辞書 id → 右行を事前解決（辞書サイズ回の
  ハッシュで、行数回のハッシュを置換）。
- **decode**: 低カード列は「バイト列→辞書照合」1 回/セル（既に払っている
  StrColumn push が id push になり、メモリも縮む）。
- 10M 標準での期待値: feed 半減 ≈ wall −80-120ms 級（CSV group ~460→~360ms
  圏、対 DuckDB 0.55×圏）。ETL は writer 側が Str 実体化するため効果小。

## 4. コストとリスク

- Chunk 表現の追加 = 全 operator の網羅確認（`value()` フォールバックで
  正しさは自動、性能パスだけ個別）。gather/filter/概算 20 箇所。
- 辞書の worker 間共有はしない（worker=file 単位で独立辞書 — merge は
  bytes 経由の従来路で、辞書はチャンク生存期間のみ）。
- カーディナリティ検出の閾値（例: distinct ≤ 4096 で辞書化）は sample 開と
  同じ場所で無料で判定できる（Stage C の sample がそのまま使える）。
- 失敗モード: 高カード列を誤って辞書化 → 辞書照合がハッシュ probe に退化
  （今日と同コスト、リグレッションなし — 閾値超過で辞書化を打ち切り
  `Str` へフォールバックする escape hatch を必須とする）。

## 5. 批准を求める点

1. `ColumnData` への variant 追加の可否（§2 の不変条件つき）。
2. 実装順: (a) ColumnData variant＋value()/gather/write_cell 対応、
   (b) CSV/JSONL reader の低カード検出＋辞書化（sample 開に同居）、
   (c) fused loop の id 直引き group/join、各段で従来ガード＋計測。
3. 名称・閾値のデフォルト（distinct ≤ 4096 / セル長制限なし）。
