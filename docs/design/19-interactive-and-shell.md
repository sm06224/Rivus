# 19. 対話ビューア・実行アナリティクス・シェル統合

> 本ドキュメントは **構想（design intent）** であり、まだ実装段階ではない。
> 「Observable First」を対話 UI まで延長したときに何を作るか、そして当面の
> 現実的な使い方（文字列でフローを直接渡す）を定義する。

## 19.1 当面の使い方 — 文字列でフローを渡す（実装済み）

シェル統合・補完が整うまでの間は、フローを **ファイルにせず文字列で直接渡す**
のが基本ワークフロー。CLI は 3 通りの入力経路を持つ（`rivus-cli`）:

```sh
# 1) ファイル
rivus run flow.riv

# 2) インライン文字列（-c / --command）
rivus run -c 'U: open users.csv |? age >= 20 |> name age save stdout as csv ;'

# 3) stdin（ヒアドキュメント）
rivus run - <<'RIV'
Adults:
    open users.csv
    |? age >= 20
    save stdout as jsonl
;
RIV
```

可視化は **stderr**、データ sink は **stdout** に分離してあるので、他シェルの
パイプにそのまま挟める（`rivus run -c '…' | jq .`）。`explain` / `check` でも
同じ `-c` / `-` 入力が使える。これは将来のシェル統合（補完つきで地続きに書く）
の前段であり、IR・実行モデルを変えずに到達できる「軽い入口」。

## 19.2 対話ビューア（PowerShell `Out-GridView` 相当）

`Out-GridView` は結果を対話グリッドに出して、ソート・フィルタ・選択を返す。
Rivus 版は **sink の一種** として設計する（フローの外付け UI ではなく、flow の
終端 operator）。

- 仮称構文: `... | view`（`Op::SinkView`）。TTY なら対話グリッド、非 TTY なら
  自動で `save stdout`（テキスト）にフォールバック → パイプ互換を壊さない。
- **重くしない**のが最優先（ユーザ要件）。全件マテリアライズ禁止の原則を守り、
  - 既定は **streaming・ウィンドウ表示**（先頭 N chunk を表示、スクロールで
    追加要求＝backpressure を UI イベントとして engine に返す）、
  - ソート/フィルタは可能なら **IR に押し戻して** 上流で実行（`|? ` 相当を
    動的に挿入）。UI 側で全件保持はしない。
- 実装は **別 crate（`rivus-tui`, dev/optional）** に隔離し、コア runtime の
  ゼロ依存を汚さない。TUI バックエンドを採用する場合も供給網チェック
  （`docs/SUPPLY-CHAIN.md`）を通す。

## 19.3 実行アナリティクスの GUI 表示

現状は実行後に ASCII で execution graph・error stream・throughput を出す。
これを **ライブ GUI** に拡張する構想:

- telemetry は既に engine 側で測っている（operator 境界の in/out・mode 遷移・
  error 件数）。GUI は **telemetry の購読者**にすぎず、operator には手を入れない
  （Observable First / 計測はエンジンで行う原則を維持）。
- 出力経路を 3 段に分ける:
  1. **ASCII**（現状・常時・ゼロ依存）
  2. **構造化ストリーム**（JSON Lines の telemetry を stderr/socket に流す）—
     外部ツール・エディタ拡張がそのまま購読できる。まずはここを実装する。
  3. **リッチ GUI**（Web/TUI フロントは 2 の購読者として別 crate）。
- 「Observable First」をそのまま延長したもの。GUI はあくまで *view* であり、
  実行のソース・オブ・トゥルースは IR と telemetry のまま。

## 19.4 シェル統合・補完（最終ゴール）

最も欲しいのは「シェルと統合してコマンド補完しつつ地続きに書ける」こと。

- **補完の源泉は IR/スキーマ**: パース途中の部分 IR から、フィールド名・operator・
  フォーマット・既知 source を補完候補として出す（`rivus complete <partial>` を
  まず CLI に用意し、各シェルの completion はその薄いラッパにする）。
- 対象シェル: PowerShell / nushell / bash / zsh / fish。各シェル固有の補完 API へ
  橋渡しする生成器を持つ（`rivus completions <shell>`）。
- nushell は構造化データが第一級なので、**Rivus の chunk ⇄ nushell value** の
  相互変換を優先実験対象にする。
- ここに至るまでは §19.1 の「文字列渡し」を正式な使い方として維持する。

## 19.5 段階表

| | 内容 |
|---|---|
| MVP（実装済み） | `-c` / stdin（`-`）でのフロー文字列入力・stderr 可視化 / stdout データ分離 |
| 次 | 構造化 telemetry ストリーム出力（JSONL）・`rivus completions <shell>` 雛形 |
| 将来 | `\| view` 対話グリッド sink（streaming・push-back）・ライブ GUI・各シェル深い統合（nushell value 連携） |
