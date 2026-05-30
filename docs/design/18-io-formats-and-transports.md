# 18. I/O 形式とトランスポート（ロードマップ）

> これは「目指す方向」を記録するロードマップ。すべてを今すぐ作るわけではなく、
> 1つずつ bench/test 付きで段階導入する。各項目は `Operator` 境界（source/sink）
> の裏に入り、中核（IR/chunk/engine）を変えずに足せることを原則とする。

## 18.1 基本方針：source と sink は対称

「ソースと同じ形式で吐けると尚良し」を設計原則にする。各フォーマットは
**reader（source）と writer（sink）を対で**実装し、`open <f>.fmt` / `save <f>.fmt`
が同じフォーマット集合を共有する。フォーマットは拡張子で自動判定（明示指定も可）。

```
open data.jsonl  | ... | save out.parquet     # 形式変換がそのままパイプ
```

## 18.2 フォーマット・マトリクス

| 形式 | 種別 | source | sink | 状態 |
|---|---|:--:|:--:|---|
| CSV / xSV(TSV等) | テキスト表 | ✅ | ✅(csv) | 実装済 |
| バイナリ固定長 (C struct dump) | バイナリ | ✅ `readbin` | ○ | source実装済 / sink予定 |
| JSONL / NDJSON | 構造化(行) | ✅ | ○ | **本PRでsource** |
| JSON (配列/単一) | 構造化 | ○ | ○ | 予定 |
| YAML / TOML / INI | 設定系 | ○ | ○ | 予定 |
| XML / HTML | 木構造 | ○ | ○ | 予定（HTMLはtable抽出） |
| Parquet / Arrow IPC | 列指向バイナリ | ○ | ○ | 予定（Arrow導入時, doc 03/12） |
| Protocol Buffers / gRPC | スキーマ付 | ○ | ○ | 予定（.proto→schema, doc 12 plugin） |
| query result（DB） | 行 | ○ | ○ | 予定（driver plugin, doc 12） |

凡例: ✅=実装済 / ○=ロードマップ。

ネスト構造（JSON/XML/proto）は **chunk が入れ子列（nested column）を持てる**よう
03 の Chunk モデルを拡張してから本格対応する（MVP は flat + ネストは raw 文字列に
退避＝continue-first）。

## 18.3 スキーマ

- **推論**（CSV/JSONL）: 値から lane を推論（既存）。
- **宣言**: `readbin (name:type ...)` のように明示。将来 `open f.csv as (age:i64 ...)`。
- **スキーマ取り込み**: `.proto` / Avro / JSON Schema → Rivus schema へ変換（plugin, doc 12）。
- スキーマは `rivus_core::Schema`（structural）に正規化し、形式差を吸収する。

## 18.4 トランスポート（受け取り方・送り方）

ファイルだけでなく**ストリーム的な入出力**を source/sink の下に足す：

| トランスポート | 例 | 備考 |
|---|---|---|
| stdin / stdout | `open -` / `save -` | シェル連携の基本 |
| ファイル | `open f.csv` | 実装済 |
| socket / TCP | `open tcp://host:port` | 行/フレーム境界 |
| HTTP(S) GET/POST | `open https://…` | pull / push |
| メッセージング | Kafka / NATS / MQTT | subscribe（下記） |

トランスポート層は「bytes ストリームを供給/消費する」抽象にし、その上に
フォーマット reader/writer を載せる（直交設計：transport × format）。

## 18.5 トリガ：subscribe / scheduled-get

source は「一度読む」だけでなく**継続供給**できる：

```
Live:
    subscribe kafka://topic        # 新着イベントを継続的に chunk 化
;

Poll:
    every 30s open https://api/...  # scheduled-get（定期取得）
;
```

- **subscribe**: 外部イベント源を購読し、到着ごとに chunk を emit（無限ストリーム）。
  backpressure（doc 05 の credit）と組み合わせる。
- **scheduled-get**: 定期的に source を再実行（`every <dur>`）。`stream`/replay と接続。
- これらは「continue-first / chunk-native」と自然に合う（停止せず流し続ける）。

## 18.6 シェル連携（PowerShell / nushell / sh / cmd）

「他シェルと連携できたら面白い」を実現する方向：

- **stdin/stdout bridge**: Rivus を他シェルのパイプに挟む（`... | rivus run - | ...`）。
  既定は CSV/JSONL でやり取り、`--in/--out` で形式指定。
- **PowerShell object pipeline**: PS の CSV/JSON 変換（`ConvertTo-Json`）経由で
  オブジェクトを授受。将来は構造化ホスト連携。
- **nushell**: nu の構造化値と JSONL でやり取り（nu は JSON/NDJSON が得意）。
- **埋め込み**: 各シェルから `rivus` を呼び、結果を構造化テキストで返す。
- 逆に Rivus から外部コマンドを source/sink にする `run "cmd"`（プロセスの
  stdout を取り込む / stdin へ流す）も検討。

## 18.7 段階導入順（案）

```
1. JSONL source（本PR）         … 構造化行の最初の一歩
2. CSV/JSONL sink の対称化       … save f.jsonl
3. stdin/stdout transport        … シェル連携の土台（open - / save -）
4. JSON(配列)/YAML/TOML          … 設定・APIレスポンス
5. Arrow IPC / Parquet           … doc 03 の Arrow 化と同時
6. proto/gRPC・DB driver         … plugin ABI（doc 12）の上に
7. subscribe / scheduled-get     … 継続供給 + backpressure
8. XML/HTML                      … 木構造（nested column 後）
```

各段階で「巨大／エラー多発／混在」をベンチし、`docs/BENCHMARKS.md` に記録する。
