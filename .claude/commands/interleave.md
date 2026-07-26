# /interleave <ref-A> <ref-B> <flow.riv> [rounds] — same-window A/B 実測

性能主張の裏取り。**絶対値の日跨ぎ比較は無意味**（箱ノイズ日内 ±40% 実測）なので、
必ず同窓 interleave で相対比較する。rounds 既定 6。

## 手順

1. 両 ref を release build して**別名保存**（`cargo build --release -p rivus-cli`
   → `cp target/release/rivus <scratch>/rivus_A` / `rivus_B`）。checkout を往復して
   ビルドし直すのではなく、バイナリを先に両方確保する。
2. fixture は **seed 固定**で生成（`rivus gen` / `rivus_runtime::gendata` / seed 付き
   スクリプト）。f64 の fold 順が絡む検証は**小数値**で作る——整数値 float（<2^53）は
   どの fold でも exact になり、差が見えない。汚れ標準を模すなら malformed 行を
   sample 窓の**外**に注入する。
3. **速度の前に正しさ**。時間を測る前に出力の byte-identity を確定する:
   - A vs B（`cmp`）— 意味論不変の最適化なら一致が前提
   - B の parallel vs 強制直列（`--memory low` / `RIVUS_NO_PARALLEL=1`）
   - 発動確認（`RIVUS_WORKER_PROF=1` の `[WPROF]` 行・strategy 文字列）——
     **発動していないベンチは何も測っていない**（無音 fallback で identity テストが
     空洞化しかけた実例が R5）。
4. **A/B の env・閾値・フラグは必ず対称**にする。片側だけ `RIVUS_PARALLEL_MIN_BYTES=0`
   のように閾値をまたがせると、比べているのは A と B ではなく**別々の実行経路**で、
   f64 集計なら fold 順の違いが ULP 差として出る（検証キャンペーン 2026-07-26 で実際に
   「identity 破れ」と誤検出しかけた）。差が出たら、まず両側の env を揃えて再実行する。
5. A→B を rounds 往復（1 round 内で A, B の順に連続実行＝同窓）:
   ```sh
   for i in $(seq $ROUNDS); do
     A=$( { /bin/bash -c "TIMEFORMAT=%R; time $BIN_A run $FLOW --memory unbounded >/dev/null 2>&1"; } 2>&1 )
     B=$( { /bin/bash -c "TIMEFORMAT=%R; time $BIN_B run $FLOW --memory unbounded >/dev/null 2>&1"; } 2>&1 )
     echo "round$i A=${A}s B=${B}s"
   done
   ```

## 報告規律

- **ペア勝敗**（x/rounds）と median を主指標に。**warmup の初回 round は除外を明記**
  して除外してよい（page cache の非対称）。
- 自分のスケール（例 2.7M 行）と申告スケール（例 10M 行）が違うときは、
  「方向・規模の整合」を主張し、絶対比は主張しない。
- 再現できない数値は「未再計測」と正直に書く（例: DuckDB 比はローカルに DuckDB が
  無ければ台帳の方法論確認まで）。測っていないものを測ったとは書かない。
