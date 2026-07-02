# 35. 分散トランスポートの dep-zero 戦略 — QUIC 要否と B2 進退

> **状態：批准済（2026-07-01・#211 GO）。** §35.5 の §33/§34 追補は反映済（§33.4・§34.1/34.4
> 追補・SUPPLY-CHAIN.md）。B2 ＝ won't-do（委譲）で close、`quic` は opt-in 温存・非出荷。
> 統括の研究ブリーフ（2026-06-24・#210）への回答。**出荷物 dep-zero は不変条件**、暗号は内蔵せず委譲（§28.12.5-2）、
> roll-your-own-crypto は禁止。本メモは B2（`quic` を `full` 搭載＝`cargo deny
> --all-features` 通過）の進退を立証するための先行研究。コードは追加しない（docs-only）。

## 35.0 結論（先出し）

| 問い | 結論 |
|---|---|
| (1) §33 分散実行に QUIC の差別化機能は要るか | **要らない。** multiplexed streams / connection migration / 0-RTT / 内蔵 TLS / user-space CC のいずれも `interpret==distribute`（WireGuard 上）には不要で、代替可能。 |
| (2) 暗号非内蔵で安全境界をどう確保するか | **kernel WireGuard（主）＋ sidecar 終端（§34.4 の一般化）** で全要件を dep-zero のまま充足。kTLS はハンドシェイクに TLS スタックが要り dep-zero にならない。OS-QUIC は未成熟/非可搬。 |
| (3) pure-Rust QUIC の転換点 | rustls + RustCrypto provider が**監査済み・quinn 公式対応・SPDX クリーン**になった時。ただし**それでも依存数十 crate ＝ dep-zero にはならず feature-gated のまま**。 |
| **B2 進退勧告** | **委譲（delegate）＝ `full` 搭載は行わない。** `quic` は **off-by-default の opt-in feature のまま温存**（#173 spike）。#176 Part 2「promote quic into full」は **won't-do で close 可**。pure-Rust 化は監視項目（コミットではない）。 |

---

## 35.1 問い (1)：分散実行に QUIC の機能は実際に要るか

§33 の分散実行は **`interpret==distribute`**：コーディネータが **IR（小）を出荷物**として送り、
ワーカが同じエンジンで実行し、結果を **credit 背圧**で stream back する。多ジョブは 1 接続を
再利用（§34.4 s2'）。この負荷特性に対し、QUIC の各機能を評価する：

| QUIC 機能 | §33 分散実行での要否 | 代替（dep-zero） |
|---|---|---|
| **多重ストリーム**（ストリーム間 HoL なし） | △ 単一ジョブでは Control/Data/Telemetry は**論理分離で十分**（Telemetry は極小・HoL 無視可）。**並行ジョブ**を 1 接続で多重化したい場合のみ価値。 | §34.1 の**フレーム・チャネルタグ**で論理分離済。並行が要れば **TCP を複数接続**（または逐次ジョブ）。 |
| **接続マイグレーション**（IP/port 変化に耐える） | ✗ ワーカは DC/エッジの**安定アドレス**（WireGuard interface IP）。モバイル・ローミングは対象外。 | 不要。 |
| **0-RTT 再開**（ハンドシェイク遅延削減） | ✗ #173 実測：分散コストは**フロー実行が支配**（200k 行 689ms・transport <1%）。1-RTT 削減は誤差。多ジョブはセッション再利用で connect 償却済。 | 不要（§34.4 s2' で償却）。 |
| **内蔵 TLS 1.3**（機密性・認証） | ✗ WireGuard に**委譲**（§28.12.5-2）。WireGuard 上で QUIC-TLS を重ねると**二重暗号＝二重 CPU**で §34.0（暗号 SIMD vs Rivus SIMD 競合）に逆行。 | WireGuard が担う（dep-zero）。 |
| **user-space 輻輳制御** | ✗ DC/WireGuard トンネル上の kernel TCP CC で十分。QUIC の user-space CC はロッシーなモバイル網向け。 | kernel TCP（dep-zero）。 |

**立証**：WireGuard 上の `interpret==distribute` では、QUIC の差別化機能は**どれも必須でない**。
チャネル分離は §34.1 のタグで論理的に達成、並行が要れば複数 TCP、暗号は WireGuard 委譲、
遅延はフロー実行支配ゆえ無関係。→ **「ratified §33 スコープに QUIC は不要」は成立。**

**残る唯一の価値**：QUIC（または TLS）が意味を持つのは「**カーネル WireGuard トンネルを前提
できない**」場合——未信頼網を跨ぐ ad-hoc 接続、WireGuard を構成できない制限ホスト、WASM 等。
これは問い (2) の sidecar で **Rivus に暗号を内蔵せず** 解ける。

**監視トリガ**：§17.3 の stage 分割 shuffle が**接続数をスケール**させる段（ワーカ数×中間
成果物ストリームで多重化の価値が変わる）に入ったら、本表の「多重ストリーム」要否を再評価する。

---

## 35.2 問い (2)：暗号非内蔵の安全トランスポート境界の比較

| 候補 | dep-zero | 移植性（musl/クロス/WASM） | 運用前提 | 評価 |
|---|---|---|---|---|
| **kernel WireGuard（ratified 主）** | ✅ Rivus は暗号コード/依存ゼロ（wg interface への bind ＋ peer allowlist のみ） | ✅ Rivus 側は std socket のみ。Linux 5.6+ 内蔵、mac/Win は wireguard-go（別プロセス） | wg を**帯域外で構成**（鍵・peers・interface） | **最良の dep-zero。** 運用コスト＝wg プロビジョニング。 |
| **kTLS（kernel TLS）** | ✗ ハンドシェイクは**ユーザ空間 TLS スタック必須**（rustls/openssl ＝依存）。kTLS は対称暗号の bulk offload のみ。 | Linux 限定・要 kernel 設定 | TLS 証明書運用 | **dep-zero にならない**（ハンドシェイク crypto が残る）。却下。 |
| **OS 提供 QUIC** | △ OS lib への FFI（vendored crypto crate ではないが std-only でもない） | ✗ 非常に OS 依存。Linux は in-kernel QUIC 未成熟（io_uring 連携が開発中）、Win は msquic | OS バージョン依存 | **現状 非可搬・未成熟。** 将来オプション。 |
| **sidecar 終端**（§34.4 一般化） | ✅ Rivus は平文で localhost/UDS に話し、**sidecar が安全境界を終端**（wg/TLS/QUIC/mTLS/service mesh いずれも可） | ✅ Rivus 側は std-only ＝どこでも | sidecar の配備/管理 | **最も柔軟。** 任意の安全トランスポートを Rivus 無改造・dep-zero のまま使える。 |

**核心の示唆**：§34.4 の**ホスト共有 Transport Service ＝ sidecar パターン**が、トランスポート
境界を**プラガブル**にしつつ Rivus を dep-zero に保つ。つまり「将来 QUIC が要る」場合の答えは
**「quinn+ring を vendor する」ではなく「QUIC 終端 sidecar を立てる」**。これは §0.1「transport を
差し替える」の具現であり、#173 で既にプレ実装された forwarding gateway（`forwarding_handler`）の
延長線上にある。

→ **(2) の dep-zero 保存解＝ WireGuard（主）＋ sidecar（非 WireGuard 時）。** kTLS は不採用、
OS-QUIC は将来監視。

---

## 35.3 問い (3)：pure-Rust TLS/QUIC の成熟度と転換点指標

B2 のブロッカーは **ring 0.17**（quinn が使う rustls の crypto provider）：**非 pure-Rust（C/asm）
＋非 SPDX ライセンス（OpenSSL 派生・帰属義務）**。pure-Rust 経路は **rustls + RustCrypto provider**：

- rustls 0.23+ は **crypto provider がプラガブル**（upstream 既定は `aws-lc-rs`〔C/asm〕・
  **本ツリーは quinn 経由で `ring`**・他に `rustls-rustcrypto`〔pure-Rust〕——どの既定でも
  C/asm 依存である点は同じで**結論不変**）。pure-Rust QUIC ＝ quinn + rustls + RustCrypto provider。
- **現状の未成熟点**：(a) RustCrypto は asm 無しで **ring/aws-lc より遅い**（§34.0 の暗号 CPU 競合が
  悪化＝Rivus には一般 Web より痛い）、(b) 全 AEAD/hash が FIPS/形式監査済みではない、
  (c) `rustls-rustcrypto` はコミュニティ provider で quinn 公式一級対応ではない。

**転換点の指標（B2 を pure-Rust 化してよい条件）**：
1. `rustls-rustcrypto`（等価 provider）が**監査済み・保守継続・quinn 公式サポート**に到達。
2. RustCrypto の AEAD/hash が**監査完了＋定数時間保証**を文書化。
3. ライセンスが全て MIT/Apache（RustCrypto は既に充足＝ring 問題が消える）。
4. **性能**が許容係数内（Rivus は暗号 CPU がデータ面 SIMD と競合＝一般 Web より性能要件が厳しい）。
5. `cargo deny --all-features` が pure-Rust ツリーで緑（advisory 無し・SPDX クリーン）。

**重要な但し書き**：pure-Rust QUIC でも**依存は数十 crate 増える**ので、**dep-zero の既定ビルドには
永遠に入らない**＝`quic` feature のまま。pure-Rust 化の利得は (a) **ring ライセンス障壁の除去**
（`quic` を CI deny-clean／`full` 搭載可能に）と (b) **可搬性向上**（C/asm 無し＝musl/クロス/WASM
摩擦減）だけ。**QUIC を dep-zero にはしない。**

**監視**：rustls/quinn のリリースノートが安定 RustCrypto provider 対応を告知した時、または年次で再評価。

---

## 35.4 B2 進退勧告

(1)（QUIC 機能は ratified スコープに不要）＋(2)（非 WireGuard も sidecar で dep-zero）＋
(3)（pure-Rust QUIC は未成熟、成熟しても feature-gated）より：

- **B2（quinn+ring を `full` 搭載）は進めない。** Rivus が必要としない機能のために、非 SPDX・
  非 pure-Rust の暗号依存を既定/配布に入れることは **dep-zero 不変条件と §34.0 に反する。**
- **`quic` feature は現状維持＝ off-by-default の opt-in のみ**（#173 spike）。「WireGuard も
  sidecar も使えず、自前で暗号化トランスポートが欲しく、ring 依存を受容する」ユーザ向けに
  **残すが、`full` 非搭載・CI deny 必須ではない**。SUPPLY-CHAIN.md に「opt-in・非出荷」と明記。
- **#176 Part 2「promote `quic` into `full` after cargo deny」は won't-do で close 可。**
  理由＝「`full` 搭載は dep-zero を壊す／QUIC は §33 に不要」。`quic` 機能自体は残る。
- **将来 pure-Rust 化**（§35.3 の転換点）は**監視項目**であってコミットではない。成熟＋具体的な
  「WireGuard/sidecar で解けない QUIC 必須ユースケース」が同時に揃った時のみ再評価。それでも
  feature-gated のまま。

**一行勧告**：**QUIC は dep-zero 経路では「委譲」（WireGuard＋sidecar）。`quic` feature は opt-in で温存、
`full` には入れない。pure-Rust 化は監視するが約束しない。** → B2 は close（won't-do）。

---

## 35.5 §33/§34 追補候補（批准対象）

本メモが批准されれば、§33/§34 に以下を追補することを提案する：

1. **§33 追補**：保護チャネルの dep-zero 主経路は **kernel WireGuard**。非 WireGuard 環境の
   安全境界は **§34.4 sidecar/Transport Service 終端**（Rivus は平文 UDS、暗号は sidecar）。
   **QUIC バックエンドは opt-in feature であり配布（`full`）には含めない**ことを明文化。
2. **§34.4 追補**：sidecar Transport Service を「**トランスポート境界をプラガブルにし Rivus を
   dep-zero に保つ正準機構**」として位置づけ（QUIC/TLS/mTLS/mesh も sidecar で終端）。
3. **§34（QUIC 関連記述）追補**：§34.1 の「QUIC backend では本物のストリームに 1:1 で載る」等の
   記述に「**QUIC は opt-in・非出荷**」の前提を明記し、over-claim を回避。

> 批准は統括の専権。本メモは研究成果＝設計提案であり、自己マージはしない。
