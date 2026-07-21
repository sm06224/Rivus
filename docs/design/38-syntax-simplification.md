# Design 38 — Syntax simplification (restoring "one obvious way")

**Status:** P1+P2 GO 済（#236 issuecomment-5017506345）・**P1+P2 移行リリース実装済み**
（削除綴りは本リリースは parse 可＋`fmt` が正典へ自動書換、次リリースで
did-you-mean エラー化。`readbin` は `open … as bin` 文法の裁可待ちでエラー化
リリースへ繰延べ）。P3（`over` 窓統一）は PR #245 で提出済み。**P4（`&asof`）実装済み**（同じ
移行方式 — 正典は `A &asof B [on k…] by ts [within]`、旧 `& … asof ts` 形は
本リリース parse 可＋fmt 正典化・次リリースでエラー化）。P5 は使用調査待ちで保留。
**Author:** 先行研究担当. **Decision:** レビュー兼統括指揮 (ratify per-item; 現運用 #240).

> 統括の指摘：「構文が難しくなってきている。シンプルさが損なわれている。」
> 正当な批判です。窓・時系列スライス（sessionize / shift / asof / date_bin …）を
> 積み増したのは主に私で、その過程で**同じ概念を二つの文法カテゴリに分けて**
> しまいました。本メモは棚卸し（§38.1）と、破壊的変更前提の具体提案（§38.2〜）
> です。各項は独立に採否できます。

---

## 38.1 Diagnosis — where the complexity crept in (measured surface)

Today's keyword surface (`is_keyword`), 44 words:

```
open sessionize readbin readcsv readjson read ls gci dir as noheader
writecsv writejson stream save print take limit head sort distinct
describe dropna explode unnest fill drop cast reorder rename where
group on map mode stop monitor watch subscribe visualize transition
log route reroute
```

plus pipes `|? |! |> |#`, DAG builders `-> + &` (`&left/&right/&full/asof`),
the `within` tolerance word, and ~36 scalar funcs. Five distinct complexity
sources:

1. **Alias families** — three spellings that do one thing, kept for
   PowerShell muscle-memory:
   - `read` = `readcsv` = `readjson` = `readbin`
   - `ls` = `gci` = `dir`
   - `take` = `limit` = `head`
   - `explode` = `unnest`
   - `save` = `writecsv` = `writejson`
   - `|?` = `where`
   Each alias is another word to learn, document, format, and test — with **zero
   added power**. ~11 keywords are pure aliases.

2. **Two spellings for one operation** (so `fmt` must normalize):
   - project rename: `name as alias` **and** `name :alias`
   - project cast: `(name:type) as alias` **and** `name :type`
   - filter AND: `a, b` **and** `a and b`
   A canonicalizing formatter is papering over redundant grammar.

3. **The window / time-series family is split across two syntactic categories**
   (the core inconsistency, and mostly my doing):
   - **scalar functions**: `bucket(ts,d)`, `hops(ts,s,h)`, `date_bin(ts,d,o)`,
     `trunc(ts,"day")` — produce a derived key column, composed with `|#`.
   - **bespoke verbs**: `sessionize ts gap "30m" by u`, `shift col lag n by u`,
     and the `& … asof ts within "d"` join sub-grammar.
   Same concept ("derive a key / look across rows by time"), two grammars. A user
   learning windows meets both and cannot predict which shape the next one takes.

4. **Bespoke keywords for what could be composition** — `shift` is `lag()`/
   `lead()`; `sessionize` is a `session()` key; `within` is one join's private
   preposition. Each new time-series idea currently costs a **new keyword**.

5. **Sub-languages inside `|>`** — the `:` definition chain
   (`id :alias :type :{ cls@0..3 dept@3..11 }`) is a mini-DSL (rename, cast,
   character sub-views) with its own rules. Powerful, but it is a second grammar
   to learn on top of the pipe grammar.

---

## 38.2 Principle — the target we simplify toward

The design philosophy already names it (README §"Continue First / Everything is
Flow"); the missing rule is **orthogonality**:

> **One obvious way.** Each operation has exactly one spelling. New capability
> is added by *composition* (a function, a join kind, a disposition) before a
> *new keyword*. If `fmt` has to rewrite form A into form B, form A should not
> exist.

Breaking changes are cheap now (pre-1.0, one integration branch, `fmt` can
auto-migrate most). They will not be cheap later. So this is the moment.

---

## 38.3 Proposal P1 — delete the alias families (−11 keywords)

Keep exactly one spelling; `fmt` auto-rewrites the removed ones for one release,
then they are hard errors with a "did you mean" pointing at the survivor.

| remove | keep | note |
|---|---|---|
| `readcsv` `readjson` `readbin` | `read` | format via `as csv/json/bin` or extension |
| `gci` `dir` | `ls` | discovery |
| `limit` `head` | `take` | bound |
| `unnest` | `explode` | list → rows |
| `writecsv` `writejson` | `save` | format via `as` |
| `where` | `\|?` | one filter spelling (the symbol is the identity) |

Net: 44 → ~33 keywords, no capability lost. (If 統括 prefers the *word* over the
*symbol* anywhere — e.g. keep `where`, drop `|?` — that is a fine inversion; the
point is **pick one**.)

---

## 38.4 Proposal P2 — one spelling for project items & filter-AND

- **Rename/cast**: the `:` chain is canonical and strictly more capable (it
  stacks). **Remove** the `name as alias` and `(name:type) as alias` rename
  forms from the parser (keep `as` *only* for computed columns `(expr) as alias`,
  where it reads naturally and has no `:` equivalent). `fmt` migrates the old
  forms first.
- **Filter AND**: keep the comma (`a, b`), **remove `and`** as a top-level
  conjunction between predicates (keep `and`/`or` *inside* a boolean expression
  where precedence matters — `a and b or c`). The comma is the list separator the
  rest of the language already uses.

Result: the formatter's "normalize A→B" passes for these disappear because A is
gone.

---

## 38.5 Proposal P3 — unify the window / time-series family (the big one)

**Collapse the bespoke time verbs into the function category** that `bucket` /
`hops` / `date_bin` already established. A window is "a derived key + an ordinary
group/sort" (design §30.4 already commits to this for `hops`). Apply it
uniformly:

| today (bespoke verb) | proposed (function, composed) |
|---|---|
| `sessionize ts gap "30m" by u` | `\|> (session(ts, "30m") over u) as s` → `\|# u s …` |
| `shift col lag 1 by u as p` | `\|> (lag(col, 1) over u) as p` |
| `shift col diff by u as d` | `\|> (col - lag(col,1) over u) as d` (or `diff(col) over u`) |
| `shift col lead 1 by u` (planned) | `\|> (lead(col, 1) over u) as n` |

- `over u` is a **single, uniform window-partition clause** (SQL's `OVER
  (PARTITION BY u)`, trimmed). It replaces the per-verb `by …` re-invention and
  reads the same on every window function. Order is source order (the existing
  serial contract); no `ORDER BY` sub-clause in slice 1.
- `lag`/`lead`/`session`/`diff` are ordinary functions in the `Func` enum —
  **no new keywords**, and the next window idea (`rolling`, `first_value`, …) is
  another function, not another verb.
- `bucket`/`hops`/`date_bin` already fit this shape; they gain `over` only if a
  per-group variant is needed (today they are pure row-wise, so they stay as-is).

This removes `sessionize` and `shift` as keywords (−2) and, more importantly,
gives the whole time-series surface **one grammar**. It is a breaking change to
the three slices I just landed (#232 shift, #228 sessionize; #64 asof below) —
I will convert them.

---

## 38.6 Proposal P4 — regularize the as-of join

`& … asof ts within "5s"` grafts two private words (`asof`, `within`) onto `&`.
Make it parallel to the other join kinds instead:

```
A &asof B on k by ts               # &asof joins as a peer of &left / &right / &full
A &asof B on k by ts within "5s"   # tolerance stays, but as the join's one option
```

- `&asof` becomes a `JoinKind` like `&left` (no new top-level keyword; `asof`
  lives only after `&`, exactly where `left`/`right`/`full` already live).
- `within "5s"` is the as-of join's single option (kept — it has no natural
  functional form). `by ts` names the temporal axis, reusing `by` from windows.

Net: `asof` leaves `is_keyword` (it is a join-kind token after `&`); the surface
shrinks and the join family becomes uniform.

---

## 38.7 Proposal P5 — audit the control-plane verbs (open question)

`mode` `stop` `monitor` `transition` `visualize` `log` `route` `reroute` are
control/observability verbs. Several may be better as **one `on` construct** or
flags than as eight top-level keywords. This needs a usage census before
cutting — flagged here, **not** proposed for this slice (I do not want to trade
one over-reach for another). Deferred pending 統括's read on which are load-bearing.

---

## 38.8 Migration & sequencing

1. Land P1 (alias deletion) + P2 (one project/filter spelling) first — pure
   subtractions, `fmt` auto-migrates, lowest risk, immediate surface reduction.
2. Land P3 (`over` window functions) as its own slice — it converts #228/#232
   and reshapes the time-series roadmap; biggest simplification, so worth its own
   review.
3. Land P4 (`&asof`) alongside or after P3.
4. P5 stays a question.

Every step keeps the invariants: IR reversibility (`fmt` round-trips), byte
identity, zero-dependency default, continue-first. `fmt` gains a one-release
**auto-migration** pass (old form → new) so real `.riv` files upgrade
mechanically; after that release the old forms are never-silent errors with a
pointer to the survivor.

**Recommendation:** ratify P1+P2 immediately (cheap, obvious), P3 next
(the real win), P4 with P3. Hold P5 for a usage census.
