# Refactor Plan

A behavior-preserving refactor of the whole codebase, organized bottom-up by layer, and within each layer by scope. This document is the product of a deep audit (7 parallel reviews, one per scope, each reading every file end to end) and is meant to be worked through over many sessions, one scope at a time. It captures not just *what* to change but *why*, so the reasoning becomes reusable judgment rather than a one-off checklist.

No item here changes RESP wire output, replication semantics, or command behavior, unless explicitly marked otherwise. Real bugs turned up during the audit too — those are called out separately in their own section, because fixing them does change behavior, and that's a decision for you to make deliberately, not something to bundle into "refactor."

## How this document is organized

**Layers, bottom-up:** `db.rs` (keyspace/storage) → `resp.rs` (wire protocol) → `client.rs` (per-connection state machine) → `command/*` (dispatch + handlers) → `networking.rs` (event loop + replication) → `cli.rs`/`main.rs`/`lib.rs` (entry point). Each depends on the ones before it, so fixing the foundation first means later layers inherit the improvement instead of building on the old shape.

**Scopes, in recommended order within a layer:**

1. **Organize** — module boundaries, does this file do two jobs that should be two files.
2. **Encapsulate** — which fields should be private + accessed through methods.
3. **Name** — fields, methods, types, variables.
4. **Error handling** — `Result`/`Option` shapes, `.unwrap()`/`.expect()` honesty, error variant granularity.
5. **Observability** — tracing spans, fields, log levels.
6. **Idiomatic Rust** — ownership, cloning, trait/newtype design, dependency usage.
7. **Method order** — final polish pass, arranging the now-settled code for readability.

This order isn't arbitrary. Organize goes first because splitting a file across modules invalidates every line-number reference in the other six passes — do it once, up front. Method order goes last because it's pure rearrangement of already-finished code; reordering before the other passes just means reordering twice. Name comes before Observability because log fields should use the names things will actually be called, not names about to be renamed. Everything in between is looser — feel free to reorder Error handling/Observability/Idiomatic within a layer if one is more pressing.

**Reading a finding:** each one gives you the location, what's wrong and which principle it violates, the concrete fix, why it's actually better (not just "cleaner"), a one-line heuristic for spotting the same class of issue elsewhere on your own, and a priority (Foundational = blocks or complicates other work in that layer, High/Medium/Low = normal triage).

---

## Stage 0 — Restore the safety net (do this first, blocks everything else)

A behavior-preserving refactor is only as trustworthy as the tests that would catch a regression. Right now: **`cargo test` runs zero tests.** The lib test binary fails to compile, which means every one of the 54 existing tests (across db.rs, resp.rs, client.rs, command/common.rs, command/list.rs, command/string.rs) is currently silent, not passing — there is no signal at all, not a weak one.

### 0a. Fix the 7 compile errors (mechanical, ~10 minutes, no design decisions)

All 7 stem from the same root cause: a prior change (replication support) grew `Client::new` from 2 params to 4, changed `on_readable`'s return type from `Disposition` to `(Disposition, Vec<RespBody>)`, and gave `Reply::Now` a second field (`Propagate`) — and the tests were never updated.

| File:line | Error | Fix |
|---|---|---|
| `client.rs:319` | `Client::new(stream, id)` — needs 4 args | pass `ClientRole::Normal` and a fresh `Rc::new(RefCell::new(...))` `ServerInfo` |
| `client.rs:328` | `matches!(on_readable(...), Disposition::Keep)` | match on `(Disposition::Keep, _)` |
| `client.rs:385` | same shape | match on `(Disposition::Drop, _)` |
| `command/list.rs:122` | `let Reply::Now(resp) = reply` | `let Reply::Now(resp, _) = reply` |
| `command/list.rs:206` | `matches!(reply, Reply::Now(_))` | `Reply::Now(_, _)` |
| `command/string.rs:54` | same as list.rs:122 | same fix |
| `command/string.rs:98,103,109` | `Reply::Now(RespBody::Simple(s))` | `Reply::Now(RespBody::Simple(s), _)` |

Run `cargo check --tests --target x86_64-unknown-linux-gnu` after, then `cargo test`, to get a real green baseline before touching anything else. (Per your standing note: macOS doesn't compile the epoll-cfg branch, so always cross-check the Linux target before trusting a green build here.)

### 0b. Close the coverage gaps that matter most for *this specific refactor*

Once 0a is green, three gaps are worth closing before Stage 1 starts, because they guard exactly the kind of logic a refactor is most likely to touch silently:

- **MULTI/EXEC ordering.** `add_to_transaction` does `queue.push_front`, `exec_transaction` drains via `queue.pop_back()` — net FIFO, but non-obvious, and untested with 2+ distinguishable commands. A "simplification" to `push_back`/`pop_back` looks equally plausible and would silently reverse transaction order.
- **WATCH dirty-flag + EXEC.** No test watches a key, dirties it, then asserts EXEC aborts.
- **Propagate::Replicate vs Skip.** No test asserts a write command's `Command::execute` returns `Some(forward)` while a read command returns `None` — the entire replication-propagation contract is unverified.

`networking.rs` (the whole mio event loop and replication handshake) and `cli.rs` (pure arg-parsing functions) also have zero tests, but they're lower stakes for *this* refactor specifically — worth doing, not blocking.

---

## Bugs found along the way (not refactors — your call whether/when to fix)

These surfaced during the audit but change observable behavior, so they don't belong in a "preserve logic" pass. Listed by severity.

1. **Empty RESP array crashes the whole server.** `Command::new` (`command/mod.rs:51-58`) does `&args[0]` after `into_args()`, which happily returns `Some(vec![])` for `*0\r\n`. Indexing an empty vec panics — and since this is a single-threaded event loop, one client sending a malformed frame kills every connected client's session. Highest severity finding in the whole audit.
2. **`ECHO` with no argument also panics.** Its arity is `-1` (argc ≥ 1), but the handler unconditionally reads `args[1]`. Sending bare `ECHO` passes arity validation, then indexes past the end. Same fix shape as #1 (change arity to exact `2`, matching `GET`/`TYPE`/`INCR`).
3. **`INCR` on `i64::MAX` silently wraps (release) or panics (debug).** `db.rs:776` does `value.as_int()? + 1` — parsing is checked, the arithmetic isn't. Real Redis returns `ERR increment or decrement would overflow`.
4. **`RPUSH` doesn't mark a WATCHed key dirty; `LPUSH` does.** `list_append` (db.rs:630) never calls `make_dirty`; `list_prepand` (db.rs:585) does. A transaction watching a key would incorrectly still execute after a concurrent RPUSH, but correctly abort after a concurrent LPUSH.
5. **Commands queued inside MULTI never get replicated.** `Client::exec_transaction` (client.rs:98) discards the `forward` value returned by `process_request`: `Ok((reply, _)) => ...`. Every write executed via EXEC is silently invisible to slaves.
6. **`INFO replication`'s `connected_slaves` always reports 0.** `ServerInfo.connected_slaves` is set once at startup and never incremented, even though `Server.slaves: HashSet<Token>` correctly tracks the real count elsewhere.
7. **Replication handshake reads bypass `Client`'s own buffering** (`networking.rs` `slave_ping`/`slave_replconf`/`slave_psync` build a fresh `BufReader` over `&master_client.stream` per call). Not a confirmed bug today, but a latent one: if the master ever pipelines bytes immediately after a handshake reply (real Redis does exactly this — the RDB payload follows `FULLRESYNC` immediately), whatever the throwaway `BufReader` buffered past the first line is silently dropped. Worth fixing before RDB-payload handling is built out further (see `rdb_design.md`).

Recommend triaging #1/#2 (crashes) separately and soon, independent of the refactor timeline — they're a few lines each and a real availability risk. The rest can wait for whenever you're touching that code anyway, or get their own small PRs.

---

## Stage 1 — `db.rs` (the keyspace)

The deepest layer: owns all in-memory state (`Object`/`Key`/`Value`/`StreamId`), expiry, and the WATCH/BLPOP/XREAD-BLOCK waiter bookkeeping. Currently 974 lines carrying four distinct responsibilities in one file.

### 1a. Organize — split before anything else touches this file

**Finding: `db.rs` holds four unrelated responsibilities; two have zero references to `Db` itself.**

I checked: `StreamId`/`StreamIdSpec` (lines 11-113) and `Key`/`Value` (135-244) never reference `Db`, `Object`, or each other's neighborly context — they're independently reusable types that happen to share a file with the thing that uses them. That's the exact trigger for a module split: a file has accumulated a second (and third) loosely-related responsibility, and the extraction seam is free because these types don't reach into `Db`'s internals.

**Fix:** Rust lets `db.rs` and a `db/` directory coexist (no rename to `db/mod.rs` needed):
- `src/db/key_value.rs` — move `Key`, `Value`, and their impls verbatim.
- `src/db/stream_id.rs` — move `StreamId`, `StreamIdSpec`, and their impls verbatim.
- In `db.rs`: `mod key_value; mod stream_id; pub use key_value::{Key, Value}; pub use stream_id::{StreamId, StreamIdSpec};`

Every external caller does `use crate::db::{Key, ...}` today — the re-export keeps every one of those import paths working unchanged. Zero call-site edits outside `db.rs`.

**Why better:** `db.rs` drops from 974 lines carrying four concerns to roughly 650 carrying one (the keyspace + watchers). Two genuinely standalone, independently-testable concepts get their own file each — "how does XADD's ID validation work" becomes "open `stream_id.rs`," not "scroll past 25 unrelated `Db` methods."

**Pattern to recognize:** if a type's own `impl` block never references the file's "main" struct, it's cohabiting, not actually part of that struct's module.

**Priority: Foundational.** Do this before 1c/1e/1f below touch the same code, so line numbers don't shift twice. (Also: delete the dead `use crate::client::Client;` import at `db.rs:133` while you're in there — grep confirms zero usage.)

### 1b. Encapsulate

**Finding: `ClientWatch` has a real method API that `Db` — in the same file — bypasses.**

`ClientWatch::add`/`remove` (db.rs:265,273) exist and are the intended way to mutate its private `keys`/`dirty` fields. But `Db::add_watchers` inserts into `.keys` directly, `Db::remove_watcher` destructures the struct directly, and `Db::is_dirty` reads `.dirty` directly (there's no `is_dirty()` accessor at all, breaking symmetry with `make_dirty()`). This compiles because Rust's field privacy is scoped to the *module*, not the struct — `Db` lives in the same file, so it can reach past the API it defined two functions earlier. Result: `add`/`remove` are flagged as dead code by the compiler, and the invariant ("watch state only changes through these two ops") is enforced by convention at 2 of 3 call sites, not by the type.

**Fix:** route `add_watchers`/`remove_watcher`/`is_dirty` through `ClientWatch::add`/`remove`, add a matching `is_dirty(&self) -> bool`. Since `ClientWatch` never leaves `db.rs` (confirmed, zero external refs), drop its `pub` too.

**Why better:** if this type's invariants ever grow (a watch-count cap, say), there's one place to change instead of three call sites that each reimplement the mutation slightly differently.

**Pattern to recognize:** a private field touched via `.field` syntax right next to a same-purpose method *in the same file* is a stronger smell than a `pub` field touched from another module — `private` doesn't protect you from your own sibling code, only from other modules. If the invariant matters, either give the type its own submodule (where the compiler would actually enforce it) or exercise self-discipline consciously.

**Priority: High.**

### 1c. Name

| Finding | Location | Fix | Priority |
|---|---|---|---|
| Two unrelated concepts both called "watch" — `ClientWatch`/`watchers` (WATCH/dirty-tracking) vs. `StreamWait.watch` (XREAD BLOCK cursor positions) | db.rs:250,253-256,281-283; command/stream.rs | Rename the XREAD side away from "watch" entirely: `StreamWait.watch` → `positions`/`cursors` | **Foundational** |
| `list_prepand` — misspelling of "prepend", paired against correctly-spelled `list_append` | db.rs:585, command/list.rs:22 | `list_prepend` | High |
| `StreamId(u64, u64)` — unnamed tuple fields, every read site must remember position 0 = ms | db.rs:13 | `struct StreamId { ms: u64, seq: u64 }` (also closes a real gap — see 1f) | High |
| Three names for the same parameter role: `cur_client`, `client_id`, and (worst) `fd` for a `ClientId` at db.rs:466,472 | db.rs, multiple | Standardize on `client_id` everywhere in `Db`; the `fd` instance actively misleads (implies OS file-descriptor semantics that don't apply to the `ClientId` abstraction) | High |
| `watchers`/`clients_watchers` — grammatically backwards relative to each other, needs a comment to disambiguate | db.rs:281-283 | `watchers` → `key_watchers`, `clients_watchers` → `client_watches` | High |
| `handle_waiters`/`waiters` read as generic but are BLPOP-specific; sibling `handle_stream_waiters` is correctly qualified | db.rs:280,426 | `waiters` → `list_waiters`, `handle_waiters` → `handle_list_waiters` | Medium |
| `HandleWaitersResult(pub A, pub B)` — positional tuple, destructured by position at every call site | db.rs:290 | Named fields: `{ replies, deadline }` | Medium |
| `Value::as_int` — fallible parse named with the "free/infallible" `as_` prefix (violates C-CONV) | db.rs:147 | `parse_int` | Medium |
| "Lazy Epiration" comment typo | db.rs:686 | "Expiration" | Low |
| `Db::key_type` — dead public method, duplicates `command::string::cmd_type`'s logic | db.rs:390 | Wire `cmd_type` to call it, or delete it | Low |

### 1d. Error handling

**Finding: two `.unwrap()` sites are provably safe but read as if they aren't — an `.expect()` should state a proof, not a hope.**

`db.rs:439` (`checked_sub(date_now).unwrap()`) is a bare `.unwrap()` with zero explanation, reached only when the surrounding branch guarantees `*value > date_now`. `db.rs:559` is worse: it has a comment that actively doubts itself (`// TODO: handle this better` / `// I think the invariant is guaranteed by upper if...`) about something that's trivially provable by reading the three lines above it.

**Fix:** replace both with `.expect("<state the specific invariant that makes this safe>")`, and delete the hedging comment at 557-558 once the proof lives in the `expect` string itself.

**Why better:** an `.expect()` message is supposed to be a proof, not a hope. If you can prove a panic can't happen by reading the same function top to bottom (both cases here qualify), write that proof down and stop hedging — a doubtful comment next to a bare `unwrap` is strictly worse than either fixing the doubt or removing it, since it tells the next reader "re-derive this yourself."

**Pattern to recognize:** if proving safety requires trusting something *external* (system clock, another thread), that's a sign you need `?` or a real fallback (contrast: `networking.rs`'s `SystemTime::now().duration_since(UNIX_EPOCH)?` correctly propagates rather than unwraps, because *that* fallibility depends on environment, not code you can see). If proving it only requires reading the enclosing function, `.expect()` with the proof spelled out is correct.

**Priority: High.**

Also, sharpen `db.rs:468,471`'s `.expect()` messages — both currently say "guaranteed by the is_empty check before" without specifying *which* check (there are two, on two different collections). Either name precisely what's relied on, or restructure so the check and the use can't be pulled apart by a future edit (e.g., `let Some(item) = list.pop_front() else { break }` instead of a separate `is_empty()` + `pop_front()`). Medium priority — there's a `// TODO: consider refactoring for better lifetimes` on this same function (db.rs:425), so this code is already expected to be touched again.

### 1e. Observability

| Finding | Location | Fix | Priority |
|---|---|---|---|
| Lazy expiry (`expire_clean`) — the only key-eviction path — has zero logging | db.rs:687 | `debug!(%key, "key expired")` inside the `is_expired` branch | High |
| `"adding outbox"`/`"adding waiter"` at `info!` — fires on every LPUSH/RPUSH-with-waiter and every blocking BLPOP, i.e. scales with request volume | db.rs:596,623 | Downgrade to `debug!` | High |
| `list_prepand` logs waiter-wakeup queuing; `list_append` (identical logic) doesn't | db.rs:596 vs 636 | Mirror the log call in `list_append` | Medium |
| `xread_wait` registers a blocking waiter with zero log, unlike its BLPOP sibling | db.rs:521 | `debug!(client_id = ?client_id, num_keys = watch.len(), "registering stream waiter")` — use `client_id` per the naming fix above, not a fresh alias | Medium |

The litmus test running through all four: **does this line's frequency scale with request volume?** If yes, it's `debug!` at best, never `info!` — `info!`'s rate should stay roughly flat regardless of traffic, reserved for milestones (server start, replica attached), not per-operation bookkeeping.

### 1f. Idiomatic Rust

**Finding: `make_dirty(&mut self, key: Key)` takes ownership it never uses — 7 unnecessary clones on every write path.**

The body only does `self.watchers.get_mut(&key)` — a read. Every one of its 7 call sites does `self.make_dirty(key.clone())` purely to satisfy the by-value signature; in 5 of 7 the caller already holds a `&Key` or reuses the owned `Key` afterward regardless.

**Fix:** `pub fn make_dirty(&mut self, key: &Key)`. Every call site drops its `.clone()`.

**Why better:** removes 7 heap allocations from the hottest path in the system (every SET/RPUSH/LPUSH/BLPOP/XADD/DEL touches this) with zero semantic change — verified every call site's later use of `key` still works.

**Pattern to recognize:** infer a function's ownership requirements from what its *body* does, not from what feels safe to write. If a `fn f(&mut self, x: T)` only ever uses `&x` internally, the signature should say `&T` — a by-value parameter that's never moved into storage is a tell that callers are cloning to appease the signature, not because ownership is actually needed. Grep the body before deciding a parameter's ownership.

**Priority: Foundational.** Best effort-to-payoff ratio in the entire audit — do this one first regardless of what else you tackle in Stage 1.

**Finding: 14 near-identical `From` impls across `Key`/`Value`/`StreamId` — now past the point where a shared abstraction is premature.**

`From<&[u8]> for Key` and `From<&[u8]> for Value` are textually identical except for `Self`. Same for the `Vec<u8>`-in/out pairs. `Key` is even missing a `From<&Key> for Vec<u8>` that `Value` has — evidence these were added ad hoc, not designed as a set.

**Fix:** a `macro_rules!` (not a shared trait — `impl<T: Trait> From<T> for Vec<u8>` is illegal under the orphan rule for a generic `T`, so this specifically needs to be a macro, not a trait) generating the byte-vector + RespBody conversions, invoked once for `Key` and once for `Value`.

**Why better:** 11 impls collapse to 2 invocations; the `Key`/`Value` asymmetry becomes structurally impossible since both get the identical generated set.

**Pattern to recognize:** two fully-identical impl shapes plus a third partial one is exactly the "3+ near-identical instances" bar that justifies an abstraction — one or two instances would still be premature.

**Priority: High.**

**Finding: `StreamId(u64, u64)` as a tuple struct lets `ms`/`seq` swap silently at construction** (same underlying fix as the naming entry above, different justification — worth doing for both reasons at once). Beyond readability: two same-typed fields in a tuple struct is the specific shape where positional construction (`StreamId(ms, seq)`) can't be checked by the compiler if the arguments get transposed by accident. Named-field construction (`StreamId { ms, seq }`) forces every call site to say which is which. **Priority: Medium** (mechanical but touches several match/construction sites — do as its own small commit).

**Finding: the `bytes` crate is a paid-for, zero-usage dependency, with its exact adoption point already marked by a TODO.** `Cargo.toml` lists `bytes = "1.3.0"`; grep confirms zero usages anywhere in `src/`. `resp.rs:45` already has `// TODO: consider migrating to Bytes/BytesMut instead of u8`. This compounds with the fixes above: `Client::inbuf.drain()` is an O(n) memmove per parsed command where `BytesMut::advance` would be O(1); every `Key`/`Value` clone (including the 7 just removed above, and the ones still needed for `ClientWatch`'s double-indexed structure) is a byte copy where `Bytes::clone()` would be a refcount bump; `parse_resp`'s `part_slice.to_vec()` plus the `From<&[u8]>` conversion downstream currently copies every key/value argument twice.

This is a real decision, not a small fix: **adopt** (change `RespBody::Bulk` to `Option<Bytes>`, `Client::inbuf`/`outbuf` to `BytesMut`, `Key`/`Value`'s inner storage to `Bytes` — a bounded migration touching resp.rs/client.rs/db.rs's newtypes and every handler currently typed around `&[u8]`) or **remove** (delete the dependency so `Cargo.toml` reflects reality). Leaving it declared-but-unused is the one option that's clearly wrong. **Priority: High to decide, but treat the migration itself as its own scoped piece of work — don't bundle it into a "quick fix" pass.**

Two smaller items in the same family: `StreamId`'s `Display` format string (`"{ms}-{seq}"`) is hand-duplicated in `From<StreamId> for String`/`Vec<u8>` instead of calling `.to_string()` (Low, pure DRY). `Value` doesn't derive `Clone` while `Key` does, so the one place that needs a `Value` copy reaches into the private field by hand instead of calling `.clone()` (Low, consistency).

### 1g. Method order (do last, after everything above has settled)

**Finding:** `impl Db`'s ~25 methods interleave public API with private helpers with no consistent rule, and multi-caller helpers (`expire_clean`, called from 6 different places) can't follow a simple "near its sole caller" rule because they don't have one.

**Fix:** group public methods by command family — mirroring the split that already exists one layer up in `command/{string,list,stream}.rs` — string ops, list ops, stream ops, watcher/transaction ops, waiter housekeeping, clock, lifecycle. Put the multi-caller private helpers in one clearly-marked section at the bottom rather than sprinkled between groups.

**Why better:** a reader can match `Db`'s internal seams to a mental model they already have from the command layer, and can answer "what's the public contract of `Db`?" by reading top to bottom without tripping over private helpers mid-scan.

**Priority: High**, but sequenced last in this stage since it's rearranging code the other six passes will have already touched.

---

## Stage 2 — `resp.rs` (wire protocol)

Small, mostly clean file. A few things worth doing.

| Scope | Finding | Fix | Priority |
|---|---|---|---|
| Name | `RespBody::RDB` vs. neighboring `Reply::Rdb` — same acronym, two casings, ten lines apart in the same file (violates Rust API Guidelines C-CASE: acronyms are ordinary words in UpperCamelCase, e.g. `Uuid` not `UUID`) | `RespBody::RDB` → `RespBody::Rdb` | High |
| Encapsulate | `Resp.body: pub RespBody` — every call site already uses the existing `body(self)` accessor; the `pub` field is a redundant second access path | Drop `pub` on the field | Low |
| Organize | `impl RespBody`'s two blocks (conversions, then constructors/encoder) are split by ~70 unrelated lines (`Propagate`/`Reply`/`Resp`) | Move them adjacent | Low |
| Observability | `parse_resp` has zero instrumentation, and its `Option<Resp>` return conflates "not enough bytes yet" (the common, benign case) with "this will never parse" (rare, currently invisible) — a malformed frame can wedge a connection silently | Add exactly one `trace!` at the one unambiguous rejection point (`validate_part_end`'s failure, the missing-trailing-CRLF case) — deliberately `trace!`, not `debug!`, since "debug" is this project's default filter and this function runs once per buffered command | Foundational |

The observability finding is the interesting one to internalize: don't log inside every early-return of a hot parser just because "instrumentation is good" — most of those returns mean "wait for more bytes," which happens on every partial read and would be pure noise. Log only the one branch that's an actual, unambiguous rejection.

---

## Stage 3 — `client.rs` (per-connection state machine)

### 3a. Encapsulate

**Finding: `Client.stream: pub TcpStream` is reached into directly from `networking.rs`'s replication handshake, bypassing the client's own buffering entirely.**

`slave_psync`/`slave_replconf`/`slave_ping` each build a fresh `std::io::BufReader::new(&master_client.stream)` per call, read one line, and let the `BufReader` (and anything buffered past that line) drop. `Client` owns `inbuf`/`outbuf` and a `consume()`/`flush()` protocol specifically so nothing else reads or writes the socket directly — this is the one place that does anyway. This is finding #7 in the bugs list above (a latent risk, not confirmed today) but the encapsulation fix stands regardless of whether the bug ever manifests.

**Fix:** make `stream` private. Add `pub(crate) fn read_line(&mut self) -> io::Result<String>` on `Client`, wrapping the identical `BufReader::new(&self.stream)` + `read_line` call already used at every site today. `networking.rs` calls `master_client.read_line()?` instead.

**Why better:** the socket becomes reachable through exactly one type. Future work on buffering/pipelining (relevant given `rdb_design.md`'s plans) has one call site to reason about instead of two uncoordinated ones.

**Pattern to recognize:** a raw I/O handle field on a type that already has read/write methods is a red flag — it means two competing consumers of the same stateful resource exist, and whichever one buffers internally can silently eat the other's bytes.

**Priority: Foundational** (this is the one item in the whole plan closest to an actual correctness fix, even though it's framed as encapsulation).

### 3b. Name

| Finding | Location | Fix | Priority |
|---|---|---|---|
| `to_propogate` — misspelled everywhere it's used as a variable, despite the type that defines the concept (`Propagate` in resp.rs) being spelled correctly | client.rs:58,160,170,197; networking.rs:310,322 | `to_propagate` | High |
| `Client.role: ClientRole` answers "what does the peer represent to me," but reads identically to `ServerInfo.role: ServerRole` which answers "what am I" — same field name, same enum shape, opposite referent | client.rs:53 | `role` → `peer_role`, and/or `ClientRole` → `PeerRole` | High |
| "improve this STATE machinge" / "withotu replying" comment typos | client.rs:204,175,262 | fix spelling | Low |

### 3c. Error handling

**Finding: `flush()` treats every `WouldBlock` write as fatal; `on_readable()` (same struct, same non-blocking socket) correctly treats `WouldBlock`/`Interrupted` reads as benign.**

```rust
// on_readable — correct:
Err(e) if e.kind() == io::ErrorKind::WouldBlock => Disposition::Keep,
// flush — treats everything, including ordinary backpressure, as fatal:
if let Err(e) = self.stream.write_all(&self.outbuf) { error!(...); return Disposition::Drop; }
```

**Fix (scoped to error classification only):** mirror `on_readable`'s match arms in `flush()` so `WouldBlock`/`Interrupted` is distinguished from a genuinely fatal error, rather than uniformly hitting `error!("flush failed")`. Note: *fully* fixing the backpressure behavior (retry once writable, keep unsent bytes across polls) is a networking-architecture change — out of scope here, flagging the boundary rather than proposing it.

**Why better:** a slow client under backpressure stops being logged and dropped identically to a genuine I/O fault.

**Pattern to recognize:** when the same error type on the same resource is handled two different ways in sibling methods of one struct, that asymmetry is very likely unintentional — diff them side by side.

**Priority: Medium** (the narrow classification fix is safe and small; full backpressure handling is separate, larger work).

### 3d. Observability

| Finding | Location | Fix | Priority |
|---|---|---|---|
| `warn!("client disconnected")` on a clean, expected `Ok(0)` EOF — the single most common TCP lifecycle event, leveled as if it's an anomaly | client.rs:164 | `info!(client_id = ?self.id, "client disconnected")`; reserve `warn!` for the genuinely-abnormal read-failure branch just below, which is already correctly leveled | High |
| `warn!("rdb finished handshake on master side")` for a *successful* replica attach | client.rs:218 | `info!` | Medium |
| `warn!("master (on slave) slave should update it offset...")` fires on every single replicated write, forever, in steady-state — this is the entire expected code path for a replica, not an edge case, and the message is really an unfinished TODO note | client.rs:264 | `trace!` (not `debug!` — "debug" is this project's default filter, and this fires per replicated write), rewritten to describe what happened rather than what's still TODO | High |
| `flushing = %self.outbuf.escape_ascii()` — field named after the verb, not the content; the only byte-dump in the codebase, no naming convention set for a future inbound counterpart | client.rs:278 | rename to something content-based and directional, e.g. `wire_out`, so a future `wire_in` (which would help resp.rs's R1 gap) doesn't have to invent its own name | Low |
| `#[instrument(skip(self))] fn flush` — a span with every field skipped, on a very hot path, carrying zero identifying context | client.rs:272 | drop the attribute, or add `fields(client_id = ?self.id)` if kept | Low |
| MULTI/EXEC/WATCH lifecycle has zero logging beyond individual command errors — no signal that a transaction started, committed, or aborted | client.rs:78-136 | `debug!` on MULTI start and EXEC entry; `info!` specifically on the WATCH-dirty abort path in `common::execute_transaction` (a real, client-visible business event, not routine bookkeeping) | Medium |

The throughline in the first three: **ask "if a human saw only this line with no other context, would they think something's broken?"** If no, it isn't `warn!`.

### 3e. Method order (last)

**Finding:** the constructor (`new`) isn't first — 7 methods precede it. Tracing one request's path requires jumping in every direction: `on_readable` (line 157) calls `consume` (defined 81 lines later, 238); `consume` calls `process_request` and `post_process_success_request`, the latter defined *above* both, which itself calls transaction methods defined even further up.

**Fix:** constructor first; then the request pipeline in call order (`on_readable` → `consume` → `process_request` → `post_process_success_request` → `write_out` → `flush`, each definition following the point it's first called from); then the transaction-workflow group; then the small state accessors last.

**Why better:** a newcomer gets one linear pass through "how a byte on the wire becomes a reply," then a second linear pass through "how MULTI/EXEC/DISCARD work" — no backtracking either time.

**Priority: High**, sequenced after 3a-3d land.

---

## Stage 4 — `command/*` (dispatch + handlers)

### 4a. Organize

**Finding: `command/common.rs` bundles three different kinds of things under "common."** I traced every call site: `execute_transaction`, `get_initial_request`, `watch_keys`, `unwatch`, `info`/`info_replication` are each called *only* from `command/mod.rs`'s dispatch — they're not shared utilities, they're two full command families (MULTI/EXEC/WATCH, and INFO) that happen to not have their own file, unlike string/list/stream. Only `CommandError`/`HandleCmdResult`/`BlockMode`/`ExpCmd`/`get_ttl`/`parse_ttl` are genuinely used across multiple family modules and earn the name "common."

**Fix:** split into `command/common.rs` (trimmed to the truly shared vocabulary), `command/transaction.rs` (MULTI/EXEC/WATCH/DISCARD), `command/info.rs` (INFO). All call sites needing updates are inside `command/mod.rs` itself — nothing outside `command/` touches these functions.

**Why better:** the module tree becomes five parallel family modules (info, list, stream, string, transaction) instead of four families plus a junk drawer — "is this shared or family-specific" stops being a question you have to answer by reading the file.

**Pattern to recognize:** grep every symbol a "common"/"utils" module exports. One caller, and that caller is the dispatcher → it's a homeless command, not a shared helper. Two-plus callers across genuinely different families → actually common.

**Priority: Medium**, independent of the db.rs/networking.rs splits — do whenever convenient within this stage, before 4c/4d touch the same code.

### 4b. Name

| Finding | Fix | Priority |
|---|---|---|
| Command-handler naming has 4 different shapes in one file: `cmd_ping`/`cmd_echo` (free functions, `cmd_` prefix), `list::push`/`string::get` (generic verb, no prefix), `stream::xadd` (mirrors wire command), `string::cmd_type` (forced exception for the `type` keyword) | Pick one convention — e.g. bare verb everywhere, no `cmd_` prefix, `type_of` for the keyword collision — before the command set grows further. This is the dispatch table every future command gets added to; whichever decision lands here compounds. | **Foundational** |
| `HandleCmdResult` abbreviates "Command" to "Cmd" while every sibling type (`Command`, `CommandKind`, `CommandError`) spells it out | `HandleCmdResult` → `CommandResult` | Medium |

### 4c. Error handling

| Finding | Location | Fix | Priority |
|---|---|---|---|
| `WrongArity(String, String)`'s error template says field 1 is "expected", but the construction site stores `argc` (actual) — the real expected value is never included anywhere | common.rs:29; mod.rs:222-230 | Either drop to real-Redis's actual single-field wire text, or fix the mapping and add a third field (`cmd, expected, actual`) — this is user-visible RESP text, so pick deliberately | High |
| `psync`'s three distinct I/O failure modes (not-found, permission, read-failure) all collapse into `CommandError::NoRdbFile`, whose message says "doesn't exist" — false for two of the three | mod.rs:244-271 | Add a second variant (`RdbIoError`) for the non-`NotFound` cases; keep `NoRdbFile` only for the true `NotFound` case | Medium |
| `CommandError::Unknown(String::new())` reused for "frame isn't shaped like a command at all" — a structurally different failure from "unrecognized command name" (which is what `Unknown`'s own message describes) | mod.rs:54 | Add `CommandError::MalformedRequest` for this one call site | Medium |
| All three `thiserror` enums derive `Clone` but nothing ever clones them (verified by grep) | common.rs, networking.rs, cli.rs | Drop the derive | Low |

Cross-cutting note verified during this pass: `main.rs` returning `Box<dyn Error>` while everything underneath uses `anyhow::Error` is *not* an inconsistency — I built a throwaway repro and confirmed the full `.context()` chain survives identically through `Box<dyn Error>`'s `Debug` output. No change needed there.

### 4d. Observability

**Finding: `info!(command = ?kind, "handling cmd")` fires on literally every command** — the highest-frequency log line in the system by construction. At `info!` (this project's default filter), it buries genuine milestones (client connected, replica attached) in per-request noise. **Fix:** downgrade to `debug!`. **Priority: Foundational** — same principle as the db.rs observability findings: if a line's rate scales with request volume, it's never `info!`.

**Finding: `Span::current().record("cmd", field::display(&kind))` is dead code, verified, not guessed.** I checked every span-creation site in the repo — none declares a `cmd` field via `field::Empty`, and `Span::record` silently no-ops when the active span doesn't pre-declare the field it's targeting. This call has looked like it's threading command context into span-based correlation and has never done anything. **Fix:** either delete it (the following `info!`/`debug!` event already carries `command` as its own field) or make it real by declaring `cmd = field::Empty` at a new span — see the `process_request` span proposed in Stage 5's cross-cutting client-identity fix, which is the natural place for this to actually work. **Priority: Foundational.** This is a compact, self-contained lesson worth internalizing on its own: `Span::record()` only mutates a field pre-declared at the span's *creation* site — it is not a general-purpose way to attach data to "whatever span happens to be active."

Smaller items: `string.rs`/`list.rs`/`stream.rs`/`common.rs` have zero tracing calls at all — the command layer logs *which verb* ran, never *which key*. Add one `debug!(%key, ...)` per handler at the point the key is known, matching db.rs's existing `%key` convention (Medium). `psync`'s two `warn!`s (mod.rs:248,260) log the *only possible outcome* (a replica attaching; no RDB ever exists on disk since there's no SAVE command implemented) as if it were an anomaly — downgrade to `info!`/`debug!`, keep the two genuine I/O-failure `error!`s as they are (Medium).

---

## Stage 5 — `networking.rs` (event loop + replication)

### 5a. Organize

**Finding: the mio event loop and the replication handshake are two different jobs sharing one file, one impl block, and — worse — the four handshake methods are defined in the *reverse* of their call order,** separated by ~150 unrelated lines of event-loop code. `slave_handshake` calls `slave_ping()` → `slave_replconf()` → `slave_psync()` in that order; they're defined as `slave_psync`, `slave_replconf`, `slave_ping` — exactly backwards.

**Fix:** extract `src/networking/replication.rs` (same `foo.rs` + `foo/` coexistence trick as Stage 1). Move `ServerRole`, `ServerInfo` (+ `new`/`rdb_path`), and a second `impl Server { slave_handshake, slave_ping, slave_replconf, slave_psync }` block. Re-export from `networking.rs` (`pub use replication::{ServerInfo, ServerRole};`) — the three external files importing `ServerInfo` today (`client.rs`, `command/common.rs`, `command/mod.rs`) need zero edits.

**On visibility, specifically:** this split needs **no `pub(crate)` widening at all**. Rust's privacy rule is "visible to the defining module and all its descendants" — since `replication` becomes a child module of `networking`, it can already see `Server`'s private fields (`master_link`, `poll`, `server_info`) because they're declared in an ancestor. The encapsulation boundary that actually matters (what code *outside* `networking` sees) doesn't move at all.

**Why better:** from `run`'s perspective, replication becomes one call, `self.slave_handshake(port)?` — the event loop's interface gets genuinely deep (small surface, real complexity hidden). Someone debugging the handshake finds all four `slave_*` methods together in one ~150-line file instead of hunting through 434 lines of poll-loop code, in the order they actually execute.

**Pattern to recognize:** (a) if you can name a struct's methods using two different verbs — "drive the loop" vs. "speak this protocol" — that's two modules pretending to be one; (b) if function A calls B→C→D in one order but the file defines them in the reverse order, that's a near-certain sign methods were added wherever felt convenient rather than in call order; (c) don't reach for `pub(crate)` reflexively when splitting into a *child* module — check whether the descendant-visibility rule already covers you first.

**Priority: Foundational.** Do this before 5c/5e touch the same code, and before or after Stage 1's db.rs split (order between the two doesn't matter, but both must precede any in-file reordering in either file).

### 5b. Encapsulate

**Finding: `ServerInfo` applies encapsulation inconsistently to its own fields.** `dir`/`dbfilename` are private behind a computed `rdb_path()` accessor; `role`/`connected_slaves`/`master_replid`/`master_repl_offset`/`replica_of` are raw `pub`, and two other modules already reach straight in to read them. Nothing mutates them externally *yet* — but the natural next features here (wiring up `connected_slaves`, per bug #6 above) are exactly the kind of change that, without an accessor, gets written as `server_info.borrow_mut().connected_slaves += 1` from wherever's convenient, with nothing enforcing that it stays truthful.

**Fix:** make all five fields private. Add read accessors, or since `info_replication` is the only real consumer needing all of them at once, a single `pub fn replication_snapshot(&self) -> ReplicationInfo` (a small owned-values struct) that both `info_replication` and `psync` consume. When mutation is needed later, add `pub(crate) fn record_slave_connected(&mut self)` etc. rather than exposing `&mut` fields — that's the seam where "connected_slaves tracks the real slave set" actually gets enforced.

**Why better:** locks in, before the next feature lands, that raw field access was never the intended API — it was drift.

**Priority: High.**

### 5c. Name

| Finding | Fix | Priority |
|---|---|---|
| "strarting the slave handshake" typo | "starting" | Low |
| `cronloops` — missing separator, inconsistent with sibling fields like `next_client_id` | `cron_loops` | Low |
| `get_increased_id` — mutates state and allocates a fresh id, named like a pure getter (Rust convention specifically discourages `get_` prefixes, doubly wrong here since it also has a side effect) | `allocate_client_id` | Medium |
| `Client.role`/`ClientRole` naming collision with `ServerInfo.role`/`ServerRole` (see Stage 3b — same fix, cross-referenced from both layers since the two types are defined in different files) | see Stage 3b | High |

### 5d. Error handling

**Finding: the replication handshake (`slave_handshake`/`slave_ping`/`slave_replconf`/`slave_psync`) uses bare `?` throughout, while `Server::new` three functions above is careful with `.context(...)` at every fallible step.** The handshake is arguably the *most* likely site of an operational failure (master down, wrong address, protocol drift) — yet it's the one place that gives you only the raw OS error with no indication of which of the three steps failed.

**Fix:** add `.context(...)`/`.with_context(...)` at each of the five `?` sites (connect, and the four `read_line`s), naming the specific step ("slave PING: reading master's reply", etc.).

**Why better:** restores anyhow's actual value proposition — cheap, composable context at each `?` so the final printed chain tells a complete story — at exactly the layer where that story currently gets dropped.

**Pattern to recognize:** grep every bare `?` in an anyhow-returning function; ask "if this fires, does the message say which step, or just the raw OS error?" A function with five `?`s and zero `.context()` sitting next to a sibling function with four `.context()` calls on four `?`s is worth normalizing.

**Priority: High.**

Also decide on `NetworkingError::MasterDisconnected` (networking.rs:41) — flagged dead by the compiler, never constructed. Either wire it up (check `read_line`'s `Ok(0)`/empty result specifically, distinguishing "master hung up" from "master sent garbage") since nothing currently tells those two cases apart, or delete it if that distinction genuinely won't be acted on. Medium priority either way.

### 5e. Observability

**Finding (the cross-cutting one — highest leverage in this stage): four different names represent "which client" across layers, and the one place identity *does* flow into nested logs today does so by accident.**

`service_client` auto-captures `token` (a `mio::Token`) via `#[instrument]`. `accept_client` logs `c_token` (same type, different variable name). `before_sleep` rebinds the real `ClientId` to a `Token` and logs it *under the name* `client_id` — so `client_id` in this one spot is actually a `Token` wearing the wrong label. `ClientId` itself — the real domain type, used pervasively in db.rs/command dispatch signatures — is never logged directly under its own name anywhere. Because `fmt`'s default formatter prints the active span stack on every nested event, a `command/mod.rs` "handling cmd" line *does* incidentally carry `token=Token(N)` today — but only because this is a single-threaded, synchronous event loop with no task boundary to break that nesting, not because anyone designed it to. Refactoring `service_client`'s signature would silently drop that identity from every nested log with no compiler warning.

**Fix:** pick `client_id` (holding the real `ClientId`, not `Token`) as the one field name used everywhere a client's identity is logged. Concretely:
1. Add `#[instrument(skip(self, db, frame), fields(client_id = ?self.id))]` to `client.rs`'s `process_request` — `self.id: ClientId` is already in scope, and this is the natural per-command boundary (also fixes CMD2's dead `Span::record` from Stage 4, and the pipelining gap below).
2. In `before_sleep`, stop rebinding `ClientId` to `Token` under the name `client_id` — keep the `Token` conversion for the `HashMap` lookup, but log the original `ClientId` under that field name.
3. Leave `token`/`c_token` as-is in `accept_client`/`service_client` — they're genuinely about the mio poller's registration token, a different concept; just don't let them masquerade as `client_id`.

**Why better:** `rg 'client_id=ClientId\(7\)'` across logs would show every layer's view of client 7 — connect, each command, each db mutation, disconnect — end to end, by design instead of by accident of call-stack nesting.

**Pattern to recognize:** when the same real-world entity has more than one type representing it in your codebase, pick exactly one to be "the field name in logs" and convert into it at every log site, rather than logging whichever type happens to be in scope.

**Priority: Foundational.**

Same fix (the `process_request` span) also closes: **`service_client`'s span currently covers one whole `on_readable` event, which can bundle several pipelined commands into a single span** — entering the new span once per parsed frame gives each pipelined command its own nested span, individually distinguishable and timeable (Medium, same fix as above).

Remaining findings in this layer, by pattern:

- **The `info!`-suitable-for-`warn!` cluster:** startup role/replicaof echo (`warn!(?role, ...)`, no message string at all — Low), two "removing client" sites that share text but only one is attributable to a client without relying on log-ordering assumptions (add the `client_id` field explicitly to both — High), the psync milestones already covered in Stage 4d.
- **`debug!(?uptime)` every event-loop tick, no message, no new information over the log timestamp** — remove (Low).
- **`debug_span!("server loop", loop = self.cronloops + 1)` entered every iteration** — the *span* is fine (loop-per-iteration is a defensible unit of work, correctly nests as parent of accept/service), but the `loop=N` *field* gets silently rebroadcast onto every nested log line by the formatter's default span-context printing, for no diagnostic value. Drop the field, keep the span (Medium).
- **No span wraps the whole replication handshake** — four sequential functions read as unrelated flat lines. `#[instrument]` on `slave_handshake` itself (matching the style already used for `run`/`accept_client`/`service_client` in this same file) groups them for free, and sets up handshake-duration timing for free if `FmtSpan::CLOSE` is ever added (High).
- **`slave_psync` logs `"starting replconf for master-slave"`** — copy-pasted from the actual `slave_replconf`, factually wrong about which phase is running. Fix the message text (High — this is a real "the log lies" bug, not just a style choice).
- **Two structurally-different failures ("lost the master link" vs. "one client's registration failed") share the identical message `"registration failed"`,** distinguishable only by which token value appears. Differentiate the text (Medium).

### 5f. Idiomatic Rust

**Finding: `Rc<RefCell<ServerInfo>>`'s `RefCell` half is currently unexercised.** Grep confirms zero `borrow_mut()` calls anywhere — every access is a read, and every field is set once at startup. The `Rc` half is unambiguously correct (single-threaded, `Client` instances live inside `Server`'s `HashMap` so a borrowed `&'a ServerInfo` isn't expressible without a self-referential-struct problem) — this is not "Rc<RefCell> is a smell," it's specifically that the interior-mutability half isn't earning its keep *yet*.

**Caveat worth flagging honestly rather than unilaterally fixing:** there's a TODO right next to `ServerInfo::new` about making `master_replid`/offset dynamic per role, which is exactly the kind of near-term change that would need `RefCell` back the moment it lands. Worth a quick check with yourself (or whoever's tracking the replication TODOs) on timing before simplifying to plain `Rc<ServerInfo>`.

**Priority: Medium**, and flagged as the one item in this whole plan where reading the code alone can't fully settle the right call — it depends on how soon `connected_slaves`/`master_repl_offset` actually get wired up (bug #6 above).

### 5g. Method order (last, after 5a's split has landed)

Once replication.rs is extracted, reorder what remains in `impl Server`: constructor first (currently `get_increased_id` precedes `new`), then `run` promoted near the top since everything else exists to support it, then `accept_client`/`service_client`/`before_sleep`/`set_current_time` in the order `run`'s loop body actually touches them. **Priority: High**, sequenced after 5a-5f.

---

## Stage 6 — `cli.rs` / `main.rs` / `lib.rs`

Small, low-stakes, can slot in anytime.

- **Doc comment says `port: u16` is "Path to the vault directory."** (cli.rs:9) — almost certainly a copy-paste leftover. A doc comment is part of a name's contract; this one actively lies. Fix or delete. (Naming, Low, but the single most concrete "the docs lie" moment in the codebase — worth 30 seconds.)
- **`#[command(long_about = "Long about")]`** — literal placeholder text shipped in `--help` output. Not an identifier, but user-facing; fix whenever convenient.
- **Zero tests on `port_in_range`/`parse_replicaof`** — pure functions, no I/O, cheapest possible tests to add. Already listed in Stage 0b; repeated here since this is where you'd actually write them.

`lib.rs`'s module-level visibility (`mod cli; mod client; mod command; mod db; mod networking; mod resp;`, all private, plus `pub use cli::Cli`) is the deep-module pattern done correctly at the crate root — nothing to change there.

---

## Cross-cutting threads (appear in more than one stage — noted at each, fixed once)

- **`client_id` naming and logging consistency.** Touches db.rs (`cur_client`/`fd` → `client_id`, Stage 1c), client.rs (the `process_request` span, Stage 5e — implemented here even though the finding is cross-referenced from networking.rs), and networking.rs (`token`/`c_token`/mislabeled `client_id`, Stage 5e). Implement the span in client.rs; the naming cleanups in db.rs and networking.rs are independent and can happen in either stage's pass.
- **`propagate`/`propogate` spelling.** The type (`Propagate` in resp.rs) is already spelled correctly; every variable named after the concept in client.rs and networking.rs isn't. Fix wherever you touch either file.
- **`Client.role`/`ServerInfo.role` naming collision.** One rename (`ClientRole`/`Client.role` → `PeerRole`/`peer_role`), referenced from both Stage 3b and Stage 5c.
- **The `bytes` crate decision.** Raised in Stage 1f since `Key`/`Value` are where it'd land first, but it also touches `RespBody::Bulk` (Stage 2) and `Client::inbuf`/`outbuf` (Stage 3). Decide once, before starting whichever stage you'd touch first; treat the actual migration (if you choose to adopt) as its own piece of work, not a rider on a smaller fix.

---

## Suggested execution order

1. **Stage 0** — mandatory first; nothing else has a safety net until this lands.
2. Decide what to do with the **bugs list** — independent of the refactor, but worth a deliberate decision rather than leaving them for whenever they're next tripped over.
3. **Stage 1 (db.rs)**, scopes 1a→1g in order.
4. **Stage 2 (resp.rs)** — small, quick.
5. **Stage 3 (client.rs)**, 3a→3e.
6. **Stage 4 (command/*)**, 4a→4d.
7. **Stage 5 (networking.rs)**, 5a→5g — the largest remaining stage, do last among the "real" layers since it depends conceptually on client.rs's `process_request` span (5e) already existing.
8. **Stage 6 (cli/main/lib)** — can genuinely slot in anywhere; lowest stakes in the plan.

Within any stage, Organize (if present) goes first and Method order goes last; the middle five scopes can be reordered by what you find most pressing without breaking anything.
