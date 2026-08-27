# ii-drive fork — parallel-upload changes vs grammers 0.10.0

Scope: how this project's upload path diverges from upstream
`grammers-client`/`grammers-mtsender` 0.10.0 to parallelize big-file
uploads, and the upstream APIs it depends on. For maintainers of the fork.

## 1. Upstream grammers 0.10.0 behavior (the baseline)

### Sender pool: one connection per DC, one request loop

`grammers-mtsender` (`src/sender_pool.rs`) is the raw RPC layer behind
every `grammers_client::Client`:

- `SenderPool::new(session: Arc<S>, api_id)` spawns a single
  `SenderPoolRunner::run()` task that owns a `Vec<ConnectionInfo>` — at
  most **one `Connection` per datacenter**.
- Callers interact through the cheap-cloneable `SenderPoolHandle`
  (`mpsc::UnboundedSender<Request>`).
- `SenderPoolHandle::invoke_in_dc(dc_id, body) -> InvokeResponse` sends a
  `Request::Invoke` into the runner's single `mpsc` inbox. The runner's
  `process_request` loop is fully serialized: it finds the one connection
  for `dc_id` (`create_connection` on first use) and pushes the body onto
  that connection's `rpc_tx` channel.
- Each DC connection runs one `run_sender` task: `tokio::select!` between
  `sender.step()` (reading responses/updates) and `rpc_rx.recv()` (queued
  bodies). Within one connection, MTProto `enqueue_body` provides
  pipelining, but there is **one TCP socket per DC** and every RPC for a
  DC passes through the runner loop's single channel.
- Key facts for the fork:
  - `SenderPoolFatHandle.thin` is the `SenderPoolHandle`; the runner owns
    the session (`Arc<ErasedSession>`) and is creator of connections.
  - `SenderPoolRunner::create_connection` calls `session.set_dc_option`
    (persisting the auth key) and `run_sender`.
  - `connect_sender` does a NORMAL (non-auth) handshake when
    `dc_option.auth_key` is `None`, then runs `InvokeWithLayer(InitConnection(GetConfig))`.

### Client upload: 4 workers, one connection

`grammers_client::Client::upload_stream` (`src/client/files.rs`):

- Constants: `MAX_CHUNK_SIZE = 512 * 1024`, `MIN_CHUNK_SIZE = 4 * 1024`,
  `BIG_FILE_SIZE = 10 * 1024 * 1024`, `WORKER_COUNT = 4`,
  `FILE_MIGRATE_ERROR = 303`.
- For `size > BIG_FILE_SIZE` it spawns `WORKER_COUNT = 4` tasks that pull
  512 KiB chunks from a `PartStream` (one `AsyncMutex` guard around a
  shared reader) and each calls `Client::invoke(&SaveBigFilePart)`. Those
  four workers share **one `Client` → one `SenderPoolHandle` → one
  connection per DC**; the parallelism is pipelining on a single socket,
  not multiple sockets.
- A `FILE_MIGRATE` (303)/`AUTH_KEY_UNREGISTERED` on upload is surfaced to
  the caller; grammers' own download path (`DownloadIter`, files.rs) reacts
  to 303 by calling `copy_auth_to_dc` and retrying — the upload path does
  not.

### The bottleneck this fork exists to remove

Because `SaveBigFilePart` for one `file_id` must all reach the same DC,
and grammers opens at most one connection to that DC, a large upload
through the stock library is bound by a single TCP socket to the DC. The
official Telegram client uses many concurrent connections/sessions to the
same DC; that is the throughput gap (measured ~6-7x slower in the
investigation that motivated this work).

## 2. Fork divergences

### 2.1 Multiple connections to one DC via extra sender pools

Files: `src/tg/session.rs`, `src/tg/mod.rs`.

- Each `TgManager` connection now opens, besides the main
  `SenderPoolHandle` (`Conn.pool`), **`AUX_UPLOAD_POOLS = 3` extra
  `SenderPoolHandle`s** (`Conn.aux_pools`), each a fully independent
  `SenderPool` on its **own copied session file** `{session_path}.aux{idx}`.
- Rationale for per-aux copies: an upstream `SenderPool` shares one
  `Session` and writes DC options / auth keys during normal operation;
  concurrent `SenderPool` runners sharing one SQLite session file would
  hit `database is locked` / corrupted state. A copy gives each aux pool
  independent state while carrying the same auth key (copied after
  `home_dc_id()` is known, i.e. after first connect).
- `Conn::all_pools()` iterates main + aux (`once(pool).chain(aux_pools)`),
  so `pools.len() >= 1` always; upload workers round-robin across them.
- Lifecycle: `aux_runners` are aborted in `Conn::close` before the main
  runner; aux session copies are removed with a 10x / 100ms retry on
  Windows (an aborted runner may briefly hold the file handle). A partial
  aux-setup failure calls `aux_cleanup` (`src/tg/session.rs`) to tear down
  already-spawned runners and remove copies before returning an error.

### 2.2 `pool_target` returns the connection context

File: `src/tg/bots.rs`.

- `PoolTarget` now returns
  `(client, peer, bot_name, dc_id, pools: Vec<SenderPoolHandle>)` where
  `dc_id` and `pools` are bound to the **same session as `client`** —
  the chosen bot's `Conn.dc_id` + `all_pools()`, or the owner's via
  `upload_pools()`.
- Why it matters: a `file_id` created by `SaveBigFilePart` is bound to the
  session that uploaded it. Posting that `InputFileBig` through a
  *different* session/DC (e.g. owner-upload, bot-post) references a
  non-existent file. Keeping `dc_id`/`pools` and `client` from one branch
  guarantees `saveBigFilePart → send_message` are session-consistent.
- `src/stream.rs` (download path) discards the new tuple fields
  (`_dc_id`, `_pools`); downloads go through grammers' `iter_download`,
  which handles its own migration.

### 2.3 `parallel_upload_stream` (replaces `upload_stream` for big files)

File: `src/tg/transfer.rs`.

```
async fn parallel_upload_stream<S: AsyncRead + Unpin>(
    client: &Client, reader: &mut S,
    size: usize, name: String,
    dc_id: i32,
    pools: &[SenderPoolHandle],
) -> Result<Uploaded, io::Error>
```

- Files `size <= BIG_FILE_THRESHOLD (10 MiB)` fall back to grammers'
  `client.upload_stream` (negligible parallelism benefit for few parts).
- Larger files use the parallel path:

  - `file_id: i64 = rand::random()`, `total_parts = size.div_ceil(512KiB)`.
  - `UPLOAD_WORKERS = 16` tasks, each pinned to one pool handle by
    round-robin (`pools[idx % pools.len()]`) → 16 workers spread across up
    to 4 sockets (main + 3 aux) to the upload DC.
  - A **bounded** channel `capacity = UPLOAD_WORKERS + 1` (17) of
    `(part, Vec<u8>)` caps peak in-flight memory (~8 MiB of payload plus
    per-worker buffers).
  - Producer reads the stream into 512 KiB chunks inline, `send`s into the
    channel (yielding so workers run), then closes it. Last-part EOF with a
    short final chunk is allowed; premature EOF is an error.
  - Each worker calls `SenderPoolHandle::invoke_in_dc(dc, body)` with a
    serialized `tl::functions::upload::SaveBigFilePart`, decodes the
    `Bool` response, and errors on rejection.
  - Constructor id is produced via `tl::Serializable::to_bytes` /
    `Deserializable::from_bytes` (grammers `tl`), so no dependency on
    grammers' private upload plumbing.

- FILE_MIGRATE (303) handling — shared DC:
  - A shared `Arc<AtomicI32> current_dc` (seeded with the initial `dc_id`)
    is read (`load(Relaxed)`) by every worker before each `invoke_in_dc`,
    and updated (`swap`) once when any worker observes a 303. This keeps
    all 16 workers on one DC for a given `file_id`, avoiding a mixed-DC
    assemble (Telegram assembles a big-file only from parts on one DC).
  - `MAX_MIGRATE_RETRIES = 3` caps per-worker redirect loops against a
    misbehaving server.

- AUTH_KEY_UNREGISTERED handling:
  - If `invoke_in_dc` returns an RPC with `name == "AUTH_KEY_UNREGISTERED"`
    (the session has no auth key for the target DC), the worker fails the
    whole upload with a descriptive error. This is a deliberate
    fail-fast (no partial/contradictory file is posted) rather than a
    silent corruption.
  - Known limitation: upstream `grammers_client::Client::copy_auth_to_dc`
    is `pub(crate)`, so the fork cannot call it. It is nonetheless
    implemented entirely with public RPCs (`auth.ExportAuthorization` /
    `auth.ImportAuthorization`); implementing that public-API equivalent
    would let cross-DC migration succeed instead of erroring (see §4).

- `bench_upload`: the bot run is `client.upload_stream` (grammers), the
  owner run forces the owner session and uses `parallel_upload_stream`, so
  `bot_secs` vs `owner_secs` compares a real owner-vs-bot throughput on
  the same buffer.

## 3. Behaviors and invariants

- **Session/DC consistency:** for any upload, `client` and the
  `dc_id`/`pools` handed to `parallel_upload_stream` come from the same
  `pool_target` branch (one bot, or the owner). The resulting `Uploaded`
  is posted with that same `client`. No cross-session `file_id`.
- **Single-DC assembly:** after any 303, all workers converge on the shared
  `current_dc` before their next part, so parts of one `file_id` stay on
  one DC.
- **Bounded memory / bounded retries:** channel length and
  `MAX_MIGRATE_RETRIES` bound worst-case resource use; a 303 storm or an
  AUTH_KEY_UNREGISTERED terminates the upload cleanly instead of hanging.
- **No orphans on normal failure:** if `SaveBigFilePart` returns `false`
  or an error, the worker error propagates to `parallel_upload_stream`,
  which never constructs `Uploaded` for posting — `send_message` is never
  reached, so no partial file is referenced.

## 4. Known gaps / follow-ups

1. **Cross-DC auth copy is not implemented.** `AUTH_KEY_UNREGISTERED` on a
   genuinely new DC fails the upload with a clear error. grammers' own
   download path migrates successfully (`copy_auth_to_dc`), so a
   cross-DC file downloads but does not re-upload (asymmetric). A hand-rolled
   `auth.ExportAuthorization` on the home DC followed by
   `auth.ImportAuthorization` into the target DC (both public `tl`
   functions via `invoke`/`invoke_in_dc`) would close this. Guard to run
   once per target DC.
2. **`order_shuffle` on `AtomicI32` visibility** uses `Ordering::Relaxed`;
   appropriate for a one-way monotone switch, but if the DC needs
   cross-thread happens-before for session writes, a stronger ordering
   (e.g. `Acquire`/`Release` on `current_dc`) should be reconsidered.
   (Grammers persists the auth key in `create_connection` itself.)
3. **Concurrency amplification:** `UPLOAD_WORKERS (16)` × up to 64 parts
   can spawn thousands of tasks. Each part>10MiB already fans 16 workers;
   at the part level uploads already run concurrently across parts by
   design. No global cap ties part-level concurrency to socket count; if
   memory or scheduler pressure appears, add a semaphore.
4. **Frontend/client-side** changes are out of scope for this SPEC (they
   are in `web/src/lib/api.ts`, independent of grammers).

## 5. Upstream APIs the fork depends on (stability surface)

These exact upstream items are load-bearing; verify them when upgrading
grammers:

- `grammers_client::sender::SenderPoolHandle { invoke_in_dc, quit, thin }`
  (via `grammers_client::sender` re-export).
- `grammers_client::sender::SenderPool::new` (independent per-connection
  pools are the whole aux design).
- `grammers_session::sqlite::SqliteSession` + `home_dc_id()` (must return a
  value before aux copies are viable).
- `tl` serialization (`to_bytes`/`from_bytes`), `tl::functions::upload::SaveBigFilePart`,
  `tl::types::InputFileBig`, `Uploaded::from_raw`.
- `InvocationError::Rpc(err)` with `err.code` (`303`) and `err.name`
  (`"AUTH_KEY_UNREGISTERED"`) for migration/auth decisions.
- `Client::invoke_in_dc` is not used by the fork's parallel path (workers
  use `SenderPoolHandle::invoke_in_dc` directly), but `Client::invoke` /
  `client.upload_stream` remain used for small files and the bench bot run.