# postgrex-d

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![D](https://img.shields.io/badge/D-LDC%20%7C%20DMD-red)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-tested%20on%2014-336791)

A PostgreSQL driver for [D](https://dlang.org), written from scratch against the wire protocol (v3) and
modelled on Elixir's [Postgrex](https://github.com/elixir-ecto/postgrex).

- **No libpq.** Pure D, talks to the server directly.
- **Fiber-aware.** Built on [vibe-core](https://github.com/vibe-d/vibe-core) sockets: a query only suspends
  the calling fiber, never the event loop or the thread. Works in plain vibe-core programs and in
  [vibe.d](https://vibed.org) request handlers.
- **`@safe` API**, injection-safe by construction (parameters travel separately from SQL).
- **GC-friendly.** One buffer per result, lazy decoding, and a zero-buffering streaming API.

## Contents

- [Installation](#installation)
- [Quick start](#quick-start)
- [Connecting](#connecting)
- [Running queries](#running-queries)
- [Reading results](#reading-results)
- [Transactions](#transactions)
- [Connection pool and vibe.d](#connection-pool-and-vibed)
- [Error handling](#error-handling)
- [LISTEN / NOTIFY](#listen--notify)
- [Type mapping](#type-mapping)
- [Memory and performance](#memory-and-performance)
- [Best practices](#best-practices)
- [Limitations](#limitations)
- [Development and testing](#development-and-testing)
- [License](#license)

## Installation

Requires a D compiler (LDC or DMD) and [dub](https://dub.pm). The package is not on the dub registry
yet; depend on it through git, pinned to a commit:

```sdl
dependency "postgrex-d" repository="git+https://github.com/bulatshafigullin/postgrex-d.git" version="122714e4265170b9fab62dda533908a62701496f"
```

or, for local development, `dependency "postgrex-d" path="../postgrex-d"`.

vibe-core is pulled in automatically. In a vibe.d project you already have it.

## Quick start

```d
import postgrex;
import vibe.core.core;
import std.stdio;

void main()
{
    // vibe-core sockets must be used from inside the event loop
    runTask({
        auto db = new PgConnection("postgres://user:secret@localhost:5432/mydb");
        db.connect();
        scope (exit) db.close();

        auto r = db.query("select id, name from users where age > $1", 18);
        foreach (row; r)
            writeln(row.get!int("id"), " ", row.get!string("name"));

        exitEventLoop();
    });
    runEventLoop();
}
```

## Connecting

```d
// from a URL
auto db = new PgConnection("postgres://user:pw@host:5432/db?application_name=api&search_path=app");

// or explicitly
ConnectOptions o;
o.host = "db.internal";
o.user = "app";
o.password = "secret";
o.database = "app";
o.connectTimeout = 5.seconds;
o.queryTimeout = 30.seconds;       // per-read timeout, Duration.max = none
o.statementCacheSize = 128;        // 0 disables the prepared-statement cache
o.parameters["search_path"] = "app";
auto db2 = new PgConnection(o);
```

Supported authentication: trust, cleartext, MD5, SCRAM-SHA-256 (the server signature is verified).
Percent-encode special characters in URL passwords (`@` -> `%40`).

## Running queries

| Method | Use it for |
|---|---|
| `query(sql, args...)` | Parameterised statement, returns a `Result` |
| `exec(sql, args...)` | Same, returns the affected row count |
| `queryEach(sql, (Row r){...}, args...)` | Stream a large result without buffering it |
| `simpleQuery(sql)` | Several `;`-separated statements (no parameters), returns `Result[]` |
| `queryParams(sql, Param[])` | Runtime-sized parameter lists |

Parameters are `$1`, `$2`, ... and are sent separately from the SQL text:

```d
db.exec("insert into users(name, age, tags) values ($1, $2, $3)", "ann", 30, ["a", "b"]);
auto id = db.query("insert into users(name) values ($1) returning id", name).scalar!int;
```

**Never build SQL by concatenating user input.** If you must interpolate an identifier, use
`PgConnection.quoteIdent(name)`; values always go through parameters.

## Reading results

```d
auto r = db.query("select id, name, age from users order by id");

r.length;                         // number of rows
r.columns[0].name;                // column metadata
r.commandTag, r.rowsAffected;     // "SELECT 3", 3

auto row = r[0];
row.get!int("id");                // by name
row.get!string(1);                // by index
row.get!(Nullable!int)("age");    // NULL-safe
row.isNull("age");
row["age"];                       // dynamically typed PgValue

auto n = db.query("select count(*) from users").scalar!long;

struct User { int id; string name; Nullable!int age; }
User[] users = r.as!User;         // columns mapped to fields by name
```

`get!T` throws `PgTypeException` on NULL (unless `T` is `Nullable!X` or `PgValue`), on a type mismatch,
and on integer overflow.

### Streaming large results

`queryEach` keeps no rows in memory; use it for exports and big scans:

```d
long total;
db.queryEach("select id, payload from events where ts > $1", (Row r) {
    total += r.get!long("id");
    // Copy anything you want to keep: the Row is only valid inside the callback.
}, since);
```

An exception thrown by the callback is rethrown after the remaining rows are drained, so the
connection stays usable.

## Transactions

```d
db.transaction((c) {
    c.exec("update accounts set balance = balance - $1 where id = $2", 100, from);
    c.exec("update accounts set balance = balance + $1 where id = $2", 100, to);
});   // COMMIT; any exception => ROLLBACK and rethrow

auto id = db.transaction((c) => c.query("insert into t default values returning id").scalar!int);
```

Calling `transaction` inside a transaction creates a `SAVEPOINT`, so calls nest and an inner failure
can be caught without aborting the outer transaction. The connection is dropped (not reused) if the
rollback itself fails.

## Connection pool and vibe.d

`PgPool` hands every fiber its own connection. When all connections are busy, further fibers wait
without blocking the event loop. Broken connections are re-established on the next checkout.

```d
import postgrex;
import vibe.vibe;

PgPool db;

void main()
{
    db = new PgPool(environment.get("DATABASE_URL"), 10);   // max 10 connections
    auto router = new URLRouter;
    router.get("/users/:id", &getUser);
    listenHTTP(new HTTPServerSettings, router);
    runApplication();
}

void getUser(HTTPServerRequest req, HTTPServerResponse res) @safe
{
    auto r = db.query("select id, name from users where id = $1", req.params["id"].to!int);
    if (r.length == 0) { res.statusCode = 404; res.writeBody("not found"); return; }
    res.writeJsonBody(["id": r[0].get!int("id"), "name": r[0].get!string("name")]);
}
```

Use `pool.transaction((c) { ... })` for multi-statement work, or `auto c = pool.lock();` to hold one
connection for several calls (it returns to the pool when `c` leaves scope). A runnable app lives in
[`examples/vibe_app`](examples/vibe_app): eight simultaneous requests that each run `pg_sleep(0.5)`
finish in ~0.5 s on a single thread.

Pools and connections belong to the thread that created them (vibe-core rule); create one pool per
thread if you run vibe.d with worker threads.

## Error handling

All exceptions derive from `PgException`:

| Exception | Meaning | Connection afterwards |
|---|---|---|
| `PgServerException` | The server rejected the statement (`code`, `error.constraint`, `error.detail`, ...) | usable |
| `PgTypeException` | Conversion/encoding problem on the client | usable |
| `PgProtocolException` | Network or protocol failure | closed; a `PgPool` reconnects |

```d
try db.exec("insert into users(email) values ($1)", email);
catch (PgServerException e)
{
    if (e.code == "23505")   // unique_violation
        return conflict(e.error.constraint);
    throw e;
}
```

Server notices (`RAISE NOTICE`, warnings) go to `db.onNotice = (PgNotice n) { ... };`.

## LISTEN / NOTIFY

```d
db.exec("listen jobs");
auto n = db.waitForNotification(30.seconds);     // Nullable!Notification, null on timeout
if (!n.isNull) writeln(n.get.channel, ": ", n.get.payload);
```

Use a dedicated `PgConnection` (not a pooled one) for listening.

## Type mapping

| PostgreSQL | D (`get!T`) | D (parameter) |
|---|---|---|
| `bool` | `bool` | `bool` |
| `int2/4/8`, `oid` | `short`/`int`/`long`/... | any integral type |
| `float4/8` | `float`/`double` | `float`/`double` |
| `numeric` | `string`, `double`, integers | `string` (exact) |
| `text`, `varchar`, `json(b)`, `uuid` | `string` | `string` |
| `bytea` | `ubyte[]` | `ubyte[]` |
| `date` | `Date`, `SysTime` | `Date` |
| `timestamp` | `DateTime`, `SysTime` (UTC) | `DateTime` |
| `timestamptz` | `SysTime` (UTC) | `SysTime` |
| arrays (nested, with NULLs) | `T[]`, `T[][]`, `Nullable!T[]` | the same |
| any NULL | `Nullable!T`, `PgValue` | `null`, `Nullable!T` |

Notes: a `null` D string is an *empty string*, not SQL NULL; use `Nullable!string` or `null` literal.
`timestamp` values are interpreted as UTC. Unknown types come back as text.

## Memory and performance

- A `Result` is one immutable byte buffer plus a flat, pointer-free `Cell[]` index; cells are decoded
  lazily by `get!T`, so there is no per-cell GC object. `get!string` aliases the buffer, so a kept
  string keeps its whole result alive (`.idup` a small slice if you want to retain only that).
- `queryEach` streams through a reused buffer: constant memory regardless of result size.
- Incoming messages reuse a per-connection buffer, parameters are encoded straight into the outgoing
  buffer, and column metadata is shared between results of the same shape.
- Prepared statements are cached per connection (LRU) and sent in a single round trip with Bind/Execute.

200k rows x 8 mixed columns (`bench/bench.d`): 256 MB -> 42 MB held, GC pause 24 ms -> 1.5 ms;
streaming grows the heap by 0.6 MB. Build your application with `-O -release` (LDC) for production.

## Best practices

- **Use `PgPool` in servers**, one connection per concurrent operation. Never share a bare `PgConnection`
  between request handlers for transactions; use `pool.transaction` or `pool.lock()`.
- **Always pass values as parameters**, and set `queryTimeout` so a stuck query cannot hang a fiber forever.
- **Stream big result sets** with `queryEach` instead of `query`.
- **Keep transactions short** and do no network I/O (HTTP calls, etc.) inside them.
- **Prefer explicit column lists** over `select *`; a cached statement is re-prepared automatically
  after schema changes, but the one failing call raises a `PgServerException`, so retry idempotent reads.
- **Handle `PgServerException.code`** (SQLSTATE) instead of matching message text.
- Do not log `ConnectOptions` or URLs: they contain the password.
- Behind PgBouncer in *transaction* pooling mode, set `statementCacheSize = 0` (server-side prepared
  statements are not portable across backend connections).

## Limitations

TLS (`sslmode=require` raises an error), COPY, unix-domain sockets, binary result format, query
cancellation, and `SASLprep` for non-ASCII SCRAM passwords are not implemented yet. Contributions
welcome.

## Development and testing

```sh
# a throwaway server
initdb -D /tmp/pgd -U postgres --auth-host=scram-sha-256 --pwfile=<(echo secret)
pg_ctl -D /tmp/pgd -o "-p 54329" start

# integration tests (103 checks: types, params, errors, cache, transactions, notify, pool, streaming)
cd tests && PGD_URL=postgres://postgres:secret@127.0.0.1:54329/postgres dub run

# GC benchmark
PGD_URL=... dub run --single bench/bench.d --build=release
```

Layout: `source/postgrex/` -- `connection.d` (protocol state machine), `pool.d`, `result.d`, `types.d`
(decoding), `encode.d` (parameter encoding), `auth.d` (MD5/SCRAM), `wire.d` (framing), `error.d`.

## License

[MIT](LICENSE)
