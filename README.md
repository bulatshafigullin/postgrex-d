# postgrex-d

A PostgreSQL wire-protocol (v3) driver written from scratch in D, modelled on Elixir's
[Postgrex](https://github.com/elixir-ecto/postgrex). It talks to the server directly (no libpq) over
[vibe-core](https://github.com/vibe-d/vibe-core) sockets, so every blocking call only suspends the
current **fiber**. It works in plain vibe-core programs and inside **vibe.d** request handlers, and
the whole API is `@safe`.

## Features

- Startup and authentication: trust, cleartext, MD5, SCRAM-SHA-256 (server signature verified)
- Extended protocol with out-of-band parameters (`$1`, `$2`, ...), so no SQL injection by construction
- Per-connection prepared-statement cache (LRU-ish, evicted on error / schema change)
- Simple protocol for multi-statement scripts (`simpleQuery`)
- Decoding by type OID: ints, floats, bool, numeric, text, bytea, json/jsonb, uuid, date, timestamp,
  timestamptz, and (nested) arrays with NULLs
- Row access by index/name, `Nullable!T`, mapping rows to structs (`result.as!MyStruct`)
- Transactions with automatic nesting through savepoints
- `LISTEN` / `NOTIFY`, notice callbacks, structured server errors (`PgServerException.code`, constraint, ...)
- Fiber-aware connection pool (`PgPool`) with transparent reconnect of broken connections

Not implemented yet: TLS, COPY, unix sockets, binary result format, query cancellation.

## Usage

```d
import postgrex;

auto db = new PgConnection("postgres://user:pw@localhost:5432/mydb");
db.connect();
auto r = db.query("select id, name from users where age > $1", 18);
foreach (row; r) writeln(row.get!int("id"), " ", row.get!string("name"));

struct User { int id; string name; }
User[] users = db.query("select id, name from users").as!User;

db.transaction((c) {
    c.exec("insert into users(name) values ($1)", "ann");
});
```

### In vibe.d

```d
auto pool = new PgPool("postgres://...", 10);   // max 10 connections

void handler(HTTPServerRequest req, HTTPServerResponse res) @safe
{
    auto n = pool.query("select count(*) from todos").scalar!long;   // suspends only this fiber
    res.writeJsonBody(["count": n]);
}
```

A full app is in [`examples/vibe_app`](examples/vibe_app): eight simultaneous requests that each run
`pg_sleep(0.5)` complete in ~0.5 s on one thread.

Parameter mapping: D `string`, integers, floats, `bool`, `ubyte[]`, `SysTime`, `Date`, `DateTime`,
arrays, `Nullable!T` and `null` are encoded automatically. NULL is `null` or an empty `Nullable`
(a null D string is an empty string, not NULL).

## Tests

```
cd tests && PGD_URL=postgres://postgres:secret@127.0.0.1:5432/postgres dub run
```

Needs a running PostgreSQL (tested with 14). The suite covers types, parameters, errors, the statement
cache, nested transactions, LISTEN/NOTIFY, and the pool under concurrent fibers.

## License

MIT
