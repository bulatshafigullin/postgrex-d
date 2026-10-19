/// Integration tests. Usage: PGD_URL=postgres://user:pw@host:port/db integration
@safe:

import postgrex;
import vibe.core.core;
import std.datetime;
import std.datetime.stopwatch : StopWatch, AutoStart;
import std.process : environment;
import std.stdio;
import std.typecons : Nullable;
import core.time;

int failures;

void report(string what, Exception e) nothrow
{
    try writeln(what, ": ", e.msg);
    catch (Exception) {}
}

Task spawn(void delegate() @safe dg)
{
    return runTask({
        try dg();
        catch (Exception e) { failures++; report("TASK EXCEPTION", e); }
    });
}
int checks;

void check(bool ok, string what, string file = __FILE__, size_t line = __LINE__)
{
    checks++;
    if (!ok)
    {
        failures++;
        writefln("FAIL %s:%s %s", file, line, what);
    }
}

void expectThrows(E : Throwable = Exception)(scope void delegate() @safe dg, string what, string file = __FILE__, size_t line = __LINE__)
{
    bool threw;
    try dg();
    catch (E) threw = true;
    check(threw, "expected exception: " ~ what, file, line);
}

struct User { int id; string name; Nullable!int age; }

void testTypes(PgConnection db)
{
    auto r = db.query("select 1::int2 a, 2::int4 b, 3::int8 c, 1.5::float8 d, true e, 'héllo wörld ✓'::text f, " ~
                      "null::int g, '\\xdeadbeef'::bytea h, 12.50::numeric i, '{1,2,NULL}'::int[] j, " ~
                      "'{\"a b\",\"c\\\"d\",x}'::text[] k, '{{1,2},{3,4}}'::int[] l, " ~
                      "'2024-03-05 06:07:08.123456+02'::timestamptz m, '2024-03-05 06:07:08.5'::timestamp n, " ~
                      "'2024-03-05'::date o, '{\"k\":[1,2]}'::jsonb p, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid q");
    auto row = r[0];
    check(row.get!short("a") == 1 && row.get!int("b") == 2 && row.get!long("c") == 3, "ints");
    check(row.get!double("d") == 1.5, "float");
    check(row.get!bool("e"), "bool");
    check(row.get!string("f") == "héllo wörld ✓", "utf8");
    check(row["g"].isNull, "null");
    check(row.get!(Nullable!int)("g").isNull, "nullable null");
    check(row.get!(ubyte[])("h") == [0xde, 0xad, 0xbe, 0xef], "bytea");
    check(row.get!string("i") == "12.50" && row.get!double("i") == 12.5, "numeric");
    check(row["j"].items.length == 3 && row["j"].items[2].isNull, "int array null");
    check(row.get!(Nullable!int[])("j")[1].get == 2, "nullable array");
    check(row.get!(string[])("k") == ["a b", "c\"d", "x"], "text array quoting");
    check(row.get!(int[][])("l") == [[1, 2], [3, 4]], "nested array");
    auto m = row.get!SysTime("m");
    check(m == SysTime(DateTime(2024, 3, 5, 4, 7, 8), 1234560.hnsecs, UTC()), "timestamptz " ~ m.toISOExtString());
    check(row.get!SysTime("n") == SysTime(DateTime(2024, 3, 5, 6, 7, 8), 5000000.hnsecs, UTC()), "timestamp");
    check(row.get!Date("o") == Date(2024, 3, 5), "date");
    check(row.get!string("p") == `{"k": [1, 2]}`, "jsonb");
    check(row.get!string("q") == "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "uuid");
    expectThrows!PgTypeException({ row.get!int("g"); }, "null to int");
    expectThrows!PgTypeException({ row.get!int("f"); }, "text to int");
}

void testParams(PgConnection db)
{
    ubyte[] blob = [0, 1, 2, 255];
    auto r = db.query("select $1::int, $2::text, $3::bool, $4::bytea, $5::int[], $6::text[], $7::timestamptz, $8::float8, $9::int",
                      42, "it's \"quoted\" \\ ✓", true, blob, [1, 2, 3], [Nullable!string("a,b"), Nullable!string("c d"), Nullable!string.init],
                      SysTime(DateTime(2020, 1, 2, 3, 4, 5), UTC()), 2.25, null);
    auto row = r[0];
    check(row.get!int(0) == 42, "p int");
    check(row.get!string(1) == "it's \"quoted\" \\ ✓", "p text");
    check(row.get!bool(2), "p bool");
    check(row.get!(ubyte[])(3) == blob, "p bytea");
    check(row.get!(int[])(4) == [1, 2, 3], "p int[]");
    check(row[5].items[0].get!string == "a,b", "p text[]");
    check(row[5].items[2].isNull, "p text[] null");
    check(row.get!SysTime(6) == SysTime(DateTime(2020, 1, 2, 3, 4, 5), UTC()), "p tstz");
    check(row.get!double(7) == 2.25, "p float");
    check(row[8].isNull, "p null");
    // text containing a single quote must not allow injection
    db.exec("create temp table inj(v text)");
    db.exec("insert into inj values ($1)", "x'); drop table inj; --");
    check(db.query("select count(*) from inj").scalar!long == 1, "no injection");
}

void testErrors(PgConnection db)
{
    try { db.query("select 1/0"); check(false, "div by zero"); }
    catch (PgServerException e) { check(e.code == "22012", "sqlstate " ~ e.code); }
    check(db.connected, "still connected after error");
    check(db.query("select 7").scalar!int == 7, "usable after error");
    expectThrows!PgServerException({ db.query("selec 1"); }, "syntax error");
    check(db.query("select 8").scalar!int == 8, "usable after syntax error");
    auto rs = db.simpleQuery("select 1 a; select 'x' b, 2 c; create temp table tt(i int)");
    check(rs.length == 3 && rs[1][0].get!string("b") == "x", "simple multi");
    expectThrows!PgServerException({ db.simpleQuery("select 1; select 1/0; select 3"); }, "multi error");
    check(db.query("select 1").scalar!int == 1, "usable after multi error");
}

void testStatementCache(PgConnection db)
{
    db.exec("create temp table sc(a int)");
    foreach (i; 0 .. 5)
        db.exec("insert into sc values ($1)", i);
    check(db.query("select a from sc order by a").length == 5, "cached 1");
    db.exec("alter table sc add column b text default 'z'");
    // result type changed under a cached plan => server error, then recovery
    bool ok;
    try { db.query("select * from sc"); ok = true; } catch (PgServerException) {}
    auto r = db.query("select * from sc");
    check(r.columns.length == 2, "select * after alter");
    foreach (i; 0 .. 300)  // exceed cache size
        db.query("select " ~ i.to_ ~ " + $1::int", 1);
    check(db.query("select 5 + $1::int", 1).scalar!int == 6, "after eviction");
}
string to_(int i) { import std.conv; return i.to!string; }

void testTransactions(PgConnection db)
{
    db.exec("create temp table tx(i int)");
    db.transaction((c) { c.exec("insert into tx values (1)"); });
    check(db.query("select count(*) from tx").scalar!long == 1, "commit");
    expectThrows!Exception({
        db.transaction((c) { c.exec("insert into tx values (2)"); throw new Exception("boom"); });
    }, "rollback");
    check(db.query("select count(*) from tx").scalar!long == 1, "rolled back");
    check(db.txStatus == TxStatus.idle, "idle after rollback");
    auto v = db.transaction((c) {
        c.exec("insert into tx values (3)");
        try c.transaction((c2) { c2.exec("insert into tx values (4)"); c2.exec("select 1/0"); });
        catch (PgServerException) {}
        return c.query("select count(*) from tx").scalar!long;
    });
    check(v == 2, "nested savepoint rollback keeps outer, got " ~ v.to_str);
    check(db.query("select count(*) from tx").scalar!long == 2, "outer committed");
    // failed statement without savepoint puts txn in failed state
    expectThrows!PgServerException({ db.transaction((c) { c.exec("select 1/0"); }); }, "failing txn");
    check(db.txStatus == TxStatus.idle, "idle after failed txn");
}
string to_str(long l) { import std.conv; return l.to!string; }

void testMapping(PgConnection db)
{
    db.exec("create temp table users(id int primary key, name text, age int)");
    db.exec("insert into users values (1,'ann',30),(2,'bob',null)");
    auto us = db.query("select * from users order by id").as!User;
    check(us.length == 2 && us[0].name == "ann" && us[0].age.get == 30 && us[1].age.isNull, "struct mapping");
    int iterated; string names;
    foreach (row; db.query("select * from users order by id")) { iterated++; names ~= row.get!string("name"); }
    check(iterated == 2 && names == "annbob", "foreach over Result");
    check(db.exec("update users set age = 1") == 2, "rowsAffected");
    try { db.exec("insert into users values (1,'dup',1)"); }
    catch (PgServerException e) { check(e.code == "23505" && e.error.constraint == "users_pkey", "unique violation fields"); }
}

void testStreaming(PgConnection db)
{
    long sum;
    string[] kept;
    int nulls;
    auto n = db.queryEach("select i, 'row ' || i, case when i % 3 = 0 then null else 1 end from generate_series(1, $1::int) i",
        (Row r) {
            sum += r.get!int(0);
            kept ~= r.get!string(1);          // must be copied: the row buffer is reused
            if (r.isNull(2)) nulls++;
        }, 1000);
    check(n == 1000 && sum == 500500, "queryEach rows/sum");
    check(kept[0] == "row 1" && kept[999] == "row 1000" && kept[500] == "row 501", "streamed strings are copies");
    check(nulls == 333, "streamed nulls");

    // exception in callback: rows are drained, connection stays usable, exception propagates
    int seen;
    expectThrows({
        db.queryEach("select generate_series(1, 100)", (Row r) {
            seen++;
            if (seen == 3) throw new Exception("stop");
        });
    }, "callback exception");
    check(seen == 3, "callback not called after exception");
    check(db.query("select 41 + 1").scalar!int == 42, "usable after callback exception");

    // results keep their data after further queries (strings alias an immutable per-result buffer)
    auto r1 = db.query("select 'alpha' a, '{x,\"y z\"}'::text[] b");
    auto s1 = r1[0].get!string("a");
    auto arr = r1[0].get!(string[])("b");
    foreach (i; 0 .. 50) db.query("select repeat('z', 10000) || $1::text", i);
    check(s1 == "alpha" && arr == ["x", "y z"], "results stay valid");

    // a message larger than the read buffer, followed by small ones
    auto big = db.query("select repeat('ab', 3000000) s");
    check(big[0].get!string("s").length == 6_000_000, "big message");
    check(db.query("select 1").scalar!int == 1, "small after big");
}

void testLru(PgConnection db)
{
    // statement cache of 4: the hot statement must survive eviction of cold ones
    auto o = ConnectOptions.fromUrl(environment.get("PGD_URL", "postgres://postgres:secret@127.0.0.1:54329/postgres"));
    o.statementCacheSize = 4;
    auto c = new PgConnection(o);
    c.connect();
    scope (exit) c.close();
    foreach (round; 0 .. 20)
    {
        check(c.query("select 'hot' || $1::int", round).scalar!string == "hot" ~ to_(round), "hot stmt");
        c.query("select " ~ to_(round) ~ " + $1::int", 1);
    }
}


string base() { return environment.get("PGD_URL", "postgres://postgres:secret@127.0.0.1:54329/postgres"); }

void testTls()
{
    static assert(tlsAvailable, "integration tests need the tls configuration");
    const certs = environment.get("PGD_CERTS", "/tmp/pgd_certs");
    bool isSsl(PgConnection c) { return c.query("select ssl from pg_stat_ssl where pid = pg_backend_pid()").scalar!bool; }

    auto o = ConnectOptions.fromUrl(base());
    o.sslMode = SslMode.disable;
    auto c = new PgConnection(o); c.connect();
    check(!c.encrypted && !isSsl(c), "sslmode=disable is plain");
    c.close();

    o.sslMode = SslMode.require;
    c = new PgConnection(o); c.connect();
    check(c.encrypted && isSsl(c), "sslmode=require encrypts");
    check(c.query("select repeat('x', 100000)").scalar!string.length == 100000, "large result over TLS");
    c.close();

    o.sslMode = SslMode.prefer;
    c = new PgConnection(o); c.connect();
    check(c.encrypted && isSsl(c), "sslmode=prefer picks TLS");
    c.close();

    o.sslMode = SslMode.verifyFull;
    o.host = "localhost";
    o.sslRootCert = certs ~ "/ca.crt";
    c = new PgConnection(o); c.connect();
    check(c.encrypted && isSsl(c), "verify-full with right CA and host name");
    c.close();

    o.host = "127.0.0.1";
    c = new PgConnection(o); c.connect();
    check(c.encrypted, "verify-full by IP (SAN)");
    c.close();

    o.sslRootCert = certs ~ "/other.crt";
    expectThrows({ auto b = new PgConnection(o); b.connect(); }, "verify-full with the wrong CA must fail");
    o.sslMode = SslMode.verifyCa;
    expectThrows({ auto b = new PgConnection(o); b.connect(); }, "verify-ca with the wrong CA must fail");
    o.sslRootCert = null;
    expectThrows({ auto b = new PgConnection(o); b.connect(); }, "verify-ca needs a root cert");

    // URL parsing
    auto u = ConnectOptions.fromUrl("postgres://u:p@h/d?sslmode=verify-full&sslrootcert=%2Ftmp%2Fca.crt");
    check(u.sslMode == SslMode.verifyFull && u.sslRootCert == "/tmp/ca.crt", "sslmode URL parsing");
}

void testCancel()
{
    auto o = ConnectOptions.fromUrl(base());
    o.statementTimeout = 300.msecs;
    auto db = new PgConnection(o);
    db.connect();
    scope (exit) db.close();
    auto sw = StopWatch(AutoStart.yes);
    try { db.query("select pg_sleep(10)"); check(false, "should time out"); }
    catch (PgServerException e) { check(e.code == "57014", "query_canceled sqlstate " ~ e.code); }
    check(sw.peek < 3.seconds, "timeout is prompt: " ~ sw.peek.toString);
    check(db.connected && db.txStatus == TxStatus.idle, "connection usable after timeout");
    check(db.query("select 5").scalar!int == 5, "query after timeout");
    // a fast command is not affected by the armed timer
    foreach (i; 0 .. 20) db.query("select $1::int", i);
    sleep(500.msecs);
    check(db.query("select 6").scalar!int == 6, "late timer does not cancel next command");

    // timeouts work for simple queries and inside transactions too
    try { db.simpleQuery("select pg_sleep(10)"); check(false, "simple should time out"); }
    catch (PgServerException e) { check(e.code == "57014", "simple query cancelled"); }
    expectThrows({ db.transaction((c) { c.query("select pg_sleep(10)"); }); }, "txn timeout");
    check(db.txStatus == TxStatus.idle, "idle after cancelled transaction");

    // explicit cancel() from another fiber
    auto db2 = new PgConnection(base());
    db2.connect();
    scope (exit) db2.close();
    spawn({ sleep(200.msecs); db2.cancel(); });
    sw.reset();
    try { db2.query("select pg_sleep(10)"); check(false, "should be cancelled"); }
    catch (PgServerException e) { check(e.code == "57014", "explicit cancel"); }
    check(sw.peek < 3.seconds, "explicit cancel is prompt");
    check(db2.query("select 7").scalar!int == 7, "usable after explicit cancel");
    db2.cancel(); // idle: harmless
    check(db2.query("select 8").scalar!int == 8, "usable after idle cancel");
}

void testCopy(PgConnection db)
{
    db.exec("create temp table cp(id int, txt text, n int, arr int[])");
    auto n = db.copyIn("copy cp from stdin", (PgConnection.CopyWriter cw) {
        cw.row(1, "plain", 10, [1, 2]);
        cw.row(2, "tab\there\nnewline\\backslash\rcr", null, [3]);
        cw.row(3, "unicode ✓ ключ", 30, null);
        cw.row(4, Nullable!string.init, 40, new int[0]);
        foreach (i; 5 .. 1005) cw.row(i, "bulk", i, [i, i + 1]);
    });
    check(n == 1004, "copyIn rows " ~ n.to_str);
    auto r = db.query("select * from cp order by id");
    check(r.length == 1004, "copyIn stored");
    check(r[1].get!string("txt") == "tab\there\nnewline\\backslash\rcr", "copy escaping round trip");
    check(r[1]["n"].isNull && r[3]["txt"].isNull && r[2]["arr"].isNull, "copy NULLs");
    check(r[0].get!(int[])("arr") == [1, 2] && r[2].get!string("txt") == "unicode ✓ ключ", "copy values");

    // raw CSV through write()
    db.exec("create temp table cp2(a int, b text)");
    db.copyIn("copy cp2 from stdin (format csv)", (PgConnection.CopyWriter cw) {
        cw.write("1,\"hello, world\"\n");
        cw.write(cast(const(ubyte)[]) "2,\"multi\nline\"\n");
    });
    check(db.query("select count(*) from cp2").scalar!long == 2, "csv copyIn");

    // callback failure aborts the copy and rethrows
    db.exec("create temp table cp3(a int)");
    expectThrows({
        db.copyIn("copy cp3 from stdin", (PgConnection.CopyWriter cw) {
            cw.row(1); cw.row(2);
            throw new Exception("producer failed");
        });
    }, "copyIn producer exception");
    check(db.query("select count(*) from cp3").scalar!long == 0, "nothing loaded after abort");
    check(db.txStatus == TxStatus.idle, "idle after aborted copy");

    // server-side error (bad data) surfaces as PgServerException
    try
    {
        db.copyIn("copy cp3 from stdin", (PgConnection.CopyWriter cw) { cw.row("not a number"); });
        check(false, "bad copy data");
    }
    catch (PgServerException e) { check(e.code == "22P02", "copyIn data error " ~ e.code); }
    check(db.query("select 1").scalar!int == 1, "usable after copy error");

    // not a COPY statement
    expectThrows({ db.copyIn("select 1", (PgConnection.CopyWriter cw) {}); }, "copyIn on non-COPY");
    expectThrows({ db.copyOut("select 1", (const(ubyte)[] d) {}); }, "copyOut on non-COPY");
    check(db.query("select 2").scalar!int == 2, "usable after wrong copy");

    // copyOut
    ubyte[] got;
    auto m = db.copyOut("copy (select i, 'v' || i from generate_series(1, 5) i) to stdout", (const(ubyte)[] d) { got ~= d; });
    check(m == 5 && got == cast(const(ubyte)[]) "1\tv1\n2\tv2\n3\tv3\n4\tv4\n5\tv5\n", "copyOut data");

    // sink failure cancels the rest of a huge COPY quickly
    auto sw = StopWatch(AutoStart.yes);
    size_t chunks;
    expectThrows({
        db.copyOut("copy (select i, md5(i::text) from generate_series(1, 20000000) i) to stdout", (const(ubyte)[] d) {
            if (++chunks == 3) throw new Exception("enough");
        });
    }, "copyOut sink exception");
    check(sw.peek < 10.seconds, "copyOut cancel is prompt: " ~ sw.peek.toString);
    check(db.query("select 3").scalar!int == 3, "usable after copyOut cancel");

    // inside a transaction, and rolled back
    db.exec("create temp table cp5(a int)");
    expectThrows({
        db.transaction((c) {
            c.copyIn("copy cp5 from stdin", (PgConnection.CopyWriter cw) { cw.row(1); cw.row(2); });
            throw new Exception("rollback please");
        });
    }, "txn with copy");
    check(db.query("select count(*) from cp5").scalar!long == 0, "copy rolled back");
}

void testNotify(PgConnection db, ConnectOptions o)
{
    db.exec("listen chan");
    auto other = new PgConnection(o);
    other.connect();
    scope (exit) other.close();
    spawn({ sleep(100.msecs); other.exec("notify chan, 'hello'"); });
    auto n = db.waitForNotification(3.seconds);
    check(!n.isNull && n.get.channel == "chan" && n.get.payload == "hello", "notify");
    check(db.waitForNotification(100.msecs).isNull, "notify timeout");
    check(db.query("select 1").scalar!int == 1, "usable after notify");
}

void testPool(ConnectOptions o)
{
    auto pool = new PgPool(o, 8);
    auto sw = StopWatch(AutoStart.yes);
    int done;
    Task[] tasks;
    foreach (i; 0 .. 8)
        tasks ~= spawn({
            auto r = pool.query("select pg_sleep(0.4), $1::int as i", i);
            check(r[0].get!int("i") == i, "pool result");
            done++;
        });
    foreach (t; tasks) t.join();
    auto el = sw.peek;
    check(done == 8, "all fibers finished");
    check(el < 1500.msecs, "fibers run concurrently: " ~ el.toString);
    writeln("8 x pg_sleep(0.4) in parallel fibers: ", el);

    // more fibers than connections: they must queue, not fail
    auto small = new PgPool(o, 2);
    done = 0; tasks = null;
    foreach (i; 0 .. 6)
        tasks ~= spawn({ small.query("select pg_sleep(0.1)"); done++; });
    foreach (t; tasks) t.join();
    check(done == 6, "queued fibers finished");
    small.closeAll();

    // transaction on pool; interleaved fibers must not share a connection mid-transaction
    pool.exec("drop table if exists pooltx");
    pool.exec("create table pooltx(i int)");
    tasks = null;
    foreach (i; 0 .. 4)
        tasks ~= spawn({
            pool.transaction((c) {
                c.exec("insert into pooltx values ($1)", i);
                sleep(50.msecs);
                c.exec("insert into pooltx values ($1)", i + 100);
            });
        });
    foreach (t; tasks) t.join();
    check(pool.query("select count(*) from pooltx").scalar!long == 8, "pool transactions");
    pool.exec("drop table pooltx");

    // reconnect after a killed backend
    PgConnection victim;
    {
        auto c = pool.lock();
        victim = c;
        auto pid = c.backendPid;
        auto c2 = pool.lock();
        c2.exec("select pg_terminate_backend($1)", pid);
    }
    sleep(100.msecs);
    bool recovered;
    foreach (attempt; 0 .. 3)
    {
        try { pool.query("select 1"); recovered = true; break; }
        catch (PgException) {}
    }
    check(recovered, "pool recovers after terminated backend");
    victim.close();
    pool.closeAll();
}

void run()
{
    auto o = ConnectOptions.fromUrl(environment.get("PGD_URL", "postgres://postgres:secret@127.0.0.1:54329/postgres"));
    check(o.port != 0 && o.user.length > 0, "url");
    auto db = new PgConnection(o);
    db.connect();
    check(db.connected && db.backendPid > 0, "connect");
    writeln("server ", db.serverVersion);
    string[] notices;
    db.onNotice = (PgNotice n) { notices ~= n.message; };
    db.simpleQuery("do $$ begin raise notice 'hi'; end $$");
    check(notices == ["hi"], "notice callback");
    testTypes(db);
    testParams(db);
    testErrors(db);
    testStatementCache(db);
    testTransactions(db);
    testMapping(db);
    testStreaming(db);
    testCopy(db);
    testLru(db);
    testNotify(db, o);
    db.close();
    check(!db.connected, "closed");
    expectThrows!PgException({ auto b = new PgConnection("postgres://postgres:wrong@127.0.0.1:54329/postgres"); b.connect(); }, "bad password");
    testTls();
    testCancel();
    testPool(o);
}

int main()
{
    runTask({
        try run();
        catch (Exception e) { failures++; report("EXCEPTION", e); }
        exitEventLoop();
    });
    runEventLoop();
    writefln("%s checks, %s failures", checks, failures);
    return failures ? 1 : 0;
}
