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
    check(db.exec("update users set age = 1") == 2, "rowsAffected");
    try { db.exec("insert into users values (1,'dup',1)"); }
    catch (PgServerException e) { check(e.code == "23505" && e.error.constraint == "users_pkey", "unique violation fields"); }
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
    testNotify(db, o);
    db.close();
    check(!db.connected, "closed");
    expectThrows!PgException({ auto b = new PgConnection("postgres://postgres:wrong@127.0.0.1:54329/postgres"); b.connect(); }, "bad password");
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
