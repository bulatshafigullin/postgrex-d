/+ dub.sdl:
    name "bench"
    dependency "postgrex-d" path=".."
+/
// GC pressure benchmark: PGD_URL=... dub run --single bench/bench.d
import postgrex;
import vibe.core.core;
import core.memory : GC;
import std.datetime;
import std.datetime.stopwatch : StopWatch, AutoStart;
import std.process : environment;
import std.stdio;

enum sql = "select i, i::bigint, i::float8 / 3, 'row number ' || i, (i % 2 = 0), " ~
           "timestamptz '2024-01-01' + (i || ' seconds')::interval, array[i, i + 1], md5(i::text) " ~
           "from generate_series(1, 200000) i";

void run()
{
    auto db = new PgConnection(environment.get("PGD_URL", "postgres://postgres:secret@127.0.0.1:54329/postgres"));
    db.connect();
    foreach (round; 0 .. 3)
    {
        GC.collect();
        const before = GC.stats().usedSize;
        const pBefore = GC.profileStats();
        auto sw = StopWatch(AutoStart.yes);
        auto r = db.query(sql);
        const tQuery = sw.peek;
        long sum;
        size_t chars;
        foreach (i; 0 .. r.length)
        {
            auto row = r[i];
            sum += row.get!long(0) + row.get!long(1) + row.get!int(4 - 4);
            sum += cast(long) row.get!double(2);
            chars += row.get!string(3).length + row.get!string(7).length;
            sum += row.get!bool(4) ? 1 : 0;
            sum += row.get!SysTime(5).year;
            sum += row.get!(long[])(6).length;
        }
        const tAll = sw.peek;
        const held = cast(long) GC.stats().usedSize - cast(long) before;
        const p = GC.profileStats();
        writefln("round %s: rows=%s query=%s total=%s held=%.1f MB collections=%s gcPause=%s (chk %s %s)",
                 round, r.length, tQuery, tAll, held / 1048576.0,
                 p.numCollections - pBefore.numCollections, p.totalCollectionTime - pBefore.totalCollectionTime, sum, chars);
    }
    static if (__traits(hasMember, PgConnection, "queryEach"))
    {
        GC.collect();
        const pBefore = GC.profileStats();
        const before = GC.stats().usedSize;
        auto sw = StopWatch(AutoStart.yes);
        long sum;
        db.queryEach(sql, (Row row) {
            sum += row.get!long(0) + row.get!long(1) + cast(long) row.get!double(2);
            sum += row.get!string(3).length + row.get!string(7).length;
            sum += row.get!bool(4) ? 1 : 0;
            sum += row.get!SysTime(5).year;
            sum += row.get!(long[])(6).length;
        });
        const p = GC.profileStats();
        writefln("streaming: total=%s heap growth=%.1f MB collections=%s gcPause=%s", sw.peek,
                 (cast(long) GC.stats().usedSize - cast(long) before) / 1048576.0,
                 p.numCollections - pBefore.numCollections, p.totalCollectionTime - pBefore.totalCollectionTime);
    }
    db.close();
}

void main()
{
    runTask({
        try run();
        catch (Exception e) { assert(0, e.msg); }
        exitEventLoop();
    });
    runEventLoop();
}
