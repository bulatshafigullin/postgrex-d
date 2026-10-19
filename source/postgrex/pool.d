/// Fiber-safe connection pool.
module postgrex.pool;

@safe:

import postgrex.connection;
import postgrex.result;
import postgrex.types;
import vibe.core.connectionpool : ConnectionPool, LockedConnection;

/// Pool of `PgConnection`s. Each call (or `lock()`) checks out a connection for the current
/// fiber only; when all `maxConnections` are busy further fibers wait (they do not block the
/// event loop). Broken connections are re-established transparently on checkout.
final class PgPool
{
    private ConnectionPool!PgConnection pool;
    private ConnectOptions opts;

    this(ConnectOptions opts, uint maxConnections = 8)
    {
        this.opts = opts;
        pool = new ConnectionPool!PgConnection(() => new PgConnection(opts), maxConnections);
    }

    this(string url, uint maxConnections = 8)
    {
        this(ConnectOptions.fromUrl(url), maxConnections);
    }

    /// Check out a connection; it returns to the pool when the returned value goes out of scope.
    auto lock()
    {
        auto c = pool.lockConnection();
        if (!c.connected)
            c.connect();
        return c;
    }

    Result query(Args...)(string sql, auto ref Args args)
    {
        auto c = lock();
        return c.query(sql, args);
    }

    long queryEach(Args...)(string sql, scope void delegate(Row) @safe dg, auto ref Args args)
    {
        auto c = lock();
        return c.queryEach(sql, dg, args);
    }

    long exec(Args...)(string sql, auto ref Args args)
    {
        auto c = lock();
        return c.exec(sql, args);
    }

    Result[] simpleQuery(string sql)
    {
        auto c = lock();
        return c.simpleQuery(sql);
    }

    /// Run `dg` in a transaction on a dedicated connection.
    R transaction(R)(scope R delegate(PgConnection) @safe dg)
    {
        auto c = lock();
        return c.transaction(dg);
    }

    /// ditto
    R transaction(R)(scope R delegate(PgConnection) dg) @system
    {
        auto c = lock();
        return c.transaction(dg);
    }

    /// Close every idle connection (busy ones are closed by their owners).
    void closeAll()
    {
        pool.removeUnused((PgConnection c) @trusted nothrow { try c.close(); catch (Exception) {} });
    }
}
