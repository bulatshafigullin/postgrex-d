/// A single PostgreSQL connection. Built on vibe-core sockets, so every blocking call only
/// suspends the calling fiber and the connection can be used from vibe.d request handlers.
module postgrex.connection;

@safe:

import postgrex.auth;
import postgrex.encode;
import postgrex.error;
import postgrex.result;
import postgrex.types;
import postgrex.wire;

import core.time : Duration, seconds;
import std.algorithm : canFind;
import std.conv : to;
import std.string : indexOf, split, startsWith, toLower;
import std.typecons : Nullable;
import std.uri : decodeComponent;
import vibe.core.net : TCPConnection, connectTCP, anyAddress, resolveHost;
import vibe.core.sync : RecursiveTaskMutex;

/// Transaction state as reported by ReadyForQuery.
enum TxStatus : char
{
    idle = 'I',
    inTransaction = 'T',
    failed = 'E',
}

struct ConnectOptions
{
    string host = "127.0.0.1";
    ushort port = 5432;
    string user = "postgres";
    string password;
    string database;
    string applicationName = "postgrex-d";
    Duration connectTimeout = 10.seconds;
    /// Per-read timeout (Duration.max = none).
    Duration queryTimeout = Duration.max;
    /// Max number of cached prepared statements per connection (0 disables caching).
    size_t statementCacheSize = 128;
    /// Additional startup parameters (e.g. "search_path").
    string[string] parameters;

    /// Parse `postgres://user:pass@host:port/db?application_name=x&search_path=y`.
    static ConnectOptions fromUrl(string url)
    {
        ConnectOptions o;
        string rest;
        if (url.startsWith("postgresql://")) rest = url["postgresql://".length .. $];
        else if (url.startsWith("postgres://")) rest = url["postgres://".length .. $];
        else throw new PgException("connection URL must start with postgres:// or postgresql://");

        string query;
        auto q = rest.indexOf('?');
        if (q >= 0)
        {
            query = rest[q + 1 .. $];
            rest = rest[0 .. q];
        }
        string path;
        auto s = rest.indexOf('/');
        if (s >= 0)
        {
            path = rest[s + 1 .. $];
            rest = rest[0 .. s];
        }
        auto at = rest.indexOf('@');
        if (at >= 0)
        {
            auto ui = rest[0 .. at];
            rest = rest[at + 1 .. $];
            auto c = ui.indexOf(':');
            if (c >= 0)
            {
                o.user = decodeComponent(ui[0 .. c]);
                o.password = decodeComponent(ui[c + 1 .. $]);
            }
            else
                o.user = decodeComponent(ui);
        }
        auto c = rest.lastIndexOfPort;
        if (c >= 0)
        {
            o.port = rest[c + 1 .. $].to!ushort;
            rest = rest[0 .. c];
        }
        if (rest.length)
            o.host = rest;
        o.database = path.length ? decodeComponent(path) : o.user;
        foreach (kv; query.split("&"))
        {
            if (!kv.length) continue;
            auto e = kv.indexOf('=');
            auto k = e >= 0 ? kv[0 .. e] : kv;
            auto v = e >= 0 ? decodeComponent(kv[e + 1 .. $]) : "";
            switch (k)
            {
            case "application_name": o.applicationName = v; break;
            case "sslmode":
                if (v.canFind("require") || v.startsWith("verify"))
                    throw new PgException("sslmode=" ~ v ~ " is not supported (TLS not implemented)");
                break;
            default: o.parameters[k] = v; break;
            }
        }
        return o;
    }
}

private ptrdiff_t lastIndexOfPort(string hostport)
{
    // IPv6 literals look like [::1]:5432
    import std.string : lastIndexOf;
    auto c = hostport.lastIndexOf(':');
    auto b = hostport.lastIndexOf(']');
    return c > b ? c : -1;
}

final class PgConnection
{
    private ConnectOptions opts;
    private TCPConnection sock;
    private bool open_;
    private RecursiveTaskMutex mutex;
    private MsgWriter w;
    private ubyte[] rbuf;              // reused for every incoming message
    private ubyte[] lastRowDesc;       // raw RowDescription of the previous result ...
    private Column[] lastCols;         // ... and its parsed columns (shared between results)
    private Cell[] rowScratch;         // cell index for streamed rows
    private TxStatus txStatus_ = TxStatus.idle;
    private int backendPid_;
    private int backendKey_;
    private string[string] serverParams_;
    private Notification[] pendingNotifications;
    private size_t savepointDepth;

    private struct Stmt { string name; ulong used; }
    private Stmt[string] stmtCache;
    private ulong useClock;
    private string[] staleStmts;    // server side statements to Close on next command
    private uint stmtCounter;

    /// Called for every NoticeResponse (warnings, RAISE NOTICE, ...).
    void delegate(PgNotice) onNotice;
    /// Called for NOTIFY messages that arrive while a command is running.
    void delegate(Notification) onNotification;

    this(ConnectOptions opts)
    {
        this.opts = opts;
        mutex = new RecursiveTaskMutex;
        w = new MsgWriter;
    }

    this(string url)
    {
        this(ConnectOptions.fromUrl(url));
    }

    @property bool connected() const { return open_; }
    @property TxStatus txStatus() const { return txStatus_; }
    @property int backendPid() const { return backendPid_; }
    @property const(string[string]) serverParameters() const { return serverParams_; }
    @property string serverVersion() const
    {
        if (auto p = "server_version" in serverParams_) return *p;
        return null;
    }

    // ------------------------------------------------------------------ connect

    void connect()
    {
        if (open_) return;
        mutex.lock();
        scope (exit) mutex.unlock();
        if (open_) return;
        try
        {
            sock = connectTCP(opts.host, opts.port, null, 0, opts.connectTimeout);
            if (opts.queryTimeout != Duration.max)
                sock.readTimeout = opts.queryTimeout;
            sock.tcpNoDelay = true;
            open_ = true;
            txStatus_ = TxStatus.idle;
            savepointDepth = 0;
            stmtCache = null;
            staleStmts = null;
            pendingNotifications = null;
            serverParams_ = null;
            startup();
        }
        catch (PgException e)
        {
            closeSocket();
            throw e;
        }
        catch (Exception e)
        {
            closeSocket();
            throw new PgProtocolException("could not connect to " ~ opts.host ~ ":" ~ opts.port.to!string ~ ": " ~ e.msg);
        }
    }

    /// Send Terminate and close the socket.
    void close()
    {
        if (!open_) return;
        mutex.lock();
        scope (exit) mutex.unlock();
        if (!open_) return;
        try
        {
            w.clear();
            w.begin('X');
            w.finish(true);
            send();
        }
        catch (Exception) {}
        closeSocket();
    }

    private void closeSocket()
    {
        open_ = false;
        try { sock.close(); } catch (Exception) {}
    }

    /// Mark the connection as unusable after an I/O or protocol failure.
    private PgException fail(Exception e)
    {
        closeSocket();
        if (auto p = cast(PgException) e)
            return p;
        return new PgProtocolException("connection lost: " ~ e.msg);
    }

    // ----------------------------------------------------------------- low level

    private void send()
    {
        try
        {
            sock.write(w.data);
            sock.flush();
        }
        catch (Exception e)
            throw fail(e);
        w.clear();
    }

    /// Read the next message, transparently handling notices, parameter changes and notifications.
    private char nextMessage(out Cursor body_, bool idle = false)
    {
        for (;;)
        {
            char type;
            size_t blen;
            try
            {
                ubyte[5] hdr;
                sock.read(hdr[]);
                type = cast(char) hdr[0];
                ubyte[4] l = hdr[1 .. 5];
                import std.bitmanip : bigEndianToNative;
                const len = bigEndianToNative!int(l);
                if (len < 4 || len > 1_073_741_824)
                    throw new PgProtocolException("invalid message length from server");
                blen = len - 4;
                if (blen > rbuf.length)
                    rbuf = new ubyte[blen < 8192 ? 8192 : blen];
                else if (rbuf.length > (1 << 20) && blen <= 65536)
                    rbuf = new ubyte[65536]; // give back memory after a huge message
                if (blen)
                    sock.read(rbuf[0 .. blen]);
            }
            catch (Exception e)
                throw fail(e);

            auto c = Cursor(rbuf[0 .. blen]);
            switch (type)
            {
            case 'N':
                if (onNotice) onNotice(parseNotice(c));
                if (idle) return '\0';
                continue;
            case 'S':
            {
                auto k = c.cstr();
                serverParams_[k] = c.cstr();
                if (idle) return '\0';
                continue;
            }
            case 'A':
            {
                Notification n;
                n.pid = c.int32();
                n.channel = c.cstr();
                n.payload = c.cstr();
                if (onNotification) onNotification(n);
                else pendingNotifications ~= n;
                if (idle) return '\0';
                continue;
            }
            default:
                body_ = c;
                return type;
            }
        }
    }

    // ------------------------------------------------------------------- startup

    private void startup()
    {
        w.clear();
        w.begin(0);
        w.int32(196608); // protocol 3.0
        void kv(string k, string v) { w.cstr(k); w.cstr(v); }
        kv("user", opts.user);
        kv("database", opts.database.length ? opts.database : opts.user);
        kv("client_encoding", "UTF8");
        kv("application_name", opts.applicationName);
        kv("DateStyle", "ISO, MDY");
        kv("extra_float_digits", "3");
        foreach (k, v; opts.parameters)
            kv(k, v);
        w.byte_(0);
        w.finish(false);
        send();

        Scram scram;
        bool scramActive;
        for (;;)
        {
            Cursor c;
            const t = nextMessage(c);
            try
            {
                switch (t)
                {
                case 'R':
                {
                    const code = c.int32();
                    switch (code)
                    {
                    case 0: break; // AuthenticationOk
                    case 3:        // cleartext
                        sendPassword(opts.password);
                        break;
                    case 5:        // md5
                        sendPassword(md5Password(opts.user, opts.password, c.bytes(4)));
                        break;
                    case 10:       // SASL
                    {
                        bool ok;
                        while (!c.empty)
                        {
                            auto m = c.cstr();
                            if (m == "SCRAM-SHA-256") ok = true;
                            if (m.length == 0) break;
                        }
                        if (!ok)
                            throw new PgProtocolException("server offers no supported SASL mechanism");
                        scram = Scram(opts.password);
                        scramActive = true;
                        auto first = scram.first();
                        w.clear();
                        w.begin('p');
                        w.cstr("SCRAM-SHA-256");
                        w.int32(cast(int) first.length);
                        w.raw(cast(const(ubyte)[]) first);
                        w.finish(true);
                        send();
                        break;
                    }
                    case 11:
                    {
                        if (!scramActive) throw new PgProtocolException("unexpected SASLContinue");
                        auto fin = scram.final_(cast(string) c.rest.idup);
                        w.clear();
                        w.begin('p');
                        w.raw(cast(const(ubyte)[]) fin);
                        w.finish(true);
                        send();
                        break;
                    }
                    case 12:
                        if (!scramActive) throw new PgProtocolException("unexpected SASLFinal");
                        scram.verify(cast(string) c.rest.idup);
                        break;
                    default:
                        throw new PgProtocolException("unsupported authentication method " ~ code.to!string);
                    }
                    break;
                }
                case 'K':
                    backendPid_ = c.int32();
                    backendKey_ = c.int32();
                    break;
                case 'Z':
                    txStatus_ = cast(TxStatus) c.byte_();
                    return;
                case 'E':
                    throw new PgServerException(parseNotice(c));
                default:
                    throw new PgProtocolException("unexpected message '" ~ t ~ "' during startup");
                }
            }
            catch (Exception e)
                throw fail(e);
        }
    }

    private void sendPassword(string pw)
    {
        w.clear();
        w.begin('p');
        w.cstr(pw);
        w.finish(true);
        send();
    }

    // ------------------------------------------------------------------- queries

    private void requireOpen()
    {
        if (!open_)
            throw new PgProtocolException("connection is closed");
    }

    /// Run SQL through the simple query protocol. Allows several `;`-separated statements and
    /// takes no parameters. Returns one Result per statement.
    Result[] simpleQuery(string sql)
    {
        mutex.lock();
        scope (exit) mutex.unlock();
        requireOpen();
        w.clear();
        w.begin('Q');
        w.cstr(sql);
        w.finish(true);
        send();

        Result[] results;
        Collector col;
        Nullable!PgNotice err;
        for (;;)
        {
            Cursor c;
            const t = nextMessage(c);
            try
            {
                switch (t)
                {
                case 'T':
                    col.begin(parseRowDescription(c));
                    break;
                case 'D':
                    col.addRow(c);
                    break;
                case 'C':
                {
                    auto tag = c.cstr();
                    results ~= col.finish(tag, affectedRows(tag));
                    col = Collector.init;
                    break;
                }
                case 'I':
                    results ~= Result.init;
                    break;
                case 'E':
                    err = parseNotice(c);
                    break;
                case 'Z':
                    txStatus_ = cast(TxStatus) c.byte_();
                    if (!err.isNull)
                        throw new PgServerException(err.get);
                    return results;
                case 'G', 'H', 'W': // COPY not supported through this API
                    throw new PgProtocolException("COPY is not supported");
                default:
                    throw new PgProtocolException("unexpected message '" ~ t ~ "'");
                }
            }
            catch (PgServerException e)
                throw e;
            catch (Exception e)
                throw fail(e);
        }
    }

    private struct ExecState
    {
        Column[] columns;
        string tag;
        long affected;
    }

    /// Extended protocol round trip: [Close stale] [Parse] Bind Describe Execute Sync, one network write.
    /// `onRow` is invoked for each DataRow with the message cursor positioned at the row.
    private void runExtended(string sql, size_t nparams, scope void delegate(MsgWriter) @safe writeParams,
                             scope void delegate(ref Cursor, Column[]) @safe onRow, ref ExecState st)
    {
        requireOpen();
        if (nparams > short.max)
            throw new PgException("too many parameters");

        string stmtName = "";
        bool needParse = true;
        const cacheable = opts.statementCacheSize > 0;
        if (cacheable)
        {
            if (auto s = sql in stmtCache)
            {
                stmtName = s.name;
                s.used = ++useClock;
                needParse = false;
            }
            else
                stmtName = "pgd_" ~ (++stmtCounter).to!string;
        }

        w.clear();
        foreach (n; staleStmts)
        {
            w.begin('C');
            w.byte_('S');
            w.cstr(n);
            w.finish(true);
        }
        staleStmts = null;

        if (needParse)
        {
            w.begin('P');
            w.cstr(stmtName);
            w.cstr(sql);
            w.int16(0); // let the server infer parameter types
            w.finish(true);
        }
        w.begin('B');
        w.cstr("");
        w.cstr(stmtName);
        w.int16(0); // all params text
        w.int16(cast(short) nparams);
        writeParams(w);
        w.int16(0); // all results text
        w.finish(true);
        w.begin('D');
        w.byte_('P');
        w.cstr("");
        w.finish(true);
        w.begin('E');
        w.cstr("");
        w.int32(0);
        w.finish(true);
        w.begin('S');
        w.finish(true);
        send();

        Nullable!PgNotice err;
        for (;;)
        {
            Cursor c;
            const t = nextMessage(c);
            try
            {
                switch (t)
                {
                case '1', '2', '3', 'n', 's', 'I': break;
                case 'T':
                    st.columns = parseRowDescription(c);
                    break;
                case 'D':
                    onRow(c, st.columns);
                    break;
                case 'C':
                    st.tag = c.cstr();
                    st.affected = affectedRows(st.tag);
                    break;
                case 'E':
                    err = parseNotice(c);
                    break;
                case 'Z':
                    txStatus_ = cast(TxStatus) c.byte_();
                    if (!err.isNull)
                    {
                        // a failed statement may be stale server-side; drop our reference to it
                        if (!needParse)
                            evict(sql);
                        throw new PgServerException(err.get);
                    }
                    if (needParse && cacheable)
                        remember(sql, stmtName);
                    return;
                default:
                    throw new PgProtocolException("unexpected message '" ~ t ~ "'");
                }
            }
            catch (PgServerException e)
                throw e;
            catch (Exception e)
                throw fail(e);
        }
    }

    /// Execute a statement with parameters given as a runtime list.
    Result queryParams(string sql, scope const(Param)[] params)
    {
        mutex.lock();
        scope (exit) mutex.unlock();
        Collector col;
        ExecState st;
        runExtended(sql, params.length, (MsgWriter mw) {
            foreach (ref p; params)
                writeParam(mw, p);
        }, (ref Cursor c, Column[] cols) {
            if (col.columns is null)
                col.begin(cols);
            col.addRow(c);
        }, st);
        if (col.columns is null)
            col.begin(st.columns);
        return col.finish(st.tag, st.affected);
    }

    /// Execute a parameterised statement; parameters are `$1`, `$2`, ... and are sent separately from
    /// the SQL text. Supported argument types: bool, integers, floats, string, ubyte[], SysTime, Date,
    /// DateTime, arrays of those, `Nullable!T`, `null`, `Param`, `PgValue`.
    Result query(Args...)(string sql, auto ref Args args)
    {
        mutex.lock();
        scope (exit) mutex.unlock();
        Collector col;
        ExecState st;
        runExtended(sql, Args.length, (MsgWriter mw) {
            static foreach (i; 0 .. Args.length)
                writeParam(mw, args[i]);
        }, (ref Cursor c, Column[] cols) {
            if (col.columns is null)
                col.begin(cols);
            col.addRow(c);
        }, st);
        if (col.columns is null)
            col.begin(st.columns);
        return col.finish(st.tag, st.affected);
    }

    /// Run a statement and return the number of affected rows.
    long exec(Args...)(string sql, auto ref Args args)
    {
        return query(sql, args).rowsAffected;
    }

    /// Stream a result set: `dg` is called once per row with a `Row` that is only valid during the
    /// call (use `get!string` etc. to copy what you need). Nothing is buffered, so memory use is
    /// constant regardless of result size. Returns the number of rows. An exception from `dg`
    /// is rethrown after the remaining rows have been drained (the connection stays usable).
    long queryEach(Args...)(string sql, scope void delegate(Row) @safe dg, auto ref Args args)
    {
        return queryEachImpl(sql, dg, (MsgWriter mw) {
            static foreach (i; 0 .. Args.length)
                writeParam(mw, args[i]);
        }, Args.length);
    }

    /// ditto
    long queryEach(Args...)(string sql, scope void delegate(Row) dg, auto ref Args args) @system
    {
        return queryEachImpl(sql, dg, (MsgWriter mw) {
            static foreach (i; 0 .. Args.length)
                writeParam(mw, args[i]);
        }, Args.length);
    }

    private long queryEachImpl(D)(string sql, scope D dg, scope void delegate(MsgWriter) @safe writeParams, size_t nparams)
    {
        mutex.lock();
        scope (exit) mutex.unlock();
        Exception pending;
        long count;
        ExecState st;
        runExtended(sql, nparams, writeParams, (ref Cursor c, Column[] cols) {
            if (pending !is null)
                return;
            const all = c.d;
            const n = c.int16();
            if (n != cols.length)
                throw new PgProtocolException("DataRow does not match RowDescription");
            if (rowScratch.length < n)
                rowScratch = new Cell[n < 16 ? 16 : n];
            foreach (i; 0 .. n)
            {
                const len = c.int32();
                if (len < 0)
                    rowScratch[i] = Cell(0, -1);
                else
                {
                    rowScratch[i] = Cell(cast(uint)(all.length - c.d.length), len);
                    c.bytes(len);
                }
            }
            count++;
            try (() @trusted { dg(Row(cols, all, rowScratch[0 .. n], false)); })();
            catch (Exception e) { pending = e; }
        }, st);
        if (pending !is null)
            throw pending;
        return count;
    }

    private void remember(string sql, string name)
    {
        stmtCache[sql] = Stmt(name, ++useClock);
        if (stmtCache.length > opts.statementCacheSize)
        {
            // evict the least recently used entry (never the one just added)
            string victim;
            ulong oldest = ulong.max;
            foreach (k, ref v; stmtCache)
                if (v.used < oldest) { oldest = v.used; victim = k; }
            evict(victim);
        }
    }

    private void evict(string sql)
    {
        if (auto s = sql in stmtCache)
        {
            staleStmts ~= s.name;
            stmtCache.remove(sql);
        }
    }

    private Column[] parseRowDescription(ref Cursor c)
    {
        const raw = c.d;
        if (lastRowDesc.length != 0 && raw == lastRowDesc)
            return lastCols; // same shape as last time: share the column metadata
        const n = c.int16();
        auto cols = new Column[n];
        foreach (ref col; cols)
        {
            col.name = c.cstr();
            col.tableOid = cast(uint) c.int32();
            col.attNum = c.int16();
            col.typeOid = cast(uint) c.int32();
            col.typeSize = c.int16();
            col.typeMod = c.int32();
            c.int16(); // format code: always text
        }
        lastRowDesc = raw.dup;
        lastCols = cols;
        return cols;
    }

    private static long affectedRows(string tag)
    {
        import std.string : lastIndexOf;
        const sp = tag.lastIndexOf(' ');
        if (sp < 0 || sp + 1 >= tag.length)
            return 0;
        long v;
        foreach (ch; tag[sp + 1 .. $])
        {
            if (ch < '0' || ch > '9')
                return 0;
            v = v * 10 + (ch - '0');
        }
        return v;
    }

    // -------------------------------------------------------------- transactions

    /// Run `dg` inside a transaction (BEGIN / COMMIT, ROLLBACK on exception). When already inside a
    /// transaction a SAVEPOINT is used, so calls nest.
    R transaction(R)(scope R delegate(PgConnection) @safe dg)
    {
        return transactionImpl!R(dg);
    }

    /// ditto
    R transaction(R)(scope R delegate(PgConnection) dg) @system
    {
        return transactionImpl!R(dg);
    }

    private R transactionImpl(R, D)(scope D dg)
    {
        mutex.lock();
        scope (exit) mutex.unlock();
        requireOpen();

        const nested = txStatus_ != TxStatus.idle;
        const sp = "pgd_sp_" ~ (++savepointDepth).to!string;
        scope (exit) savepointDepth--;
        simpleQuery(nested ? "SAVEPOINT " ~ sp : "BEGIN");
        bool settled;
        // an Error (not Exception) escaping leaves the transaction state unknown: drop the connection
        scope (exit) if (!settled) closeSocket();
        try
        {
            static if (is(R == void))
            {
                dg(this);
                simpleQuery(nested ? "RELEASE SAVEPOINT " ~ sp : "COMMIT");
                settled = true;
            }
            else
            {
                auto r = dg(this);
                simpleQuery(nested ? "RELEASE SAVEPOINT " ~ sp : "COMMIT");
                settled = true;
                return r;
            }
        }
        catch (Exception e)
        {
            if (open_)
            {
                try
                {
                    simpleQuery(nested ? "ROLLBACK TO SAVEPOINT " ~ sp : "ROLLBACK");
                    settled = true;
                }
                catch (Exception) {} // closed by scope(exit): unknown state, do not reuse
            }
            throw e;
        }
    }

    // ------------------------------------------------------------ LISTEN/NOTIFY

    /// Wait up to `timeout` for a NOTIFY (after `LISTEN channel`). Returns null on timeout.
    Nullable!Notification waitForNotification(Duration timeout = Duration.max)
    {
        mutex.lock();
        scope (exit) mutex.unlock();
        requireOpen();
        if (pendingNotifications.length == 0)
        {
            bool gotData;
            try gotData = sock.waitForData(timeout);
            catch (Exception e) throw fail(e);
            if (!gotData)
                return Nullable!Notification.init;
            Cursor c;
            // only asynchronous messages are expected while idle
            const nm = nextMessage(c, true);
            if (nm == 'E')
                throw fail(new PgServerException(parseNotice(c)));
            if (nm != '\0')
                throw fail(new PgProtocolException("unexpected message '" ~ nm ~ "' while idle"));
        }
        if (pendingNotifications.length == 0)
            return Nullable!Notification.init;
        auto n = pendingNotifications[0];
        pendingNotifications = pendingNotifications[1 .. $];
        return Nullable!Notification(n);
    }

    /// Escape an identifier for interpolation into SQL.
    static string quoteIdent(string s)
    {
        import std.array : replace;
        return '"' ~ s.replace("\"", "\"\"") ~ '"';
    }
}
