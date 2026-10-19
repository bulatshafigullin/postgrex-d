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
import postgrex.transport;
import vibe.core.core : createTimer, Timer;
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
    /// Server host name or IP. A value starting with `/` is a unix-domain socket directory (the socket
    /// `<dir>/.s.PGSQL.<port>` is used, as in libpq); TLS is not used on such connections.
    string host = "127.0.0.1";
    ushort port = 5432;
    string user = "postgres";
    string password;
    string database;
    string applicationName = "postgrex-d";
    Duration connectTimeout = 10.seconds;
    /// Socket read timeout: fails the connection when the server is silent for this long
    /// (Duration.max = none).
    Duration queryTimeout = Duration.max;
    /// Maximum run time of one command. When exceeded a CancelRequest is sent, the server aborts the
    /// statement (SQLSTATE 57014) and the connection stays usable (Duration.max = none).
    Duration statementTimeout = Duration.max;
    /// TLS policy, see `SslMode`. Default `prefer` uses TLS when the build and the server support it.
    SslMode sslMode = SslMode.prefer;
    /// PEM file with the CA certificate(s) for `verifyCa` / `verifyFull`.
    string sslRootCert;
    /// Max number of cached prepared statements per connection (0 disables caching).
    size_t statementCacheSize = 128;
    /// Additional startup parameters (e.g. "search_path").
    string[string] parameters;

    /// Parse `postgres://user:pass@host:port/db?application_name=x&search_path=y`.
    /// For a unix-domain socket use an absolute path as host, percent-encoded in the authority
    /// (`postgres://user@%2Fvar%2Frun%2Fpostgresql/db`) or as `?host=/var/run/postgresql`.
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
            o.host = decodeComponent(rest); // %2Fvar%2Frun%2Fpostgresql => unix socket directory
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
                switch (v)
                {
                    case "disable": o.sslMode = SslMode.disable; break;
                    case "allow", "prefer": o.sslMode = SslMode.prefer; break;
                    case "require": o.sslMode = SslMode.require; break;
                    case "verify-ca": o.sslMode = SslMode.verifyCa; break;
                    case "verify-full": o.sslMode = SslMode.verifyFull; break;
                    default: throw new PgException("invalid sslmode: " ~ v);
                }
                break;
            case "sslrootcert": o.sslRootCert = v; break;
            case "host": o.host = v; break;     // e.g. ?host=/var/run/postgresql
            case "port": o.port = v.to!ushort; break;
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
    private Transport sock;
    private ulong cmdSeq;              // identifies the running command for timeout cancellation
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
    /// True when the connection is encrypted with TLS.
    @property bool encrypted() const { return open_ && sock.encrypted; }
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
            sock = Transport.connect(opts.host, opts.port, opts.connectTimeout, opts.queryTimeout,
                                     opts.sslMode, opts.sslRootCert);
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

    // ----------------------------------------------------------------- cancellation

    /// Ask the server to abort the command this connection is currently running. May be called from
    /// another fiber while a query is in flight; the running call then fails with a
    /// `PgServerException` with code "57014" and the connection stays usable. Does nothing useful
    /// when the connection is idle. (The protocol is inherently racy: a cancel that arrives just
    /// after the command finished can hit the next one.)
    void cancel()
    {
        if (!open_)
            return;
        auto t = Transport.connect(opts.host, opts.port, opts.connectTimeout, opts.connectTimeout,
                                   SslMode.disable, null);
        scope (exit) t.close();
        ubyte[16] msg;
        import std.bitmanip : nativeToBigEndian;
        msg[0 .. 4] = nativeToBigEndian(16);
        msg[4 .. 8] = nativeToBigEndian(80877102);
        msg[8 .. 12] = nativeToBigEndian(backendPid_);
        msg[12 .. 16] = nativeToBigEndian(backendKey_);
        t.write(msg[]);
        t.flush();
        // the server closes the socket once it has processed the request
        try { ubyte[1] b; t.read(b[]); } catch (Exception) {}
    }

    /// Arms `statementTimeout` for the command about to run; call `stop` on the result when done.
    private struct Deadline
    {
        Timer timer;
        bool armed;
        void stop() { if (armed) { timer.stop(); armed = false; } }
    }

    private Deadline armDeadline()
    {
        Deadline d;
        if (opts.statementTimeout == Duration.max)
            return d;
        const seq = ++cmdSeq;
        d.timer = createTimer(() nothrow @safe {
            if (seq != cmdSeq)
                return;
            try cancel();
            catch (Exception) {}
        });
        d.timer.rearm(opts.statementTimeout);
        d.armed = true;
        return d;
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
        auto deadline = armDeadline();
        scope (exit) deadline.stop();

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
                case 'G', 'H', 'W': // COPY needs copyIn / copyOut
                    throw new PgProtocolException("COPY must be run with copyIn / copyOut");
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
        auto deadline = armDeadline();
        scope (exit) deadline.stop();

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

    // ----------------------------------------------------------------------- COPY

    /// Sink handed to the `copyIn` callback.
    static final class CopyWriter
    {
        private PgConnection c;
        private this(PgConnection c) { this.c = c; }

        /// Send raw COPY data (any chunking; format must match the COPY statement).
        void write(const(ubyte)[] data)
        {
            while (data.length)
            {
                const n = data.length > 65536 ? 65536 : data.length;
                c.w.begin('d');
                c.w.raw(data[0 .. n]);
                c.w.finish(true);
                data = data[n .. $];
                c.flushCopy();
            }
        }

        /// ditto
        void write(string data) { write(cast(const(ubyte)[]) data); }

        /// Send one row in text format (tab separated, `\N` for NULL) for `COPY t FROM STDIN`.
        /// Accepts the same argument types as query parameters.
        void row(Args...)(auto ref Args args)
        {
            c.w.begin('d');
            auto sink = ByteSink(c.w);
            static foreach (i; 0 .. Args.length)
            {
                static if (i > 0)
                    sink.put('\t');
                if (isNullArg(args[i]))
                    sink.put("\\N");
                else
                {
                    auto es = CopyEscSink!ByteSink(sink);
                    writeText(es, args[i]);
                }
            }
            sink.put('\n');
            c.w.finish(true);
            c.flushCopy(false);
        }
    }

    private void flushCopy(bool force = true)
    {
        if (force || w.length >= 65536)
            send();
    }

    /// `COPY ... FROM STDIN`: the callback streams data through the `CopyWriter`. Returns the number of
    /// rows copied. If the callback throws, the COPY is aborted (nothing is loaded) and the exception
    /// is rethrown; the connection stays usable.
    ///
    /// ---
    /// db.copyIn("copy users(id, name) from stdin", (CopyWriter cw) {
    ///     foreach (u; users) cw.row(u.id, u.name);
    /// });
    /// ---
    long copyIn(string sql, scope void delegate(CopyWriter) @safe fill)
    {
        mutex.lock();
        scope (exit) mutex.unlock();
        requireOpen();
        w.clear();
        w.begin('Q');
        w.cstr(sql);
        w.finish(true);
        send();
        auto deadline = armDeadline();
        scope (exit) deadline.stop();

        Nullable!PgNotice err;
        bool ready;
        // wait for CopyInResponse (or an error)
        while (!ready)
        {
            Cursor c;
            const t = nextMessage(c);
            switch (t)
            {
            case 'G': ready = true; break;
            case 'E': err = parseNotice(c); break;
            case 'Z':
                txStatus_ = cast(TxStatus) c.byte_();
                if (!err.isNull) throw new PgServerException(err.get);
                throw new PgException("statement is not a COPY FROM STDIN");
            case 'T', 'D', 'C', 'I': break; // not a COPY statement: drain to ReadyForQuery
            default: throw fail(new PgProtocolException("unexpected message '" ~ t ~ "' starting COPY"));
            }
        }

        Exception userErr;
        try fill(new CopyWriter(this));
        catch (Exception e)
        {
            if (!open_)
                throw e;
            userErr = e;
        }

        if (userErr is null)
        {
            w.begin('c'); // appended to any rows still buffered
            w.finish(true);
        }
        else
        {
            w.clear(); // drop a possibly half-written row
            w.begin('f');
            w.cstr(userErr.msg);
            w.finish(true);
        }
        send();

        long affected;
        for (;;)
        {
            Cursor c;
            const t = nextMessage(c);
            switch (t)
            {
            case 'C': affected = affectedRows(c.cstr()); break;
            case 'E': err = parseNotice(c); break;
            case 'Z':
                txStatus_ = cast(TxStatus) c.byte_();
                if (userErr !is null)
                    throw userErr;
                if (!err.isNull)
                    throw new PgServerException(err.get);
                return affected;
            default: throw fail(new PgProtocolException("unexpected message '" ~ t ~ "' during COPY"));
            }
        }
    }

    /// `COPY ... TO STDOUT`: `sink` receives the data in chunks (valid only during the call; the
    /// boundaries are the server's, not row boundaries). Returns the number of rows. If `sink`
    /// throws, the command is cancelled and the exception rethrown.
    long copyOut(string sql, scope void delegate(const(ubyte)[]) @safe sink)
    {
        mutex.lock();
        scope (exit) mutex.unlock();
        requireOpen();
        w.clear();
        w.begin('Q');
        w.cstr(sql);
        w.finish(true);
        send();
        auto deadline = armDeadline();
        scope (exit) deadline.stop();

        Nullable!PgNotice err;
        Exception userErr;
        long affected;
        bool started;
        for (;;)
        {
            Cursor c;
            const t = nextMessage(c);
            switch (t)
            {
            case 'T', 'D', 'I': break; // not a COPY statement: drain to ReadyForQuery
            case 'H': started = true; break;
            case 'd':
                if (userErr is null)
                {
                    try sink(c.rest);
                    catch (Exception e)
                    {
                        userErr = e;
                        try cancel(); catch (Exception) {} // stop the server from sending the rest
                    }
                }
                break;
            case 'c': break;
            case 'C': affected = affectedRows(c.cstr()); break;
            case 'E': err = parseNotice(c); break;
            case 'Z':
                txStatus_ = cast(TxStatus) c.byte_();
                if (userErr !is null)
                    throw userErr;
                if (!err.isNull)
                    throw new PgServerException(err.get);
                if (!started)
                    throw new PgException("statement is not a COPY TO STDOUT");
                return affected;
            default: throw fail(new PgProtocolException("unexpected message '" ~ t ~ "' during COPY"));
            }
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
