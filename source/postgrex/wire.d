/// Low level encoding / decoding of PostgreSQL v3 protocol messages.
module postgrex.wire;

@safe:

import postgrex.error;
import std.array : Appender;
import std.bitmanip : bigEndianToNative, nativeToBigEndian;

package:

/// Builds one frontend message: type byte, int32 length, payload.
struct MsgWriter
{
    Appender!(ubyte[]) buf;
    private size_t start;

    /// Begin a typed message (type == 0 for the untyped startup packet).
    void begin(char type)
    {
        start = buf.data.length;
        if (type != 0)
            buf.put(cast(ubyte) type);
        ubyte[4] zero;
        buf.put(zero[]);
    }

    void int32(int v) { buf.put(nativeToBigEndian(v)[]); }
    void int16(short v) { buf.put(nativeToBigEndian(v)[]); }
    void byte_(ubyte v) { buf.put(v); }
    void raw(const(ubyte)[] d) { buf.put(d); }
    void cstr(const(char)[] s)
    {
        buf.put(cast(const(ubyte)[]) s);
        buf.put(cast(ubyte) 0);
    }

    private void patch(size_t lenPos)
    {
        const len = cast(int)(buf.data.length - lenPos);
        buf.data[lenPos .. lenPos + 4] = nativeToBigEndian(len)[];
    }

    /// Finish a message started with begin(); `typed` tells whether a type byte was written.
    void finish(bool typed)
    {
        patch(start + (typed ? 1 : 0));
    }

    const(ubyte)[] data() { return buf.data; }
    void clear() { buf.clear(); }
}

/// Sequential reader over a received message body.
struct Cursor
{
    const(ubyte)[] d;

    bool empty() const { return d.length == 0; }

    private void need(size_t n)
    {
        if (d.length < n)
            throw new PgProtocolException("truncated message from server");
    }

    ubyte byte_()
    {
        need(1);
        auto v = d[0];
        d = d[1 .. $];
        return v;
    }

    short int16()
    {
        need(2);
        ubyte[2] a = d[0 .. 2];
        d = d[2 .. $];
        return bigEndianToNative!short(a);
    }

    int int32()
    {
        need(4);
        ubyte[4] a = d[0 .. 4];
        d = d[4 .. $];
        return bigEndianToNative!int(a);
    }

    const(ubyte)[] bytes(size_t n)
    {
        need(n);
        auto r = d[0 .. n];
        d = d[n .. $];
        return r;
    }

    string cstr()
    {
        foreach (i, b; d)
            if (b == 0)
            {
                auto s = cast(string) d[0 .. i].idup;
                d = d[i + 1 .. $];
                return s;
            }
        throw new PgProtocolException("unterminated string in server message");
    }

    const(ubyte)[] rest()
    {
        auto r = d;
        d = null;
        return r;
    }
}

PgNotice parseNotice(Cursor c)
{
    PgNotice n;
    while (!c.empty)
    {
        const f = c.byte_();
        if (f == 0)
            break;
        auto v = c.cstr();
        switch (f)
        {
        case 'S': if (!n.severity.length) n.severity = v; break;
        case 'V': n.severity = v; break;
        case 'C': n.code = v; break;
        case 'M': n.message = v; break;
        case 'D': n.detail = v; break;
        case 'H': n.hint = v; break;
        case 'P': n.position = v; break;
        case 's': n.schema = v; break;
        case 't': n.table = v; break;
        case 'c': n.column = v; break;
        case 'd': n.dataType = v; break;
        case 'n': n.constraint = v; break;
        case 'W': n.where = v; break;
        default: break;
        }
    }
    return n;
}
