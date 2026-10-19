/// Writes query parameters (text format) straight into the outgoing message buffer,
/// without intermediate strings.
module postgrex.encode;

@safe:

import postgrex.types;
import postgrex.wire;
import std.datetime;
import std.format : formattedWrite;
import std.traits;
import std.typecons : Nullable;

package:

struct ByteSink
{
    MsgWriter w;
    void put(char c) { w.buf.put(cast(ubyte) c); }
    void put(const(char)[] s) { w.buf.put(cast(const(ubyte)[]) s); }
}

/// Escapes `"` and `\` for use inside a quoted array element.
struct EscSink(S)
{
    S inner;
    void put(char c)
    {
        if (c == '"' || c == '\\')
            inner.put('\\');
        inner.put(c);
    }
    void put(const(char)[] s) { foreach (c; s) put(c); }
}

/// Escapes values for COPY ... FROM STDIN text format (`\\`, tab, newline, carriage return).
struct CopyEscSink(S)
{
    S inner;
    void put(char c)
    {
        switch (c)
        {
        case '\\': inner.put("\\\\"); break;
        case '\t': inner.put("\\t"); break;
        case '\n': inner.put("\\n"); break;
        case '\r': inner.put("\\r"); break;
        default: inner.put(c);
        }
    }
    void put(const(char)[] s) { foreach (c; s) put(c); }
}

private enum isByteArray(T) = is(T : const(ubyte)[]);
private enum isNestedArray(T) = isArray!T && !isSomeString!T && !isByteArray!T;

bool isNullArg(T)(auto ref const T v)
{
    static if (is(T == typeof(null))) return true;
    else static if (isInstanceOf!(Nullable, T)) return v.isNull;
    else static if (is(Unqual!T == Param)) return v.isNull;
    else static if (is(Unqual!T == PgValue)) return v.isNull;
    else return false;
}

private void putInt(S)(ref S s, long v)
{
    char[20] buf;
    size_t i = buf.length;
    ulong u = v < 0 ? -cast(ulong) v : v;
    do { buf[--i] = cast(char)('0' + u % 10); u /= 10; } while (u);
    if (v < 0) buf[--i] = '-';
    s.put(buf[i .. $]);
}

private void putUInt(S)(ref S s, ulong u)
{
    char[20] buf;
    size_t i = buf.length;
    do { buf[--i] = cast(char)('0' + u % 10); u /= 10; } while (u);
    s.put(buf[i .. $]);
}

private void putHex(S)(ref S s, const(ubyte)[] d)
{
    static immutable digits = "0123456789abcdef";
    s.put("\\x");
    foreach (b; d)
    {
        s.put(digits[b >> 4]);
        s.put(digits[b & 15]);
    }
}

private void putPadded(S)(ref S s, long v, int width)
{
    char[20] buf;
    size_t i = buf.length;
    foreach (_; 0 .. width) { buf[--i] = cast(char)('0' + v % 10); v /= 10; }
    s.put(buf[i .. $]);
}

private void putTime(S)(ref S s, SysTime t)
{
    t = t.toUTC();
    putPadded(s, t.year, 4); s.put('-'); putPadded(s, t.month, 2); s.put('-'); putPadded(s, t.day, 2);
    s.put(' ');
    putPadded(s, t.hour, 2); s.put(':'); putPadded(s, t.minute, 2); s.put(':'); putPadded(s, t.second, 2);
    s.put('.');
    putPadded(s, t.fracSecs.total!"usecs", 6);
    s.put("+00");
}

/// Write the text representation of a non-NULL value.
void writeText(S, T)(ref S s, auto ref const T v)
{
    static if (is(Unqual!T == Param))
        s.put(v.text);
    else static if (is(Unqual!T == PgValue))
    {
        if (v.kind == PgKind.bytea) putHex(s, v.bytes);
        else s.put(v.toString());
    }
    else static if (isInstanceOf!(Nullable, T))
        writeText(s, v.get);
    else static if (is(Unqual!T == bool))
        s.put(v ? 't' : 'f');
    else static if (isIntegral!T && isSigned!T)
        putInt(s, v);
    else static if (isIntegral!T)
        putUInt(s, v);
    else static if (isFloatingPoint!T)
    {
        static if (is(Unqual!T == float)) formattedWrite(s, "%.9g", v);
        else formattedWrite(s, "%.17g", cast(double) v);
    }
    else static if (isSomeString!T)
    {
        static if (is(T : const(char)[])) s.put(v);
        else { import std.conv : to; s.put(v.to!string); }
    }
    else static if (is(Unqual!T == SysTime))
        putTime(s, v);
    else static if (is(Unqual!T == DateTime))
    {
        putPadded(s, v.year, 4); s.put('-'); putPadded(s, v.month, 2); s.put('-'); putPadded(s, v.day, 2);
        s.put(' ');
        putPadded(s, v.hour, 2); s.put(':'); putPadded(s, v.minute, 2); s.put(':'); putPadded(s, v.second, 2);
    }
    else static if (is(Unqual!T == Date))
    {
        putPadded(s, v.year, 4); s.put('-'); putPadded(s, v.month, 2); s.put('-'); putPadded(s, v.day, 2);
    }
    else static if (isByteArray!T)
        putHex(s, v);
    else static if (isArray!T)
    {
        alias E = ForeachType!T;
        s.put('{');
        foreach (i, ref e; v)
        {
            if (i) s.put(',');
            if (isNullArg(e))
                s.put("NULL");
            else static if (isNestedArray!E)
                writeText(s, e);
            else
            {
                s.put('"');
                auto es = EscSink!S(s);
                writeText(es, e);
                s.put('"');
            }
        }
        s.put('}');
    }
    else
        static assert(false, "cannot encode " ~ T.stringof ~ " as a PostgreSQL parameter");
}

/// Write one Bind parameter: int32 length (-1 for NULL) followed by the text.
void writeParam(T)(MsgWriter w, auto ref const T v)
{
    if (isNullArg(v))
    {
        w.int32(-1);
        return;
    }
    const pos = w.length;
    w.int32(0);
    auto s = ByteSink(w);
    writeText(s, v);
    w.patchInt32(pos, cast(int)(w.length - pos - 4));
}
