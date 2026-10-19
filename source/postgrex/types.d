/// Dynamic value type, OID based decoding and parameter encoding.
module postgrex.types;

@safe:

import postgrex.error;
import std.conv : to, ConvException;
import std.datetime;
import std.format : format;
import std.traits;
import std.typecons : Nullable;

/// Well known type OIDs.
enum Oid : uint
{
    bool_ = 16, bytea = 17, char_ = 18, name = 19, int8 = 20, int2 = 21, int4 = 23,
    text = 25, oid = 26, json = 114, float4 = 700, float8 = 701, bpchar = 1042,
    varchar = 1043, date = 1082, timestamp = 1114, timestamptz = 1184,
    numeric = 1700, uuid = 2950, jsonb = 3802,
}

enum PgKind : ubyte
{
    null_, boolean, integer, floating, numeric, text, bytea, json, uuid,
    date, timestamp, timestamptz, array,
}

/// A decoded cell. Use `get!T` to extract a native D value.
struct PgValue
{
    PgKind kind;
    private union
    {
        long l;
        double f;
    }
    string s;           /// text, numeric, json and uuid payload
    ubyte[] bytes;      /// bytea payload
    PgValue[] items;    /// array elements
    SysTime time;       /// date / timestamp / timestamptz (UTC for timestamptz)

    @property bool isNull() const { return kind == PgKind.null_; }

    static PgValue null_() { return PgValue.init; }
    static PgValue ofBool(bool v) { PgValue r; r.kind = PgKind.boolean; r.l = v ? 1 : 0; return r; }
    static PgValue ofInt(long v) { PgValue r; r.kind = PgKind.integer; r.l = v; return r; }
    static PgValue ofFloat(double v) { PgValue r; r.kind = PgKind.floating; r.f = v; return r; }
    static PgValue ofText(string v, PgKind k = PgKind.text) { PgValue r; r.kind = k; r.s = v; return r; }

    /// Convert to a D type: bool, integers, floats, string, ubyte[], SysTime, Date,
    /// T[] (arrays), Nullable!T, or PgValue itself.
    T get(T)() const
    {
        static if (is(T == PgValue))
            return cast() this;
        else static if (isInstanceOf!(Nullable, T))
        {
            alias U = typeof(T.init.get());
            if (isNull)
                return T.init;
            return T(get!U);
        }
        else
        {
            if (isNull)
                throw new PgTypeException("NULL cannot be converted to " ~ T.stringof);
            static if (is(T == bool))
            {
                if (kind == PgKind.boolean) return l != 0;
                if (kind == PgKind.integer) return l != 0;
            }
            else static if (isIntegral!T)
            {
                if (kind == PgKind.integer)
                {
                    if (l < T.min || l > T.max)
                        throw new PgTypeException("value " ~ l.to!string ~ " out of range for " ~ T.stringof);
                    return cast(T) l;
                }
                if (kind == PgKind.numeric)
                    return s.to!T;
            }
            else static if (isFloatingPoint!T)
            {
                if (kind == PgKind.floating) return cast(T) f;
                if (kind == PgKind.integer) return cast(T) l;
                if (kind == PgKind.numeric) return s.to!T;
            }
            else static if (isSomeString!T)
            {
                switch (kind)
                {
                case PgKind.text, PgKind.numeric, PgKind.json, PgKind.uuid:
                    return s.to!T;
                case PgKind.integer: return l.to!T;
                case PgKind.floating: return f.to!T;
                case PgKind.boolean: return (l != 0 ? "true" : "false").to!T;
                default: break;
                }
            }
            else static if (is(T == ubyte[]) || is(T == const(ubyte)[]) || is(T == immutable(ubyte)[]))
            {
                if (kind == PgKind.bytea) return cast(T) bytes.dup;
            }
            else static if (is(T == SysTime))
            {
                if (kind == PgKind.timestamp || kind == PgKind.timestamptz || kind == PgKind.date)
                    return time;
            }
            else static if (is(T == DateTime))
            {
                if (kind == PgKind.timestamp || kind == PgKind.timestamptz || kind == PgKind.date)
                    return cast(DateTime) time;
            }
            else static if (is(T == Date))
            {
                if (kind == PgKind.date || kind == PgKind.timestamp || kind == PgKind.timestamptz)
                    return cast(Date) time;
            }
            else static if (isArray!T)
            {
                if (kind == PgKind.array)
                {
                    alias E = ForeachType!T;
                    Unqual!E[] r;
                    r.reserve(items.length);
                    foreach (ref it; items)
                        r ~= it.get!(Unqual!E);
                    return cast(T) r;
                }
            }
            throw new PgTypeException(format("cannot convert %s value to %s", kind, T.stringof));
        }
    }

    string toString() const
    {
        final switch (kind)
        {
        case PgKind.null_: return "NULL";
        case PgKind.boolean: return l != 0 ? "true" : "false";
        case PgKind.integer: return l.to!string;
        case PgKind.floating: return f.to!string;
        case PgKind.numeric, PgKind.text, PgKind.json, PgKind.uuid: return s;
        case PgKind.bytea: return format("%(\\x%02x%)", bytes);
        case PgKind.date: return (cast(Date) time).toISOExtString();
        case PgKind.timestamp, PgKind.timestamptz: return time.toISOExtString();
        case PgKind.array:
            string r = "{";
            foreach (i, ref it; items)
                r ~= (i ? "," : "") ~ it.toString();
            return r ~ "}";
        }
    }
}

// ---------------------------------------------------------------------------
// decoding (text format)
// ---------------------------------------------------------------------------

/// Element type for known array OIDs, 0 if `oid` is not a supported array type.
uint arrayElementOid(uint oid) pure nothrow @nogc @safe
{
    switch (oid)
    {
    case 1000: return Oid.bool_;
    case 1001: return Oid.bytea;
    case 1005: return Oid.int2;
    case 1007: return Oid.int4;
    case 1016: return Oid.int8;
    case 1009: return Oid.text;
    case 1015: return Oid.varchar;
    case 1014: return Oid.bpchar;
    case 1021: return Oid.float4;
    case 1022: return Oid.float8;
    case 1231: return Oid.numeric;
    case 2951: return Oid.uuid;
    case 199:  return Oid.json;
    case 3807: return Oid.jsonb;
    case 1182: return Oid.date;
    case 1115: return Oid.timestamp;
    case 1185: return Oid.timestamptz;
    default: return 0;
    }
}

/// Decode a text-format cell of type `oid`.
PgValue decodeText(uint oid, const(char)[] text)
{
    if (const el = arrayElementOid(oid))
    {
        size_t pos = 0;
        // skip optional dimension decoration "[1:3]={...}"
        if (text.length && text[0] == '[')
        {
            import std.string : indexOf;
            auto eq = text.indexOf('=');
            if (eq >= 0) pos = eq + 1;
        }
        return parseArray(el, text, pos);
    }
    switch (oid)
    {
    case Oid.bool_:
        return PgValue.ofBool(text.length && (text[0] == 't' || text[0] == 'T'));
    case Oid.int2, Oid.int4, Oid.int8, Oid.oid:
        return PgValue.ofInt(text.to!long);
    case Oid.float4, Oid.float8:
        return PgValue.ofFloat(text.to!double); // handles NaN / Infinity
    case Oid.numeric:
        return PgValue.ofText(text.idup, PgKind.numeric);
    case Oid.bytea:
    {
        PgValue v;
        v.kind = PgKind.bytea;
        v.bytes = decodeBytea(text);
        return v;
    }
    case Oid.json, Oid.jsonb:
        return PgValue.ofText(text.idup, PgKind.json);
    case Oid.uuid:
        return PgValue.ofText(text.idup, PgKind.uuid);
    case Oid.date:
    {
        PgValue v;
        if (parseDate(text, v.time)) { v.kind = PgKind.date; return v; }
        return PgValue.ofText(text.idup); // infinity, BC dates...
    }
    case Oid.timestamp, Oid.timestamptz:
    {
        PgValue v;
        if (parseTimestamp(text, oid == Oid.timestamptz, v.time))
        {
            v.kind = oid == Oid.timestamptz ? PgKind.timestamptz : PgKind.timestamp;
            return v;
        }
        return PgValue.ofText(text.idup);
    }
    default:
        return PgValue.ofText(text.idup);
    }
}

ubyte[] decodeBytea(const(char)[] t)
{
    if (t.length >= 2 && t[0] == '\\' && t[1] == 'x')
    {
        auto h = t[2 .. $];
        auto r = new ubyte[h.length / 2];
        static int nib(char c)
        {
            if (c >= '0' && c <= '9') return c - '0';
            if (c >= 'a' && c <= 'f') return c - 'a' + 10;
            if (c >= 'A' && c <= 'F') return c - 'A' + 10;
            throw new PgTypeException("invalid hex digit in bytea");
        }
        foreach (i; 0 .. r.length)
            r[i] = cast(ubyte)(nib(h[2 * i]) << 4 | nib(h[2 * i + 1]));
        return r;
    }
    // legacy escape format
    ubyte[] r;
    for (size_t i = 0; i < t.length; i++)
    {
        if (t[i] != '\\') { r ~= cast(ubyte) t[i]; continue; }
        if (i + 1 < t.length && t[i + 1] == '\\') { r ~= '\\'; i++; }
        else if (i + 3 < t.length)
        {
            r ~= cast(ubyte)((t[i + 1] - '0') * 64 + (t[i + 2] - '0') * 8 + (t[i + 3] - '0'));
            i += 3;
        }
    }
    return r;
}

private bool parseDate(const(char)[] t, out SysTime st)
{
    // YYYY-MM-DD (years > 9999 and BC are left as text)
    if (t.length != 10 || t[4] != '-' || t[7] != '-')
        return false;
    try
    {
        st = SysTime(DateTime(t[0 .. 4].to!int, t[5 .. 7].to!int, t[8 .. 10].to!int), UTC());
        return true;
    }
    catch (Exception)
        return false;
}

private bool parseTimestamp(const(char)[] t, bool withTz, out SysTime st)
{
    // YYYY-MM-DD HH:MM:SS[.ffffff][+TZ[:MM[:SS]]]
    if (t.length < 19 || t[4] != '-' || t[7] != '-' || t[10] != ' ' || t[13] != ':' || t[16] != ':')
        return false;
    try
    {
        auto dt = DateTime(t[0 .. 4].to!int, t[5 .. 7].to!int, t[8 .. 10].to!int,
                           t[11 .. 13].to!int, t[14 .. 16].to!int, t[17 .. 19].to!int);
        size_t i = 19;
        long hnsecs = 0;
        if (i < t.length && t[i] == '.')
        {
            size_t j = i + 1;
            while (j < t.length && t[j] >= '0' && t[j] <= '9')
                j++;
            hnsecs = fractionToHnsecs(t[i + 1 .. j]);
            i = j;
        }
        Duration offset;
        if (withTz && i < t.length && (t[i] == '+' || t[i] == '-'))
        {
            const sign = t[i] == '-' ? -1 : 1;
            // +HH[:MM[:SS]]
            int[3] part;
            size_t k = i + 1, pi = 0;
            for (; k < t.length && pi < 3; pi++)
            {
                int v = 0;
                const startK = k;
                while (k < t.length && t[k] >= '0' && t[k] <= '9')
                    v = v * 10 + (t[k++] - '0');
                if (k == startK) return false;
                part[pi] = v;
                if (k < t.length && t[k] == ':') k++;
                else break;
            }
            if (k != t.length) return false;
            offset = sign * (hours(part[0]) + minutes(part[1]) + seconds(part[2]));
        }
        else if (i != t.length)
            return false;
        // value in tz is local time with `offset` from UTC => UTC = local - offset
        st = SysTime(dt, hnsecs.hnsecs, UTC()) - offset;
        return true;
    }
    catch (Exception)
        return false;
}

private long fractionToHnsecs(const(char)[] digits)
{
    long v = 0;
    foreach (k; 0 .. 7)
        v = v * 10 + (k < digits.length ? digits[k] - '0' : 0);
    return v;
}

// text array literal parser: {a,b,"c d",NULL,{nested}}
private PgValue parseArray(uint elemOid, const(char)[] t, ref size_t pos)
{
    if (pos >= t.length || t[pos] != '{')
        throw new PgTypeException("malformed array literal: " ~ t.idup);
    pos++;
    PgValue arr;
    arr.kind = PgKind.array;
    if (pos < t.length && t[pos] == '}') { pos++; return arr; }
    while (pos < t.length)
    {
        if (t[pos] == '{')
            arr.items ~= parseArray(elemOid, t, pos);
        else if (t[pos] == '"')
        {
            pos++;
            char[] buf;
            while (pos < t.length && t[pos] != '"')
            {
                if (t[pos] == '\\') pos++;
                buf ~= t[pos++];
            }
            pos++; // closing quote
            arr.items ~= decodeText(elemOid, buf);
        }
        else
        {
            size_t st = pos;
            while (pos < t.length && t[pos] != ',' && t[pos] != '}') pos++;
            auto tok = t[st .. pos];
            arr.items ~= tok == "NULL" ? PgValue.null_ : decodeText(elemOid, tok);
        }
        if (pos >= t.length) break;
        if (t[pos] == ',') { pos++; continue; }
        if (t[pos] == '}') { pos++; return arr; }
    }
    throw new PgTypeException("unterminated array literal: " ~ t.idup);
}

// ---------------------------------------------------------------------------
// parameter encoding (text format, server infers the type)
// ---------------------------------------------------------------------------

/// A bound parameter in text format. `null` data means SQL NULL.
struct Param
{
    bool isNull = true;
    string text;

    static Param null_() { return Param.init; }
    static Param of(string t) { return Param(false, t); }
}

private string hex(const(ubyte)[] d)
{
    static immutable digits = "0123456789abcdef";
    char[] r = new char[2 + d.length * 2];
    r[0] = '\\'; r[1] = 'x';
    foreach (i, b; d)
    {
        r[2 + 2 * i] = digits[b >> 4];
        r[3 + 2 * i] = digits[b & 15];
    }
    return (() @trusted => cast(string) r)();
}

private string quoteArrayElem(string s)
{
    string r = "\"";
    foreach (c; s)
    {
        if (c == '"' || c == '\\') r ~= '\\';
        r ~= c;
    }
    return r ~ '"';
}

/// Encode any supported D value as a parameter.
Param toParam(T)(auto ref const T v)
{
    static if (is(T == Param))
        return v;
    else static if (is(T == typeof(null)))
        return Param.null_;
    else static if (is(T == PgValue))
        return v.isNull ? Param.null_ : Param.of(v.kind == PgKind.bytea ? hex(v.bytes) : v.toString());
    else static if (isInstanceOf!(Nullable, T))
        return v.isNull ? Param.null_ : toParam(v.get);
    else static if (is(T == bool))
        return Param.of(v ? "t" : "f");
    else static if (isIntegral!T)
        return Param.of(v.to!string);
    else static if (isFloatingPoint!T)
        return Param.of(v.to!string);
    else static if (isSomeString!T)
        return Param.of(v.to!string);
    else static if (is(Unqual!T == SysTime))
        return Param.of(v.toUTC().toISOExtString());
    else static if (is(Unqual!T == DateTime))
        return Param.of(v.toISOExtString());
    else static if (is(Unqual!T == Date))
        return Param.of(v.toISOExtString());
    else static if (is(T : const(ubyte)[]))
        return Param.of(hex(v));
    else static if (isArray!T)
    {
        string r = "{";
        foreach (i, ref e; v)
        {
            if (i) r ~= ",";
            auto p = toParam(e);
            if (p.isNull) r ~= "NULL";
            else if (p.text.length && p.text[0] == '{') r ~= p.text; // nested array
            else r ~= quoteArrayElem(p.text);
        }
        return Param.of(r ~ "}");
    }
    else
        static assert(false, "cannot encode " ~ T.stringof ~ " as a PostgreSQL parameter");
}

// ---------------------------------------------------------------------------
// typed decoding straight from text (no PgValue intermediate)
// ---------------------------------------------------------------------------

private bool parseLong(const(char)[] t, out long r)
{
    if (t.length == 0) return false;
    size_t i = 0;
    bool neg;
    if (t[0] == '-') { neg = true; i = 1; }
    if (i == t.length) return false;
    ulong v;
    for (; i < t.length; i++)
    {
        const d = t[i] - '0';
        if (d < 0 || d > 9) return false;
        if (v > (ulong.max - d) / 10) return false;
        v = v * 10 + d;
    }
    if (neg) { if (v > cast(ulong) long.max + 1) return false; r = cast(long)(-v); }
    else { if (v > long.max) return false; r = cast(long) v; }
    return true;
}

/// Decode a text-format cell directly into `T`. If `stable` the text lives in an immutable,
/// never reused buffer and strings may alias it instead of being copied.
T decodeAs(T)(uint oid, const(char)[] txt, bool stable)
{
    static if (is(T == PgValue))
        return decodeText(oid, txt);
    else static if (isInstanceOf!(Nullable, T))
        return T(decodeAs!(typeof(T.init.get()))(oid, txt, stable));
    else static if (is(T == bool))
    {
        if (oid == Oid.bool_) return txt.length && (txt[0] == 't' || txt[0] == 'T');
        long l;
        if (parseLong(txt, l)) return l != 0;
    }
    else static if (isIntegral!T)
    {
        long l;
        if (parseLong(txt, l))
        {
            static if (isSigned!T) { if (l < T.min || l > T.max) throw new PgTypeException("value " ~ txt.idup ~ " out of range for " ~ T.stringof); }
            else { if (l < 0 || cast(ulong) l > T.max) throw new PgTypeException("value " ~ txt.idup ~ " out of range for " ~ T.stringof); }
            return cast(T) l;
        }
        if (oid == Oid.numeric)
        {
            try return txt.to!T;
            catch (ConvException) {}
        }
    }
    else static if (isFloatingPoint!T)
    {
        if (oid == Oid.float4 || oid == Oid.float8 || oid == Oid.numeric || oid == Oid.int2 || oid == Oid.int4 || oid == Oid.int8)
        {
            try return txt.to!T;
            catch (ConvException) {}
        }
    }
    else static if (is(T == string))
    {
        if (stable) return (() @trusted => cast(string) txt)();
        return txt.idup;
    }
    else static if (isSomeString!T)
        return txt.to!T;
    else static if (is(T == ubyte[]))
    {
        if (oid == Oid.bytea) return decodeBytea(txt);
    }
    else static if (is(T == SysTime))
    {
        SysTime st;
        if (oid == Oid.date ? parseDate(txt, st) : parseTimestamp(txt, oid == Oid.timestamptz, st)) return st;
    }
    else static if (is(T == DateTime) || is(T == Date))
    {
        SysTime st;
        if (oid == Oid.date ? parseDate(txt, st) : parseTimestamp(txt, oid == Oid.timestamptz, st))
            return cast(T) st;
    }
    else static if (isArray!T)
    {
        const el = arrayElementOid(oid);
        if (el)
        {
            size_t pos = 0;
            if (txt.length && txt[0] == '[')
            {
                import std.string : indexOf;
                const eq = txt.indexOf('=');
                if (eq >= 0) pos = eq + 1;
            }
            return parseArrayAs!T(el, txt, pos, stable);
        }
    }
    else
        static assert(false, "cannot decode into " ~ T.stringof);
    throw new PgTypeException("cannot convert value '" ~ txt.idup ~ "' (oid " ~ oid.to!string ~ ") to " ~ T.stringof);
}

private enum isNestedArr(E) = isArray!E && !isSomeString!E && !is(E == ubyte[]);

private T parseArrayAs(T)(uint elemOid, const(char)[] t, ref size_t pos, bool stable)
{
    import std.array : Appender;
    alias E = Unqual!(ForeachType!T);
    if (pos >= t.length || t[pos] != '{')
        throw new PgTypeException("malformed array literal: " ~ t.idup);
    pos++;
    Appender!(E[]) r;
    if (pos < t.length && t[pos] == '}') { pos++; return cast(T) r.data; }
    while (pos < t.length)
    {
        static if (isNestedArr!E)
            r.put(parseArrayAs!E(elemOid, t, pos, stable));
        else
        {
            static if (isInstanceOf!(Nullable, E)) alias U = typeof(E.init.get());
            else alias U = E;
            const(char)[] tok;
            bool quoted, aliasable = stable;
            if (t[pos] == '"')
            {
                quoted = true;
                pos++;
                const st = pos;
                bool esc;
                while (pos < t.length && t[pos] != '"')
                {
                    if (t[pos] == '\\') { esc = true; pos++; }
                    pos++;
                }
                if (esc)
                {
                    char[] buf;
                    for (size_t j = st; j < pos; j++)
                    {
                        if (t[j] == '\\') j++;
                        buf ~= t[j];
                    }
                    tok = buf;
                    aliasable = false;
                }
                else
                    tok = t[st .. pos];
                pos++; // closing quote
            }
            else
            {
                const st = pos;
                while (pos < t.length && t[pos] != ',' && t[pos] != '}') pos++;
                tok = t[st .. pos];
            }
            if (!quoted && tok == "NULL")
            {
                static if (isInstanceOf!(Nullable, E)) r.put(E.init);
                else throw new PgTypeException("NULL array element cannot be converted to " ~ E.stringof);
            }
            else
            {
                static if (isInstanceOf!(Nullable, E)) r.put(E(decodeAs!U(elemOid, tok, aliasable)));
                else r.put(decodeAs!U(elemOid, tok, aliasable));
            }
        }
        if (pos >= t.length) break;
        if (t[pos] == ',') { pos++; continue; }
        if (t[pos] == '}') { pos++; return cast(T) r.data; }
    }
    throw new PgTypeException("unterminated array literal: " ~ t.idup);
}
