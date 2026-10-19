/// Query results.
///
/// A `Result` keeps all cell bytes of a query in ONE immutable buffer plus a flat, pointer-free
/// `Cell[]` index, so the GC sees three allocations per result instead of one per cell. Cells
/// are decoded lazily by `Row.get!T`; strings returned from a `Result` alias the buffer.
module postgrex.result;

@safe:

import postgrex.error;
import postgrex.types;
import postgrex.wire;
import std.array : Appender;
import std.traits;
import std.typecons : Nullable;

/// Column metadata from a RowDescription message.
struct Column
{
    string name;
    uint tableOid;
    short attNum;
    uint typeOid;
    short typeSize;
    int typeMod;
}

/// Location of one cell inside the result buffer; `len < 0` means NULL.
struct Cell
{
    uint off;
    int len;
}

/// A view of one result row. Holds no allocations of its own.
///
/// Rows handed to `queryEach` callbacks are only valid during the callback (`stable == false`):
/// `get!string` then copies. Rows from a `Result` may be kept freely.
struct Row
{
    const(Column)[] columns;
    const(ubyte)[] data;
    const(Cell)[] cells;
    bool stable;

    size_t length() const { return cells.length; }

    bool isNull(size_t i) const { return cells[i].len < 0; }
    bool isNull(string name) const { return isNull(indexOf(name)); }

    /// Raw text of a non-NULL cell.
    const(char)[] raw(size_t i) const
    {
        const c = cells[i];
        if (c.len < 0)
            throw new PgTypeException("NULL has no text");
        return cast(const(char)[]) data[c.off .. c.off + c.len];
    }

    /// Dynamically typed value (allocates for text/array cells).
    PgValue opIndex(size_t i) const
    {
        if (i >= cells.length)
            throw new PgTypeException("column index out of range");
        if (cells[i].len < 0)
            return PgValue.null_;
        return decodeText(columns[i].typeOid, raw(i));
    }

    PgValue opIndex(string name) const { return opIndex(indexOf(name)); }

    size_t indexOf(string name) const
    {
        foreach (i, ref c; columns)
            if (c.name == name)
                return i;
        throw new PgTypeException("no such column: " ~ name);
    }

    /// Decode column `i` as `T` (bool, integers, floats, string, ubyte[], SysTime, Date, DateTime,
    /// arrays of these, `Nullable!X`, or `PgValue`).
    T get(T)(size_t i) const
    {
        if (i >= cells.length)
            throw new PgTypeException("column index out of range");
        const c = cells[i];
        static if (isInstanceOf!(Nullable, T))
        {
            if (c.len < 0)
                return T.init;
        }
        else static if (is(T == PgValue))
        {
            if (c.len < 0)
                return PgValue.null_;
        }
        else
        {
            if (c.len < 0)
                throw new PgTypeException("NULL cannot be converted to " ~ T.stringof);
        }
        return decodeAs!T(columns[i].typeOid, raw(i), stable);
    }

    T get(T)(string name) const { return get!T(indexOf(name)); }

    /// Map columns to the fields of struct `S` by name. Missing columns leave the field at its default;
    /// extra columns are ignored.
    S as(S)() const if (is(S == struct))
    {
        enum n = FieldNameTuple!S.length;
        size_t[n] map = size_t.max;
        static foreach (i, name; FieldNameTuple!S)
            foreach (ci, ref c; columns)
                if (c.name == name) { map[i] = ci; break; }
        return as!S(map[]);
    }

    package S as(S)(const(size_t)[] map) const
    {
        S s;
        static foreach (i, name; FieldNameTuple!S)
            if (map[i] != size_t.max)
                s.tupleof[i] = get!(typeof(s.tupleof[i]))(map[i]);
        return s;
    }
}

/// Forward range over the rows of a `Result`.
struct RowRange
{
    private const(Column)[] columns;
    private const(ubyte)[] data;
    private const(Cell)[] cells;
    private size_t n; // columns per row

    bool empty() const { return n == 0 ? true : cells.length == 0; }
    size_t length() const { return n == 0 ? 0 : cells.length / n; }
    Row front() const { return Row(columns, data, cells[0 .. n], true); }
    void popFront() { cells = cells[n .. $]; }
    RowRange save() const { return this; }
}

/// Result of one SQL statement.
struct Result
{
    Column[] columns;
    immutable(ubyte)[] data;
    Cell[] cells;
    size_t rowCount;
    string commandTag;   /// e.g. "SELECT 3", "INSERT 0 1"
    long rowsAffected;   /// parsed from the command tag (rows returned for SELECT)

    size_t length() const { return rowCount; }
    bool empty() const { return rowCount == 0; }

    Row opIndex(size_t i) const
    {
        if (i >= rowCount)
            throw new PgTypeException("row index out of range");
        const n = columns.length;
        return Row(columns, data, cells[i * n .. (i + 1) * n], true);
    }

    /// `foreach (row; result)` iterates through this.
    RowRange opSlice() const
    {
        return RowRange(columns, data, cells, columns.length);
    }

    /// First column of the first row.
    T scalar(T = PgValue)() const
    {
        if (rowCount == 0 || columns.length == 0)
            throw new PgTypeException("query returned no rows");
        return this[0].get!T(0);
    }

    /// All rows mapped to a struct.
    S[] as(S)() const
    {
        enum n = FieldNameTuple!S.length;
        size_t[n] map = size_t.max;
        static foreach (i, name; FieldNameTuple!S)
            foreach (ci, ref c; columns)
                if (c.name == name) { map[i] = ci; break; }
        auto r = new S[rowCount];
        foreach (i; 0 .. rowCount)
            r[i] = this[i].as!S(map[]);
        return r;
    }
}

/// Accumulates DataRow messages into a Result.
package struct Collector
{
    Column[] columns;
    Appender!(ubyte[]) arena;
    Appender!(Cell[]) cells;
    size_t rows;

    void begin(Column[] cols)
    {
        columns = cols;
        arena = Appender!(ubyte[]).init;
        cells = Appender!(Cell[]).init;
        rows = 0;
    }

    void addRow(ref Cursor c)
    {
        const n = c.int16();
        if (n != columns.length)
            throw new PgProtocolException("DataRow does not match RowDescription");
        foreach (_; 0 .. n)
        {
            const len = c.int32();
            if (len < 0)
            {
                cells.put(Cell(0, -1));
                continue;
            }
            const off = arena.data.length;
            if (off + len > uint.max)
                throw new PgProtocolException("result set too large (use queryEach to stream it)");
            arena.put(c.bytes(len));
            cells.put(Cell(cast(uint) off, len));
        }
        rows++;
    }

    Result finish(string tag, long affected)
    {
        Result r;
        r.columns = columns;
        r.data = (() @trusted => cast(immutable(ubyte)[]) arena.data)();
        r.cells = cells.data;
        r.rowCount = rows;
        r.commandTag = tag;
        r.rowsAffected = affected;
        return r;
    }
}

/// An asynchronous NOTIFY message.
struct Notification
{
    int pid;
    string channel;
    string payload;
}
