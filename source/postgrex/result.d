/// Query results.
module postgrex.result;

@safe:

import postgrex.error;
import postgrex.types;
import std.traits;

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

/// One result row.
struct Row
{
    PgValue[] values;
    const(Column)[] columns;

    size_t length() const { return values.length; }

    ref const(PgValue) opIndex(size_t i) const
    {
        if (i >= values.length)
            throw new PgTypeException("column index out of range");
        return values[i];
    }

    ref const(PgValue) opIndex(string name) const
    {
        return values[indexOf(name)];
    }

    size_t indexOf(string name) const
    {
        foreach (i, ref c; columns)
            if (c.name == name)
                return i;
        throw new PgTypeException("no such column: " ~ name);
    }

    T get(T)(size_t i) const { return opIndex(i).get!T; }
    T get(T)(string name) const { return opIndex(name).get!T; }

    /// Map columns to the fields of struct `S` by name. Missing columns leave the field at its default;
    /// extra columns are ignored.
    S as(S)() const if (is(S == struct))
    {
        S s;
        static foreach (i, name; FieldNameTuple!S)
        {{
            foreach (ci, ref c; columns)
                if (c.name == name)
                {
                    alias F = typeof(s.tupleof[i]);
                    s.tupleof[i] = values[ci].get!F;
                    break;
                }
        }}
        return s;
    }
}

/// Result of one SQL statement.
struct Result
{
    Column[] columns;
    Row[] rows;
    string commandTag;   /// e.g. "SELECT 3", "INSERT 0 1"
    long rowsAffected;   /// parsed from the command tag (rows returned for SELECT)

    alias rows this;

    /// First column of the first row.
    T scalar(T = PgValue)() const
    {
        if (rows.length == 0 || rows[0].length == 0)
            throw new PgTypeException("query returned no rows");
        return rows[0][0].get!T;
    }

    /// All rows mapped to a struct.
    S[] as(S)() const
    {
        S[] r;
        r.reserve(rows.length);
        foreach (ref row; rows)
            r ~= row.as!S;
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
