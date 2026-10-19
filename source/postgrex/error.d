/// Exceptions raised by the driver.
module postgrex.error;

@safe:

import std.format : format;

/// Base class of every exception thrown by this library.
class PgException : Exception
{
    this(string msg, string file = __FILE__, size_t line = __LINE__, Throwable next = null) @safe pure nothrow
    {
        super(msg, file, line, next);
    }
}

/// Network / protocol level failure. The connection is closed when this is thrown.
class PgProtocolException : PgException
{
    this(string msg, string file = __FILE__, size_t line = __LINE__) @safe pure nothrow
    {
        super(msg, file, line);
    }
}

/// Raised on type conversion problems (`PgValue.get!T`, parameter encoding, ...).
class PgTypeException : PgException
{
    this(string msg, string file = __FILE__, size_t line = __LINE__) @safe pure nothrow
    {
        super(msg, file, line);
    }
}

/// Fields of an ErrorResponse / NoticeResponse message.
struct PgNotice
{
    string severity;
    string code; /// SQLSTATE
    string message;
    string detail;
    string hint;
    string position;
    string schema;
    string table;
    string column;
    string dataType;
    string constraint;
    string where;

    string toString() const
    {
        auto s = format("%s: %s (SQLSTATE %s)", severity, message, code);
        if (detail.length)
            s ~= "\n  DETAIL: " ~ detail;
        if (hint.length)
            s ~= "\n  HINT: " ~ hint;
        return s;
    }
}

/// An error reported by the server. The connection remains usable.
class PgServerException : PgException
{
    PgNotice error;

    this(PgNotice e, string file = __FILE__, size_t line = __LINE__)
    {
        error = e;
        super(e.toString(), file, line);
    }

    /// SQLSTATE code, e.g. "23505" for unique_violation.
    @property string code() const @safe pure nothrow @nogc
    {
        return error.code;
    }
}
