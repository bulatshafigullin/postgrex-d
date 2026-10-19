/// Authentication helpers: MD5 and SCRAM-SHA-256 (RFC 5802 / 7677).
module postgrex.auth;

@safe:

import postgrex.error;
import std.base64 : Base64;
import std.digest.hmac : hmac;
import std.digest.md : md5Of;
import std.digest.sha : SHA256, sha256Of;
import std.string : indexOf, split, startsWith;
import std.conv : to;
import std.array : array;
import std.algorithm : map;

package:

string md5Password(string user, string password, const(ubyte)[] salt)
{
    import std.digest : toHexString, LetterCase;
    auto inner = md5Of(password, user).toHexString!(LetterCase.lower)().idup;
    ubyte[] buf = cast(ubyte[]) inner.dup;
    buf ~= salt;
    return "md5" ~ md5Of(buf).toHexString!(LetterCase.lower)().idup;
}

private ubyte[32] hmacSha256(const(ubyte)[] key, const(ubyte)[] msg)
{
    return hmac!SHA256(msg, key);
}

private ubyte[32] pbkdf2Sha256(const(ubyte)[] password, const(ubyte)[] salt, uint iterations)
{
    ubyte[32] u = hmacSha256(password, salt ~ cast(ubyte[]) [0, 0, 0, 1]);
    ubyte[32] result = u;
    foreach (_; 1 .. iterations)
    {
        u = hmacSha256(password, u[]);
        result[] ^= u[];
    }
    return result;
}

private string randomNonce()
{
    ubyte[18] raw;
    version (Posix)
    {
        import std.stdio : File;
        auto f = File("/dev/urandom", "rb");
        f.rawRead(raw[]);
    }
    else
    {
        import std.random : uniform, unpredictableSeed, Random;
        auto rng = Random(unpredictableSeed);
        foreach (ref b; raw)
            b = cast(ubyte) uniform(0, 256, rng);
    }
    return Base64.encode(raw[]).idup;
}

/// Client side of the SCRAM-SHA-256 exchange.
struct Scram
{
    private string password;
    private string nonce;
    private string clientFirstBare;
    private ubyte[32] serverKeySig;
    private bool haveSig;

    this(string password, string fixedNonce = null)
    {
        this.password = password;
        nonce = fixedNonce.length ? fixedNonce : randomNonce();
    }

    /// client-first-message
    string first()
    {
        clientFirstBare = "n=,r=" ~ nonce;
        return "n,," ~ clientFirstBare;
    }

    /// Consume server-first-message, produce client-final-message.
    string final_(string serverFirst)
    {
        string r, s;
        uint iter;
        foreach (part; serverFirst.split(","))
        {
            if (part.startsWith("r=")) r = part[2 .. $];
            else if (part.startsWith("s=")) s = part[2 .. $];
            else if (part.startsWith("i=")) iter = part[2 .. $].to!uint;
        }
        if (!r.startsWith(nonce) || s.length == 0 || iter == 0)
            throw new PgProtocolException("invalid SCRAM server-first message");

        auto salt = Base64.decode(s);
        auto salted = pbkdf2Sha256(cast(const(ubyte)[]) password, salt, iter);
        auto clientKey = hmacSha256(salted[], cast(const(ubyte)[]) "Client Key");
        auto storedKey = sha256Of(clientKey[]);
        const withoutProof = "c=biws,r=" ~ r;
        const authMessage = clientFirstBare ~ "," ~ serverFirst ~ "," ~ withoutProof;
        auto clientSig = hmacSha256(storedKey[], cast(const(ubyte)[]) authMessage);
        ubyte[32] proof = clientKey;
        proof[] ^= clientSig[];
        auto serverKey = hmacSha256(salted[], cast(const(ubyte)[]) "Server Key");
        serverKeySig = hmacSha256(serverKey[], cast(const(ubyte)[]) authMessage);
        haveSig = true;
        return withoutProof ~ ",p=" ~ Base64.encode(proof[]).idup;
    }

    /// Verify server-final-message.
    void verify(string serverFinal)
    {
        if (serverFinal.startsWith("e="))
            throw new PgProtocolException("SCRAM authentication failed: " ~ serverFinal[2 .. $]);
        if (!haveSig || !serverFinal.startsWith("v="))
            throw new PgProtocolException("invalid SCRAM server-final message");
        auto got = Base64.decode(serverFinal[2 .. $]);
        if (got != serverKeySig[])
            throw new PgProtocolException("SCRAM server signature mismatch");
    }
}

unittest
{
    // the first message has a fixed shape (PostgreSQL ignores the user name here)
    auto sc = Scram("pencil", "rOprNGfwEbeRWgbNEkqO");
    assert(sc.first() == "n,,n=,r=rOprNGfwEbeRWgbNEkqO");
}
