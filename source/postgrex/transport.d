/// Byte transport to the server: plain TCP, optionally upgraded to TLS (SSLRequest handshake).
///
/// TLS support is compiled in with the dub configuration `tls` (version `PostgrexTLS`), which
/// adds a dependency on vibe-stream:tls (OpenSSL). Without it, `SslMode.require` and the
/// `verify-*` modes fail with a clear error and `prefer` silently uses plain TCP.
module postgrex.transport;

@safe:

import postgrex.error;
import core.time : Duration;
import vibe.core.net : TCPConnection, connectTCP;

version (PostgrexTLS)
{
    import vibe.stream.tls;
}

/// Mirrors libpq's `sslmode`.
enum SslMode
{
    disable,    /// never use TLS
    prefer,     /// use TLS when the server offers it, no certificate checks (default)
    require,    /// TLS required, no certificate checks
    verifyCa,   /// TLS required, certificate must chain to `sslRootCert`
    verifyFull, /// like verifyCa, and the certificate must match the host name
}

/// True when this build can speak TLS.
version (PostgrexTLS) enum tlsAvailable = true;
else enum tlsAvailable = false;

package struct Transport
{
    private TCPConnection sock;
    version (PostgrexTLS) private TLSStream tls;
    private bool useTls;

    bool encrypted() const { return useTls; }

    static Transport connect(string host, ushort port, Duration connectTimeout, Duration readTimeout,
                             SslMode mode, string rootCert)
    {
        Transport t;
        t.sock = connectTCP(host, port, null, 0, connectTimeout);
        t.sock.tcpNoDelay = true;
        if (readTimeout != Duration.max)
            t.sock.readTimeout = readTimeout;
        try
            t.negotiate(host, mode, rootCert);
        catch (Exception e)
        {
            t.close();
            throw e;
        }
        return t;
    }

    private void negotiate(string host, SslMode mode, string rootCert)
    {
        if (mode == SslMode.disable)
            return;
        static if (!tlsAvailable)
        {
            if (mode == SslMode.prefer)
                return;
            throw new PgException("sslmode requires TLS, but postgrex-d was built without it " ~
                                  "(use subConfiguration \"postgrex-d\" \"tls\")");
        }
        else
        {
            if ((mode == SslMode.verifyCa || mode == SslMode.verifyFull) && rootCert.length == 0)
                throw new PgException("sslmode=verify-ca/verify-full needs ConnectOptions.sslRootCert");

            // SSLRequest: int32 length 8, int32 code 80877103
            static immutable ubyte[8] req = [0, 0, 0, 8, 4, 210, 22, 47];
            sock.write(req[]);
            sock.flush();
            ubyte[1] answer;
            sock.read(answer[]);
            if (answer[0] == 'N')
            {
                if (mode == SslMode.prefer)
                    return;
                throw new PgException("server does not support TLS");
            }
            if (answer[0] != 'S')
                throw new PgProtocolException("unexpected reply to SSLRequest");

            auto ctx = createTLSContext(TLSContextKind.client);
            final switch (mode)
            {
            case SslMode.disable, SslMode.prefer, SslMode.require:
                ctx.peerValidationMode = TLSPeerValidationMode.none;
                break;
            case SslMode.verifyCa:
                ctx.peerValidationMode = TLSPeerValidationMode.requireCert | TLSPeerValidationMode.checkCert |
                                         TLSPeerValidationMode.checkTrust;
                break;
            case SslMode.verifyFull:
                ctx.peerValidationMode = TLSPeerValidationMode.trustedCert;
                break;
            }
            if (rootCert.length)
                ctx.useTrustedCertificateFile(rootCert);
            tls = createTLSStream(sock, ctx, host, sock.remoteAddress);
            useTls = true;
        }
    }

    void read(ubyte[] dst)
    {
        version (PostgrexTLS) if (useTls) { tls.read(dst); return; }
        sock.read(dst);
    }

    void write(const(ubyte)[] data)
    {
        version (PostgrexTLS) if (useTls) { tls.write(data); return; }
        sock.write(data);
    }

    void flush()
    {
        version (PostgrexTLS) if (useTls) { tls.flush(); return; }
        sock.flush();
    }

    /// Wait until bytes can be read; false on timeout.
    bool waitForData(Duration timeout)
    {
        version (PostgrexTLS) if (useTls && tls.dataAvailableForRead) return true;
        return sock.waitForData(timeout);
    }

    void close()
    {
        version (PostgrexTLS) if (useTls)
        {
            try tls.finalize(); catch (Exception) {}
            useTls = false;
        }
        try sock.close(); catch (Exception) {}
    }
}
