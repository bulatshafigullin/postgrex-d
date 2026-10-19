/+ dub.sdl:
    name "basic"
    dependency "postgrex-d" path=".."
+/
import postgrex;
import std.stdio;

void main()
{
    auto db = new PgConnection("postgres://localhost/postgres");
    db.connect();
    scope (exit) db.close();
    auto r = db.query("select $1::int + $2::int as sum, $3::text as s", 40, 2, "hi");
    writeln(r[0].get!int("sum"), " ", r[0].get!string("s"));
}
