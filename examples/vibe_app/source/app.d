import postgrex;
import vibe.vibe;
import std.process : environment;

PgPool db;

void main()
{
    db = new PgPool(environment.get("DATABASE_URL", "postgres://postgres:secret@127.0.0.1:54329/postgres"), 10);
    db.exec("create table if not exists todos(id serial primary key, title text not null, done bool not null default false)");

    auto router = new URLRouter;
    router.get("/todos", &list);
    router.post("/todos", &create);
    router.get("/slow", &slow);

    auto settings = new HTTPServerSettings;
    settings.port = environment.get("PORT", "8080").to!ushort;
    settings.bindAddresses = ["127.0.0.1"];
    listenHTTP(settings, router);
    runApplication();
}

void list(HTTPServerRequest req, HTTPServerResponse res) @safe
{
    auto rows = db.query("select id, title, done from todos order by id");
    Json[] arr;
    foreach (row; rows)
        arr ~= serializeToJson(["id": Json(row.get!int("id")), "title": Json(row.get!string("title")),
                                "done": Json(row.get!bool("done"))]);
    res.writeJsonBody(arr);
}

void create(HTTPServerRequest req, HTTPServerResponse res) @safe
{
    auto title = req.json["title"].get!string;
    // parameters are sent separately from the SQL text: no injection possible
    auto id = db.query("insert into todos(title) values ($1) returning id", title).scalar!int;
    res.statusCode = 201;
    res.writeJsonBody(["id": id]);
}

/// Each request sleeps 500ms inside Postgres; the event loop stays free so requests overlap.
void slow(HTTPServerRequest req, HTTPServerResponse res) @safe
{
    db.query("select pg_sleep(0.5)");
    res.writeBody("ok\n");
}
