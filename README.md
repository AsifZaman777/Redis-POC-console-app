# Redis Trade POC - C# Console Application

This project mirrors the Node.js Redis trade POC with a .NET 8 console interface.

## Run

Start Redis from the repository root:

```powershell
docker compose up -d
```

Run the console app:

```powershell
dotnet run --project .\csharp\RedisTradePoc.csproj
```

The app uses `127.0.0.1:6379` by default. Override the connection with `REDIS_HOST`,
`REDIS_PORT`, `REDIS_USERNAME`, and `REDIS_PASSWORD`.

The console supports buy trades, position queries, Redis stream replay, and resetting
the POC state. Buy trades use one Redis stream append followed by a batched hash/set
update, matching the Node.js implementation.
