using System.Globalization;
using System.Text.Json;
using StackExchange.Redis;

const string streamKey = "stream:trades";
var redisHost = Environment.GetEnvironmentVariable("REDIS_HOST") ?? "127.0.0.1";
var redisPort = Environment.GetEnvironmentVariable("REDIS_PORT") ?? "6379";
var redisPassword = Environment.GetEnvironmentVariable("REDIS_PASSWORD");
var redisUsername = Environment.GetEnvironmentVariable("REDIS_USERNAME");

var configuration = new ConfigurationOptions
{
    EndPoints = { $"{redisHost}:{redisPort}" },
    AbortOnConnectFail = false,
    ConnectRetry = 3
};

if (!string.IsNullOrWhiteSpace(redisUsername))
    configuration.User = redisUsername;
if (!string.IsNullOrWhiteSpace(redisPassword))
    configuration.Password = redisPassword;

using var redis = await ConnectionMultiplexer.ConnectAsync(configuration);
var database = redis.GetDatabase();

Console.WriteLine($"Redis Trade POC connected to {redisHost}:{redisPort}");
Console.WriteLine($"PING: {await database.PingAsync()}");

while (true)
{
    Console.WriteLine("\n1. Execute buy trade");
    Console.WriteLine("2. Show all positions");
    Console.WriteLine("3. Show account positions");
    Console.WriteLine("4. Show trade stream");
    Console.WriteLine("5. Reset Redis state");
    Console.WriteLine("0. Exit");
    Console.Write("Select an option: ");

    switch (Console.ReadLine()?.Trim())
    {
        case "1":
            await ExecuteBuyAsync(database);
            break;
        case "2":
            await ShowAllPositionsAsync(database);
            break;
        case "3":
            await ShowAccountPositionsAsync(database);
            break;
        case "4":
            await ShowTradeStreamAsync(database);
            break;
        case "5":
            await ResetAsync(redis, database);
            break;
        case "0":
            return;
        default:
            Console.WriteLine("Invalid option.");
            break;
    }
}

static async Task ExecuteBuyAsync(IDatabase database)
{
    var account = ReadOrDefault("Account", "ACC_001");
    var symbol = ReadRequired("Symbol").ToUpperInvariant();
    var quantity = ReadPositiveDecimal("Quantity");
    var price = ReadPositiveDecimal("Price");
    var timestamp = DateTimeOffset.UtcNow;
    var tradeId = $"TRD_{timestamp.ToUnixTimeMilliseconds()}_{Guid.NewGuid():N}"[..20].ToUpperInvariant();

    var streamEntryId = await database.StreamAddAsync(
        streamKey,
        new[]
        {
            new NameValueEntry("tradeId", tradeId),
            new NameValueEntry("event", "TRADE_EXEC"),
            new NameValueEntry("side", "BUY"),
            new NameValueEntry("account", account),
            new NameValueEntry("symbol", symbol),
            new NameValueEntry("quantity", quantity.ToString(CultureInfo.InvariantCulture)),
            new NameValueEntry("price", price.ToString(CultureInfo.InvariantCulture)),
            new NameValueEntry("timestamp", timestamp.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture))
        });

    var positionKey = $"position:{account}:{symbol}";
    var existing = await database.HashGetAllAsync(positionKey);
    var currentQuantity = GetDecimal(existing, "shares");
    var currentAveragePrice = GetDecimal(existing, "avg_price");
    var totalInvested = currentQuantity * currentAveragePrice;
    var newQuantity = currentQuantity + quantity;
    var newTotalInvested = totalInvested + quantity * price;
    var newAveragePrice = newTotalInvested / newQuantity;

    var batch = database.CreateBatch();
    var hashTask = batch.HashSetAsync(positionKey, new[]
    {
        new HashEntry("symbol", symbol),
        new HashEntry("account", account),
        new HashEntry("shares", newQuantity.ToString("F4", CultureInfo.InvariantCulture)),
        new HashEntry("avg_price", newAveragePrice.ToString("F4", CultureInfo.InvariantCulture)),
        new HashEntry("total_invested", newTotalInvested.ToString("F2", CultureInfo.InvariantCulture)),
        new HashEntry("last_trade_id", tradeId),
        new HashEntry("last_updated", timestamp.ToString("O"))
    });
    var symbolTask = batch.SetAddAsync($"account:{account}:symbols", symbol);
    var accountTask = batch.SetAddAsync("accounts:all", account);
    batch.Execute();
    await Task.WhenAll(hashTask, symbolTask, accountTask);

    Console.WriteLine(JsonSerializer.Serialize(new
    {
        success = true,
        trade = new { tradeId, streamEntryId = streamEntryId.ToString(), account, symbol, side = "BUY", quantity, price },
        updatedPosition = new { symbol, shares = newQuantity, avgPrice = newAveragePrice, totalInvested = newTotalInvested }
    }, new JsonSerializerOptions { WriteIndented = true }));
}

static async Task ShowAllPositionsAsync(IDatabase database)
{
    var accounts = await database.SetMembersAsync("accounts:all");
    var positions = new List<Dictionary<string, string>>();

    foreach (var account in accounts)
    {
        var symbols = await database.SetMembersAsync($"account:{account}:symbols");
        foreach (var symbol in symbols)
            AddPosition(positions, await database.HashGetAllAsync($"position:{account}:{symbol}"));
    }

    Console.WriteLine(JsonSerializer.Serialize(new { positions }, new JsonSerializerOptions { WriteIndented = true }));
}

static async Task ShowAccountPositionsAsync(IDatabase database)
{
    var account = ReadRequired("Account");
    var symbols = await database.SetMembersAsync($"account:{account}:symbols");
    var positions = new List<Dictionary<string, string>>();

    foreach (var symbol in symbols)
        AddPosition(positions, await database.HashGetAllAsync($"position:{account}:{symbol}"));

    Console.WriteLine(JsonSerializer.Serialize(new { account, positions }, new JsonSerializerOptions { WriteIndented = true }));
}

static async Task ShowTradeStreamAsync(IDatabase database)
{
    var entries = await database.StreamRangeAsync(streamKey);
    var trades = entries.Reverse().Select(entry =>
        new Dictionary<string, string> { ["streamId"] = entry.Id.ToString() }
            .Concat(entry.Values.ToDictionary(value => value.Name.ToString(), value => value.Value.ToString()))
            .ToDictionary(pair => pair.Key, pair => pair.Value));

    Console.WriteLine(JsonSerializer.Serialize(new { total = entries.Length, trades }, new JsonSerializerOptions { WriteIndented = true }));
}

static async Task ResetAsync(ConnectionMultiplexer redis, IDatabase database)
{
    await database.KeyDeleteAsync(streamKey);
    var server = redis.GetServer(redis.GetEndPoints()[0]);
    var keys = server.Keys(pattern: "position:*")
        .Concat(server.Keys(pattern: "account:*"))
        .Concat(new[] { (RedisKey)"accounts:all" })
        .Distinct()
        .ToArray();

    if (keys.Length > 0)
        await database.KeyDeleteAsync(keys);

    Console.WriteLine("Redis stream and position cache cleared.");
}

static void AddPosition(List<Dictionary<string, string>> positions, HashEntry[] entries)
{
    if (entries.Length > 0 && entries.Any(entry => entry.Name == "symbol"))
        positions.Add(entries.ToDictionary(entry => entry.Name.ToString(), entry => entry.Value.ToString()));
}

static decimal GetDecimal(HashEntry[] entries, string name)
{
    var value = entries.FirstOrDefault(entry => entry.Name == name).Value;
    return decimal.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out var result) ? result : 0;
}

static string ReadRequired(string label)
{
    while (true)
    {
        Console.Write($"{label}: ");
        var value = Console.ReadLine()?.Trim();
        if (!string.IsNullOrWhiteSpace(value))
            return value;
        Console.WriteLine($"{label} is required.");
    }
}

static string ReadOrDefault(string label, string defaultValue)
{
    Console.Write($"{label} [{defaultValue}]: ");
    var value = Console.ReadLine()?.Trim();
    return string.IsNullOrWhiteSpace(value) ? defaultValue : value;
}

static decimal ReadPositiveDecimal(string label)
{
    while (true)
    {
        Console.Write($"{label}: ");
        if (decimal.TryParse(Console.ReadLine(), NumberStyles.Number, CultureInfo.InvariantCulture, out var value) && value > 0)
            return value;
        Console.WriteLine($"{label} must be greater than zero.");
    }
}
