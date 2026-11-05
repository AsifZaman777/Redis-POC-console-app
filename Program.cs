// ============================================================================
// File Name    : Program.cs
// Project      : Redis-POC-console-app
// Description  : Proof of Concept (POC) console application demonstrating
//                real-time trade event streaming and portfolio position tracking
//                using Redis Streams, Hashes, and Sets via StackExchange.Redis.
// ============================================================================
// REVISION HISTORY:
// Date         Author        Version   Description
// -----------  -----------   -------   ---------------------------------------
// 2026-11-05   Asif Zaman    1.0.0     Highlevel POC implementation with buy orders,
//                                      position tracking, trade streams, and reset.
// ============================================================================

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

// ============================================================================
// FUNCTION: ExecuteBuyAsync
// ----------------------------------------------------------------------------
// WHAT HAS BEEN DONE:
//   Prompts user for trade inputs (account, symbol, quantity, price), creates
//   a new trade event record, appends it to a Redis stream, recalculates the
//   weighted average price and total invested for the symbol position, and
//   atomically updates Redis hashes and index sets in a batch.
//
// HOW IT IS DONE:
//   1. Collects and validates trade parameters from the console.
//   2. Generates a unique trade ID using timestamp and GUID.
//   3. Calls StreamAddAsync on 'stream:trades' to publish an immutable trade log.
//   4. Fetches existing position data via HashGetAllAsync('position:{account}:{symbol}').
//   5. Computes updated total shares, total invested capital, and weighted avg price:
//      newTotalInvested = totalInvested + (quantity * price)
//      newAveragePrice = newTotalInvested / newQuantity
//   6. Batches three Redis commands via CreateBatch():
//      - HashSetAsync: stores updated position metrics in the hash.
//      - SetAddAsync: indexes the symbol in 'account:{account}:symbols'.
//      - SetAddAsync: indexes the account in 'accounts:all'.
//   7. Executes batch and awaits completion with Task.WhenAll.
//   8. Outputs the JSON representation of the trade confirmation and updated position.
//
// PARAMETERS:
//   database (IDatabase) : Redis database instance to perform stream and batch ops.
// ============================================================================
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

// ============================================================================
// FUNCTION: ShowAllPositionsAsync
// ----------------------------------------------------------------------------
// WHAT HAS BEEN DONE:
//   Retrieves and displays all active positions across every registered account.
//
// HOW IT IS DONE:
//   1. Reads all registered accounts by querying the 'accounts:all' Redis Set.
//   2. For each account, queries 'account:{account}:symbols' Set to obtain held symbols.
//   3. For each symbol, fetches the full position hash 'position:{account}:{symbol}'.
//   4. Aggregates valid position entries into a list and prints them formatted as JSON.
//
// PARAMETERS:
//   database (IDatabase) : Redis database instance.
// ============================================================================
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

// ============================================================================
// FUNCTION: ShowAccountPositionsAsync
// ----------------------------------------------------------------------------
// WHAT HAS BEEN DONE:
//   Retrieves and displays all positions associated with a single user-specified account.
//
// HOW IT IS DONE:
//   1. Prompts the user to enter an account identifier using ReadRequired().
//   2. Queries Redis Set 'account:{account}:symbols' to find symbols owned by the account.
//   3. Reads each symbol's hash data from 'position:{account}:{symbol}'.
//   4. Parses valid entries and prints them formatted as JSON.
//
// PARAMETERS:
//   database (IDatabase) : Redis database instance.
// ============================================================================
static async Task ShowAccountPositionsAsync(IDatabase database)
{
    var account = ReadRequired("Account");
    var symbols = await database.SetMembersAsync($"account:{account}:symbols");
    var positions = new List<Dictionary<string, string>>();

    foreach (var symbol in symbols)
        AddPosition(positions, await database.HashGetAllAsync($"position:{account}:{symbol}"));

    Console.WriteLine(JsonSerializer.Serialize(new { account, positions }, new JsonSerializerOptions { WriteIndented = true }));
}

// ============================================================================
// FUNCTION: ShowTradeStreamAsync
// ----------------------------------------------------------------------------
// WHAT HAS BEEN DONE:
//   Queries and displays trade execution events stored in the Redis Stream.
//
// HOW IT IS DONE:
//   1. Invokes StreamRangeAsync on the configured 'stream:trades' key.
//   2. Reverses the entries to display newest trades first.
//   3. Maps each stream entry's ID and name/value field pairs into dictionary objects.
//   4. Serializes the total count and trade records to the console in indented JSON.
//
// PARAMETERS:
//   database (IDatabase) : Redis database instance.
// ============================================================================
static async Task ShowTradeStreamAsync(IDatabase database)
{
    var entries = await database.StreamRangeAsync(streamKey);
    var trades = entries.Reverse().Select(entry =>
        new Dictionary<string, string> { ["streamId"] = entry.Id.ToString() }
            .Concat(entry.Values.ToDictionary(value => value.Name.ToString(), value => value.Value.ToString()))
            .ToDictionary(pair => pair.Key, pair => pair.Value));

    Console.WriteLine(JsonSerializer.Serialize(new { total = entries.Length, trades }, new JsonSerializerOptions { WriteIndented = true }));
}

// ============================================================================
// FUNCTION: ResetAsync
// ----------------------------------------------------------------------------
// WHAT HAS BEEN DONE:
//   Cleans up all POC test data from the Redis database, deleting streams,
//   positions, and account sets to restore a pristine state.
//
// HOW IT IS DONE:
//   1. Deletes the stream log at 'stream:trades' via KeyDeleteAsync.
//   2. Obtains the Redis server handle from the ConnectionMultiplexer.
//   3. Scans and finds all keys matching 'position:*', 'account:*', and 'accounts:all'.
//   4. Deletes matching keys en masse using KeyDeleteAsync.
//   5. Outputs a confirmation message to the console.
//
// PARAMETERS:
//   redis    (ConnectionMultiplexer) : Connection multiplexer used to resolve server endpoint.
//   database (IDatabase)             : Redis database instance for deletion commands.
// ============================================================================
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

// ============================================================================
// FUNCTION: AddPosition
// ----------------------------------------------------------------------------
// WHAT HAS BEEN DONE:
//   Extracts key-value pairs from a Redis hash entry array and appends them
//   to the positions collection if a valid symbol entry exists.
//
// HOW IT IS DONE:
//   1. Checks if entries array is non-empty and contains an entry with name "symbol".
//   2. Converts HashEntry[] into a Dictionary<string, string>.
//   3. Adds the resulting dictionary to the target positions list.
//
// PARAMETERS:
//   positions (List<Dictionary<string, string>>) : Aggregation list for positions.
//   entries   (HashEntry[])                     : Array of key-value pairs from Redis hash.
// ============================================================================
static void AddPosition(List<Dictionary<string, string>> positions, HashEntry[] entries)
{
    if (entries.Length > 0 && entries.Any(entry => entry.Name == "symbol"))
        positions.Add(entries.ToDictionary(entry => entry.Name.ToString(), entry => entry.Value.ToString()));
}

// ============================================================================
// FUNCTION: GetDecimal
// ----------------------------------------------------------------------------
// WHAT HAS BEEN DONE:
//   Extracts and parses a decimal value for a specific field from a Redis HashEntry array.
//
// HOW IT IS DONE:
//   1. Locates the HashEntry matching the requested field name.
//   2. Attempts to parse its value to decimal using InvariantCulture.
//   3. Returns the parsed decimal value, or 0 if missing or invalid.
//
// PARAMETERS:
//   entries (HashEntry[]) : Array of hash entries retrieved from Redis.
//   name    (string)      : Field name to look up.
//
// RETURNS:
//   decimal : The parsed decimal value or 0 on failure.
// ============================================================================
static decimal GetDecimal(HashEntry[] entries, string name)
{
    var value = entries.FirstOrDefault(entry => entry.Name == name).Value;
    return decimal.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out var result) ? result : 0;
}

// ============================================================================
// FUNCTION: ReadRequired
// ----------------------------------------------------------------------------
// WHAT HAS BEEN DONE:
//   Prompts the console user repeatedly until a non-empty string input is provided.
//
// HOW IT IS DONE:
//   1. Enters a while loop printing the prompt label.
//   2. Reads console input and trims leading/trailing whitespace.
//   3. Returns value if not null or whitespace; otherwise displays a warning and loops.
//
// PARAMETERS:
//   label (string) : Prompt text shown to the user.
//
// RETURNS:
//   string : Non-empty user input string.
// ============================================================================
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

// ============================================================================
// FUNCTION: ReadOrDefault
// ----------------------------------------------------------------------------
// WHAT HAS BEEN DONE:
//   Prompts the console user for an optional input, defaulting to a fallback value.
//
// HOW IT IS DONE:
//   1. Displays the prompt label along with the default value in brackets.
//   2. Reads and trims console input.
//   3. Returns default value if input is null or whitespace; otherwise returns entered text.
//
// PARAMETERS:
//   label        (string) : Prompt text.
//   defaultValue (string) : Fallback value if user presses Enter without typing.
//
// RETURNS:
//   string : Entered text or defaultValue.
// ============================================================================
static string ReadOrDefault(string label, string defaultValue)
{
    Console.Write($"{label} [{defaultValue}]: ");
    var value = Console.ReadLine()?.Trim();
    return string.IsNullOrWhiteSpace(value) ? defaultValue : value;
}

// ============================================================================
// FUNCTION: ReadPositiveDecimal
// ----------------------------------------------------------------------------
// WHAT HAS BEEN DONE:
//   Prompts and validates that the console input is a numeric decimal strictly > 0.
//
// HOW IT IS DONE:
//   1. Enters a while loop printing the prompt label.
//   2. Parses console input using decimal.TryParse with InvariantCulture.
//   3. Verifies value is strictly greater than 0; if valid, returns the decimal.
//   4. Otherwise, prints an error message and reprompts.
//
// PARAMETERS:
//   label (string) : Prompt text.
//
// RETURNS:
//   decimal : Validated positive decimal number.
// ============================================================================
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
