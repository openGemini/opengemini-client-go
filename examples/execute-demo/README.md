# OpenGemini Client Execute Interface

English | [简体中文](README_CN.md)

![License](https://img.shields.io/badge/license-Apache2.0-green)
![Language](https://img.shields.io/badge/Language-Go-blue.svg)

This document demonstrates the unified Execute interface for OpenGemini Client, which provides a single entry point for executing different types of SQL-like statements with automatic routing and parameter support.

## About Execute Interface

The Execute interface is a unified SQL execution interface that automatically routes different types of statements to appropriate underlying methods:

- **Query Statements** (`SELECT`, `SHOW`, `EXPLAIN`, etc.) → Routed to `Query()` method
- **Command Statements** (`CREATE`, `DROP`, `ALTER`, etc.) → Routed to `Query()` method  
- **Insert Statements** (`INSERT`) → Routed to `Write()` methods

## Features

- 🚀 **Unified Interface**: Single method for all statement types
- 🔄 **Automatic Routing**: Smart routing based on statement type
- 🎯 **Parameter Support**: Parameterized queries with type safety
- 📊 **Rich Results**: Comprehensive result information including statement type and affected rows
- ⚡ **Context Support**: Full context support for timeout and cancellation
- 🛡️ **Type Safety**: Automatic type conversion for parameters

## Requirements

- Go 1.20+
- OpenGemini Server running

## Usage

### Basic Usage

Import the Client Library:

```go
import "github.com/openGemini/opengemini-client-go/opengemini"
```

Create a Client:

```go
config := &opengemini.Config{
    Addresses: []opengemini.Address{
        {
            Host: "127.0.0.1",
            Port: 8086,
        },
    },
}
client, err := opengemini.NewClient(config)
if err != nil {
    log.Fatalf("Failed to create client: %v", err)
}
defer client.Close()
```

### 1. Database Commands (COMMAND Type)

Execute database management commands:

```go
// Create Database
result, err := client.Execute(opengemini.Statement{
    Database: "test_db",
    Command:  "CREATE DATABASE test_db",
})
if err != nil {
    log.Printf("Failed to create database: %v", err)
} else {
    fmt.Printf("Statement type: %s, Affected rows: %d\n", 
        result.StatementType, result.AffectedRows)
}

// Drop Database
result, err = client.Execute(opengemini.Statement{
    Database: "test_db",
    Command:  "DROP DATABASE test_db",
})
```

### 2. Data Insertion (INSERT Type)

Insert data using line protocol format:

```go
// Simple Insert
result, err := client.Execute(opengemini.Statement{
    Database: "test_db",
    Command:  "INSERT weather,location=beijing,sensor=001 temperature=25.5,humidity=60i",
})
if err != nil {
    log.Printf("Failed to insert data: %v", err)
} else {
    fmt.Printf("Inserted %d points\n", result.AffectedRows)
}
```

### 3. Parameterized Insert

Use parameters for dynamic data insertion:

```go
result, err := client.Execute(opengemini.Statement{
    Database: "test_db",
    Command:  "INSERT weather,location=$location,sensor=$sensor temperature=$temp,humidity=$hum",
    Params: map[string]any{
        "location": "shanghai",
        "sensor":   "002",
        "temp":     30.2,
        "hum":      70,
    },
})
if err != nil {
    log.Printf("Parameterized insert failed: %v", err)
} else {
    fmt.Printf("Statement type: %s, Affected rows: %d\n", 
        result.StatementType, result.AffectedRows)
}
```

### 4. Data Queries (QUERY Type)

Execute queries and retrieve results:

```go
result, err := client.Execute(opengemini.Statement{
    Database: "test_db",
    Command:  "SELECT * FROM weather ORDER BY time DESC LIMIT 5",
})
if err != nil {
    log.Printf("Failed to query data: %v", err)
} else {
    fmt.Printf("Statement type: %s\n", result.StatementType)
    if result.QueryResult != nil && len(result.QueryResult.Results) > 0 {
        series := result.QueryResult.Results[0].Series
        if len(series) > 0 {
            fmt.Printf("Query results (%d records):\n", len(series[0].Values))
            fmt.Printf("Columns: %v\n", series[0].Columns)
            for i, row := range series[0].Values {
                fmt.Printf("Row %d: %v\n", i+1, row)
            }
        }
    }
}
```

### 5. Parameterized Queries

Use parameters in WHERE clauses and other parts of queries:

```go
result, err := client.Execute(opengemini.Statement{
    Database: "test_db",
    Command:  "SELECT * FROM weather WHERE location=$loc ORDER BY time DESC",
    Params: map[string]any{
        "loc": "beijing",
    },
})
if err != nil {
    log.Printf("Parameterized query failed: %v", err)
} else {
    fmt.Printf("Statement type: %s\n", result.StatementType)
    // Process query results...
}
```

### 6. Show Commands

Execute SHOW commands to inspect database structure:

```go
result, err := client.Execute(opengemini.Statement{
    Database: "test_db",
    Command:  "SHOW MEASUREMENTS",
})
if err != nil {
    log.Printf("SHOW statement failed: %v", err)
} else {
    fmt.Printf("Statement type: %s\n", result.StatementType)
    if result.QueryResult != nil && len(result.QueryResult.Results) > 0 {
        series := result.QueryResult.Results[0].Series
        if len(series) > 0 {
            fmt.Printf("Measurements: %v\n", series[0].Values)
        }
    }
}
```

### 7. Context Support

Use context for timeout and cancellation:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

result, err := client.ExecuteContext(ctx, opengemini.Statement{
    Database: "test_db",
    Command:  "SELECT * FROM weather",
})
```

## Statement Types

The Execute interface automatically recognizes and routes three types of statements:

| Statement Type | Keywords | Routing | Result |
|---------------|----------|---------|---------|
| **Query** | `SELECT`, `SHOW`, `EXPLAIN`, `DESCRIBE`, `WITH` | → `Query()` | `QueryResult` populated |
| **Command** | `CREATE`, `DROP`, `ALTER`, `UPDATE`, `DELETE` | → `Query()` | `AffectedRows = 1` |
| **Insert** | `INSERT` | → `Write()` methods | `AffectedRows = point count` |

## Parameter Types

The Execute interface supports various parameter types with automatic conversion:

| Go Type | Line Protocol Format | Example |
|---------|---------------------|---------|
| `string` | Plain string | `"beijing"` → `beijing` |
| `int`, `int32`, `int64` | Integer with `i` suffix | `42` → `42i` |
| `uint`, `uint32`, `uint64` | Unsigned integer with `u` suffix | `42` → `42u` |
| `float32`, `float64` | Plain number | `3.14` → `3.14` |
| `bool` | Boolean | `true` → `true` |

## ExecuteResult Structure

The Execute method returns a comprehensive result structure:

```go
type ExecuteResult struct {
    QueryResult   *QueryResult  // Query results (for Query/Command types)
    AffectedRows  int64         // Number of affected rows
    StatementType StatementType // Type of executed statement (Query/Command/Insert)
    Error         error         // Execution error (if any)
}
```

## Error Handling

The Execute interface provides detailed error information:

```go
result, err := client.Execute(stmt)
if err != nil {
    // Check both the returned error and result.Error
    log.Printf("Execution failed: %v", err)
    if result != nil && result.Error != nil {
        log.Printf("Result error: %v", result.Error)
    }
}
```

## Running the Example

1. Make sure OpenGemini server is running on `localhost:8086`
2. Navigate to the execute-demo directory:
   ```bash
   cd examples/execute-demo
   ```
3. Run the example:
   ```bash
   go run execute_example.go
   ```

## Best Practices

1. **Use Parameters**: Always use parameterized statements for dynamic values to prevent injection attacks
2. **Handle Errors**: Check both the returned error and `result.Error` field
3. **Check Statement Types**: Verify the statement type in results to ensure correct routing
4. **Use Context**: Use `ExecuteContext` for timeout control in production applications
5. **Resource Management**: Always close the client connection when done

## Advanced Features

### Retention Policy Support

```go
result, err := client.Execute(opengemini.Statement{
    Database:        "test_db",
    RetentionPolicy: "custom_rp",
    Command:         "INSERT weather,location=beijing temperature=25.5",
})
```

### Complex Parameter Types

```go
params := map[string]any{
    "measurement": "weather",
    "location":    "beijing",
    "temp":        25.5,        // float64 → 25.5
    "humidity":    60,          // int → 60i
    "active":      true,        // bool → true
    "count":       uint64(100), // uint64 → 100u
}
```

For more examples and detailed usage, check the complete example in `execute_example.go`.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](../../LICENSE) file for details.