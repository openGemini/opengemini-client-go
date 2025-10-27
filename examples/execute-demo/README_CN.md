# OpenGemini 客户端 Execute 接口

[English](README.md) | 简体中文

![License](https://img.shields.io/badge/license-Apache2.0-green)
![Language](https://img.shields.io/badge/Language-Go-blue.svg)

本文档演示了 OpenGemini 客户端的统一 Execute 接口，它为执行不同类型的类 SQL 语句提供了单一入口点，支持自动路由和参数化功能。

## 关于 Execute 接口

Execute 接口是一个统一的 SQL 执行接口，可以自动将不同类型的语句路由到相应的底层方法：

- **查询语句** (`SELECT`, `SHOW`, `EXPLAIN` 等) → 路由至 `Query()` 方法
- **命令语句** (`CREATE`, `DROP`, `ALTER` 等) → 路由至 `Query()` 方法
- **插入语句** (`INSERT`) → 路由至 `Write()` 方法

## 功能特性

- 🚀 **统一接口**：所有语句类型使用单一方法
- 🔄 **自动路由**：基于语句类型的智能路由
- 🎯 **参数支持**：类型安全的参数化查询
- 📊 **丰富结果**：包括语句类型和影响行数的全面结果信息
- ⚡ **上下文支持**：完整的超时和取消上下文支持
- 🛡️ **类型安全**：参数的自动类型转换

## 系统要求

- Go 1.20+
- OpenGemini 服务器运行中

## 使用方法

### 基本用法

导入客户端库：

```go
import "github.com/openGemini/opengemini-client-go/opengemini"
```

创建客户端：

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
    log.Fatalf("创建客户端失败: %v", err)
}
defer client.Close()
```

### 1. 数据库命令（COMMAND 类型）

执行数据库管理命令：

```go
// 创建数据库
result, err := client.Execute(opengemini.Statement{
    Database: "test_db",
    Command:  "CREATE DATABASE test_db",
})
if err != nil {
    log.Printf("创建数据库失败: %v", err)
} else {
    fmt.Printf("语句类型: %s, 影响行数: %d\n",
        result.StatementType, result.AffectedRows)
}

// 删除数据库
result, err = client.Execute(opengemini.Statement{
    Database: "test_db",
    Command:  "DROP DATABASE test_db",
})
```

### 2. 数据插入（INSERT 类型）

使用行协议格式插入数据：

```go
// 简单插入
result, err := client.Execute(opengemini.Statement{
    Database: "test_db",
    Command:  "INSERT weather,location=beijing,sensor=001 temperature=25.5,humidity=60i",
})
if err != nil {
    log.Printf("插入数据失败: %v", err)
} else {
    fmt.Printf("插入了 %d 个数据点\n", result.AffectedRows)
}
```

### 3. 参数化插入

使用参数进行动态数据插入：

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
    log.Printf("参数化插入失败: %v", err)
} else {
    fmt.Printf("语句类型: %s, 影响行数: %d\n",
        result.StatementType, result.AffectedRows)
}
```

### 4. 数据查询（QUERY 类型）

执行查询并检索结果：

```go
result, err := client.Execute(opengemini.Statement{
    Database: "test_db",
    Command:  "SELECT * FROM weather ORDER BY time DESC LIMIT 5",
})
if err != nil {
    log.Printf("查询数据失败: %v", err)
} else {
    fmt.Printf("语句类型: %s\n", result.StatementType)
    if result.QueryResult != nil && len(result.QueryResult.Results) > 0 {
        series := result.QueryResult.Results[0].Series
        if len(series) > 0 {
            fmt.Printf("查询结果 (%d 条记录):\n", len(series[0].Values))
            fmt.Printf("列名: %v\n", series[0].Columns)
            for i, row := range series[0].Values {
                fmt.Printf("第 %d 行: %v\n", i+1, row)
            }
        }
    }
}
```

### 5. 参数化查询

在 WHERE 子句和查询的其他部分使用参数：

```go
result, err := client.Execute(opengemini.Statement{
    Database: "test_db",
    Command:  "SELECT * FROM weather WHERE location=$loc ORDER BY time DESC",
    Params: map[string]any{
        "loc": "beijing",
    },
})
if err != nil {
    log.Printf("参数化查询失败: %v", err)
} else {
    fmt.Printf("语句类型: %s\n", result.StatementType)
    // 处理查询结果...
}
```

### 6. SHOW 命令

执行 SHOW 命令来检查数据库结构：

```go
result, err := client.Execute(opengemini.Statement{
    Database: "test_db",
    Command:  "SHOW MEASUREMENTS",
})
if err != nil {
    log.Printf("SHOW 语句失败: %v", err)
} else {
    fmt.Printf("语句类型: %s\n", result.StatementType)
    if result.QueryResult != nil && len(result.QueryResult.Results) > 0 {
        series := result.QueryResult.Results[0].Series
        if len(series) > 0 {
            fmt.Printf("测量表: %v\n", series[0].Values)
        }
    }
}
```

### 7. 上下文支持

使用上下文进行超时和取消控制：

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

result, err := client.ExecuteContext(ctx, opengemini.Statement{
    Database: "test_db",
    Command:  "SELECT * FROM weather",
})
```

## 语句类型

Execute 接口自动识别并路由三种类型的语句：

| 语句类型 | 关键词 | 路由 | 结果 |
|---------|-------|------|------|
| **查询** | `SELECT`, `SHOW`, `EXPLAIN`, `DESCRIBE`, `WITH` | → `Query()` | `QueryResult` 被填充 |
| **命令** | `CREATE`, `DROP`, `ALTER`, `UPDATE`, `DELETE` | → `Query()` | `AffectedRows = 1` |
| **插入** | `INSERT` | → `Write()` 方法 | `AffectedRows = 数据点数量` |

## 参数类型

Execute 接口支持各种参数类型并自动转换：

| Go 类型 | 行协议格式 | 示例 |
|--------|-----------|------|
| `string` | 纯字符串 | `"beijing"` → `beijing` |
| `int`, `int32`, `int64` | 带 `i` 后缀的整数 | `42` → `42i` |
| `uint`, `uint32`, `uint64` | 带 `u` 后缀的无符号整数 | `42` → `42u` |
| `float32`, `float64` | 纯数字 | `3.14` → `3.14` |
| `bool` | 布尔值 | `true` → `true` |

## ExecuteResult 结构

Execute 方法返回一个综合的结果结构：

```go
type ExecuteResult struct {
    QueryResult   *QueryResult  // 查询结果（用于查询/命令类型）
    AffectedRows  int64         // 影响的行数
    StatementType StatementType // 执行的语句类型（查询/命令/插入）
    Error         error         // 执行错误（如果有）
}
```

## 错误处理

Execute 接口提供详细的错误信息：

```go
result, err := client.Execute(stmt)
if err != nil {
    // 检查返回的错误和 result.Error
    log.Printf("执行失败: %v", err)
    if result != nil && result.Error != nil {
        log.Printf("结果错误: %v", result.Error)
    }
}
```

## 运行示例

1. 确保 OpenGemini 服务器在 `localhost:8086` 运行
2. 导航到 execute-demo 目录：
   ```bash
   cd examples/execute-demo
   ```
3. 运行示例：
   ```bash
   go run execute_example.go
   ```

## 最佳实践

1. **使用参数**：始终为动态值使用参数化语句以防止注入攻击
2. **处理错误**：检查返回的错误和 `result.Error` 字段
3. **检查语句类型**：验证结果中的语句类型以确保正确路由
4. **使用上下文**：在生产应用中使用 `ExecuteContext` 进行超时控制
5. **资源管理**：完成后始终关闭客户端连接

## 高级功能

### 保留策略支持

```go
result, err := client.Execute(opengemini.Statement{
    Database:        "test_db",
    RetentionPolicy: "custom_rp",
    Command:         "INSERT weather,location=beijing temperature=25.5",
})
```

### 复杂参数类型

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

更多示例和详细用法，请查看 `execute_example.go` 中的完整示例。

## 许可证

本项目基于 Apache License 2.0 许可证 - 详情请查看 [LICENSE](../../LICENSE) 文件。
