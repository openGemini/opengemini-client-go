// Copyright 2024 openGemini Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"log"
	"time"

	"github.com/openGemini/opengemini-client-go/opengemini"
)

func main() {
	// Create client configuration
	config := &opengemini.Config{
		Addresses: []opengemini.Address{{
			Host: "127.0.0.1", // Replace with your OpenGemini service address
			Port: 8086,
		}},
	}

	// Create client instance
	client, err := opengemini.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	database := "test_execute_db"

	// ===== 1. Create Database (COMMAND type) =====
	fmt.Println("=== 1. Create Database ===")
	result, err := client.Execute(opengemini.Statement{
		Database: database,
		Command:  "CREATE DATABASE " + database,
	})
	if err != nil {
		fmt.Printf("Failed to create database: %v\n", err)
	} else {
		fmt.Printf("✅ Statement type: %s, Affected rows: %d\n", result.StatementType, result.AffectedRows)
	}

	// ===== 2. Insert Single Point (INSERT type) =====
	fmt.Println("\n=== 2. Insert Single Point (without parameters) ===")
	result, err = client.Execute(opengemini.Statement{
		Database: database,
		Command:  "INSERT weather,location=beijing,sensor=001 temperature=25.5,humidity=60i",
	})
	if err != nil {
		fmt.Printf("Failed to insert data: %v\n", err)
	} else {
		fmt.Printf("✅ Statement type: %s, Affected rows: %d\n", result.StatementType, result.AffectedRows)
	}

	// ===== 3. Insert Multiple Points (Batch INSERT) =====
	fmt.Println("\n=== 3. Insert Multiple Points (Batch) ===")
	result, err = client.Execute(opengemini.Statement{
		Database: database,
		Command: `INSERT weather,location=shanghai,sensor=002 temperature=28.0,humidity=65i
weather,location=guangzhou,sensor=003 temperature=32.5,humidity=80i
weather,location=shenzhen,sensor=004 temperature=30.0,humidity=75i`,
	})
	if err != nil {
		fmt.Printf("Failed to batch insert: %v\n", err)
	} else {
		fmt.Printf("✅ Statement type: %s, Affected rows: %d\n", result.StatementType, result.AffectedRows)
	}

	// ===== 4. Parameterized Insert (Single Point with Structured Params) =====
	fmt.Println("\n=== 4. Parameterized Insert (Single Point) ===")
	result, err = client.Execute(opengemini.Statement{
		Database: database,
		Command:  "INSERT weather,location=$location,sensor=$sensor temperature=$temp,humidity=$humi",
		Params: map[string]any{
			"location": "chengdu",
			"sensor":   "005",
			"temp":     22.5,
			"humi":     int64(55), // Using int64, type will be preserved
		},
	})
	if err != nil {
		fmt.Printf("Parameterized insert failed: %v\n", err)
	} else {
		fmt.Printf("✅ Statement type: %s, Affected rows: %d\n", result.StatementType, result.AffectedRows)
	}

	// ===== 5. Parameterized Batch Insert (Multiple Points with Different Params) =====
	fmt.Println("\n=== 5. Parameterized Batch Insert (Multiple Points) ===")
	result, err = client.Execute(opengemini.Statement{
		Database: database,
		Command: `INSERT weather,location=$loc1,sensor=$sensor1 temperature=$temp1,humidity=$hum1
weather,location=$loc2,sensor=$sensor2 temperature=$temp2,humidity=$hum2
weather,location=$loc3,sensor=$sensor3 temperature=$temp3,humidity=$hum3`,
		Params: map[string]any{
			"loc1": "hangzhou", "sensor1": "006", "temp1": 26.0, "hum1": 70,
			"loc2": "nanjing", "sensor2": "007", "temp2": 24.5, "hum2": 68,
			"loc3": "suzhou", "sensor3": "008", "temp3": 25.0, "hum3": 72,
		},
	})
	if err != nil {
		fmt.Printf("Parameterized batch insert failed: %v\n", err)
	} else {
		fmt.Printf("✅ Statement type: %s, Affected rows: %d\n", result.StatementType, result.AffectedRows)
	}

	// Wait for data to be flushed to disk
	fmt.Println("\n⏳ Waiting for data to flush...")
	time.Sleep(3 * time.Second)

	// ===== 6. Query All Data (QUERY type) =====
	fmt.Println("\n=== 6. Query All Data ===")
	result, err = client.Execute(opengemini.Statement{
		Database: database,
		Command:  "SELECT * FROM weather ORDER BY time DESC LIMIT 10",
	})
	if err != nil {
		fmt.Printf("Failed to query data: %v\n", err)
	} else {
		fmt.Printf("✅ Statement type: %s\n", result.StatementType)
		if result.QueryResult != nil && len(result.QueryResult.Results) > 0 {
			series := result.QueryResult.Results[0].Series
			if len(series) > 0 {
				fmt.Printf("📊 Query results (%d records):\n", len(series[0].Values))
				fmt.Printf("Columns: %v\n", series[0].Columns)
				for i, row := range series[0].Values {
					fmt.Printf("Row %d: %v\n", i+1, row)
				}
			} else {
				fmt.Println("📊 No data found")
			}
		}
	}

	// ===== 7. Parameterized Query (Server-side parameter replacement) =====
	fmt.Println("\n=== 7. Parameterized Query (Server-side) ===")
	result, err = client.Execute(opengemini.Statement{
		Database: database,
		Command:  "SELECT * FROM weather WHERE location=$loc ORDER BY time DESC",
		Params: map[string]any{
			"loc": "shanghai",
		},
	})
	if err != nil {
		fmt.Printf("Parameterized query failed: %v\n", err)
	} else {
		fmt.Printf("✅ Statement type: %s\n", result.StatementType)
		if result.QueryResult != nil && len(result.QueryResult.Results) > 0 {
			series := result.QueryResult.Results[0].Series
			if len(series) > 0 {
				fmt.Printf("📊 Shanghai data: %d records\n", len(series[0].Values))
				fmt.Printf("Columns: %v\n", series[0].Columns)
				for _, row := range series[0].Values {
					fmt.Printf("  %v\n", row)
				}
			} else {
				fmt.Println("📊 No Shanghai data found")
			}
		}
	}

	// ===== 8. SHOW Statement (QUERY type) =====
	fmt.Println("\n=== 8. SHOW Statement ===")
	result, err = client.Execute(opengemini.Statement{
		Database: database,
		Command:  "SHOW MEASUREMENTS",
	})
	if err != nil {
		fmt.Printf("SHOW statement failed: %v\n", err)
	} else {
		fmt.Printf("✅ Statement type: %s\n", result.StatementType)
		if result.QueryResult != nil && len(result.QueryResult.Results) > 0 {
			series := result.QueryResult.Results[0].Series
			if len(series) > 0 {
				fmt.Printf("📊 Measurements: %v\n", series[0].Values)
			}
		}
	}

	// ===== 9. Test Error Handling: Missing Parameter =====
	fmt.Println("\n=== 9. Test Error Handling (Missing Parameter) ===")
	result, err = client.Execute(opengemini.Statement{
		Database: database,
		Command:  "INSERT weather,location=$missing temperature=20.0,humidity=50i",
		Params: map[string]any{
			"other": "value", // missing 'missing' parameter
		},
	})
	if err != nil {
		fmt.Printf("❌ Expected error: %v\n", err)
	} else {
		fmt.Printf("⚠️  Should have failed but didn't\n")
	}

	// ===== 10. Cleanup: Drop Database (COMMAND type) =====
	fmt.Println("\n=== 10. Drop Database ===")
	result, err = client.Execute(opengemini.Statement{
		Database: database,
		Command:  "DROP DATABASE " + database,
	})
	if err != nil {
		fmt.Printf("Failed to drop database: %v\n", err)
	} else {
		fmt.Printf("✅ Statement type: %s, Affected rows: %d\n", result.StatementType, result.AffectedRows)
	}

	fmt.Println("\n🎉 Execute interface demo completed!")
	fmt.Println("\n📝 Summary:")
	fmt.Println("  ✅ Single point insert")
	fmt.Println("  ✅ Batch insert (multiple points)")
	fmt.Println("  ✅ Parameterized insert (client-side structured replacement)")
	fmt.Println("  ✅ Parameterized batch insert (different params per point)")
	fmt.Println("  ✅ Parameterized query (server-side replacement)")
	fmt.Println("  ✅ Error handling")
}
