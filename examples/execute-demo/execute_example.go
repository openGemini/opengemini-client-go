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

	// ===== 2. Insert Data (INSERT type) =====
	fmt.Println("\n=== 2. Insert Data (without parameters) ===")
	result, err = client.Execute(opengemini.Statement{
		Database: database,
		Command:  "INSERT weather,location=beijing,sensor=001 temperature=25.5,humidity=60i",
	})
	if err != nil {
		fmt.Printf("Failed to insert data: %v\n", err)
	} else {
		fmt.Printf("✅ Statement type: %s, Affected rows: %d\n", result.StatementType, result.AffectedRows)
	}

	// ===== 3. Parameterized Insert (INSERT type + Params) =====
	fmt.Println("\n=== 3. Parameterized Insert ===")
	result, err = client.Execute(opengemini.Statement{
		Database: database,
		Command:  "INSERT weather,location=$location,sensor=$sensor temperature=$temp,humidity=$hum",
		Params: map[string]any{
			"location": "shanghai",
			"sensor":   "002",
			"temp":     30.2,
			"hum":      70,
		},
	})
	if err != nil {
		fmt.Printf("Parameterized insert failed: %v\n", err)
	} else {
		fmt.Printf("✅ Statement type: %s, Affected rows: %d\n", result.StatementType, result.AffectedRows)
	}

	// Wait for data to be flushed to disk
	fmt.Println("\n⏳ Waiting for data to flush...")
	time.Sleep(3 * time.Second)

	// ===== 4. Query Data (QUERY type) =====
	fmt.Println("\n=== 4. Query Data ===")
	result, err = client.Execute(opengemini.Statement{
		Database: database,
		Command:  "SELECT * FROM weather ORDER BY time DESC LIMIT 5",
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
			}
		}
	}

	// ===== 5. Parameterized Query (QUERY type + Params) =====
	fmt.Println("\n=== 5. Parameterized Query ===")
	result, err = client.Execute(opengemini.Statement{
		Database: database,
		Command:  "SELECT * FROM weather WHERE location=$loc ORDER BY time DESC",
		Params: map[string]any{
			"loc": "beijing",
		},
	})
	if err != nil {
		fmt.Printf("Parameterized query failed: %v\n", err)
	} else {
		fmt.Printf("✅ Statement type: %s\n", result.StatementType)
		if result.QueryResult != nil && len(result.QueryResult.Results) > 0 {
			series := result.QueryResult.Results[0].Series
			if len(series) > 0 {
				fmt.Printf("📊 Beijing data: %d records\n", len(series[0].Values))
			}
		}
	}

	// ===== 6. SHOW Statement (QUERY type) =====
	fmt.Println("\n=== 6. SHOW Statement ===")
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

	// ===== 7. Cleanup: Drop Database (COMMAND type) =====
	fmt.Println("\n=== 7. Drop Database ===")
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
}
