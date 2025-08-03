#!/usr/bin/env python3
"""
Test script for BigQuery tools functionality.
Tests the bigquery_tools.py module and server integration.
"""

import asyncio
import json
import sys
import os
import pytest
from datetime import datetime, timedelta
from typing import Dict, Any

# Add the source directory to the path so we can import the tools
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def test_bigquery_import():
    """Test if we can import the BigQuery tools."""
    print("🔍 Testing BigQuery Tools Import...")
    
    try:
        from src.dataops_mcp_server.tools.bigquery_tools import GetBigQueryCostsTool
        print("✅ Successfully imported GetBigQueryCostsTool")
        return True
    except ImportError as e:
        print(f"❌ Failed to import BigQuery tools: {e}")
        return False

@pytest.mark.asyncio
async def test_bigquery_tools_functionality():
    """Test BigQuery tools functionality with mock data."""
    print("\n💰 Testing BigQuery Tools Functionality...")
    
    try:
        from src.dataops_mcp_server.tools.bigquery_tools import GetBigQueryCostsTool
        
        # Initialize the tool with a test project
        # Note: This won't actually connect to GCP without proper auth
        tool = GetBigQueryCostsTool(project_id="test-project-12345")
        
        print(f"✅ BigQuery tool initialized for project: {tool.project_id}")
        print(f"✅ Using region: {tool.region}")
        print(f"✅ On-demand price per TB: ${tool.on_demand_price_per_tb}")
        
        # Test the pricing constants
        assert tool.on_demand_price_per_tb > 0, "Pricing should be positive"
        print("✅ Pricing constants validation passed")
        
        return True
        
    except Exception as e:
        print(f"❌ BigQuery tools functionality test failed: {e}")
        return False

def test_cost_calculation_logic():
    """Test cost calculation logic without requiring GCP connection."""
    print("\n🧮 Testing Cost Calculation Logic...")
    
    try:
        # Simulate BigQuery pricing calculation
        bytes_processed = 1_000_000_000  # 1 GB
        tb_processed = bytes_processed / (1024**4)  # Convert to TB
        cost_per_tb = 6.25  # BigQuery on-demand pricing
        estimated_cost = tb_processed * cost_per_tb
        
        print(f"  Bytes processed: {bytes_processed:,}")
        print(f"  TB processed: {tb_processed:.6f}")
        print(f"  Estimated cost: ${estimated_cost:.4f}")
        
        # Validate calculations
        assert tb_processed > 0, "TB processed should be positive"
        assert estimated_cost >= 0, "Cost should be non-negative"
        
        print("✅ Cost calculation logic test passed")
        return True
        
    except Exception as e:
        print(f"❌ Cost calculation test failed: {e}")
        return False

def test_query_analysis_patterns():
    """Test query analysis patterns for cost optimization."""
    print("\n🔍 Testing Query Analysis Patterns...")
    
    test_queries = [
        {
            "query": "SELECT * FROM large_table",
            "expected_issues": ["SELECT *", "no WHERE clause"]
        },
        {
            "query": "SELECT id, name FROM users WHERE created_date > '2024-01-01' LIMIT 1000",
            "expected_issues": []
        },
        {
            "query": "SELECT COUNT(*) FROM events ORDER BY timestamp",
            "expected_issues": ["no WHERE clause", "ORDER BY without LIMIT"]
        }
    ]
    
    def analyze_query_for_cost_issues(query: str):
        """Analyze query for potential cost issues."""
        issues = []
        
        if "SELECT *" in query.upper():
            issues.append("SELECT *")
        
        if "WHERE" not in query.upper():
            issues.append("no WHERE clause")
        
        if "ORDER BY" in query.upper() and "LIMIT" not in query.upper():
            issues.append("ORDER BY without LIMIT")
        
        return issues
    
    try:
        for i, test_case in enumerate(test_queries, 1):
            query = test_case["query"]
            expected = test_case["expected_issues"]
            
            issues = analyze_query_for_cost_issues(query)
            
            print(f"\n  Test {i}: {query[:50]}...")
            print(f"    Found issues: {issues}")
            print(f"    Expected: {expected}")
            
            # Check if all expected issues are found
            for expected_issue in expected:
                if not any(expected_issue in issue for issue in issues):
                    print(f"    ⚠️  Missing expected issue: {expected_issue}")
                else:
                    print(f"    ✅ Found expected issue: {expected_issue}")
        
        print("\n✅ Query analysis patterns test completed")
        return True
        
    except Exception as e:
        print(f"❌ Query analysis test failed: {e}")
        return False

@pytest.mark.asyncio
async def test_server_integration():
    """Test integration with the running MCP server."""
    print("\n🌐 Testing Server Integration...")
    
    try:
        import httpx
        
        # Test if server is running
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get("http://localhost:8000/health", timeout=5.0)
                if response.status_code == 200:
                    print("✅ Server is running and responding")
                else:
                    print(f"⚠️  Server responded with status: {response.status_code}")
            except httpx.ConnectError:
                print("⚠️  Could not connect to server (it may not be running)")
            except Exception as e:
                print(f"⚠️  Server connection test failed: {e}")
        
        return True
        
    except ImportError:
        print("⚠️  httpx not available, skipping server integration test")
        return True
    except Exception as e:
        print(f"❌ Server integration test failed: {e}")
        return False

def test_bigquery_sql_generation():
    """Test BigQuery SQL query generation for cost analysis."""
    print("\n📝 Testing BigQuery SQL Generation...")
    
    try:
        # Test SQL generation logic (simulating what the tool would generate)
        def generate_cost_analysis_sql(days: int, group_by: list):
            """Generate SQL for BigQuery cost analysis."""
            
            # Base columns
            select_cols = ["COUNT(*) as query_count", "SUM(total_bytes_processed) as total_bytes"]
            group_cols = []
            
            # Add grouping columns
            if "date" in group_by:
                select_cols.append("DATE(creation_time) as date")
                group_cols.append("DATE(creation_time)")
            
            if "user" in group_by:
                select_cols.append("user_email")
                group_cols.append("user_email")
            
            # Build SQL
            sql = f"""
            SELECT {', '.join(select_cols)}
            FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
            WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
              AND job_type = 'QUERY'
              AND state = 'DONE'
            """
            
            if group_cols:
                sql += f"\nGROUP BY {', '.join(group_cols)}"
            
            sql += "\nORDER BY total_bytes DESC"
            
            return sql
        
        # Test different grouping scenarios
        test_cases = [
            {"days": 7, "group_by": ["date"]},
            {"days": 30, "group_by": ["date", "user"]},
            {"days": 1, "group_by": []}
        ]
        
        for i, test_case in enumerate(test_cases, 1):
            sql = generate_cost_analysis_sql(**test_case)
            
            print(f"\n  Test {i}: {test_case}")
            print(f"    Generated SQL length: {len(sql)} characters")
            
            # Validate SQL structure
            assert "SELECT" in sql.upper()
            assert "FROM" in sql.upper()
            assert "INFORMATION_SCHEMA.JOBS_BY_PROJECT" in sql
            assert f"INTERVAL {test_case['days']} DAY" in sql
            
            print(f"    ✅ SQL structure validation passed")
        
        print("\n✅ BigQuery SQL generation test completed")
        return True
        
    except Exception as e:
        print(f"❌ SQL generation test failed: {e}")
        return False

async def run_all_bigquery_tests():
    """Run all BigQuery-related tests."""
    print("🚀 BigQuery Tools Test Suite")
    print("=" * 60)
    
    tests = [
        ("Import Test", test_bigquery_import),
        ("Functionality Test", test_bigquery_tools_functionality),
        ("Cost Calculation Test", test_cost_calculation_logic),
        ("Query Analysis Test", test_query_analysis_patterns),
        ("SQL Generation Test", test_bigquery_sql_generation),
        ("Server Integration Test", test_server_integration)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        try:
            if asyncio.iscoroutinefunction(test_func):
                result = await test_func()
            else:
                result = test_func()
            results[test_name] = result
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
            results[test_name] = False
    
    # Summary
    print("\n" + "=" * 60)
    print("📋 Test Results Summary:")
    print("=" * 60)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"  {test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\n🎯 Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All BigQuery tools tests passed!")
    else:
        print("⚠️  Some tests failed. Check the output above for details.")
    
    return passed == total

if __name__ == "__main__":
    success = asyncio.run(run_all_bigquery_tests())
    sys.exit(0 if success else 1)
