#!/usr/bin/env python3
"""
Simple CLI for running DataOps MCP Server tools.
Usage: python run_tool.py <tool_name> [options]
"""

import argparse
import asyncio
import json
import sys
import os

# Add the source directory to the path  
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'dataops-mcp-server'))

async def run_get_costs(args):
    """Run BigQuery cost analysis."""
    try:
        from tools.bigquery_wrapper import get_cost_summary_direct, get_daily_costs_direct
        
        # Use the appropriate function based on options
        if args.details:
            result = get_cost_summary_direct(args.project, days=args.days)
        else:
            result = get_daily_costs_direct(args.project, days=args.days)
        
        print("✅ BigQuery Cost Analysis Results:")
        print(result)
        
    except Exception as e:
        print(f"❌ Failed to run cost analysis: {e}")

async def run_analyze_query(args):
    """Run query cost analysis."""
    try:
        print("❌ Query analysis not available in FastMCP version")
        print("Use the basic cost analysis tools instead:")
        print("  - get_daily_costs")
        print("  - get_top_users") 
        print("  - get_cost_summary")
        
    except Exception as e:
        print(f"❌ Failed to analyze query: {e}")

async def run_health_check(args):
    """Run health check on tools."""
    try:
        from tools.bigquery_wrapper import health_check_direct
        
        result = health_check_direct(args.project)
        print("🏥 Health Check Results:")
        print(result)
        
    except Exception as e:
        print(f"❌ Health check failed: {e}")

def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(description="DataOps MCP Server Tool Runner")
    parser.add_argument("--project", default="gcp-wow-wiq-tsr-dev", help="GCP Project ID")
    
    subparsers = parser.add_subparsers(dest="tool", help="Available tools")
    
    # Cost analysis tool
    costs_parser = subparsers.add_parser("costs", help="Analyze BigQuery costs")
    costs_parser.add_argument("--days", type=int, default=7, help="Number of days to analyze")
    costs_parser.add_argument("--predictions", action="store_true", help="Include cost predictions")
    costs_parser.add_argument("--group-by", help="Grouping dimensions (comma-separated)")
    costs_parser.add_argument("--details", action="store_true", help="Include query details")
    
    # Query analysis tool
    query_parser = subparsers.add_parser("query", help="Analyze query cost")
    query_parser.add_argument("sql", help="SQL query to analyze")
    query_parser.add_argument("--optimize", action="store_true", help="Include optimization suggestions")
    
    # Health check tool
    health_parser = subparsers.add_parser("health", help="Check tool health")
    
    args = parser.parse_args()
    
    if not args.tool:
        parser.print_help()
        return
    
    print(f"🚀 Running tool: {args.tool}")
    print(f"📊 Project: {args.project}")
    print()
    
    try:
        if args.tool == "costs":
            asyncio.run(run_get_costs(args))
        elif args.tool == "query":
            asyncio.run(run_analyze_query(args))
        elif args.tool == "health":
            asyncio.run(run_health_check(args))
        else:
            print(f"❌ Unknown tool: {args.tool}")
    
    except KeyboardInterrupt:
        print("\n⏹️  Cancelled by user")
    except Exception as e:
        print(f"❌ Execution failed: {e}")

if __name__ == "__main__":
    main()
