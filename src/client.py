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
        from tools.bigquery_tools import GetBigQueryCostsTool
        
        tool = GetBigQueryCostsTool(project_id=args.project)
        result = await tool.execute(
            days=args.days,
            include_predictions=args.predictions,
            group_by=args.group_by.split(',') if args.group_by else ['date'],
            include_query_details=args.details
        )
        
        print("✅ BigQuery Cost Analysis Results:")
        data = json.loads(result)
        print(json.dumps(data, indent=2))
        
    except Exception as e:
        print(f"❌ Failed to run cost analysis: {e}")

async def run_analyze_query(args):
    """Run query cost analysis."""
    try:
        from tools.cost_analysis_tools import AnalyzeQueryCostTool
        
        tool = AnalyzeQueryCostTool(project_id=args.project)
        result = await tool.execute(
            sql=args.sql,
            include_optimization=args.optimize,
            optimization_model="pattern_based"
        )
        
        print("✅ Query Analysis Results:")
        data = json.loads(result)
        print(json.dumps(data, indent=2))
        
    except Exception as e:
        print(f"❌ Failed to analyze query: {e}")

async def run_health_check(args):
    """Run health check on tools."""
    try:
        from tools.bigquery_tools import GetBigQueryCostsTool
        
        tool = GetBigQueryCostsTool(project_id=args.project)
        is_healthy = await tool.health_check()
        
        print(f"🏥 Health Check: {'✅ Healthy' if is_healthy else '❌ Unhealthy'}")
        
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
