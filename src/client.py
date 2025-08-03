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
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src', 'dataops-mcp-server'))

async def run_get_costs(args):
    """Run BigQuery cost analysis."""
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'dataops-mcp-server'))
        from tools.bigquery_wrapper import get_cost_summary, get_daily_costs
        
        # Use the appropriate function based on options
        if args.details:
            result = get_cost_summary(args.project, days=args.days)
        else:
            result = get_daily_costs(args.project, days=args.days)
        
        print("✅ BigQuery Cost Analysis Results:")
        print(result)
        
    except Exception as e:
        print(f"❌ Failed to run cost analysis: {e}")

async def run_service_account_analysis(args):
    """Run service account cost analysis."""
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'dataops-mcp-server'))
        from tools.bigquery_wrapper import bigquery_cost_analyzer
        
        result = bigquery_cost_analyzer(
            project_id=args.project,
            days=args.days,
            service_account_filter=args.filter or "",
            include_query_text=args.include_queries,
            min_cost_threshold=args.min_cost
        )
        
        print("🔍 Service Account Analysis Results:")
        print(result)
        
    except Exception as e:
        print(f"❌ Failed to run service account analysis: {e}")

async def run_expensive_queries_analysis(args):
    """Run expensive queries analysis."""
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'dataops-mcp-server'))
        from tools.bigquery_wrapper import analyze_expensive_queries_direct
        
        result = analyze_expensive_queries_direct(
            project_id=args.project,
            days=args.days,
            min_cost_threshold=args.min_cost,
            categorize_by=args.categorize_by
        )
        
        print("💰 Expensive Queries Analysis Results:")
        print(result)
        
    except Exception as e:
        print(f"❌ Failed to analyze expensive queries: {e}")

async def run_optimization_patterns(args):
    """Run optimization patterns detection."""
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'dataops-mcp-server'))
        from tools.bigquery_wrapper import detect_optimization_patterns_direct
        
        result = detect_optimization_patterns_direct(
            project_id=args.project,
            days=args.days,
            min_cost_threshold=args.min_cost
        )
        
        print("🔍 Optimization Patterns Analysis Results:")
        print(result)
        
    except Exception as e:
        print(f"❌ Failed to detect optimization patterns: {e}")

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
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'dataops-mcp-server'))
        from tools.bigquery_wrapper import health_check
        
        result = health_check(args.project)
        print("🏥 Health Check Results:")
        print(result)
        
    except Exception as e:
        print(f"❌ Health check failed: {e}")

async def run_cost_forecast(args):
    """Run cost forecast analysis."""
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'dataops-mcp-server'))
        from tools.bigquery_wrapper import create_cost_forecast_direct
        
        result = create_cost_forecast_direct(
            days_historical=args.historical_days,
            days_forecast=args.forecast_days,
            growth_assumptions=args.growth or "current_trend"
        )
        
        print("📈 Cost Forecast Results:")
        print(result)
        
    except Exception as e:
        print(f"❌ Failed to generate cost forecast: {e}")

async def run_table_hotspots(args):
    """Run table hotspots analysis."""
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'dataops-mcp-server'))
        from tools.bigquery_wrapper import analyze_table_hotspots_direct
        
        result = analyze_table_hotspots_direct(
            days=args.days,
            min_access_cost=args.min_cost
        )
        
        print("🔥 Table Hotspots Analysis Results:")
        print(result)
        
    except Exception as e:
        print(f"❌ Failed to analyze table hotspots: {e}")

async def run_materialized_views(args):
    """Run materialized view recommendations."""
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'dataops-mcp-server'))
        from tools.bigquery_wrapper import generate_materialized_view_recommendations_direct
        
        result = generate_materialized_view_recommendations_direct(
            days=args.days,
            min_repetition_count=args.min_repetitions,
            min_cost_per_execution=args.min_cost
        )
        
        print("🏗️ Materialized View Recommendations:")
        print(result)
        
    except Exception as e:
        print(f"❌ Failed to generate materialized view recommendations: {e}")

async def run_optimization_report(args):
    """Run comprehensive optimization report."""
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'dataops-mcp-server'))
        from tools.bigquery_wrapper import create_optimization_report_direct
        
        result = create_optimization_report_direct(
            days=args.days,
            report_type=args.report_type
        )
        
        print("📊 Optimization Report:")
        print(result)
        
    except Exception as e:
        print(f"❌ Failed to generate optimization report: {e}")

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
    
    # Service account analysis tool
    sa_parser = subparsers.add_parser("service-accounts", help="Analyze service account costs")
    sa_parser.add_argument("--days", type=int, default=7, help="Number of days to analyze")
    sa_parser.add_argument("--filter", help="Service account email filter (partial match)")
    sa_parser.add_argument("--include-queries", action="store_true", help="Include query text in results")
    sa_parser.add_argument("--min-cost", type=float, default=0.0, help="Minimum cost threshold")
    
    # Expensive queries analysis tool
    eq_parser = subparsers.add_parser("expensive-queries", help="Analyze expensive queries with categorization")
    eq_parser.add_argument("--days", type=int, default=7, help="Number of days to analyze")
    eq_parser.add_argument("--min-cost", type=float, default=10.0, help="Minimum cost threshold")
    eq_parser.add_argument("--categorize-by", default="cost_driver", 
                          choices=["cost_driver", "usage_pattern", "optimization_opportunity"],
                          help="Categorization method")
    
    # Optimization patterns tool
    op_parser = subparsers.add_parser("optimization-patterns", help="Detect optimization patterns in queries")
    op_parser.add_argument("--days", type=int, default=7, help="Number of days to analyze")
    op_parser.add_argument("--min-cost", type=float, default=5.0, help="Minimum cost threshold")
    
    # Cost forecast tool
    cf_parser = subparsers.add_parser("cost-forecast", help="Generate cost forecast based on historical data")
    cf_parser.add_argument("--historical-days", type=int, default=30, help="Days of historical data (7-90)")
    cf_parser.add_argument("--forecast-days", type=int, default=30, help="Days to forecast (1-365)")
    cf_parser.add_argument("--growth", choices=["current_trend", "conservative", "aggressive"], 
                          default="current_trend", help="Growth assumption model")
    
    # Table hotspots tool
    th_parser = subparsers.add_parser("table-hotspots", help="Analyze expensive table access patterns")
    th_parser.add_argument("--days", type=int, default=7, help="Number of days to analyze")
    th_parser.add_argument("--min-cost", type=float, default=5.0, help="Minimum access cost threshold")
    
    # Materialized views tool
    mv_parser = subparsers.add_parser("materialized-views", help="Generate materialized view recommendations")
    mv_parser.add_argument("--days", type=int, default=14, help="Number of days to analyze (7-30)")
    mv_parser.add_argument("--min-repetitions", type=int, default=3, help="Minimum repetition count (2-10)")
    mv_parser.add_argument("--min-cost", type=float, default=5.0, help="Minimum cost per execution")
    
    # Optimization report tool
    or_parser = subparsers.add_parser("optimization-report", help="Generate comprehensive optimization report")
    or_parser.add_argument("--days", type=int, default=7, help="Number of days to analyze")
    or_parser.add_argument("--report-type", choices=["executive", "technical", "stakeholder"], 
                          default="executive", help="Type of report to generate")
    
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
        elif args.tool == "service-accounts":
            asyncio.run(run_service_account_analysis(args))
        elif args.tool == "expensive-queries":
            asyncio.run(run_expensive_queries_analysis(args))
        elif args.tool == "optimization-patterns":
            asyncio.run(run_optimization_patterns(args))
        elif args.tool == "cost-forecast":
            asyncio.run(run_cost_forecast(args))
        elif args.tool == "table-hotspots":
            asyncio.run(run_table_hotspots(args))
        elif args.tool == "materialized-views":
            asyncio.run(run_materialized_views(args))
        elif args.tool == "optimization-report":
            asyncio.run(run_optimization_report(args))
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
