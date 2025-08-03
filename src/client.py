#!/usr/bin/env python3
"""
Simple CLI for running DataOps MCP Server tools.

Updated to use the new bigquery_core.py for core functionality and 
bigquery_wrapper.py for advanced analysis features.

Usage: python client.py <tool_name> [options]
"""

import argparse
import asyncio
import json
import sys
import os
from pathlib import Path

# Add both the servers and tools directories to path
servers_path = Path(__file__).parent / 'dataops-mcp-server' / 'servers'
tools_path = Path(__file__).parent / 'dataops-mcp-server' / 'tools'
dataops_path = Path(__file__).parent / 'dataops-mcp-server'

sys.path.insert(0, str(servers_path))
sys.path.insert(0, str(tools_path))  
sys.path.insert(0, str(dataops_path))

async def run_get_costs(args):
    """Enhanced cost analysis using bigquery_core.py."""
    if args.details:
        # Use get_cost_summary for comprehensive analysis with insights
        result = await call_mcp_tool(
            args, 
            bigquery_core, 
            "get_cost_summary", 
            {
                "project_id": args.project,
                "days": args.days
            }
        )
        
        if result:
            print("🎯 === BIGQUERY COST SUMMARY WITH INSIGHTS ===")
            print(f"Analysis Period: {args.days} days")
            print()
            
            # Extract and display key metrics
            content = result.get("content", "No data available")
            
            # Enhanced formatting for insights
            if "insights" in content.lower() or "recommendation" in content.lower():
                print("� === KEY INSIGHTS & RECOMMENDATIONS ===")
                print(content)
            else:
                print("📊 === COST ANALYSIS RESULTS ===")
                print(content)
    else:
        # Use get_daily_costs for standard daily analysis
        result = await call_mcp_tool(
            args, 
            bigquery_core, 
            "get_daily_costs", 
            {
                "project_id": args.project,
                "days": args.days
            }
        )
        
        if result:
            print("📊 === DAILY BIGQUERY COSTS ===")
            print(f"Period: Last {args.days} days")
            print()
            print(result.get("content", "No data available"))
    
    print(f"\n✅ Cost analysis complete (Enhanced by bigquery_core.py)")

async def run_top_users(args):
    """Run top users analysis using new bigquery_core architecture."""
    try:
        # Set environment variable for the core module
        os.environ["GOOGLE_CLOUD_PROJECT"] = args.project
        
        # Import from bigquery_core
        from bigquery_core import get_top_users
        
        result = get_top_users(args.days, args.limit)
        result_data = json.loads(result)
        
        print("👥 Top BigQuery Users:")
        print(json.dumps(result_data, indent=2))
        
        # Show quick summary
        if result_data.get("success") and "data" in result_data:
            summary = result_data["data"].get("summary", {})
            print(f"\n👑 Quick Summary:")
            print(f"   Users Analyzed: {summary.get('unique_users', 0)}")
            print(f"   Total Cost: ${summary.get('total_analyzed_cost', 0):.2f}")
            print(f"   Avg Cost per User: ${summary.get('cost_per_user_avg', 0):.2f}")
        
    except Exception as e:
        print(f"❌ Failed to get top users: {e}")

async def run_service_account_analysis(args):
    """Run service account analysis using legacy wrapper (for now)."""
    try:
        from bigquery_wrapper import bigquery_cost_analyzer
        
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
    """Run expensive queries analysis using legacy wrapper."""
    try:
        from bigquery_wrapper import analyze_expensive_queries
        
        result = analyze_expensive_queries(
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
    """Run optimization patterns detection using legacy wrapper."""
    try:
        from bigquery_wrapper import detect_optimization_patterns
        
        result = detect_optimization_patterns(
            project_id=args.project,
            days=args.days,
            min_cost_threshold=args.min_cost
        )
        
        print("🔍 Optimization Patterns Analysis Results:")
        print(result)
        
    except Exception as e:
        print(f"❌ Failed to detect optimization patterns: {e}")

async def run_analyze_query(args):
    """Query analysis not available - suggest alternatives."""
    print("❌ Individual query analysis not available in current version")
    print("💡 Available alternatives:")
    print("  • Use 'costs --details' for comprehensive cost analysis")
    print("  • Use 'expensive-queries' to find costly query patterns")
    print("  • Use 'optimization-patterns' for query optimization suggestions")

async def run_health_check(args):
    """Run health check using new bigquery_core architecture."""
    try:
        # Set environment variable for the core module
        os.environ["GOOGLE_CLOUD_PROJECT"] = args.project
        
        # Import from bigquery_core
        from bigquery_core import health_check
        
        result = health_check()
        result_data = json.loads(result)
        
        print("🏥 Health Check Results:")
        print(json.dumps(result_data, indent=2))
        
        # Show quick status
        if result_data.get("success"):
            print("\n✅ BigQuery connectivity: HEALTHY")
        else:
            print("\n❌ BigQuery connectivity: UNHEALTHY")
            if "data" in result_data and "suggestions" in result_data["data"]:
                print("💡 Suggestions:")
                for suggestion in result_data["data"]["suggestions"]:
                    print(f"   • {suggestion}")
        
    except Exception as e:
        print(f"❌ Health check failed: {e}")

async def run_cost_forecast(args):
    """Run cost forecast using legacy wrapper."""
    try:
        from bigquery_wrapper import create_cost_forecast
        
        result = create_cost_forecast(
            project_id=args.project,
            days_historical=args.historical_days,
            days_forecast=args.forecast_days,
            growth_assumptions=args.growth or "current_trend"
        )
        
        print("📈 Cost Forecast Results:")
        print(result)
        
    except Exception as e:
        print(f"❌ Failed to generate cost forecast: {e}")

async def run_table_hotspots(args):
    """Run table hotspots analysis using legacy wrapper."""
    try:
        from bigquery_wrapper import analyze_table_hotspots
        
        result = analyze_table_hotspots(
            project_id=args.project,
            days=args.days,
            min_access_cost=args.min_cost
        )
        
        print("🔥 Table Hotspots Analysis Results:")
        print(result)
        
    except Exception as e:
        print(f"❌ Failed to analyze table hotspots: {e}")

async def run_materialized_views(args):
    """Run materialized view recommendations using legacy wrapper."""
    try:
        from bigquery_wrapper import generate_materialized_view_recommendations
        
        result = generate_materialized_view_recommendations(
            project_id=args.project,
            days=args.days,
            min_repetition_count=args.min_repetitions,
            min_cost_per_execution=args.min_cost
        )
        
        print("🏗️ Materialized View Recommendations:")
        print(result)
        
    except Exception as e:
        print(f"❌ Failed to generate materialized view recommendations: {e}")

async def run_optimization_report(args):
    """Run comprehensive optimization report using legacy wrapper."""
    try:
        from bigquery_wrapper import create_optimization_report
        
        result = create_optimization_report(
            project_id=args.project,
            days=args.days,
            report_type=args.report_type
        )
        
        print("📊 Optimization Report:")
        print(result)
        
    except Exception as e:
        print(f"❌ Failed to generate optimization report: {e}")

def main():
    """Main CLI function with updated architecture."""
    parser = argparse.ArgumentParser(
        description="BigQuery Analysis CLI - Enhanced with BigQuery Core",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Architecture:
  Core Tools (bigquery_core.py):    health, costs, top-users  
  Advanced Tools (bigquery_wrapper.py): service-accounts, expensive-queries, etc.

Examples:
  python client.py health --project my-project
  python client.py costs --days 14 --details
  python client.py top-users --limit 20
  python client.py service-accounts --filter my-sa
        """
    )
    parser.add_argument("--project", default="gcp-wow-wiq-tsr-dev", help="GCP Project ID")
    
    subparsers = parser.add_subparsers(dest="tool", help="Available analysis tools")
    
    # === CORE TOOLS (bigquery_core.py) ===
    # Health check
    health_parser = subparsers.add_parser("health", help="🏥 Check BigQuery connectivity")
    
    # Core cost analysis 
    costs_parser = subparsers.add_parser("costs", help="📊 Daily cost analysis (ENHANCED)")
    costs_parser.add_argument("--days", type=int, default=7, help="Number of days to analyze")
    costs_parser.add_argument("--details", action="store_true", help="Get comprehensive cost summary with insights")
    
    # Top users analysis
    users_parser = subparsers.add_parser("top-users", help="👥 Top BigQuery users by cost (ENHANCED)")
    users_parser.add_argument("--days", type=int, default=7, help="Number of days to analyze")
    users_parser.add_argument("--limit", type=int, default=10, help="Number of top users to show")
    
    # === ADVANCED TOOLS (bigquery_wrapper.py) ===
    # Service account analysis
    sa_parser = subparsers.add_parser("service-accounts", help="🔍 Service account cost analysis")
    sa_parser.add_argument("--days", type=int, default=7, help="Number of days to analyze")
    sa_parser.add_argument("--filter", help="Service account email filter (partial match)")
    sa_parser.add_argument("--include-queries", action="store_true", help="Include query text")
    sa_parser.add_argument("--min-cost", type=float, default=0.0, help="Minimum cost threshold")
    
    # Expensive queries analysis
    eq_parser = subparsers.add_parser("expensive-queries", help="💰 Expensive queries analysis")
    eq_parser.add_argument("--days", type=int, default=7, help="Number of days to analyze")
    eq_parser.add_argument("--min-cost", type=float, default=10.0, help="Minimum cost threshold")
    eq_parser.add_argument("--categorize-by", default="cost_driver", 
                          choices=["cost_driver", "usage_pattern", "optimization_opportunity"],
                          help="Categorization method")
    
    # Optimization patterns
    op_parser = subparsers.add_parser("optimization-patterns", help="🔍 Query optimization patterns")
    op_parser.add_argument("--days", type=int, default=7, help="Number of days to analyze")
    op_parser.add_argument("--min-cost", type=float, default=5.0, help="Minimum cost threshold")
    
    # Cost forecasting
    cf_parser = subparsers.add_parser("cost-forecast", help="📈 Cost forecasting")
    cf_parser.add_argument("--historical-days", type=int, default=30, help="Historical data days")
    cf_parser.add_argument("--forecast-days", type=int, default=30, help="Days to forecast")
    cf_parser.add_argument("--growth", choices=["current_trend", "conservative", "aggressive"], 
                          default="current_trend", help="Growth assumption")
    
    # Table hotspots
    th_parser = subparsers.add_parser("table-hotspots", help="🔥 Table access analysis")
    th_parser.add_argument("--days", type=int, default=7, help="Number of days to analyze")
    th_parser.add_argument("--min-cost", type=float, default=5.0, help="Minimum access cost")
    
    # Materialized views
    mv_parser = subparsers.add_parser("materialized-views", help="🏗️ Materialized view recommendations")
    mv_parser.add_argument("--days", type=int, default=14, help="Number of days to analyze")
    mv_parser.add_argument("--min-repetitions", type=int, default=3, help="Minimum repetitions")
    mv_parser.add_argument("--min-cost", type=float, default=5.0, help="Minimum cost per execution")
    
    # Optimization report
    or_parser = subparsers.add_parser("optimization-report", help="📋 Comprehensive optimization report")
    or_parser.add_argument("--days", type=int, default=7, help="Number of days to analyze")
    or_parser.add_argument("--report-type", choices=["executive", "technical", "stakeholder"], 
                          default="executive", help="Report type")
    
    # Deprecated command
    query_parser = subparsers.add_parser("query", help="❌ Single query analysis (deprecated)")
    query_parser.add_argument("sql", nargs='?', help="SQL query to analyze")
    query_parser.add_argument("--optimize", action="store_true", help="Include optimization suggestions")
    
    args = parser.parse_args()
    
    if not args.tool:
        parser.print_help()
        print("\n� Quick start examples:")
        print("  python client.py health                           # Test connectivity")
        print("  python client.py costs --days 7                  # Core cost analysis")
        print("  python client.py costs --days 30 --details       # Detailed insights")
        print("  python client.py top-users --limit 20            # Enhanced user analysis")
        print("  python client.py service-accounts --filter sa    # Service account deep dive")
        print("\n📖 Architecture:")
        print("  🎯 Core Tools: health, costs, top-users (bigquery_core.py)")
        print("  🔧 Advanced Tools: service-accounts, expensive-queries, etc. (bigquery_wrapper.py)")
        return
    
    print(f"🚀 Running tool: {args.tool}")
    print(f"📊 Project: {args.project}")
    
    # Determine which architecture to use
    core_tools = ["health", "costs", "top-users"]
    if args.tool in core_tools:
        print(f"🎯 Using: bigquery_core.py (Enhanced)")
    else:
        print(f"🔧 Using: bigquery_wrapper.py (Advanced)")
    print()
    
    try:
        # === CORE TOOLS (bigquery_core.py) ===
        if args.tool == "health":
            asyncio.run(run_health_check(args))
        elif args.tool == "costs":
            asyncio.run(run_get_costs(args))
        elif args.tool == "top-users":
            asyncio.run(run_top_users(args))
        
        # === ADVANCED TOOLS (bigquery_wrapper.py) ===  
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
        else:
            print(f"❌ Unknown tool: {args.tool}")
    
    except KeyboardInterrupt:
        print("\n⏹️  Cancelled by user")
    except Exception as e:
        print(f"❌ Execution failed: {e}")
        print(f"💡 Suggestion: Check your Google Cloud authentication and project permissions")

if __name__ == "__main__":
    main()
