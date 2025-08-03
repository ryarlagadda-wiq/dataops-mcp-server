#!/usr/bin/env python3
"""
Working MCP Server - Properly handles imports and demonstrates tool usage
"""

import asyncio
import json
import sys
import os
from pathlib import Path

# Add the correct paths
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir / "src" / "dataops-mcp-server"))

async def run_server_tools_demo():
    """Demonstrate running tools directly without the problematic server"""
    
    print("🚀 DataOps MCP Tools Demo")
    print("=" * 50)
    
    try:
        # Import the working tools directly
        from tools.bigquery_tools import GetBigQueryCostsTool
        from tools.cost_analysis_tools import AnalyzeQueryCostTool
        
        project_id = "gcp-wow-wiq-tsr-dev"
        
        print(f"✅ Successfully imported tools")
        print(f"📊 Project: {project_id}")
        print()
        
        # Demo 1: Health Check
        print("🏥 Demo 1: Health Check")
        print("-" * 30)
        cost_tool = GetBigQueryCostsTool(project_id=project_id)
        is_healthy = await cost_tool.health_check()
        print(f"Server Health: {'✅ Healthy' if is_healthy else '❌ Unhealthy'}")
        
        # Demo 2: Quick Cost Analysis
        print("\n💰 Demo 2: Quick Cost Analysis (Last 3 Days)")
        print("-" * 45)
        result = await cost_tool.execute(
            days=3,
            include_predictions=True,
            group_by=["date"],
            include_query_details=False
        )
        
        data = json.loads(result)
        if data.get("success"):
            analysis = data.get("cost_analysis", {})
            summary = analysis.get("cost_summary", {})
            
            print(f"📈 Total Cost: ${summary.get('total_cost_usd', 0):.2f}")
            print(f"📊 Total Queries: {summary.get('total_queries', 0):,}")
            print(f"💲 Avg Cost/Query: ${summary.get('average_cost_per_query', 0):.4f}")
            
            # Show daily breakdown
            daily = analysis.get("daily_breakdown", [])[:3]  # Last 3 days
            if daily:
                print(f"\n📅 Daily Breakdown:")
                for day in daily:
                    date = day.get("date", "Unknown")
                    cost = day.get("cost_usd", 0)
                    queries = day.get("query_count", 0)
                    print(f"   {date}: ${cost:.2f} ({queries} queries)")
        
        # Demo 3: Query Analysis on the velocities_rolling query
        print("\n🔍 Demo 3: Analyze Complex Query (velocities_rolling)")
        print("-" * 55)
        
        # Read the query file we created
        query_file = Path("velocities_rolling_query.sql")
        if query_file.exists():
            with open(query_file, 'r') as f:
                complex_query = f.read()
            
            query_tool = AnalyzeQueryCostTool(project_id=project_id)
            result = await query_tool.execute(
                sql=complex_query,
                include_optimization=True,
                optimization_model="pattern_based"
            )
            
            data = json.loads(result)
            if data.get("success"):
                analysis = data.get("analysis", {})
                
                print(f"💰 Estimated Cost: ${analysis.get('estimated_cost_usd', 0):.2f}")
                print(f"📊 Data Volume: {analysis.get('bytes_to_process', 0) / (1024**3):.1f} GB")
                print(f"⚡ Est. Duration: {analysis.get('performance_predictions', {}).get('estimated_duration_seconds', 0)/60:.1f} minutes")
                print(f"🚨 Risk Level: {analysis.get('risk_assessment', {}).get('risk_level', 'Unknown')}")
                
                # Show optimization suggestions
                suggestions = analysis.get("optimization_suggestions", [])
                if suggestions:
                    print(f"\n🔧 Optimization Suggestions ({len(suggestions)}):")
                    for i, suggestion in enumerate(suggestions[:3], 1):
                        print(f"   {i}. {suggestion}")
                    if len(suggestions) > 3:
                        print(f"   ... and {len(suggestions) - 3} more")
        
        # Demo 4: Show how to run as MCP server
        print("\n🌐 Demo 4: MCP Server Integration")
        print("-" * 40)
        
        print("To use these tools with Claude Desktop:")
        print("1. Add to claude_desktop_config.json:")
        print("""
{
  "mcpServers": {
    "gcp-cost-optimization": {
      "command": "uv",
      "args": ["run", "python", "working_server.py"],
      "cwd": "/Users/ryarlagadda/repos/dataops-mcp-server",
      "env": {
        "GCP_PROJECT_ID": "gcp-wow-wiq-tsr-dev"
      }
    }
  }
}""")
        
        print("\n2. Available natural language commands in Claude:")
        commands = [
            "Analyze my BigQuery costs for the last 30 days",
            "Find queries costing more than $50",
            "Optimize this SQL query for cost efficiency",
            "Detect any unusual cost spikes",
            "Generate a cost optimization report"
        ]
        
        for cmd in commands:
            print(f"   • \"{cmd}\"")
        
        print("\n✨ All demos completed successfully!")
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()

def show_cli_usage():
    """Show CLI usage examples"""
    
    print("\n📚 CLI Usage Examples")
    print("=" * 30)
    
    examples = [
        ("Health Check", "uv run python run_tool.py health"),
        ("Cost Analysis (7 days)", "uv run python run_tool.py costs --days 7 --predictions"),
        ("Cost Analysis (30 days)", "uv run python run_tool.py costs --days 30 --group-by date,user --details"),
        ("Query Analysis", 'uv run python run_tool.py query "SELECT COUNT(*) FROM table" --optimize'),
        ("Complex Query Analysis", 'uv run python run_tool.py query "$(cat velocities_rolling_query.sql)" --optimize')
    ]
    
    for title, command in examples:
        print(f"\n🔹 {title}:")
        print(f"   {command}")

async def main():
    """Main function"""
    
    print("🎯 DataOps MCP Server - Complete Demo")
    print("=" * 60)
    
    # Run the tool demos
    await run_server_tools_demo()
    
    # Show CLI usage
    show_cli_usage()
    
    print("\n" + "=" * 60)
    print("🎉 Demo Complete!")
    print("\nYou can now:")
    print("1. Use the CLI tools: run_tool.py")
    print("2. Configure Claude Desktop with the MCP server")
    print("3. Run tools directly in Python scripts")
    print("4. Integrate with your dbt workflows")

if __name__ == "__main__":
    asyncio.run(main())
