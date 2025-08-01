#!/usr/bin/env python3

import sys
import os
import argparse
from typing import List, Callable, Any, Type, Optional, Dict
from functools import wraps
import asyncio
import logging
from datetime import datetime

from mcp.server.fastmcp import FastMCP
from resources.gcp_cost_resource import GCPCostResource
from tools.cost_analysis_tools import CostAnalysisTools
from tools.query_optimization_tools import QueryOptimizationTools
from tools.anomaly_detection_tools import AnomalyDetectionTools
from tools.github_integration_tools import GitHubIntegrationTools
from tools.slack_integration_tools import SlackIntegrationTools
from tools.agent_management_tools import AgentManagementTools
from tools.dbt_integration_tools import DBTIntegrationTools
from tools.sla_monitoring_tools import SLAMonitoringTools

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Parse command line arguments
parser = argparse.ArgumentParser(description="GCP Cost Optimization MCP Server")
parser.add_argument(
    "--project", type=str, help="GCP project ID to use for cost analysis"
)
parser.add_argument(
    "--region", type=str, default="us-central1", help="GCP region for regional resources"
)
parser.add_argument(
    "--allow-write", action="store_true", help="Enable write operations (PR creation, alerts)"
)
parser.add_argument(
    "--allow-sensitive-data-access", action="store_true", 
    help="Enable access to sensitive data (detailed billing, query logs)"
)
parser.add_argument(
    "--enable-multi-agent", action="store_true", help="Enable multi-agent architecture"
)
parser.add_argument(
    "--debug", action="store_true", help="Enable debug logging"
)
args, unknown = parser.parse_known_args()

# Set debug logging if requested
if args.debug:
    logging.getLogger().setLevel(logging.DEBUG)
    logger.debug("Debug logging enabled")

# Add the current directory to the path so we can import our modules
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

# Create the MCP server for GCP cost optimization
mcp = FastMCP("GCP Cost Optimization")

# Initialize configuration from args and environment
project_id = args.project or os.getenv('GCP_PROJECT_ID')
region = args.region or os.getenv('GCP_REGION', 'us-central1')
allow_write = args.allow_write or os.getenv('ALLOW_WRITE_OPERATIONS', 'false').lower() == 'true'
allow_sensitive = args.allow_sensitive_data_access or os.getenv('ALLOW_SENSITIVE_DATA_ACCESS', 'false').lower() == 'true'
enable_agents = args.enable_multi_agent or os.getenv('ENABLE_MULTI_AGENT', 'false').lower() == 'true'

if not project_id:
    logger.error("GCP project ID is required. Use --project or set GCP_PROJECT_ID environment variable")
    sys.exit(1)

logger.info(f"Starting GCP Cost Optimization MCP Server for project: {project_id}")
logger.info(f"Configuration: region={region}, write_enabled={allow_write}, sensitive_data={allow_sensitive}, agents={enable_agents}")

# Initialize our resource and tools classes with the specified GCP project and region
try:
    cost_resource = GCPCostResource(project_id=project_id, region=region)
    cost_analysis_tools = CostAnalysisTools(project_id=project_id, region=region)
    query_optimization_tools = QueryOptimizationTools(project_id=project_id, region=region)
    anomaly_detection_tools = AnomalyDetectionTools(project_id=project_id, region=region)
    
    # Optional integrations (only if write operations enabled)
    github_tools = GitHubIntegrationTools(project_id=project_id) if allow_write else None
    slack_tools = SlackIntegrationTools(project_id=project_id) if allow_write else None
    
    # Multi-agent tools (only if agents enabled)
    agent_tools = AgentManagementTools(project_id=project_id, region=region) if enable_agents else None
    
    # Additional tools
    dbt_tools = DBTIntegrationTools(project_id=project_id, region=region)
    sla_tools = SLAMonitoringTools(project_id=project_id, region=region)
    
    logger.info("All tool classes initialized successfully")
    
except Exception as e:
    logger.error(f"Failed to initialize tool classes: {e}")
    sys.exit(1)

# Capture the parsed CLI configuration in separate variables
default_project = project_id
default_region = region


# Helper decorator to handle GCP configuration parameters for tools
def with_gcp_config(tool_class: Type, method_name: Optional[str] = None) -> Callable:
    """
    Decorator that handles the project and region parameters for tool functions.
    Creates a new instance of the specified tool class with the correct project and region.

    Args:
        tool_class: The class to instantiate with the project and region
        method_name: Optional method name if different from the decorated function
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            try:
                project = kwargs.pop("project_id", None) or default_project
                region = kwargs.pop("region", None) or default_region
                
                # Create tool instance with proper configuration
                if tool_class in [GitHubIntegrationTools, SlackIntegrationTools]:
                    tool_instance = tool_class(project_id=project)
                elif tool_class == AgentManagementTools:
                    tool_instance = tool_class(project_id=project, region=region) if enable_agents else None
                    if tool_instance is None:
                        raise RuntimeError("Multi-agent functionality is not enabled. Use --enable-multi-agent")
                else:
                    tool_instance = tool_class(project_id=project, region=region)
                
                target_method = method_name or func.__name__
                method = getattr(tool_instance, target_method)
                result = method(**kwargs)
                
                if asyncio.iscoroutine(result):
                    return await result
                return result
                
            except AttributeError as e:
                logger.error(f"Method {target_method} not found in {tool_class.__name__}: {e}")
                raise RuntimeError(
                    f"Method {target_method} not found in {tool_class.__name__}"
                ) from e
            except Exception as e:
                logger.error(f"Error executing {target_method} in {tool_class.__name__}: {e}")
                raise RuntimeError(
                    f"An error occurred while executing {target_method} in {tool_class.__name__}: {str(e)}"
                ) from e

        return wrapper

    return decorator


# ==============================
# Resource Handlers
# ==============================


@mcp.resource("gcp://costs/daily")
def get_daily_costs() -> str:
    """Get daily cost breakdown for the current project"""
    days = 7  # Default to last 7 days
    return cost_resource.get_daily_costs(days)


@mcp.resource("gcp://costs/daily/{date}")
def get_costs_for_date(date: str) -> str:
    """
    Get cost breakdown for a specific date

    Args:
        date: Date in YYYY-MM-DD format
    """
    return cost_resource.get_costs_for_date(date)


@mcp.resource("gcp://costs/monthly/{month}")
def get_monthly_costs(month: str) -> str:
    """
    Get monthly cost summary and trends

    Args:
        month: Month in YYYY-MM format
    """
    return cost_resource.get_monthly_costs(month)


@mcp.resource("gcp://costs/project/{project_id}")
def get_project_costs(project_id: str) -> str:
    """Get project-specific cost analysis and attribution"""
    return cost_resource.get_project_costs(project_id)


@mcp.resource("gcp://queries/expensive/{days}")
def get_expensive_queries_resource(days: str) -> str:
    """
    Get most expensive queries over the last N days

    Args:
        days: Number of days to analyze
    """
    days_int = int(days)
    limit = 20  # Default limit
    min_cost = 10.0  # Default minimum cost
    return cost_resource.get_expensive_queries(days_int, min_cost, limit)


@mcp.resource("gcp://queries/patterns/{pattern_hash}")
def get_query_pattern_analysis(pattern_hash: str) -> str:
    """Get analysis of similar query patterns and optimization opportunities"""
    return cost_resource.get_query_pattern_analysis(pattern_hash)


@mcp.resource("gcp://queries/optimization/{query_id}")
def get_optimization_details(query_id: str) -> str:
    """Get detailed optimization recommendations for a specific query"""
    return cost_resource.get_optimization_details(query_id)


@mcp.resource("gcp://anomalies/recent")
def get_recent_anomalies() -> str:
    """Get recently detected cost anomalies and alerts"""
    days = 7  # Default to last 7 days
    return cost_resource.get_recent_anomalies(days)


@mcp.resource("gcp://forecasts/costs/{days}")
def get_cost_forecast_resource(days: str) -> str:
    """
    Get cost forecasts for the next N days

    Args:
        days: Number of days to forecast
    """
    days_int = int(days)
    return cost_resource.get_cost_forecast(days_int)


@mcp.resource("gcp://agents/insights/{agent_type}")
def get_agent_insights_resource(agent_type: str) -> str:
    """Get latest insights from a specific agent type"""
    if not enable_agents:
        return '{"error": "Multi-agent functionality is not enabled"}'
    return cost_resource.get_agent_insights(agent_type)


@mcp.resource("gcp://agents/status/all")
def get_all_agents_status() -> str:
    """Get status and health of all deployed agents"""
    if not enable_agents:
        return '{"error": "Multi-agent functionality is not enabled"}'
    return cost_resource.get_all_agents_status()


# ==============================
# Prompts
# ==============================


@mcp.prompt()
def analyze_bigquery_costs(
    days: int = 7, project_id: str = None, include_predictions: bool = True
) -> str:
    """
    Prompt for analyzing BigQuery costs and identifying optimization opportunities.

    Args:
        days: Number of days to analyze (default: 7)
        project_id: Optional GCP project ID (uses default if not specified)
        include_predictions: Include ML-based cost forecasting (default: true)
    """
    project_text = f" for project '{project_id}'" if project_id else f" for project '{default_project}'"
    
    return f"""I'll help you analyze BigQuery costs{project_text} over the last {days} days.

Let me start by gathering comprehensive cost data and identifying optimization opportunities.

For this analysis, I'll:
1. **Retrieve cost breakdown** by day, dataset, user, and query type
2. **Identify expensive queries** that are driving high costs
3. **Detect cost anomalies** and unusual spending patterns
4. **Analyze query patterns** for optimization opportunities
5. **Provide AI-powered optimization suggestions** with estimated savings
6. **Generate cost forecasts** to predict future spending trends

Based on this analysis, I can help you:
- Create GitHub PRs with optimized queries
- Set up proactive cost monitoring and alerts
- Implement automated cost controls and budget management
- Optimize dbt models and materialization strategies

Would you like me to focus on any specific area, such as:
- Queries from a particular user or team
- Specific datasets or tables
- Recent cost spikes or anomalies
- Long-term budget planning and forecasting
"""


@mcp.prompt()
def optimize_expensive_query(
    sql: str, target_savings_pct: int = 30, project_id: str = None
) -> str:
    """
    Prompt for optimizing a specific expensive SQL query with AI-powered suggestions.

    Args:
        sql: The SQL query to optimize
        target_savings_pct: Target cost reduction percentage (default: 30)
        project_id: Optional GCP project ID (uses default if not specified)
    """
    project_text = f" in project '{project_id}'" if project_id else f" in project '{default_project}'"
    
    return f"""I'll analyze and optimize this BigQuery query{project_text} with a target of {target_savings_pct}% cost reduction.

**Query to optimize:**
```sql
{sql[:500]}{'...' if len(sql) > 500 else ''}
```

My optimization process will include:

1. **Cost Analysis**
   - Predict execution cost using dry-run API
   - Analyze data volume and complexity
   - Identify cost-driving operations

2. **AI-Powered Optimization**
   - Apply partition filtering strategies
   - Optimize JOIN operations and order
   - Improve column selection and aggregations
   - Suggest materialized view opportunities

3. **Validation & Testing**
   - Ensure result consistency with original query
   - Estimate performance improvements
   - Assess risk level of changes

4. **Implementation Support**
   - Generate optimized SQL with detailed explanations
   - Create implementation checklist
   - Provide rollback plan if needed

Would you like me to proceed with the optimization analysis? I can also:
- Create a GitHub PR with the optimized query
- Set up monitoring to track the optimization impact
- Suggest broader optimization opportunities for similar queries
"""


@mcp.prompt()
def investigate_cost_spike(
    spike_date: str = None, project_id: str = None
) -> str:
    """
    Prompt for investigating and resolving cost spikes with comprehensive root cause analysis.

    Args:
        spike_date: Optional date of the cost spike (YYYY-MM-DD format)
        project_id: Optional GCP project ID (uses default if not specified)
    """
    project_text = f" in project '{project_id}'" if project_id else f" in project '{default_project}'"
    date_text = f" on {spike_date}" if spike_date else " recently"
    
    return f"""I'll help you investigate the cost spike{date_text}{project_text} and provide actionable solutions.

My investigation will include:

1. **Spike Detection & Quantification**
   - Compare actual vs expected costs
   - Calculate deviation percentage and impact
   - Identify time windows of unusual activity

2. **Root Cause Analysis**
   - Find the most expensive queries during the spike period
   - Identify users, datasets, or applications responsible
   - Analyze query patterns and data volume changes

3. **Impact Assessment**
   - Calculate total financial impact
   - Assess ongoing cost risk if unaddressed
   - Predict future occurrences based on patterns

4. **Immediate & Long-term Solutions**
   - Provide emergency cost controls if spike is ongoing
   - Suggest query optimizations for high-impact queries
   - Recommend proactive monitoring to prevent future spikes

5. **Prevention Strategy**
   - Set up intelligent cost alerts and thresholds
   - Deploy monitoring agents for continuous oversight
   - Create automated optimization workflows

Let me start by analyzing the cost data and identifying the primary contributors to the spike. I'll provide both immediate remediation steps and long-term prevention strategies.
"""


# ==============================
# Tool Handlers
# ==============================


@mcp.tool()
@with_gcp_config(CostAnalysisTools)
async def get_bigquery_costs(
    days: int = 7,
    project_id: str = None,
    include_predictions: bool = True,
    group_by: List[str] = None,
    include_query_details: bool = False,
    region: str = None,
) -> str:
    """
    Retrieve comprehensive BigQuery cost analysis for specified time periods.

    Args:
        days: Number of days to analyze (1-90, default: 7)
        project_id: GCP project ID for cost analysis (uses default if not specified)
        include_predictions: Include ML-based cost forecasting (default: true)
        group_by: Grouping dimensions (default: ["date"])
        include_query_details: Include individual query cost breakdowns (default: false)
        region: GCP region for regional resources

    Returns:
        JSON string with comprehensive cost analysis
    """
    # Function body is handled by the decorator
    pass


@mcp.tool()
@with_gcp_config(CostAnalysisTools)
async def analyze_query_cost(
    sql: str,
    project_id: str = None,
    include_optimization: bool = True,
    optimization_model: str = "claude",
    create_pr_if_savings: bool = False,
    region: str = None,
) -> str:
    """
    Analyze the cost impact of a SQL query before execution using BigQuery's dry-run API.

    Args:
        sql: SQL query to analyze for cost impact
        project_id: GCP project ID for query analysis (uses default if not specified)
        include_optimization: Include AI-powered optimization suggestions (default: true)
        optimization_model: AI model to use for optimization ("claude", "gpt-4", default: "claude")
        create_pr_if_savings: Automatically create GitHub PR if savings > threshold (default: false)
        region: GCP region for analysis

    Returns:
        JSON string with detailed query cost analysis
    """
    # Function body is handled by the decorator
    pass


@mcp.tool()
@with_gcp_config(AnomalyDetectionTools)
async def detect_cost_anomalies(
    days: int = 30,
    sensitivity: str = "medium",
    project_id: str = None,
    alert_threshold: float = 0.25,
    send_slack_alert: bool = False,
    region: str = None,
) -> str:
    """
    Identify unusual cost patterns and spending spikes using machine learning models.

    Args:
        days: Historical period to analyze for anomaly detection (7-90, default: 30)
        sensitivity: Anomaly detection sensitivity level ("low", "medium", "high", default: "medium")
        project_id: Specific project to analyze (analyzes default project if not specified)
        alert_threshold: Percentage increase threshold for alerts (default: 0.25 = 25%)
        send_slack_alert: Send Slack notification for detected anomalies (default: false)
        region: GCP region for analysis

    Returns:
        JSON string with detected cost anomalies
    """
    # Function body is handled by the decorator
    pass


@mcp.tool()
@with_gcp_config(QueryOptimizationTools)
async def optimize_query(
    sql: str,
    optimization_goals: List[str] = None,
    preserve_results: bool = True,
    include_explanation: bool = True,
    target_savings_pct: int = 30,
    dbt_model_path: str = None,
    project_id: str = None,
    region: str = None,
) -> str:
    """
    Provide AI-powered SQL query optimization using Large Language Models.

    Args:
        sql: SQL query to optimize for cost and performance
        optimization_goals: Optimization objectives (default: ["cost", "performance"])
        preserve_results: Ensure optimized query returns identical results (default: true)
        include_explanation: Include detailed explanation of optimizations (default: true)
        target_savings_pct: Target cost reduction percentage (default: 30)
        dbt_model_path: Optional path to dbt model file for context
        project_id: GCP project ID for optimization context
        region: GCP region for analysis

    Returns:
        JSON string with comprehensive optimization analysis
    """
    # Function body is handled by the decorator
    pass


@mcp.tool()
@with_gcp_config(CostAnalysisTools)
async def find_expensive_queries(
    days: int = 7,
    min_cost: float = 10.0,
    limit: int = 20,
    project_id: str = None,
    include_optimization_priority: bool = True,
    region: str = None,
) -> str:
    """
    Identify the most expensive BigQuery queries over a specified time period.

    Args:
        days: Time period to analyze for expensive queries (1-30, default: 7)
        min_cost: Minimum cost threshold in USD to include (default: 10.0)
        limit: Maximum number of queries to return (1-100, default: 20)
        project_id: GCP project ID for analysis
        include_optimization_priority: Include optimization priority scoring (default: true)
        region: GCP region for analysis

    Returns:
        JSON string with list of expensive queries
    """
    # Function body is handled by the decorator
    pass


# Write-enabled tools (only available if --allow-write is specified)
if allow_write:
    
    @mcp.tool()
    @with_gcp_config(GitHubIntegrationTools)
    async def create_optimization_pr(
        optimization_id: str,
        repository: str = "data-platform",
        base_branch: str = "main",
        title_prefix: str = "🚀 Cost Optimization",
        assign_reviewers: bool = True,
        include_tests: bool = True,
        project_id: str = None,
    ) -> str:
        """
        Create a GitHub pull request with query optimizations and cost reduction implementations.

        Args:
            optimization_id: ID from previous optimization analysis to implement
            repository: GitHub repository name for PR creation (default: "data-platform")
            base_branch: Base branch for the pull request (default: "main")
            title_prefix: PR title prefix (default: "🚀 Cost Optimization")
            assign_reviewers: Automatically assign relevant reviewers (default: true)
            include_tests: Generate validation tests for optimizations (default: true)
            project_id: GCP project ID for context

        Returns:
            JSON string with GitHub PR creation result
        """
        # Function body is handled by the decorator
        pass

    @mcp.tool()
    @with_gcp_config(SlackIntegrationTools)
    async def send_cost_alert(
        alert_type: str,
        cost_data: Dict[str, Any],
        severity: str = "medium",
        channel: str = "#data-ops-alerts",
        mention_users: List[str] = None,
        include_remediation: bool = True,
        project_id: str = None,
    ) -> str:
        """
        Send intelligent cost alerts to Slack with rich context and remediation suggestions.

        Args:
            alert_type: Type of alert ("anomaly", "budget_warning", "optimization_opportunity")
            cost_data: Cost analysis data to include in alert message
            severity: Alert severity level ("low", "medium", "high", "critical", default: "medium")
            channel: Slack channel for notification (default: "#data-ops-alerts")
            mention_users: List of users to mention in the alert (default: [])
            include_remediation: Include suggested remediation steps (default: true)
            project_id: GCP project ID for context

        Returns:
            JSON string with Slack alert delivery result
        """
        # Function body is handled by the decorator
        pass


@mcp.tool()
@with_gcp_config(DBTIntegrationTools)
async def get_dbt_model_costs(
    model_path: str = None,
    include_dependencies: bool = True,
    days: int = 7,
    materialization_analysis: bool = True,
    suggest_optimizations: bool = True,
    project_id: str = None,
    region: str = None,
) -> str:
    """
    Analyze costs associated with dbt models and provide optimization recommendations.

    Args:
        model_path: Specific dbt model to analyze (analyzes all models if not specified)
        include_dependencies: Include downstream model impact analysis (default: true)
        days: Historical period for cost analysis (1-30, default: 7)
        materialization_analysis: Analyze materialization strategy costs (default: true)
        suggest_optimizations: Include optimization recommendations (default: true)
        project_id: GCP project ID for analysis
        region: GCP region for analysis

    Returns:
        JSON string with dbt model cost analysis
    """
    # Function body is handled by the decorator
    pass


@mcp.tool()
@with_gcp_config(SLAMonitoringTools)
async def monitor_sla_compliance(
    sla_type: str = "all",
    time_window: str = "24h",
    include_cost_correlation: bool = True,
    alert_on_breach: bool = False,
    optimization_suggestions: bool = True,
    project_id: str = None,
    region: str = None,
) -> str:
    """
    Monitor data pipeline SLA compliance and correlate with cost efficiency metrics.

    Args:
        sla_type: SLA category to monitor ("latency", "freshness", "success_rate", "all", default: "all")
        time_window: Time window for SLA analysis ("1h", "24h", "7d", default: "24h")
        include_cost_correlation: Correlate SLA metrics with cost efficiency data (default: true)
        alert_on_breach: Send alert for SLA violations (default: false)
        optimization_suggestions: Include cost-aware optimization suggestions (default: true)
        project_id: GCP project ID for analysis
        region: GCP region for analysis

    Returns:
        JSON string with SLA compliance analysis
    """
    # Function body is handled by the decorator
    pass


@mcp.tool()
@with_gcp_config(CostAnalysisTools)
async def forecast_costs(
    forecast_days: int = 30,
    include_confidence_intervals: bool = True,
    breakdown_by: List[str] = None,
    scenario_analysis: bool = False,
    budget_recommendations: bool = True,
    project_id: str = None,
    region: str = None,
) -> str:
    """
    Provide ML-powered cost forecasting and budget planning recommendations.

    Args:
        forecast_days: Number of days to forecast into the future (7-365, default: 30)
        include_confidence_intervals: Include prediction confidence ranges (default: true)
        breakdown_by: Forecast breakdown dimensions (default: ["service"])
        scenario_analysis: Include optimistic/pessimistic scenarios (default: false)
        budget_recommendations: Include budget planning suggestions (default: true)
        project_id: GCP project ID for forecasting
        region: GCP region for analysis

    Returns:
        JSON string with comprehensive cost forecast
    """
    # Function body is handled by the decorator
    pass


# Multi-agent tools (only available if --enable-multi-agent is specified)
if enable_agents:
    
    @mcp.tool()
    @with_gcp_config(AgentManagementTools)
    async def deploy_cost_agent(
        agent_type: str,
        monitoring_interval: int = 300,
        auto_optimize: bool = False,
        notification_channels: List[str] = None,
        cost_thresholds: Dict[str, float] = None,
        project_id: str = None,
        region: str = None,
    ) -> str:
        """
        Deploy a specialized AI agent for continuous cost monitoring and optimization.

        Args:
            agent_type: Type of agent to deploy ("cost_guard", "query_optimizer", "sla_sentinel")
            monitoring_interval: Monitoring frequency in seconds (60-3600, default: 300)
            auto_optimize: Enable automatic optimization without human approval (default: false)
            notification_channels: Channels for agent notifications (default: ["slack"])
            cost_thresholds: Custom cost thresholds for agent actions
            project_id: GCP project ID for agent deployment
            region: GCP region for agent deployment

        Returns:
            JSON string with agent deployment confirmation
        """
        # Function body is handled by the decorator
        pass

    @mcp.tool()
    @with_gcp_config(AgentManagementTools)
    async def get_agent_insights(
        agent_name: str = None,
        insight_type: str = "all",
        priority_filter: str = "medium",
        days: int = 7,
        correlation_analysis: bool = True,
        project_id: str = None,
        region: str = None,
    ) -> str:
        """
        Retrieve prioritized insights and recommendations from deployed AI agents.

        Args:
            agent_name: Specific agent to query (returns insights from all agents if not specified)
            insight_type: Type of insights to retrieve ("cost", "performance", "security", "all", default: "all")
            priority_filter: Minimum priority level ("low", "medium", "high", "critical", default: "medium")
            days: Historical period for insight generation (default: 7)
            correlation_analysis: Include cross-agent correlation analysis (default: true)
            project_id: GCP project ID for insights
            region: GCP region for insights

        Returns:
            JSON string with list of agent insights
        """
        # Function body is handled by the decorator
        pass


# ==============================
# Error Handlers and Validation
# ==============================


def validate_write_permissions(operation_name: str):
    """Validate that write operations are allowed"""
    if not allow_write:
        raise RuntimeError(
            f"Write operation '{operation_name}' is not allowed. "
            "Use --allow-write to enable write operations."
        )


def validate_sensitive_data_access(operation_name: str):
    """Validate that sensitive data access is allowed"""
    if not allow_sensitive:
        raise RuntimeError(
            f"Sensitive data operation '{operation_name}' is not allowed. "
            "Use --allow-sensitive-data-access to enable access to sensitive data."
        )


def validate_agent_functionality(operation_name: str):
    """Validate that multi-agent functionality is enabled"""
    if not enable_agents:
        raise RuntimeError(
            f"Multi-agent operation '{operation_name}' is not available. "
            "Use --enable-multi-agent to enable agent functionality."
        )


# ==============================
# Health Check and Status
# ==============================


@mcp.tool()
async def health_check() -> str:
    """
    Perform a health check of the MCP server and all integrations.

    Returns:
        JSON string with health status of all components
    """
    health_status = {
        "server": "healthy",
        "timestamp": datetime.now().isoformat(),
        "configuration": {
            "project_id": default_project,
            "region": default_region,
            "write_enabled": allow_write,
            "sensitive_data_enabled": allow_sensitive,
            "multi_agent_enabled": enable_agents
        },
        "integrations": {}
    }
    
    try:
        # Test GCP connectivity
        test_result = await cost_analysis_tools.health_check()
        health_status["integrations"]["gcp"] = "healthy" if test_result else "unhealthy"
    except Exception as e:
        health_status["integrations"]["gcp"] = f"unhealthy: {str(e)}"
    
    # Test optional integrations
    if allow_write:
        try:
            if github_tools:
                github_status = await github_tools.health_check()
                health_status["integrations"]["github"] = "healthy" if github_status else "unhealthy"
        except Exception as e:
            health_status["integrations"]["github"] = f"unhealthy: {str(e)}"
        
        try:
            if slack_tools:
                slack_status = await slack_tools.health_check()
                health_status["integrations"]["slack"] = "healthy" if slack_status else "unhealthy"
        except Exception as e:
            health_status["integrations"]["slack"] = f"unhealthy: {str(e)}"
    
    # Test agent functionality
    if enable_agents:
        try:
            agent_status = await agent_tools.health_check()
            health_status["integrations"]["agents"] = "healthy" if agent_status else "unhealthy"
        except Exception as e:
            health_status["integrations"]["agents"] = f"unhealthy: {str(e)}"
    
    return str(health_status)


if __name__ == "__main__":
    logger.info("Starting GCP Cost Optimization MCP Server...")
    logger.info(f"Server configuration: project={default_project}, region={default_region}")
    logger.info(f"Write operations: {'enabled' if allow_write else 'disabled'}")
    logger.info(f"Sensitive data access: {'enabled' if allow_sensitive else 'disabled'}")
    logger.info(f"Multi-agent architecture: {'enabled' if enable_agents else 'disabled'}")
    
    try:
        # Run the MCP server
        mcp.run()
    except KeyboardInterrupt:
        logger.info("Server shutdown requested by user")
    except Exception as e:
        logger.error(f"Server failed to start: {e}")
        sys.exit(1)
