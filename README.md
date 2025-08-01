# GCP Cost Optimization Multi-Agent MCP Server

## Prerequisites
* Have a GCP account with Application Default Credentials configured
* Install uv from [Astral](https://docs.astral.sh/uv/getting-started/installation/) or the [GitHub README](https://github.com/astral-sh/uv#installation)
* Install Python 3.10 or newer using `uv python install 3.10` (or a more recent version)
* Have access to BigQuery projects with cost data (requires `bigquery.jobs.list permission`)
* Install [gcloud CLI](https://cloud.google.com/sdk/docs/install-sdk) for authentication
* Optional: GitHub token for PR automation, Slack webhook for notifications


## Installation
| Cursor | VS Code |
|:------:|:-------:|

You can download the GCP Cost Optimization MCP Server from GitHub. To get started using your favorite AI assistant with MCP support, like Claude Desktop, Cursor.

Add the following code to your MCP client configuration. The GCP Cost Optimization MCP server uses the default GCP project from your ADC by default. Specify a value in `GCP_PROJECT_ID` if you want to use a different project. Similarly, adjust the `GCP_REGION` configuration values as per your setup.
```json
{
  "mcpServers": {
    "gcp-cost-optimization": {
      "command": "uvx",
      "args": [
        "quantium.gcp-cost-optimization-mcp@latest",
        "--allow-write",
        "--allow-sensitive-data-access"
      ],
      "env": {
        "GCP_PROJECT_ID": "your-gcp-project",
        "GCP_REGION": "us-central1",
        "ENABLE_MULTI_AGENT": "true"
      },
      "disabled": false,
      "autoApprove": []
    }
  }
}
```

## Using service account credentials
```json
{
  "mcpServers": {
    "gcp-cost-optimization": {
      "command": "uvx",
      "args": ["quantium.gcp-cost-optimization-mcp@latest"],
      "env": {
        "GOOGLE_APPLICATION_CREDENTIALS": "/path/to/service-account.json",
        "GCP_PROJECT_ID": "your-gcp-project",
        "GCP_REGION": "us-central1"
      },
      "disabled": false,
      "autoApprove": []
    }
  }
}
```

## Cost Optimization MCP Server configuration options
### `--allow-write`
Enables write access mode, which allows mutating operations like creating GitHub PRs, sending Slack notifications, and deploying optimization changes. By default, the server runs in read-only mode, which restricts operations to only perform cost analysis and query examination, preventing any changes to external systems.

Mutating operations:
- `create_optimization_pr`: Creates GitHub pull requests with query optimizations and cost reduction recommendations
- `send_cost_alert`: Sends Slack notifications for cost anomalies and budget threshold breaches
- `deploy_optimization`: Applies approved optimizations to dbt models and BigQuery resources
- `update_cost_budgets`: Modifies BigQuery cost controls and spending alerts

### `--allow-sensitive-data-access`
Enables access to sensitive data such as detailed billing information, query logs, and performance metrics. By default, the server restricts access to aggregate cost data only.
Operations returning sensitive data:
- `get_detailed_billing`: Returns itemized billing data with resource-level cost breakdowns
- `analyze_query_logs`: Accesses BigQuery audit logs for detailed query performance analysis
- `get_user_query_patterns`: Returns user-specific query patterns and cost attribution

### `--enable-multi-agent`
Activates the multi-agent architecture with specialized AI agents for different optimization domains. When disabled, the server operates as a simple MCP tool server without agent orchestration.
Multi-agent features:
- CostGuard Agent: Specialized in billing analysis and budget management
- QueryOptimizer Agent: Expert in SQL optimization and performance tuning
- SLA Sentinel Agent: Focused on SLA compliance and data freshness monitoring
- Automation Agent: Handles GitHub, Slack, and workflow integrations

## Local development

To make changes to this MCP locally and run it:

1. Clone this repository:
   ```bash
   git clone https://github.com/quantium/gcp-cost-optimization-mcp.git
   cd gcp-cost-optimization-mcp
   ```

2. Install dependencies:
   ```bash
   pip install -e .
   ```

3. Configure GCP credentials:
  - Ensure you have GCP credentials configured with `gcloud auth application-default login`
  - You can also set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to a service account key

4. Run the server:
   ```bash
    python -m quantium.gcp_cost_optimization_mcp.server
   ```

5. To use this MCP server with AI clients, add the following to your MCP configuration:
```json
{
  "mcpServers": {
    "gcp-cost-optimization": {
      "command": "python",
      "args": ["-m", "quantium.gcp_cost_optimization_mcp.server"],
      "env": {
        "GCP_PROJECT_ID": "your-gcp-project",
        "GCP_REGION": "us-central1"
      },
      "disabled": false,
      "autoApprove": []
    }
  }
}
```

## Environment variables
By default, the server uses Application Default Credentials and the default GCP project. However, the server can be configured through environment variables in the MCP configuration:

- `GCP_PROJECT_ID`: GCP project ID to use for cost analysis
- `GCP_REGION`: GCP region for regional resources (default: `us-central1`)
- `GOOGLE_APPLICATION_CREDENTIALS`: Path to service account key file (alternative to ADC)
- `CLAUDE_API_KEY`: Anthropic API key for AI-powered query optimization (optional)
- `GITHUB_TOKEN`: GitHub personal access token for PR automation (optional)
- `SLACK_WEBHOOK_URL`: Slack webhook URL for cost notifications (optional)
- `ENABLE_MULTI_AGENT`: Enable multi-agent architecture (default: false)
- `COST_ALERT_THRESHOLD`: Daily cost threshold for alerts in USD (default: 1000)
- `OPTIMIZATION_MIN_SAVINGS`: Minimum savings threshold for creating PRs in USD (default: 50)
- `MCP_LOG_LEVEL`: Logging level (ERROR, WARNING, INFO, DEBUG)

## Available resources
The server provides the following resources:

### Cost resources
- `cost://daily/{date}`: Daily cost breakdown for a specific date in JSON format
- `cost://monthly/{month}`: Monthly cost summary with trends and comparisons
- `cost://project/{project_id}`: Project-level cost analysis and attribution

### Query resources
- `query://expensive/{days}`: List of most expensive queries over the last N days
- `query://patterns/{hash}`: Analysis of similar query patterns and optimization opportunities
- `query://optimization/{query_id}`: Detailed optimization recommendations for a specific query

### Agent resources
- `agent://insights/{agent_name}`: Latest insights and recommendations from a specific agent
- `agent://status/all`: Status and health of all active agents
- `agent://reports/{report_type}`: Generated reports from agent analysis

## Available tools
The server exposes cost optimization capabilities as tools:

### get_bigquery_costs
Retrieves comprehensive BigQuery cost analysis for specified time periods.
This tool provides detailed cost breakdowns by project, dataset, user, and query type.
Use this tool to understand spending patterns and identify cost optimization opportunities.

**Parameters:**
- `days` (default: 7): Number of days to analyze (1-90)
- `project_id` (optional): Specific GCP project ID (uses default if not specified)
- `include_predictions` (default: true): Include ML-based cost forecasting
- `group_by` (default: ["date"]): Grouping dimensions (date, user, dataset, query_type)
- `include_query_details` (default: false): Include individual query cost breakdowns

### analyze_query_cost
Analyzes the cost impact of a SQL query before execution using BigQuery's dry-run API.
This tool predicts query costs, identifies optimization opportunities, and provides
AI-powered suggestions for improving query performance and reducing costs.

**Parameters:**
- `sql` (required): SQL query to analyze
- `project_id` (optional): GCP project ID for query analysis
- `include_optimization` (default: true): Include AI-powered optimization suggestions
- `optimization_model` (default: "claude"): AI model to use for optimization ("claude", "gpt-4")
- `create_pr_if_savings` (default: false): Automatically create GitHub PR if savings > threshold

### detect_cost_anomalies
Identifies unusual cost patterns and spending spikes using machine learning models.
This tool analyzes historical cost data to detect anomalies, predict budget overruns,
and provide early warning alerts for unexpected cost increases.

**Parameters:**
- `days` (default: 30): Historical period to analyze for anomaly detection
- `sensitivity` (default: "medium"): Anomaly detection sensitivity (low, medium, high)
- `project_id` (optional): Specific project to analyze (analyzes all accessible projects if not specified)
- `alert_threshold` (default: 0.25): Percentage increase threshold for alerts (0.25 = 25%)
- `send_slack_alert` (default: false): Send Slack notification for detected anomalies

### optimize_query
Provides AI-powered query optimization using Large Language Models.
This tool takes a SQL query and returns an optimized version with detailed explanations
of changes made and estimated cost savings. Supports integration with dbt workflows.

**Parameters:**
- `sql` (required): SQL query to optimize
- `optimization_goals` (default: ["cost", "performance"]): Optimization objectives
- `preserve_results` (default: true): Ensure optimized query returns identical results
- `include_explanation` (default: true): Include detailed explanation of optimizations
- `target_savings_pct` (default: 30): Target cost reduction percentage
- `dbt_model_path` (optional): Path to dbt model file for context

### create_optimization_pr
Creates a GitHub pull request with query optimizations and cost reduction implementations.
This tool automatically generates optimized SQL, creates appropriate file changes,
and submits a PR with detailed cost analysis and testing instructions.

**Parameters:**
- `optimization_id` (required): ID from previous optimization analysis
- `repository` (default: "data-platform"): GitHub repository name
- `base_branch` (default: "main"): Base branch for the pull request
- `title_prefix` (default: "🚀 Cost Optimization"): PR title prefix
- `assign_reviewers` (default: true): Automatically assign relevant reviewers
- `include_tests` (default: true): Generate validation tests for optimizations

### send_cost_alert
Sends intelligent cost alerts to Slack with rich context and remediation suggestions.
This tool formats cost anomalies, optimization opportunities, and budget warnings
into actionable Slack messages with appropriate urgency and stakeholder targeting.

**Parameters:**
- `alert_type` (required): Type of alert (anomaly, budget_warning, optimization_opportunity)
- `cost_data` (required): Cost analysis data to include in alert
- `severity` (default: "medium"): Alert severity level (low, medium, high, critical)
- `channel` (default: "#data-ops-alerts"): Slack channel for notification
- `mention_users` (default: []): List of users to mention in the alert
- `include_remediation` (default: true): Include suggested remediation steps

### get_dbt_model_costs
Analyzes costs associated with dbt models and provides optimization recommendations.
This tool examines dbt model execution costs, dependency impacts, and suggests
materialization strategy improvements for cost efficiency.

**Parameters:**
- `model_path` (optional): Specific dbt model to analyze (analyzes all models if not specified)
- `include_dependencies` (default: true): Include downstream model impact analysis
- `materialization_analysis` (default: true): Analyze materialization strategy costs
- `days` (default: 7): Historical period for cost analysis
- `suggest_optimizations` (default: true): Include optimization recommendations

### monitor_sla_compliance
Monitors data pipeline SLA compliance and correlates with cost efficiency metrics.
This tool tracks query latency, data freshness, and pipeline success rates while
analyzing the cost-performance trade-offs of different optimization strategies.

**Parameters:**
- `sla_type` (default: "all"): SLA category to monitor (latency, freshness, success_rate, all)
- `time_window` (default: "24h"): Time window for SLA analysis
- `include_cost_correlation` (default: true): Correlate SLA metrics with cost data
- `alert_on_breach` (default: false): Send alert for SLA violations
- `optimization_suggestions` (default: true): Include cost-aware optimization suggestions

### forecast_costs
Provides ML-powered cost forecasting and budget planning recommendations.
This tool uses historical data patterns, seasonality analysis, and growth trends
to predict future costs and suggest budget allocations and cost controls.

**Parameters:**
- `forecast_days` (default: 30): Number of days to forecast
- `include_confidence_intervals` (default: true): Include prediction confidence ranges
- `breakdown_by` (default: ["service"]): Forecast breakdown dimensions
- `scenario_analysis` (default: false): Include optimistic/pessimistic scenarios
- `budget_recommendations` (default: true): Include budget planning suggestions
