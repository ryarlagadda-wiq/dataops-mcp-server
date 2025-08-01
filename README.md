# GCP Cost Optimization Multi-Agent MCP Server

## 🚀 Installation

### Prerequisites
* The uv Python package and project manager from [Astral](https://docs.astral.sh/uv/getting-started/installation/) or the [GitHub README](https://github.com/astral-sh/uv#installation)
* A GCP account
* Configured [GCP credentials](https://github.com/rohithay/cloud-dataops-agent/blob/main/docs/gcp-config.md)
* Install [gcloud CLI](https://cloud.google.com/sdk/docs/install-sdk) for authentication

**GCP Client Requirements**
1. **Credentials**: Configure GCP credentials via gcloud CLI or environment variables
2. **Permissions**: Ensure your GCP credentials have the required permissions (see [Permissions](https://github.com/rohithay/cloud-dataops-agent/blob/main/docs/permissions.md))
* Optional: GitHub token for PR automation, Slack webhook for notifications

## Local development

To make changes to this MCP locally and run it:
   ```bash
   # Clone this repository
   git clone https://github.com/rohithay/dataops-mcp-server.git
   cd dataops-mcp-server

   # Create a virtual environment and install dependencies
   uv sync
   source .venv/bin/activate # On Windows, use `.venv\Scripts\activate`
   ```

## 🚦 Quick Start
1. Make sure to have configured your GCP credentials as [described here](https://github.com/rohithay/cloud-dataops-agent/blob/main/docs/gcp-config.md)
2. Update your `claude_desktop_config.json` file with proper configuration outlined in the [AI integration guide](https://github.com/rohithay/cloud-dataops-agent/blob/main/docs/ai-integration.md)
3. Open Claude for Desktop and start prompting!
For more examples and advanced usage, see the [detailed usage guide](https://github.com/rohithay/cloud-dataops-agent/blob/main/docs/usage.md).

## 🤖 AI Integration
To get started using this MCP server with your AI assistants offering MCP support, like Claude Desktop, Cursor. Refer to this [Integration guide](https://github.com/rohithay/cloud-dataops-agent/blob/main/docs/ai-integration.md).

## Tools
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
