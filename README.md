# GCP Cost Optimization MCP Server

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

# Available Toolsets

The following sets of tools are available (all are on by deafult):

<!-- START AUTOMATED TOOLSETS -->
Toolset | Description
---|---
`get_bigquery_costs` | Retrieve BigQuery cost breakdowns
`analyze_query_cost` | Predict query cost before execution
`detect_cost_anomalies` | Detect unusual spending using ML
`optimize_query` | Use LLMs to auto-optimize SQL queries
`create_optimization_pr` | Auto-generate GitHub PRs
`send_cost_alert` | Send cost alerts to Slack
`get_dbt_model_costs` | Analyze dbt model costs
`monitor_sla_compliance` | Track SLA compliance and cost-performance trade-offs
`forecast_costs` | Forecast future costs
`slack_post_message` | Post message to Slack

<!--END AUTOMATED TOOLSETS -->
---

## Tools


<!-- START AUTOMATED TOOLS -->
<details>

<summary>Actions</summary>

### Tool: `get_bigquery_costs`
Retrieve comprehensive BigQuery cost analysis for specified time periods.

| Parameters | Type     | Description |
|------------|----------|-------------|
| `days` | number (default: 7) | Number of days to analyze (1–90) |
| `project_id` | string (optional) | Specific GCP project ID |
| `include_predictions` | boolean (default: true) | Include ML-based cost forecasting |
| `group_by` | array (default: ["date"]) | Grouping dimensions (date, user, dataset, query_type) |
| `include_query_details` | boolean (default: false) | Include individual query cost breakdowns |

</details>

<details>

<summary>Actions</summary>

### Tool: `analyze_query_cost`
Predict cost of a SQL query before execution and get optimization suggestions.

| Parameters | Type     | Description |
|------------|----------|-------------|
| `sql` | string (required) | SQL query to analyze |
| `project_id` | string (optional) | GCP project ID |
| `include_optimization` | boolean (default: true) | Include AI-powered optimization suggestions |
| `optimization_model` | string (default: "claude") | Model to use ("claude", "gpt-4") |
| `create_pr_if_savings` | boolean (default: false) | Create GitHub PR if savings exceed threshold |

</details>

<details>

<summary>Actions</summary>

### Tool: `detect_cost_anomalies`
Use ML to detect cost spikes, anomalies, and early signs of overruns.

| Parameters | Type     | Description |
|------------|----------|-------------|
| `days` | number (default: 30) | Historical days to analyze |
| `sensitivity` | string (default: "medium") | Sensitivity (low, medium, high) |
| `project_id` | string (optional) | GCP project ID |
| `alert_threshold` | number (default: 0.25) | Alert threshold (e.g., 0.25 = 25% increase) |
| `send_slack_alert` | boolean (default: false) | Send alert to Slack |

</details>

<details>

<summary>Actions</summary>

### Tool: `optimize_query`
LLM-powered query optimization with cost-saving recommendations.

| Parameters | Type     | Description |
|------------|----------|-------------|
| `sql` | string (required) | SQL query to optimize |
| `optimization_goals` | array (default: ["cost", "performance"]) | Objectives for optimization |
| `preserve_results` | boolean (default: true) | Ensure results are unchanged |
| `include_explanation` | boolean (default: true) | Include explanation of changes |
| `target_savings_pct` | number (default: 30) | Target savings percentage |
| `dbt_model_path` | string (optional) | Path to dbt model for context |

</details>

<details>

<summary>Actions</summary>

### Tool: `create_optimization_pr`
Auto-create GitHub PRs with optimized SQL and validation tests.

| Parameters | Type     | Description |
|------------|----------|-------------|
| `optimization_id` | string (required) | Optimization analysis ID |
| `repository` | string (default: "data-platform") | GitHub repo name |
| `base_branch` | string (default: "main") | Base branch for the PR |
| `title_prefix` | string (default: "🚀 Cost Optimization") | Prefix for PR title |
| `assign_reviewers` | boolean (default: true) | Auto-assign reviewers |
| `include_tests` | boolean (default: true) | Generate validation tests |

</details>

<details>

<summary>Actions</summary>

### Tool: `send_cost_alert`
Send actionable cost alerts to Slack with rich context.

| Parameters | Type     | Description |
|------------|----------|-------------|
| `alert_type` | string (required) | Type of alert (anomaly, budget_warning, optimization_opportunity) |
| `cost_data` | object (required) | Data to include in the alert |
| `severity` | string (default: "medium") | Alert severity level |
| `channel` | string (default: "#data-ops-alerts") | Slack channel for alert |
| `mention_users` | array (default: []) | Users to mention |
| `include_remediation` | boolean (default: true) | Include fix suggestions |

</details>

<details>

<summary>Actions</summary>

### Tool: `get_dbt_model_costs`
Analyze dbt model execution costs and optimization opportunities.

| Parameters | Type     | Description |
|------------|----------|-------------|
| `model_path` | string (optional) | Specific dbt model path |
| `include_dependencies` | boolean (default: true) | Analyze downstream impacts |
| `materialization_analysis` | boolean (default: true) | Suggest materialization strategy improvements |
| `days` | number (default: 7) | Time period for analysis |
| `suggest_optimizations` | boolean (default: true) | Include cost-saving suggestions |

</details>

<details>

<summary>Actions</summary>

### Tool: `monitor_sla_compliance`
Monitor pipeline SLAs and correlate with cost-performance metrics.

| Parameters | Type     | Description |
|------------|----------|-------------|
| `sla_type` | string (default: "all") | SLA type (latency, freshness, success_rate, all) |
| `time_window` | string (default: "24h") | Time window for analysis |
| `include_cost_correlation` | boolean (default: true) | Link SLA with cost data |
| `alert_on_breach` | boolean (default: false) | Send alerts for SLA breaches |
| `optimization_suggestions` | boolean (default: true) | Suggest cost-aware fixes |

</details>

<details>

<summary>Actions</summary>

### Tool: `forecast_costs`
Forecast future GCP spend using ML and scenario modeling.

| Parameters | Type     | Description |
|------------|----------|-------------|
| `forecast_days` | number (default: 30) | Days to forecast |
| `include_confidence_intervals` | boolean (default: true) | Include prediction ranges |
| `breakdown_by` | array (default: ["service"]) | Forecast by (e.g., service, project) |
| `scenario_analysis` | boolean (default: false) | Include optimistic/pessimistic forecasts |
| `budget_recommendations` | boolean (default: true) | Suggest budget allocations |

</details>

<details>

<summary>Actions</summary>

### Tool: `slack_post_message`
Post a message to a Slack channel.

| Parameters | Type     | Description |
|------------|----------|-------------|
| `channel_id` | string | ID of the Slack channel |
| `text` | string | Message text to post |

</details>
