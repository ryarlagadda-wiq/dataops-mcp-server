# Detailed Usage Guide

## 🌐 MCP Clients

AI assistants can leverage this MCP server. To understand more check out the [AI Integration Guide](./ai-integration.md)

## 🖥️ Standalone Server

The MCP server exposes GCP project data and analysis tools to AI assistants and MCP clients:

```bash
python -m quantium.gcp_cost_optimization_mcp.server [--profile your-profile] [--region australia-south-east-1]
```

The server runs in the foreground by default. To run it in the background, you can use:

```bash
python quantium.gcp_cost_optimization_mcp.server &
```

# GCP Cost Optimization MCP Usage Guide

## 📟 CLI Client (one off usage)

The project includes a command-line client for interacting with the MCP server:

```bash
# Get current BigQuery costs
python src/client.py get-costs [--days 7] [--project your-project] [--region us-central1]

# Get costs with forecasting
python src/client.py get-costs --days 30 --include-predictions [--project your-project]

# Analyze a specific query for cost impact
python src/client.py analyze-query "SELECT * FROM dataset.table" [--project your-project]

# Find the most expensive queries
python src/client.py expensive-queries [--days 7] [--min-cost 10.0] [--limit 20] [--project your-project]

# Detect cost anomalies with sensitivity control
python src/client.py detect-anomalies [--days 30] [--sensitivity medium] [--project your-project]

# Get AI-powered query optimization
python src/client.py optimize-query "SELECT user_id, COUNT(*) FROM events GROUP BY user_id" [--project your-project]

# Generate cost optimization opportunities
python src/client.py optimization-opportunities [--min-savings 100] [--project your-project]

# Analyze dbt model costs
python src/client.py dbt-costs [--model-path "models/marts/customer_analytics"] [--days 7] [--project your-project]

# Monitor SLA compliance with cost correlation
python src/client.py sla-compliance [--sla-type all] [--time-window 24h] [--project your-project]

# Generate executive cost report
python src/client.py cost-report [--period monthly] [--format pdf] [--project your-project]

# Forecast future costs with scenarios
python src/client.py forecast-costs [--days 30] [--include-scenarios] [--project your-project]

# Create optimization GitHub PR
python src/client.py create-pr --optimization-id "opt-12345" [--repository data-platform] [--project your-project]

# Send cost alert to Slack
python src/client.py send-alert --type budget_warning --cost-data costs.json [--channel "#data-ops"] [--project your-project]
```

*You can use --project and --region with any command to target a specific GCP project or region.*

## 🧩 Example Workflows

### Investigating a BigQuery cost spike using the standalone server directly

```bash
# 1. Get recent cost breakdown to identify the spike
python src/client.py get-costs --days 7 --include-predictions [--project your-project]

# 2. Find the most expensive queries during the spike period
python src/client.py expensive-queries --days 3 --min-cost 50.0 [--project your-project]

# 3. Analyze the top expensive query for optimization opportunities
python src/client.py analyze-query "SELECT * FROM analytics.user_events WHERE date > '2024-01-01'" [--project your-project]

# 4. Get AI-powered optimization suggestions
python src/client.py optimize-query "SELECT * FROM analytics.user_events WHERE date > '2024-01-01'" [--project your-project]

# 5. Create a GitHub PR with the optimization
python src/client.py create-pr --optimization-id "opt-67890" --repository "data-platform" [--project your-project]
```

### Proactive cost monitoring and anomaly detection

```bash
# 1. Set up continuous anomaly detection
python src/client.py detect-anomalies --days 30 --sensitivity high [--project your-project]

# 2. Get cost forecast to predict budget burn
python src/client.py forecast-costs --days 30 --include-scenarios [--project your-project]

# 3. Monitor SLA compliance vs cost efficiency
python src/client.py sla-compliance --sla-type all --include-cost-correlation [--project your-project]

# 4. Send proactive alert if anomalies detected
python src/client.py send-alert --type anomaly --cost-data anomalies.json --severity high [--project your-project]
```

### Optimizing dbt model costs across the data platform

```bash
# 1. Analyze all dbt model costs
python src/client.py dbt-costs --include-dependencies [--project your-project]

# 2. Find models with high cost and optimization potential
python src/client.py optimization-opportunities --min-savings 200 --focus dbt-models [--project your-project]

# 3. Optimize specific expensive dbt model
python src/client.py optimize-query --dbt-model "models/marts/customer_lifetime_value.sql" [--project your-project]

# 4. Generate executive summary of optimization impact
python src/client.py cost-report --period monthly --include-optimizations [--project your-project]
```

### Multi-agent deployment and monitoring

```bash
# Deploy specialized cost monitoring agents
python src/client.py deploy-agent --type cost_guard --interval 300 [--project your-project]
python src/client.py deploy-agent --type query_optimizer --auto-optimize false [--project your-project]
python src/client.py deploy-agent --type sla_sentinel --alert-threshold 0.95 [--project your-project]

# Get insights from all deployed agents
python src/client.py agent-insights --priority high [--project your-project]

# Monitor agent health and performance
python src/client.py agent-status --include-metrics [--project your-project]
```

## 🔗 Resource URIs

The MCP server exposes GCP cost and performance data through the following resource URIs:

| Resource URI | Description |
|--------------|-------------|
| `gcp://costs/daily` | List daily cost breakdowns for the current project |
| `gcp://costs/daily/{date}` | Get cost breakdown for a specific date (YYYY-MM-DD) |
| `gcp://costs/monthly/{month}` | Get monthly cost summary and trends (YYYY-MM) |
| `gcp://costs/project/{project_id}` | Get project-specific cost analysis and attribution |
| `gcp://queries/expensive/{days}` | List most expensive queries over the last N days |
| `gcp://queries/patterns/{pattern_hash}` | Get analysis of similar query patterns and optimization opportunities |
| `gcp://queries/optimization/{query_id}` | Get detailed optimization recommendations for a specific query |
| `gcp://dbt/models/costs` | Get cost analysis for all dbt models |
| `gcp://dbt/models/{model_name}/performance` | Get performance metrics for a specific dbt model |
| `gcp://anomalies/recent` | Get recently detected cost anomalies and alerts |
| `gcp://anomalies/{anomaly_id}` | Get detailed analysis of a specific cost anomaly |
| `gcp://forecasts/costs/{days}` | Get cost forecasts for the next N days |
| `gcp://sla/compliance/{service}` | Get SLA compliance metrics for specific services |
| `gcp://agents/insights/{agent_type}` | Get latest insights from a specific agent type |
| `gcp://agents/status/all` | Get status and health of all deployed agents |
| `gcp://optimizations/opportunities` | Get current optimization opportunities with ROI analysis |
| `gcp://reports/executive/{period}` | Get executive cost reports (daily, weekly, monthly) |

## 🧰 Tool Handlers

The server provides the following tool handlers for AI assistants:

| Tool | Description |
|------|-------------|
| `get_bigquery_costs` | Retrieve comprehensive BigQuery cost analysis with trends and forecasting |
| `analyze_query_cost` | Predict query costs before execution using dry-run API with optimization suggestions |
| `detect_cost_anomalies` | Identify unusual spending patterns using ML-based anomaly detection |
| `optimize_query` | Generate AI-powered query optimizations with cost and performance improvements |
| `find_expensive_queries` | Locate high-cost queries with optimization priority scoring |
| `create_optimization_pr` | Automatically create GitHub PRs with optimization implementations |
| `send_cost_alert` | Send intelligent Slack notifications for cost issues with context and remediation |
| `get_dbt_model_costs` | Analyze dbt model execution costs and materialization strategy optimization |
| `monitor_sla_compliance` | Track data pipeline SLA metrics correlated with cost efficiency |
| `forecast_costs` | Provide ML-powered cost forecasting and budget planning recommendations |
| `deploy_cost_agent` | Deploy specialized AI agents for continuous cost monitoring and optimization |
| `get_agent_insights` | Retrieve prioritized recommendations from deployed agents with cross-correlation |
| `generate_cost_report` | Create executive-level cost analysis reports with optimization roadmaps |
| `get_optimization_opportunities` | Identify and prioritize cost optimization opportunities across the platform |
| `validate_optimization` | Test and validate optimization implementations before production deployment |

## 🎯 Advanced Workflows

### Weekly cost optimization routine

```bash
# Monday: Analyze previous week's costs and detect anomalies
python src/client.py get-costs --days 7 --include-predictions
python src/client.py detect-anomalies --days 7 --sensitivity medium

# Tuesday: Find and optimize expensive queries
python src/client.py expensive-queries --days 7 --min-cost 25.0
python src/client.py optimization-opportunities --min-savings 100

# Wednesday: Review dbt model efficiency
python src/client.py dbt-costs --include-dependencies
python src/client.py sla-compliance --include-cost-correlation

# Thursday: Create optimization PRs for high-impact changes
python src/client.py create-pr --optimization-id "opt-weekly-batch" --repository "data-platform"

# Friday: Generate executive summary and forecast
python src/client.py cost-report --period weekly --include-optimizations
python src/client.py forecast-costs --days 30 --include-scenarios
```

### Emergency cost spike investigation

```bash
# 1. Immediate cost spike analysis
python src/client.py detect-anomalies --days 1 --sensitivity high --alert-threshold 0.50

# 2. Identify the culprit queries
python src/client.py expensive-queries --days 1 --min-cost 100.0 --limit 10

# 3. Get emergency optimization suggestions
python src/client.py optimize-query "SELECT * FROM large_table WHERE expensive_condition" --emergency-mode

# 4. Send critical alert to on-call team
python src/client.py send-alert --type emergency --severity critical --channel "#data-ops-critical"

# 5. Generate incident report with root cause analysis
python src/client.py cost-report --period incident --include-root-cause
```

### Multi-project cost governance

```bash
# Analyze costs across multiple projects
python src/client.py get-costs --project project-prod --days 30
python src/client.py get-costs --project project-staging --days 30
python src/client.py get-costs --project project-dev --days 30

# Cross-project anomaly detection
python src/client.py detect-anomalies --project project-prod --cross-project-analysis
python src/client.py detect-anomalies --project project-staging --cross-project-analysis

# Generate consolidated governance report
python src/client.py cost-report --period monthly --multi-project --governance-focus
```

### AI Agent management and insights

```bash
# Deploy comprehensive agent monitoring
python src/client.py deploy-agent --type cost_guard --monitoring-interval 300
python src/client.py deploy-agent --type query_optimizer --auto-optimize false
python src/client.py deploy-agent --type sla_sentinel --alert-threshold 0.95

# Get consolidated agent insights
python src/client.py agent-insights --priority high --correlation-analysis

# Monitor agent performance and effectiveness
python src/client.py agent-status --include-roi-metrics --optimization-success-rate
```

## 🎯 Real-World Usage Examples

### Example 1: Daily cost monitoring conversation with Claude

```
Human: "Check our BigQuery costs for this week and let me know if anything looks unusual"

Claude (via MCP): 
Using get_bigquery_costs...
Using detect_cost_anomalies...

"📊 This week's BigQuery costs: $1,247.83 (↑23% vs last week)

🚨 Anomaly detected: Tuesday had a 67% cost spike ($347.23 vs $208.40 expected)

Top contributors to the spike:
• user_engagement_analysis query: $89.45 (usually $32.10)
• daily_revenue_report: $67.22 (processed 3x more data than usual)

Would you like me to optimize these queries and create PRs with the fixes?"
```

### Example 2: Query optimization workflow

```
Human: "I need to run this query in production. Is it safe?"
[Shares expensive SQL query]

Claude (via MCP):
Using analyze_query_cost...
Using optimize_query...

"⚠️ Cost Alert: This query will cost ~$127 and process 2.3TB of data.

🎯 Optimized version reduces cost to $23 (82% savings):
• Added partition filters: _PARTITIONTIME >= '2024-07-01'  
• Replaced SELECT * with specific columns
• Added LIMIT 10000 to prevent runaway results

Shall I create a GitHub PR with this optimization?"
