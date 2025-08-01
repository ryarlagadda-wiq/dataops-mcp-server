## 🎛️ MCP Server Configuration Arguments

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
