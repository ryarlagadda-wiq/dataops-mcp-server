# 🔐 Configuration Guide

### Environment variables
To allow the MCP server to access GCP Logs, configure Application Default Credentials (ADC). If running outside GCP, you'll need a service account key and set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable. More on this in the [GCP auth guide](https://cloud.google.com/docs/authentication/getting-started).
```bash
# Core configuration
export GCP_PROJECT_ID="your-gcp-project"
export GCP_REGION="us-central1" 
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

### Integration settings
```bash
export GITHUB_TOKEN="ghp_your_token"          # For PR automation
export SLACK_WEBHOOK_URL="https://hooks.slack.com/..."  # For notifications
export CLAUDE_API_KEY="sk-ant-your-key"      # For AI optimizations
```

### Optimization settings
```bash
export COST_ALERT_THRESHOLD="1000"           # Daily cost alert threshold in USD
export OPTIMIZATION_MIN_SAVINGS="50"         # Minimum savings for PR creation in USD
```

### Agent configuration settings
```bash
export ENABLE_MULTI_AGENT="true"
```

### Performance tuning settings
```bash
export CACHE_TTL_SECONDS="300"               # Cache timeout for expensive operations
export MAX_CONCURRENT_QUERIES="5"            # Limit concurrent BigQuery operations
export ENABLE_DETAILED_LOGGING="false"       # Enable debug logging
```
