### Permissions
**GCP IAM Permissions**
```json
{
  "description": "GCP IAM Policy for Cost Optimization MCP Server",
  "version": "1.0",
  "phases": {
    "phase_1_minimum": {
      "description": "Minimum permissions for basic cost analysis",
      "required_roles": [
        "roles/bigquery.jobUser",
        "roles/bigquery.dataViewer", 
        "roles/billing.viewer"
      ],
      "custom_permissions": [
        "bigquery.jobs.list",
        "bigquery.jobs.get",
        "bigquery.jobs.create",
        "bigquery.datasets.get",
        "bigquery.tables.get",
        "bigquery.tables.list",
        "billing.accounts.get",
        "billing.budgets.get",
        "billing.budgets.list"
      ]
    },
    "phase_2_intelligence": {
      "description": "Additional permissions for AI optimization and automation",
      "additional_roles": [
        "roles/logging.viewer",
        "roles/monitoring.viewer"
      ],
      "additional_permissions": [
        "logging.logEntries.list",
        "logging.logs.list",
        "monitoring.metricDescriptors.list",
        "monitoring.timeSeries.list",
        "bigquery.routines.get",
        "bigquery.routines.list"
      ]
    },
    "phase_3_agents": {
      "description": "Full permissions for multi-agent deployment",
      "additional_roles": [
        "roles/aiplatform.user",
        "roles/composer.user"
      ],
      "additional_permissions": [
        "aiplatform.pipelines.list",
        "aiplatform.pipelines.get",
        "aiplatform.jobs.list",
        "aiplatform.jobs.get",
        "composer.environments.list",
        "composer.dags.list",
        "composer.dags.get"
      ]
    }
  },
  "complete_iam_policy": {
    "title": "Complete IAM Policy for Service Account",
    "bindings": [
      {
        "role": "projects/YOUR_PROJECT_ID/roles/costOptimizationMCPServer",
        "members": [
          "serviceAccount:cost-optimization-mcp@YOUR_PROJECT_ID.iam.gserviceaccount.com"
        ]
      }
    ]
  },
  "custom_role_definition": {
    "title": "Cost Optimization MCP Server",
    "description": "Custom role for GCP Cost Optimization MCP Server",
    "stage": "GA",
    "includedPermissions": [
      "bigquery.jobs.list",
      "bigquery.jobs.get", 
      "bigquery.jobs.create",
      "bigquery.datasets.get",
      "bigquery.datasets.list",
      "bigquery.tables.get",
      "bigquery.tables.list",
      "bigquery.tables.getData",
      "bigquery.routines.get",
      "bigquery.routines.list",
      "billing.accounts.get",
      "billing.budgets.get",
      "billing.budgets.list",
      "logging.logEntries.list",
      "logging.logs.list",
      "monitoring.metricDescriptors.list",
      "monitoring.timeSeries.list",
      "aiplatform.pipelines.list",
      "aiplatform.pipelines.get",
      "aiplatform.jobs.list",
      "aiplatform.jobs.get",
      "composer.environments.list",
      "composer.dags.list",
      "composer.dags.get",
      "resourcemanager.projects.get"
    ]
  },
  "gcloud_commands": {
    "create_service_account": "gcloud iam service-accounts create cost-optimization-mcp --display-name=\"Cost Optimization MCP Server\" --description=\"Service account for GCP cost optimization MCP server\"",
    "create_custom_role": "gcloud iam roles create costOptimizationMCPServer --project=YOUR_PROJECT_ID --title=\"Cost Optimization MCP Server\" --description=\"Custom role for cost optimization MCP server\" --permissions=\"bigquery.jobs.list,bigquery.jobs.get,bigquery.jobs.create,bigquery.datasets.get,bigquery.datasets.list,bigquery.tables.get,bigquery.tables.list,bigquery.tables.getData,bigquery.routines.get,bigquery.routines.list,billing.accounts.get,billing.budgets.get,billing.budgets.list,logging.logEntries.list,logging.logs.list,monitoring.metricDescriptors.list,monitoring.timeSeries.list,aiplatform.pipelines.list,aiplatform.pipelines.get,aiplatform.jobs.list,aiplatform.jobs.get,composer.environments.list,composer.dags.list,composer.dags.get,resourcemanager.projects.get\"",
    "bind_role_to_service_account": "gcloud projects add-iam-policy-binding YOUR_PROJECT_ID --member=\"serviceAccount:cost-optimization-mcp@YOUR_PROJECT_ID.iam.gserviceaccount.com\" --role=\"projects/YOUR_PROJECT_ID/roles/costOptimizationMCPServer\"",
    "create_key": "gcloud iam service-accounts keys create ~/cost-optimization-mcp-key.json --iam-account=cost-optimization-mcp@YOUR_PROJECT_ID.iam.gserviceaccount.com"
  }
}
```
