# 🔐 GCP Configuration Guide

To allow the MCP server to access GCP Logs, configure Application Default Credentials (ADC). If running outside GCP, you'll need a service account key and set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable. More on this in the [GCP auth guide](https://cloud.google.com/docs/authentication/getting-started).

1. **Environment variables**:
   ```bash
   export GCP_REGION="us-central1"
   ```




You can set up your GCP credentials using the gcloud CLI:

```bash
gcloud init
```
