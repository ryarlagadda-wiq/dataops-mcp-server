"""Configuration settings for the application."""

from typing import List, Optional, Dict
from pydantic_settings import BaseSettings
from pydantic import Field, BaseModel
from version import __version__ as MCP_VERSION

class ProjectSettings(BaseModel):
    id: str = Field(..., description="GCP Project ID")
    region: str = Field(..., description="GCP Region")
    billing_account: Optional[str] = Field(None, description="GCP Billing Account ID")

class ThresholdSettings(BaseModel):
    daily_cost_alert: float = 1000.0
    query_cost_warning: float = 50.0
    anomaly_sensitivity: str = "medium"  # low, medium, high

class OptimizationSettings(BaseModel):
    min_savings_for_pr: float = 100.0
    auto_optimize_threshold: float = 25.0
    max_risk_level: str = "medium"  # low, medium, high

class GithubIntegration(BaseModel):
    repository: str = "quantium/data-platform"
    default_reviewers: List[str] = ["data-engineering-team"]

class SlackIntegration(BaseModel):
    default_channel: str = "#data-ops-alerts"
    critical_channel: str = "#data-ops-critical"

class IntegrationsSettings(BaseModel):
    github: Optional[GithubIntegration] = None
    slack: Optional[SlackIntegration] = None

class CostGuardAgent(BaseModel):
    enabled: bool = True
    monitoring_interval: int = 300
    auto_alert: bool = True

class QueryOptimizerAgent(BaseModel):
    enabled: bool = True
    auto_optimize: bool = False
    ai_model: str = "claude-3-sonnet"

class SlaSentinelAgent(BaseModel):
    enabled: bool = True
    sla_threshold: float = 0.95

class AgentsSettings(BaseModel):
    cost_guard: Optional[CostGuardAgent] = None
    query_optimizer: Optional[QueryOptimizerAgent] = None
    sla_sentinel: Optional[SlaSentinelAgent] = None

class Settings(BaseSettings):
    """Application settings loaded from environment variables or .env file."""

    # Core configuration
    project: ProjectSettings
    GOOGLE_APPLICATION_CREDENTIALS: str

    # Integrations
    integrations: Optional[IntegrationsSettings] = None
    GITHUB_TOKEN: Optional[str] = None
    SLACK_WEBHOOK_URL: Optional[str] = None
    CLAUDE_API_KEY: Optional[str] = None

    # Thresholds and Optimization
    thresholds: ThresholdSettings = ThresholdSettings()
    optimization: OptimizationSettings = OptimizationSettings()

    # Agents
    agents: Optional[AgentsSettings] = None
    ENABLE_MULTI_AGENT: bool = True

    # Performance tuning
    CACHE_TTL_SECONDS: int = 300
    MAX_CONCURRENT_QUERIES: int = 5
    ENABLE_DETAILED_LOGGING: bool = False

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "allow"
        case_sensitive = False

