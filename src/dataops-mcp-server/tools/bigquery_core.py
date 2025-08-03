#!/usr/bin/env python3
"""
BigQuery Core MCP Server

Phase 1 implementation providing essential BigQuery operations using shared components.
Focused on cost analysis, usage monitoring, and basic health checks.

Features:
- Health checks and connectivity testing
- Daily cost analysis with detailed breakdowns
- Top users analysis by cost and usage
- Comprehensive cost summaries with insights and projections
- Error handling with informative responses
- Standardized JSON responses
"""

import os
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional
from functools import wraps

# Add the parent directory to the path to access shared modules
current_dir = Path(__file__).parent
parent_dir = current_dir.parent
sys.path.insert(0, str(parent_dir))

from fastmcp import FastMCP
from bigquery_client import UnifiedBigQueryClient, QueryConfig, CostMetrics, create_standard_response
from logger import get_bigquery_logger, log_bigquery_operation, PerformanceLogger


# Error Handling Decorator
def handle_bigquery_errors(func):
    """Decorator for consistent error handling across all tools."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ValueError as e:
            return create_standard_response(
                success=False,
                data={},
                project_id=project_id,
                error=f"Validation Error: {str(e)}",
                suggestions=["Check input parameters", "Ensure values are within valid ranges"]
            )
        except Exception as e:
            return create_standard_response(
                success=False,
                data={},
                project_id=project_id,
                error=f"Unexpected Error: {str(e)}",
                suggestions=[
                    "Check GOOGLE_APPLICATION_CREDENTIALS environment variable",
                    "Verify BigQuery API is enabled for the project",
                    "Ensure IAM permissions: bigquery.jobs.listAll, bigquery.jobs.create"
                ]
            )
    return wrapper


# Initialize logging
logger = get_bigquery_logger()
perf_logger = PerformanceLogger()

# Initialize MCP server
mcp = FastMCP("BigQuery Core Analysis")

# Initialize BigQuery client with validation
project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
if not project_id:
    raise ValueError("GOOGLE_CLOUD_PROJECT environment variable must be set")

try:
    bq_client = UnifiedBigQueryClient(project_id=project_id)
    logger.info(f"Successfully initialized BigQuery client for project: {project_id}")
except Exception as e:
    logger.error(f"Failed to initialize BigQuery client: {e}")
    raise RuntimeError(f"Failed to initialize BigQuery client: {e}")


@mcp.tool()
@handle_bigquery_errors
@log_bigquery_operation("health_check")
def health_check() -> str:
    """
    Check BigQuery connectivity and permissions with comprehensive diagnostics.
    
    Performs multiple validation checks:
    - Basic BigQuery API connectivity 
    - INFORMATION_SCHEMA access permissions
    - Project accessibility
    - Service account validation
    
    Returns:
        JSON string with detailed health status and diagnostic information
    """
    health_result = bq_client.health_check()
    
    # Add additional diagnostics for better troubleshooting
    diagnostics = {
        "project_id": project_id,
        "timestamp": datetime.now().isoformat(),
        "checks_performed": [
            "basic_connectivity",
            "information_schema_access", 
            "project_permissions",
            "service_account_validation"
        ]
    }
    
    # Merge health check result with diagnostics
    health_result.update(diagnostics)
    
    return create_standard_response(
        success=health_result.get("success", False),
        data=health_result,
        project_id=project_id
    )


@mcp.tool()
@handle_bigquery_errors
@log_bigquery_operation("daily_costs")
def get_daily_costs(days: int = 7) -> str:
    """
    Get daily BigQuery costs for the specified period with detailed analytics.
    
    Features:
    - Daily cost breakdown and trends
    - Query volume analysis
    - Performance metrics (avg duration)
    - User activity patterns
    - Cost optimization insights
    
    Args:
        days: Number of days to analyze (1-90, default: 7)
            - 1-7 days: Recent activity analysis
            - 8-30 days: Weekly/monthly trends
            - 31-90 days: Long-term cost patterns
    
    Returns:
        JSON string with comprehensive daily cost breakdown and analytics
    """
    # Enhanced parameter validation
    if not isinstance(days, int):
        raise ValueError("Days parameter must be an integer")
    
    config = QueryConfig(days=days, project_id=project_id)
    query = bq_client.build_daily_costs_query(config)
    results = bq_client.execute_query(query)
    processed_data = bq_client.process_daily_costs_results(results)
    
    # Calculate additional analytics
    daily_costs = processed_data.get("daily_costs", [])
    total_cost = processed_data.get("total_cost", 0)
    
    analytics = {
        "cost_trends": _calculate_cost_trends(daily_costs),
        "peak_usage_day": _find_peak_usage_day(daily_costs),
        "cost_efficiency": _calculate_cost_efficiency(daily_costs),
        "recommendations": _generate_daily_cost_recommendations(daily_costs, days)
    }
    
    response_data = {
        "analysis_period": {
            "days": days,
            "start_date": (datetime.now() - timedelta(days=days)).date().isoformat(),
            "end_date": datetime.now().date().isoformat(),
            "analysis_type": _get_analysis_type(days)
        },
        "summary": {
            "total_cost_usd": round(total_cost, 2),
            "average_daily_cost": round(total_cost / max(len(daily_costs), 1), 2),
            "total_queries": sum(day.get("query_count", 0) for day in daily_costs),
            "active_days": len([d for d in daily_costs if d.get("query_count", 0) > 0])
        },
        "daily_breakdown": daily_costs,
        "analytics": analytics,
        **processed_data
    }
    
    return create_standard_response(
        success=True,
        data=response_data,
        project_id=project_id
    )


def _get_analysis_type(days: int) -> str:
    """Determine analysis type based on time period."""
    if days <= 7:
        return "recent_activity"
    elif days <= 30:
        return "trend_analysis" 
    else:
        return "long_term_pattern"


def _calculate_cost_trends(daily_costs: list) -> Dict[str, Any]:
    """Calculate cost trend analytics."""
    if len(daily_costs) < 2:
        return {"trend": "insufficient_data", "change_pct": 0}
    
    recent_avg = sum(d.get("cost_usd", 0) for d in daily_costs[:3]) / min(3, len(daily_costs))
    older_avg = sum(d.get("cost_usd", 0) for d in daily_costs[-3:]) / min(3, len(daily_costs))
    
    if older_avg > 0:
        change_pct = ((recent_avg - older_avg) / older_avg) * 100
        trend = "increasing" if change_pct > 10 else ("decreasing" if change_pct < -10 else "stable")
    else:
        trend = "stable"
        change_pct = 0
    
    return {
        "trend": trend,
        "change_pct": round(change_pct, 1),
        "recent_avg_daily": round(recent_avg, 2),
        "older_avg_daily": round(older_avg, 2)
    }


def _find_peak_usage_day(daily_costs: list) -> Dict[str, Any]:
    """Find the day with highest cost and query volume."""
    if not daily_costs:
        return {"date": None, "cost_usd": 0, "query_count": 0}
    
    peak_cost_day = max(daily_costs, key=lambda x: x.get("cost_usd", 0))
    peak_query_day = max(daily_costs, key=lambda x: x.get("query_count", 0))
    
    return {
        "highest_cost": {
            "date": peak_cost_day.get("date"),
            "cost_usd": peak_cost_day.get("cost_usd", 0),
            "query_count": peak_cost_day.get("query_count", 0)
        },
        "highest_volume": {
            "date": peak_query_day.get("date"),
            "cost_usd": peak_query_day.get("cost_usd", 0), 
            "query_count": peak_query_day.get("query_count", 0)
        }
    }


def _calculate_cost_efficiency(daily_costs: list) -> Dict[str, float]:
    """Calculate cost efficiency metrics."""
    if not daily_costs:
        return {"avg_cost_per_query": 0, "efficiency_score": 0}
    
    total_cost = sum(d.get("cost_usd", 0) for d in daily_costs)
    total_queries = sum(d.get("query_count", 0) for d in daily_costs)
    
    avg_cost_per_query = total_cost / max(total_queries, 1)
    
    # Efficiency score: lower cost per query = higher efficiency
    efficiency_score = max(0, 100 - (avg_cost_per_query * 1000))  # Scale for readability
    
    return {
        "avg_cost_per_query": round(avg_cost_per_query, 4),
        "efficiency_score": round(efficiency_score, 1)
    }


def _generate_daily_cost_recommendations(daily_costs: list, days: int) -> list:
    """Generate actionable recommendations based on daily cost patterns."""
    recommendations = []
    
    if not daily_costs:
        return ["No data available for recommendations"]
    
    # High cost variability
    costs = [d.get("cost_usd", 0) for d in daily_costs]
    if len(costs) > 1:
        avg_cost = sum(costs) / len(costs)
        max_cost = max(costs)
        if max_cost > avg_cost * 3:
            recommendations.append("High cost variability detected - investigate expensive query patterns")
    
    # Weekend vs weekday analysis (if sufficient data)
    if days >= 7:
        recommendations.append("Consider implementing query scheduling to optimize costs")
    
    # Query volume recommendations
    total_queries = sum(d.get("query_count", 0) for d in daily_costs)
    if total_queries > days * 100:  # More than 100 queries per day average
        recommendations.append("High query volume - consider query optimization and caching")
    
    return recommendations if recommendations else ["Current usage patterns appear optimal"]


@mcp.tool()
@handle_bigquery_errors
@log_bigquery_operation("top_users")
def get_top_users(days: int = 7, limit: int = 10) -> str:
    """
    Get top BigQuery users by cost with comprehensive usage analytics.
    
    Features:
    - Cost ranking and usage patterns
    - User type classification (service accounts vs users)
    - Query efficiency analysis per user
    - Usage behavior insights
    - Cost optimization recommendations per user
    
    Args:
        days: Number of days to analyze (1-90, default: 7)
        limit: Number of top users to return (1-500, default: 10)
    
    Returns:
        JSON string with detailed top users analysis and recommendations
    """
    # Enhanced parameter validation
    if not isinstance(days, int) or not isinstance(limit, int):
        raise ValueError("Days and limit parameters must be integers")
    
    config = QueryConfig(days=days, project_id=project_id, limit=limit)
    query = bq_client.build_top_users_query(config)
    results = bq_client.execute_query(query)
    processed_data = bq_client.process_top_users_results(results)
    
    # Enhanced user analytics
    top_users = processed_data.get("top_users", [])
    enhanced_users = []
    
    for user in top_users:
        enhanced_user = {
            **user,
            "user_type": _classify_user_type(user.get("user_email", "")),
            "efficiency_metrics": _calculate_user_efficiency(user),
            "usage_pattern": _analyze_usage_pattern(user, days),
            "recommendations": _generate_user_recommendations(user)
        }
        enhanced_users.append(enhanced_user)
    
    # Calculate user distribution analytics
    user_analytics = _calculate_user_distribution_analytics(enhanced_users)
    cost_concentration = _calculate_cost_concentration(enhanced_users)
    
    response_data = {
        "analysis_period": {
            "days": days,
            "start_date": (datetime.now() - timedelta(days=days)).date().isoformat(),
            "end_date": datetime.now().date().isoformat()
        },
        "query_params": {
            "limit": limit,
            "users_analyzed": len(enhanced_users)
        },
        "summary": {
            "total_analyzed_cost": round(sum(u.get("cost_usd", 0) for u in enhanced_users), 2),
            "total_queries": sum(u.get("query_count", 0) for u in enhanced_users),
            "unique_users": len(enhanced_users),
            "cost_per_user_avg": round(sum(u.get("cost_usd", 0) for u in enhanced_users) / max(len(enhanced_users), 1), 2)
        },
        "user_analytics": user_analytics,
        "cost_concentration": cost_concentration,
        "top_users": enhanced_users,
        **processed_data
    }
    
    return create_standard_response(
        success=True,
        data=response_data,
        project_id=project_id
    )


def _classify_user_type(email: str) -> Dict[str, Any]:
    """Classify user type and extract insights from email."""
    if not email:
        return {"type": "unknown", "category": "unidentified"}
    
    if "gserviceaccount.com" in email:
        # Extract service name from service account email
        service_name = email.split("@")[0] if "@" in email else "unknown"
        return {
            "type": "service_account",
            "category": "automated",
            "service_name": service_name,
            "is_automation": True
        }
    elif "@" in email and "." in email:
        domain = email.split("@")[1] if "@" in email else "unknown"
        return {
            "type": "user_account", 
            "category": "human",
            "domain": domain,
            "is_automation": False
        }
    else:
        return {"type": "unknown", "category": "unidentified", "is_automation": None}


def _calculate_user_efficiency(user: Dict[str, Any]) -> Dict[str, float]:
    """Calculate efficiency metrics for a user."""
    cost = user.get("cost_usd", 0)
    queries = user.get("query_count", 0)
    duration = user.get("avg_duration_ms", 0)
    
    avg_cost_per_query = cost / max(queries, 1)
    
    # Efficiency scoring (lower cost per query and duration = higher efficiency)
    cost_efficiency = max(0, 100 - (avg_cost_per_query * 1000))
    time_efficiency = max(0, 100 - (duration / 10000))  # Scale duration appropriately
    overall_efficiency = (cost_efficiency + time_efficiency) / 2
    
    return {
        "avg_cost_per_query": round(avg_cost_per_query, 4),
        "cost_efficiency_score": round(cost_efficiency, 1),
        "time_efficiency_score": round(time_efficiency, 1),
        "overall_efficiency_score": round(overall_efficiency, 1)
    }


def _analyze_usage_pattern(user: Dict[str, Any], days: int) -> Dict[str, Any]:
    """Analyze user's usage patterns."""
    cost = user.get("cost_usd", 0)
    queries = user.get("query_count", 0)
    
    daily_query_avg = queries / max(days, 1)
    daily_cost_avg = cost / max(days, 1)
    
    # Classify usage intensity
    if daily_query_avg > 50:
        intensity = "high"
    elif daily_query_avg > 10:
        intensity = "medium"
    else:
        intensity = "low"
    
    return {
        "daily_query_average": round(daily_query_avg, 1),
        "daily_cost_average": round(daily_cost_avg, 2),
        "usage_intensity": intensity,
        "query_frequency": "frequent" if queries > days * 5 else "occasional"
    }


def _generate_user_recommendations(user: Dict[str, Any]) -> list:
    """Generate personalized recommendations for each user."""
    recommendations = []
    
    efficiency = user.get("efficiency_metrics", {})
    cost_per_query = efficiency.get("avg_cost_per_query", 0)
    overall_efficiency = efficiency.get("overall_efficiency_score", 100)
    
    # High cost per query
    if cost_per_query > 1.0:
        recommendations.append("High cost per query - review query optimization opportunities")
    
    # Low efficiency
    if overall_efficiency < 50:
        recommendations.append("Below average efficiency - consider query performance tuning")
    
    # Service account specific recommendations
    user_type = user.get("user_type", {})
    if user_type.get("is_automation"):
        recommendations.append("Automated user - consider query caching and scheduled optimization")
    
    # High usage recommendations
    usage_pattern = user.get("usage_pattern", {})
    if usage_pattern.get("usage_intensity") == "high":
        recommendations.append("High usage detected - implement query result caching")
    
    return recommendations if recommendations else ["Usage patterns appear optimal"]


def _calculate_user_distribution_analytics(users: list) -> Dict[str, Any]:
    """Calculate analytics about user distribution."""
    if not users:
        return {}
    
    service_accounts = [u for u in users if u.get("user_type", {}).get("is_automation")]
    human_users = [u for u in users if u.get("user_type", {}).get("is_automation") == False]
    
    return {
        "user_types": {
            "service_accounts": len(service_accounts),
            "human_users": len(human_users),
            "unidentified": len(users) - len(service_accounts) - len(human_users)
        },
        "cost_by_type": {
            "service_accounts_cost": round(sum(u.get("cost_usd", 0) for u in service_accounts), 2),
            "human_users_cost": round(sum(u.get("cost_usd", 0) for u in human_users), 2)
        },
        "efficiency_distribution": {
            "high_efficiency": len([u for u in users if u.get("efficiency_metrics", {}).get("overall_efficiency_score", 0) >= 75]),
            "medium_efficiency": len([u for u in users if 50 <= u.get("efficiency_metrics", {}).get("overall_efficiency_score", 0) < 75]),
            "low_efficiency": len([u for u in users if u.get("efficiency_metrics", {}).get("overall_efficiency_score", 0) < 50])
        }
    }


def _calculate_cost_concentration(users: list) -> Dict[str, Any]:
    """Calculate cost concentration metrics (how costs are distributed among users)."""
    if not users:
        return {}
    
    costs = [u.get("cost_usd", 0) for u in users]
    total_cost = sum(costs)
    
    if total_cost == 0:
        return {"concentration": "no_cost", "top_user_percentage": 0}
    
    # Calculate what percentage the top user represents
    top_user_cost = max(costs) if costs else 0
    top_user_percentage = (top_user_cost / total_cost) * 100
    
    # Calculate top 3 users percentage
    sorted_costs = sorted(costs, reverse=True)
    top_3_cost = sum(sorted_costs[:3])
    top_3_percentage = (top_3_cost / total_cost) * 100
    
    # Determine concentration level
    if top_user_percentage > 50:
        concentration = "high"
    elif top_user_percentage > 25:
        concentration = "medium"
    else:
        concentration = "low"
    
    return {
        "concentration": concentration,
        "top_user_percentage": round(top_user_percentage, 1),
        "top_3_users_percentage": round(top_3_percentage, 1),
        "cost_distribution": "concentrated" if top_3_percentage > 70 else "distributed"
    }


@mcp.tool()
@handle_bigquery_errors
@log_bigquery_operation("cost_summary")
def get_cost_summary(days: int = 7) -> str:
    """
    Get comprehensive cost summary with advanced insights, projections, and recommendations.
    
    Features:
    - Comprehensive cost analysis and breakdowns
    - Intelligent insights and pattern detection
    - Cost projections with confidence intervals
    - Actionable optimization recommendations
    - Performance benchmarking
    - Risk assessment and alerts
    
    Args:
        days: Number of days to analyze (1-90, default: 7)
            - Short term (1-7): Recent patterns and immediate optimization
            - Medium term (8-30): Trend analysis and forecasting
            - Long term (31-90): Strategic insights and planning
    
    Returns:
        JSON string with comprehensive cost analysis, insights, and strategic recommendations
    """
    # Enhanced parameter validation
    if not isinstance(days, int):
        raise ValueError("Days parameter must be an integer")
    
    config = QueryConfig(days=days, project_id=project_id)
    query = bq_client.build_cost_summary_query(config)
    results = bq_client.execute_query(query)
    result = results[0]
    
    # Extract and validate metrics
    metrics = CostMetrics(
        total_cost=float(result.total_cost_usd or 0),
        query_count=int(result.total_queries),
        avg_cost=float(result.avg_cost_per_query or 0),
        max_cost=float(result.max_query_cost or 0),
        unique_users=int(result.unique_users or 0),
        active_days=int(result.active_days),
        cache_hit_rate=round((int(result.cache_hits or 0) / max(int(result.total_queries), 1)) * 100, 1)
    )
    
    # Generate comprehensive insights and analytics
    insights = bq_client.generate_cost_insights(metrics, config)
    projections = bq_client.calculate_cost_projections(metrics.total_cost, metrics.active_days)
    
    # Advanced analytics
    cost_efficiency = _calculate_cost_efficiency_summary(metrics)
    usage_patterns = _analyze_usage_patterns_summary(metrics, days)
    risk_assessment = _perform_risk_assessment(metrics, days)
    optimization_opportunities = _identify_optimization_opportunities(metrics, result)
    benchmarking = _calculate_benchmarking_metrics(metrics, days)
    
    response_data = {
        "analysis_metadata": {
            "analysis_period": {
                "days": days,
                "start_date": (datetime.now() - timedelta(days=days)).date().isoformat(),
                "end_date": datetime.now().date().isoformat(),
                "analysis_scope": _get_analysis_scope(days)
            },
            "data_completeness": {
                "active_days": metrics.active_days,
                "data_coverage_pct": round((metrics.active_days / days) * 100, 1),
                "total_data_points": metrics.query_count
            }
        },
        "cost_summary": {
            "total_cost_usd": round(metrics.total_cost, 2),
            "total_queries": metrics.query_count,
            "avg_cost_per_query": round(metrics.avg_cost, 4),
            "max_query_cost": round(metrics.max_cost, 2),
            "unique_users": metrics.unique_users,
            "active_days": metrics.active_days,
            "cache_hit_rate": metrics.cache_hit_rate,
            "datasets_accessed": int(result.datasets_accessed or 0),
            "avg_duration_ms": round(float(result.avg_duration_ms or 0), 2),
            "total_bytes_processed": int(result.total_bytes_processed or 0)
        },
        "cost_efficiency": cost_efficiency,
        "usage_patterns": usage_patterns,
        "projections": projections,
        "risk_assessment": risk_assessment,
        "optimization_opportunities": optimization_opportunities,
        "benchmarking": benchmarking,
        "insights": insights,
        "recommendations": _generate_strategic_recommendations(metrics, cost_efficiency, risk_assessment, days),
        "generated_at": datetime.now().isoformat()
    }
    
    return create_standard_response(
        success=True,
        data=response_data,
        project_id=project_id
    )


def _get_analysis_scope(days: int) -> str:
    """Determine the scope and strategic focus of the analysis."""
    if days <= 7:
        return "operational_optimization"  # Focus on immediate improvements
    elif days <= 30:
        return "tactical_planning"  # Focus on short-term trends and planning
    else:
        return "strategic_analysis"  # Focus on long-term patterns and strategy


def _calculate_cost_efficiency_summary(metrics: CostMetrics) -> Dict[str, Any]:
    """Calculate comprehensive cost efficiency metrics."""
    # Calculate efficiency scores
    query_efficiency = max(0, 100 - (metrics.avg_cost * 1000))  # Lower cost = higher efficiency
    user_efficiency = (metrics.query_count / max(metrics.unique_users, 1))  # Queries per user
    cache_efficiency = metrics.cache_hit_rate
    
    overall_efficiency = (query_efficiency + cache_efficiency) / 2
    
    return {
        "overall_efficiency_score": round(overall_efficiency, 1),
        "query_cost_efficiency": round(query_efficiency, 1),
        "cache_utilization": metrics.cache_hit_rate,
        "user_productivity": round(user_efficiency, 1),
        "efficiency_grade": _calculate_efficiency_grade(overall_efficiency),
        "cost_per_user": round(metrics.total_cost / max(metrics.unique_users, 1), 2)
    }


def _calculate_efficiency_grade(efficiency_score: float) -> str:
    """Calculate efficiency grade based on score."""
    if efficiency_score >= 85:
        return "A"
    elif efficiency_score >= 70:
        return "B"
    elif efficiency_score >= 55:
        return "C"
    elif efficiency_score >= 40:
        return "D"
    else:
        return "F"


def _analyze_usage_patterns_summary(metrics: CostMetrics, days: int) -> Dict[str, Any]:
    """Analyze usage patterns and behaviors."""
    daily_query_avg = metrics.query_count / max(days, 1)
    daily_cost_avg = metrics.total_cost / max(days, 1)
    daily_user_avg = metrics.unique_users / max(metrics.active_days, 1)
    
    # Classify usage patterns
    usage_intensity = "high" if daily_query_avg > 100 else ("medium" if daily_query_avg > 20 else "low")
    cost_intensity = "high" if daily_cost_avg > 50 else ("medium" if daily_cost_avg > 10 else "low")
    
    return {
        "usage_intensity": usage_intensity,
        "cost_intensity": cost_intensity,
        "daily_averages": {
            "queries_per_day": round(daily_query_avg, 1),
            "cost_per_day": round(daily_cost_avg, 2),
            "active_users_per_day": round(daily_user_avg, 1)
        },
        "activity_distribution": {
            "active_day_ratio": round((metrics.active_days / days) * 100, 1),
            "consistency": "consistent" if metrics.active_days >= days * 0.8 else "sporadic"
        }
    }


def _perform_risk_assessment(metrics: CostMetrics, days: int) -> Dict[str, Any]:
    """Perform comprehensive risk assessment."""
    risks = []
    risk_score = 0
    
    # High cost risk
    if metrics.max_cost > 100:
        risks.append("Very expensive single queries detected")
        risk_score += 30
    elif metrics.max_cost > 25:
        risks.append("Expensive queries present")
        risk_score += 15
    
    # Cost variability risk
    if metrics.avg_cost > 0 and metrics.max_cost / metrics.avg_cost > 10:
        risks.append("High cost variability between queries")
        risk_score += 20
    
    # Low cache utilization risk
    if metrics.cache_hit_rate < 10:
        risks.append("Very low cache utilization")
        risk_score += 25
    elif metrics.cache_hit_rate < 30:
        risks.append("Low cache utilization")
        risk_score += 10
    
    # Usage pattern risks
    daily_cost = metrics.total_cost / max(days, 1)
    if daily_cost > 100:
        risks.append("High daily cost trend")
        risk_score += 20
    
    risk_level = "high" if risk_score > 50 else ("medium" if risk_score > 25 else "low")
    
    return {
        "risk_level": risk_level,
        "risk_score": min(risk_score, 100),
        "identified_risks": risks,
        "mitigation_priority": "immediate" if risk_score > 70 else ("planned" if risk_score > 40 else "monitoring")
    }


def _identify_optimization_opportunities(metrics: CostMetrics, result: Any) -> Dict[str, Any]:
    """Identify specific optimization opportunities."""
    opportunities = []
    potential_savings = 0
    
    # Cache optimization
    if metrics.cache_hit_rate < 50:
        cache_opportunity = (50 - metrics.cache_hit_rate) / 100 * metrics.total_cost * 0.3
        opportunities.append({
            "type": "cache_optimization",
            "description": "Improve query caching and reuse",
            "potential_savings_usd": round(cache_opportunity, 2),
            "difficulty": "medium",
            "timeframe": "2-4 weeks"
        })
        potential_savings += cache_opportunity
    
    # Query optimization for expensive queries
    if metrics.max_cost > 10:
        query_opportunity = min(metrics.total_cost * 0.2, metrics.max_cost * 0.5)
        opportunities.append({
            "type": "query_optimization", 
            "description": "Optimize expensive queries",
            "potential_savings_usd": round(query_opportunity, 2),
            "difficulty": "high",
            "timeframe": "4-8 weeks"
        })
        potential_savings += query_opportunity
    
    # User training opportunity
    if metrics.unique_users > 5 and metrics.avg_cost > 0.5:
        training_opportunity = metrics.total_cost * 0.15
        opportunities.append({
            "type": "user_training",
            "description": "User training on cost-efficient querying",
            "potential_savings_usd": round(training_opportunity, 2),
            "difficulty": "low",
            "timeframe": "2-3 weeks"
        })
        potential_savings += training_opportunity
    
    return {
        "total_opportunities": len(opportunities),
        "total_potential_savings": round(potential_savings, 2),
        "savings_percentage": round((potential_savings / max(metrics.total_cost, 1)) * 100, 1),
        "opportunities": opportunities
    }


def _calculate_benchmarking_metrics(metrics: CostMetrics, days: int) -> Dict[str, Any]:
    """Calculate benchmarking metrics against industry standards."""
    # Industry benchmark estimates (these would typically come from external data)
    benchmark_avg_cost_per_query = 0.25  # Industry average
    benchmark_cache_hit_rate = 35  # Industry average
    benchmark_queries_per_user_per_day = 15  # Industry average
    
    user_queries_per_day = (metrics.query_count / max(metrics.unique_users, 1)) / max(days, 1)
    
    return {
        "cost_performance": {
            "vs_benchmark": "above" if metrics.avg_cost > benchmark_avg_cost_per_query else "below",
            "cost_ratio": round(metrics.avg_cost / benchmark_avg_cost_per_query, 2),
            "benchmark_avg_cost": benchmark_avg_cost_per_query
        },
        "cache_performance": {
            "vs_benchmark": "above" if metrics.cache_hit_rate > benchmark_cache_hit_rate else "below",
            "cache_ratio": round(metrics.cache_hit_rate / benchmark_cache_hit_rate, 2),
            "benchmark_cache_rate": benchmark_cache_hit_rate
        },
        "productivity": {
            "vs_benchmark": "above" if user_queries_per_day > benchmark_queries_per_user_per_day else "below",
            "productivity_ratio": round(user_queries_per_day / benchmark_queries_per_user_per_day, 2),
            "benchmark_queries_per_user": benchmark_queries_per_user_per_day
        }
    }


def _generate_strategic_recommendations(metrics: CostMetrics, cost_efficiency: Dict, risk_assessment: Dict, days: int) -> list:
    """Generate strategic, prioritized recommendations."""
    recommendations = []
    
    # High priority recommendations based on risk
    if risk_assessment.get("risk_level") == "high":
        recommendations.append({
            "priority": "critical",
            "category": "cost_control",
            "recommendation": "Implement immediate cost monitoring and query approval process",
            "timeframe": "1-2 weeks",
            "impact": "high"
        })
    
    # Efficiency recommendations
    efficiency_score = cost_efficiency.get("overall_efficiency_score", 100)
    if efficiency_score < 60:
        recommendations.append({
            "priority": "high",
            "category": "optimization",
            "recommendation": "Develop comprehensive query optimization program",
            "timeframe": "4-6 weeks", 
            "impact": "high"
        })
    
    # Cache recommendations
    if metrics.cache_hit_rate < 30:
        recommendations.append({
            "priority": "medium",
            "category": "performance",
            "recommendation": "Implement query result caching strategy",
            "timeframe": "2-3 weeks",
            "impact": "medium"
        })
    
    # User training recommendations
    if metrics.unique_users > 3 and metrics.avg_cost > 1.0:
        recommendations.append({
            "priority": "medium",
            "category": "education",
            "recommendation": "Conduct BigQuery cost optimization training for users",
            "timeframe": "3-4 weeks",
            "impact": "medium"
        })
    
    # Long-term strategic recommendations
    if days >= 30:
        recommendations.append({
            "priority": "low",
            "category": "strategic",
            "recommendation": "Develop long-term data strategy and cost governance framework",
            "timeframe": "8-12 weeks",
            "impact": "high"
        })
    
    return recommendations


if __name__ == "__main__":
    import argparse
    from logger import setup_logging
    
    # Parse command line arguments with enhanced options
    parser = argparse.ArgumentParser(
        description="BigQuery Core MCP Server - Essential BigQuery analysis tools",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --project my-project
  %(prog)s --project my-project --debug
  %(prog)s --project my-project --health-check

For more information, visit: https://github.com/your-org/dataops-mcp-server
        """
    )
    parser.add_argument(
        "--project", 
        type=str, 
        help="GCP project ID (or set GOOGLE_CLOUD_PROJECT environment variable)"
    )
    parser.add_argument(
        "--debug", 
        action="store_true", 
        help="Enable debug logging"
    )
    parser.add_argument(
        "--health-check", 
        action="store_true", 
        help="Run health check and exit"
    )
    parser.add_argument(
        "--version", 
        action="version", 
        version="BigQuery Core MCP Server 1.0.0"
    )
    
    args = parser.parse_args()
    
    # Setup logging with command line options
    main_logger = setup_logging(
        level="DEBUG" if args.debug else "INFO",
        enable_debug=args.debug,
        module_name="bigquery_core"
    )
    
    # Set project ID if provided
    if args.project:
        os.environ["GOOGLE_CLOUD_PROJECT"] = args.project
        main_logger.info(f"Using project ID: {args.project}")
    
    # Validate environment
    if not os.getenv("GOOGLE_CLOUD_PROJECT"):
        main_logger.error("GOOGLE_CLOUD_PROJECT environment variable not set")
        print("Error: GOOGLE_CLOUD_PROJECT environment variable must be set")
        print("Use --project flag or set the environment variable")
        exit(1)
    
    # Run health check if requested
    if args.health_check:
        main_logger.info("Running health check...")
        try:
            health_result = health_check()
            health_data = json.loads(health_result)
            if health_data.get("success"):
                print("✅ Health check passed")
                print(json.dumps(health_data, indent=2))
                exit(0)
            else:
                print("❌ Health check failed")
                print(json.dumps(health_data, indent=2))
                exit(1)
        except Exception as e:
            main_logger.error(f"Health check error: {e}")
            print(f"❌ Health check error: {e}")
            exit(1)
    
    # Start the MCP server
    try:
        main_logger.info(f"Starting BigQuery Core MCP Server for project: {project_id}")
        main_logger.info("Available tools: health_check, get_daily_costs, get_top_users, get_cost_summary")
        mcp.run()
    except KeyboardInterrupt:
        main_logger.info("Server shutting down...")
    except Exception as e:
        main_logger.error(f"Server error: {e}")
        raise
