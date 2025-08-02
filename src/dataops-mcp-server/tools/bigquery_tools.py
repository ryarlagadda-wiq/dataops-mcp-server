#!/usr/bin/env python3

"""
BigQuery Cost Analysis Tool
Retrieves comprehensive BigQuery cost breakdowns and analysis.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import statistics

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

logger = logging.getLogger(__name__)

class GetBigQueryCostsTool:
    """Tool for retrieving and analyzing BigQuery costs"""
    
    def __init__(self, project_id: str, region: str = "us-central1"):
        self.project_id = project_id
        self.region = region
        self.bq_client = bigquery.Client(project=project_id)
        
        # BigQuery pricing constants
        self.on_demand_price_per_tb = 6.25
        
    async def execute(
        self,
        days: int = 7,
        include_predictions: bool = True,
        group_by: List[str] = None,
        include_query_details: bool = False
    ) -> str:
        """
        Retrieve comprehensive BigQuery cost analysis.
        
        Args:
            days: Number of days to analyze (1-90)
            include_predictions: Include cost forecasting
            group_by: Grouping dimensions (date, user, dataset, query_type)
            include_query_details: Include individual query breakdowns
            
        Returns:
            JSON string with cost analysis
        """
        try:
            logger.info(f"Analyzing BigQuery costs for {days} days")
            
            # Validate parameters
            if days < 1 or days > 90:
                raise ValueError("Days must be between 1 and 90")
            
            if group_by is None:
                group_by = ["date"]
            
            # Fetch cost data
            cost_data = await self._fetch_cost_data(days, group_by, include_query_details)
            
            # Generate cost analysis
            analysis = {
                "cost_summary": self._generate_cost_summary(cost_data),
                "daily_breakdown": cost_data["daily_costs"],
                "trends": self._analyze_trends(cost_data["daily_costs"]),
                "top_cost_drivers": self._identify_cost_drivers(cost_data),
                "optimization_opportunities": self._identify_opportunities(cost_data)
            }
            
            # Add predictions if requested
            if include_predictions:
                analysis["cost_forecast"] = self._generate_forecast(cost_data["daily_costs"], 7)
            
            result = {
                "success": True,
                "project_id": self.project_id,
                "analysis_period": {
                    "days": days,
                    "start_date": (datetime.now() - timedelta(days=days)).date().isoformat(),
                    "end_date": datetime.now().date().isoformat()
                },
                "cost_analysis": analysis,
                "metadata": {
                    "generated_at": datetime.now().isoformat(),
                    "total_queries_analyzed": cost_data.get("total_queries", 0),
                    "data_completeness": self._assess_data_completeness(cost_data)
                }
            }
            
            total_cost = analysis["cost_summary"]["total_cost_usd"]
            logger.info(f"Cost analysis completed: ${total_cost:.2f} total cost over {days} days")
            
            return json.dumps(result, indent=2)
            
        except Exception as e:
            logger.error(f"BigQuery cost analysis failed: {e}")
            return json.dumps({
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__,
                "suggestion": "Check project permissions and ensure BigQuery usage exists"
            })
    
    async def _fetch_cost_data(
        self, 
        days: int, 
        group_by: List[str], 
        include_details: bool
    ) -> Dict[str, Any]:
        """Fetch cost data from BigQuery INFORMATION_SCHEMA"""
        
        # Build grouping clause
        group_columns = []
        select_columns = []
        
        if "date" in group_by:
            group_columns.append("DATE(creation_time)")
            select_columns.append("DATE(creation_time) as date")
        
        if "user" in group_by:
            group_columns.append("user_email")
            select_columns.append("user_email")
        
        if "dataset" in group_by:
            group_columns.append("destination_table.dataset_id")
            select_columns.append("destination_table.dataset_id as dataset_id")
        
        if "query_type" in group_by:
            group_columns.append("""
                CASE 
                    WHEN query LIKE 'SELECT%' THEN 'SELECT'
                    WHEN query LIKE 'INSERT%' THEN 'INSERT'
                    WHEN query LIKE 'CREATE%' THEN 'CREATE'
                    WHEN query LIKE 'UPDATE%' THEN 'UPDATE'
                    WHEN query LIKE 'DELETE%' THEN 'DELETE'
                    ELSE 'OTHER'
                END
            """)
            select_columns.append("""
                CASE 
                    WHEN query LIKE 'SELECT%' THEN 'SELECT'
                    WHEN query LIKE 'INSERT%' THEN 'INSERT'
                    WHEN query LIKE 'CREATE%' THEN 'CREATE'
                    WHEN query LIKE 'UPDATE%' THEN 'UPDATE'
                    WHEN query LIKE 'DELETE%' THEN 'DELETE'
                    ELSE 'OTHER'
                END as query_type
            """)
        
        # Default grouping if none specified
        if not group_columns:
            group_columns = ["DATE(creation_time)"]
            select_columns = ["DATE(creation_time) as date"]
        
        group_clause = ", ".join(group_columns)
        select_clause = ", ".join(select_columns)
        
        query = f"""
        WITH cost_analysis AS (
            SELECT 
                {select_clause},
                COUNT(*) as query_count,
                SUM(total_bytes_processed) as total_bytes,
                SUM(total_bytes_processed) / POW(10, 12) * {self.on_demand_price_per_tb} as cost_usd,
                AVG(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)) as avg_duration_ms,
                MAX(total_bytes_processed) / POW(10, 12) * {self.on_demand_price_per_tb} as max_query_cost
            FROM `{self.project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
            WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
                AND job_type = 'QUERY'
                AND state = 'DONE'
                AND error_result IS NULL
                AND total_bytes_processed IS NOT NULL
            GROUP BY {group_clause}
        )
        SELECT * FROM cost_analysis
        ORDER BY cost_usd DESC
        """
        
        results = list(self.bq_client.query(query))
        
        # Process results
        daily_costs = []
        total_cost = 0
        total_queries = 0
        
        for row in results:
            cost_entry = {
                "date": getattr(row, 'date', datetime.now().date()).isoformat(),
                "user_email": getattr(row, 'user_email', None),
                "dataset_id": getattr(row, 'dataset_id', None),
                "query_type": getattr(row, 'query_type', None),
                "query_count": int(row.query_count),
                "total_bytes": int(row.total_bytes or 0),
                "cost_usd": float(row.cost_usd or 0),
                "avg_duration_ms": float(row.avg_duration_ms or 0),
                "max_query_cost": float(row.max_query_cost or 0)
            }
            
            # Remove None values to clean up response
            cost_entry = {k: v for k, v in cost_entry.items() if v is not None}
            
            daily_costs.append(cost_entry)
            total_cost += cost_entry["cost_usd"]
            total_queries += cost_entry["query_count"]
        
        return {
            "daily_costs": daily_costs,
            "total_cost": total_cost,
            "total_queries": total_queries,
            "average_cost_per_query": total_cost / max(total_queries, 1)
        }
    
    def _generate_cost_summary(self, cost_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate high-level cost summary"""
        
        daily_costs = cost_data["daily_costs"]
        total_cost = cost_data["total_cost"]
        total_queries = cost_data["total_queries"]
        
        # Calculate daily statistics
        costs_only = [entry["cost_usd"] for entry in daily_costs if "cost_usd" in entry]
        
        summary = {
            "total_cost_usd": total_cost,
            "total_queries": total_queries,
            "average_cost_per_query": cost_data["average_cost_per_query"],
            "daily_statistics": {
                "average_daily_cost": statistics.mean(costs_only) if costs_only else 0,
                "min_daily_cost": min(costs_only) if costs_only else 0,
                "max_daily_cost": max(costs_only) if costs_only else 0,
                "daily_std_deviation": statistics.stdev(costs_only) if len(costs_only) > 1 else 0
            },
            "cost_efficiency_metrics": {
                "cost_per_gb": total_cost / max(sum(entry.get("total_bytes", 0) for entry in daily_costs) / (1024**3), 1),
                "queries_per_dollar": total_queries / max(total_cost, 0.01)
            }
        }
        
        return summary
    
    def _analyze_trends(self, daily_costs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze cost trends over the period"""
        
        # Extract daily costs in chronological order
        date_costs = {}
        for entry in daily_costs:
            if "date" in entry and "cost_usd" in entry:
                date_costs[entry["date"]] = entry["cost_usd"]
        
        if len(date_costs) < 2:
            return {"trend": "insufficient_data"}
        
        # Sort by date and get cost sequence
        sorted_dates = sorted(date_costs.keys())
        cost_sequence = [date_costs[date] for date in sorted_dates]
        
        # Calculate trend using linear regression
        x_values = list(range(len(cost_sequence)))
        n = len(x_values)
        
        if n < 2:
            return {"trend": "insufficient_data"}
        
        # Simple linear regression
        sum_x = sum(x_values)
        sum_y = sum(cost_sequence)
        sum_xy = sum(x_values[i] * cost_sequence[i] for i in range(n))
        sum_x2 = sum(x * x for x in x_values)
        
        denominator = n * sum_x2 - sum_x * sum_x
        if denominator == 0:
            slope = 0
        else:
            slope = (n * sum_xy - sum_x * sum_y) / denominator
        
        # Determine trend direction
        if slope > 0.1:
            trend_direction = "increasing"
        elif slope < -0.1:
            trend_direction = "decreasing"
        else:
            trend_direction = "stable"
        
        # Calculate weekly and monthly projections
        daily_avg = statistics.mean(cost_sequence)
        weekly_projection = daily_avg * 7
        monthly_projection = daily_avg * 30
        
        # Calculate volatility
        volatility = statistics.stdev(cost_sequence) / daily_avg if daily_avg > 0 else 0
        
        return {
            "trend_direction": trend_direction,
            "slope": slope,
            "daily_average": daily_avg,
            "weekly_projection": weekly_projection,
            "monthly_projection": monthly_projection,
            "volatility": volatility,
            "cost_stability": "stable" if volatility < 0.2 else "moderate" if volatility < 0.5 else "volatile"
        }
    
    def _identify_cost_drivers(self, cost_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify top cost drivers from the data"""
        
        daily_costs = cost_data["daily_costs"]
        drivers = []
        
        # Group by different dimensions to find top drivers
        if any("user_email" in entry for entry in daily_costs):
            user_costs = {}
            for entry in daily_costs:
                if "user_email" in entry:
                    user = entry["user_email"]
                    if user not in user_costs:
                        user_costs[user] = 0
                    user_costs[user] += entry["cost_usd"]
            
            # Top 5 users by cost
            top_users = sorted(user_costs.items(), key=lambda x: x[1], reverse=True)[:5]
            for user, cost in top_users:
                drivers.append({
                    "type": "user",
                    "identifier": user,
                    "cost_usd": cost,
                    "percentage_of_total": (cost / cost_data["total_cost"] * 100) if cost_data["total_cost"] > 0 else 0
                })
        
        if any("dataset_id" in entry for entry in daily_costs):
            dataset_costs = {}
            for entry in daily_costs:
                if "dataset_id" in entry and entry["dataset_id"]:
                    dataset = entry["dataset_id"]
                    if dataset not in dataset_costs:
                        dataset_costs[dataset] = 0
                    dataset_costs[dataset] += entry["cost_usd"]
            
            # Top 5 datasets by cost
            top_datasets = sorted(dataset_costs.items(), key=lambda x: x[1], reverse=True)[:5]
            for dataset, cost in top_datasets:
                drivers.append({
                    "type": "dataset",
                    "identifier": dataset,
                    "cost_usd": cost,
                    "percentage_of_total": (cost / cost_data["total_cost"] * 100) if cost_data["total_cost"] > 0 else 0
                })
        
        # Sort all drivers by cost
        drivers.sort(key=lambda x: x["cost_usd"], reverse=True)
        
        return drivers[:10]  # Return top 10 drivers
    
    def _identify_opportunities(self, cost_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify cost optimization opportunities"""
        
        opportunities = []
        daily_costs = cost_data["daily_costs"]
        
        # High average query cost opportunity
        avg_cost_per_query = cost_data["average_cost_per_query"]
        if avg_cost_per_query > 2.0:  # $2 per query is high
            potential_savings = avg_cost_per_query * 0.3 * cost_data["total_queries"]  # 30% savings
            opportunities.append({
                "type": "high_average_query_cost",
                "title": "Optimize high-cost queries",
                "description": f"Average query cost is ${avg_cost_per_query:.2f}, above recommended threshold",
                "current_monthly_cost": avg_cost_per_query * cost_data["total_queries"] * 4.33,  # Approximate monthly
                "potential_monthly_savings": potential_savings * 4.33,
                "implementation_effort": "medium",
                "priority": "high" if avg_cost_per_query > 5.0 else "medium"
            })
        
        # Cost volatility opportunity
        costs_only = [entry["cost_usd"] for entry in daily_costs if "cost_usd" in entry]
        if len(costs_only) > 1:
            daily_avg = statistics.mean(costs_only)
            volatility = statistics.stdev(costs_only) / daily_avg if daily_avg > 0 else 0
            
            if volatility > 0.4:  # High volatility
                opportunities.append({
                    "type": "cost_volatility",
                    "title": "Stabilize cost patterns",
                    "description": f"Daily costs are highly volatile (coefficient: {volatility:.2f})",
                    "current_monthly_cost": daily_avg * 30,
                    "potential_monthly_savings": max(costs_only) * 0.3 * 30,  # Reduce peaks by 30%
                    "implementation_effort": "low",
                    "priority": "medium"
                })
        
        # Large query opportunity
        max_query_costs = [entry.get("max_query_cost", 0) for entry in daily_costs]
        max_single_query = max(max_query_costs) if max_query_costs else 0
        
        if max_single_query > 20.0:  # Very expensive single query
            opportunities.append({
                "type": "expensive_single_queries",
                "title": "Optimize extremely expensive queries",
                "description": f"Single query costs up to ${max_single_query:.2f}",
                "current_monthly_cost": max_single_query * 30,  # Assume daily execution
                "potential_monthly_savings": max_single_query * 0.5 * 30,  # 50% savings potential
                "implementation_effort": "high",
                "priority": "critical" if max_single_query > 100 else "high"
            })
        
        return opportunities
    
    def _generate_forecast(self, daily_costs: List[Dict[str, Any]], forecast_days: int) -> Dict[str, Any]:
        """Generate simple cost forecast"""
        
        # Extract cost values
        costs = []
        for entry in daily_costs:
            if "cost_usd" in entry:
                costs.append(entry["cost_usd"])
        
        if len(costs) < 3:
            return {
                "forecast_available": False,
                "reason": "Insufficient data for forecasting"
            }
        
        # Simple moving average forecast
        window_size = min(7, len(costs))
        recent_avg = statistics.mean(costs[-window_size:])
        
        # Generate daily forecasts
        forecast_points = []
        for i in range(1, forecast_days + 1):
            forecast_date = datetime.now().date() + timedelta(days=i)
            forecast_points.append({
                "date": forecast_date.isoformat(),
                "predicted_cost_usd": recent_avg,
                "confidence": "medium" if len(costs) >= 7 else "low"
            })
        
        return {
            "forecast_available": True,
            "daily_forecasts": forecast_points,
            "total_forecast_cost": recent_avg * forecast_days,
            "forecast_method": "moving_average",
            "confidence_note": "Forecast accuracy improves with more historical data"
        }
    
    def _assess_data_completeness(self, cost_data: Dict[str, Any]) -> Dict[str, Any]:
        """Assess the completeness and quality of cost data"""
        
        daily_costs = cost_data["daily_costs"]
        
        # Check for missing days
        if daily_costs:
            dates = [entry.get("date") for entry in daily_costs if entry.get("date")]
            date_range = len(set(dates)) if dates else 0
        else:
            date_range = 0
        
        # Data quality indicators
        completeness = {
            "has_cost_data": cost_data["total_cost"] > 0,
            "has_query_data": cost_data["total_queries"] > 0,
            "date_coverage": date_range,
            "avg_queries_per_day": cost_data["total_queries"] / max(date_range, 1),
            "data_quality": "good" if cost_data["total_cost"] > 0 and date_range >= 3 else "limited"
        }
        
        return completeness
    
    async def health_check(self) -> bool:
        """Check if the tool can access BigQuery successfully"""
        try:
            # Test basic query access
            test_query = "SELECT 1 as test"
            job_config = bigquery.QueryJobConfig(dry_run=True)
            self.bq_client.query(test_query, job_config=job_config)
            
            # Test INFORMATION_SCHEMA access
            schema_query = f"""
            SELECT COUNT(*) as job_count
            FROM `{self.project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
            WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
            LIMIT 1
            """
            job_config = bigquery.QueryJobConfig(dry_run=True)
            self.bq_client.query(schema_query, job_config=job_config)
            
            return True
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
