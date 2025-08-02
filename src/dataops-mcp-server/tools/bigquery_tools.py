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
