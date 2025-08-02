#!/usr/bin/env python3

"""
Cost Anomaly Detection Tool
Uses ML and statistical methods to detect unusual BigQuery spending patterns.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import statistics
import math

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

logger = logging.getLogger(__name__)

@dataclass
class CostAnomaly:
    """Detected cost anomaly"""
    anomaly_id: str
    date: str
    actual_cost: float
    expected_cost: float
    deviation_percentage: float
    severity: str
    confidence_score: float
    detection_method: str
    contributing_factors: List[str]
    affected_resources: List[str]
    remediation_steps: List[str]

class DetectCostAnomalies:
    """Tool for detecting cost anomalies using multiple ML techniques"""
    
    def __init__(self, project_id: str, region: str = "us-central1"):
        self.project_id = project_id
        self.region = region
        self.bq_client = bigquery.Client(project=project_id)
        
        # Sensitivity configuration
        self.sensitivity_config = {
            "low": {"z_score": 2.5, "deviation_threshold": 0.5},     # 50% deviation
            "medium": {"z_score": 2.0, "deviation_threshold": 0.3},  # 30% deviation
            "high": {"z_score": 1.5, "deviation_threshold": 0.2}     # 20% deviation
        }
        
        # Severity thresholds
        self.severity_thresholds = {
            "LOW": (0.0, 0.3),        # 0-30% deviation
            "MEDIUM": (0.3, 0.6),     # 30-60% deviation  
            "HIGH": (0.6, 1.0),       # 60-100% deviation
            "CRITICAL": (1.0, float('inf'))  # 100%+ deviation
        }
    
    async def execute(
        self,
        days: int = 30,
        sensitivity: str = "medium",
        alert_threshold: float = 0.25,
        send_slack_alert: bool = False
    ) -> str:
        """
        Detect cost anomalies using multiple detection methods.
        
        Args:
            days: Historical period to analyze (7-90)
            sensitivity: Detection sensitivity ("low", "medium", "high")
            alert_threshold: Minimum deviation for anomaly alerts
            send_slack_alert: Send Slack notifications for detected anomalies
            
        Returns:
            JSON string with detected anomalies and analysis
        """
        try:
            logger.info(f"Detecting cost anomalies over {days} days with {sensitivity} sensitivity")
            
            # Validate parameters
            if days < 7 or days > 90:
                raise ValueError("Days must be between 7 and 90")
            
            if sensitivity not in self.sensitivity_config:
                raise ValueError("Sensitivity must be 'low', 'medium', or 'high'")
            
            # Fetch cost time series data
            cost_time_series = await self._fetch_cost_time_series(days)
            
            if len(cost_time_series) < 7:
                return json.dumps({
                    "success": False,
                    "error": "Insufficient data for anomaly detection (minimum 7 days required)",
                    "data_points_available": len(cost_time_series)
                })
            
            # Apply multiple anomaly detection methods
            detected_anomalies = []
            
            # Statistical anomaly detection (Z-score)
            statistical_anomalies = self._detect_statistical_anomalies(
                cost_time_series, sensitivity
            )
            detected_anomalies.extend(statistical_anomalies)
            
            # Moving average anomaly detection
            moving_avg_anomalies = self._detect_moving_average_anomalies(
                cost_time_series, sensitivity
            )
            detected_anomalies.extend(moving_avg_anomalies)
            
            # Seasonal pattern anomaly detection
            seasonal_anomalies = self._detect_seasonal_anomalies(
                cost_time_series, sensitivity
            )
            detected_anomalies.extend(seasonal_anomalies)
            
            # Filter and deduplicate anomalies
            significant_anomalies = self._filter_and_deduplicate_anomalies(
                detected_anomalies, alert_threshold
            )
            
            # Enrich anomalies with root cause analysis
            enriched_anomalies = []
            for anomaly in significant_anomalies:
                enriched = await self._enrich_anomaly_with_context(anomaly, cost_time_series)
                enriched_anomalies.append(enriched)
            
            # Generate anomaly summary and insights
            summary = self._generate_anomaly_summary(enriched_anomalies, cost_time_series)
            
            # Future risk assessment
            risk_assessment = self._assess_future_risk(cost_time_series, enriched_anomalies)
            
            result = {
                "success": True,
                "anomalies_detected": [asdict(anomaly) for anomaly in enriched_anomalies],
                "summary": summary,
                "risk_assessment": risk_assessment,
                "analysis_metadata": {
                    "analysis_period_days": days,
                    "sensitivity": sensitivity,
                    "alert_threshold": alert_threshold,
                    "detection_methods": ["statistical", "moving_average", "seasonal"],
                    "total_data_points": len(cost_time_series),
                    "analyzed_at": datetime.now().isoformat()
                }
            }
            
            logger.info(f"Anomaly detection completed: {len(enriched_anomalies)} anomalies found")
            
            # Send alerts if requested and anomalies found
            if send_slack_alert and enriched_anomalies:
                result["alert_sent"] = await self._send_anomaly_alerts(enriched_anomalies)
            
            return json.dumps(result, indent=2)
            
        except Exception as e:
            logger.error(f"Cost anomaly detection failed: {e}")
            return json.dumps({
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__
            })
    
    async def _fetch_cost_time_series(self, days: int) -> List[Dict[str, Any]]:
        """Fetch daily cost time series with additional context"""
        
        query = f"""
        WITH daily_metrics AS (
            SELECT 
                DATE(creation_time) as date,
                SUM(total_bytes_processed) / POW(10, 12) * 6.25 as daily_cost,
                COUNT(*) as query_count,
                COUNT(DISTINCT user_email) as unique_users,
                AVG(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)) as avg_duration_ms,
                MAX(total_bytes_processed) / POW(10, 12) * 6.25 as max_single_query_cost,
                EXTRACT(DAYOFWEEK FROM creation_time) as day_of_week
            FROM `{self.project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
            WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
                AND job_type = 'QUERY'
                AND state = 'DONE'
                AND error_result IS NULL
                AND total_bytes_processed IS NOT NULL
            GROUP BY DATE(creation_time), EXTRACT(DAYOFWEEK FROM creation_time)
        )
        SELECT * FROM daily_metrics
        ORDER BY date ASC
        """
        
        results = list(self.bq_client.query(query))
        
        time_series = []
        for row in results:
            time_series.append({
                "date": row.date.isoformat(),
                "daily_cost": float(row.daily_cost or 0),
                "query_count": int(row.query_count),
                "unique_users": int(row.unique_users),
                "avg_duration_ms": float(row.avg_duration_ms or 0),
                "max_single_query_cost": float(row.max_single_query_cost or 0),
                "day_of_week": int(row.day_of_week)
            })
        
        return time_series
    
    def _detect_statistical_anomalies(
        self, 
        time_series: List[Dict[str, Any]], 
        sensitivity: str
    ) -> List[CostAnomaly]:
        """Detect anomalies using Z-score statistical method"""
        
        costs = [entry["daily_cost"] for entry in time_series]
        
        if len(costs) < 7:
            return []
        
        # Calculate statistical measures
        mean_cost = statistics.mean(costs)
        std_cost = statistics.stdev(costs) if len(costs) > 1 else 0
        
        if std_cost == 0:  # No variation in costs
            return []
        
        config = self.sensitivity_config[sensitivity]
        anomalies = []
        
        for entry in time_series:
            actual_cost = entry["daily_cost"]
            
            # Calculate Z-score
            z_score = abs(actual_cost - mean_cost) / std_cost
            deviation_pct = abs(actual_cost - mean_cost) / mean_cost if mean_cost > 0 else 0
            
            # Check if anomaly based on Z-score and deviation thresholds
            if z_score >= config["z_score"] and deviation_pct >= config["deviation_threshold"]:
                
                severity = self._determine_severity(deviation_pct)
                confidence = min(z_score / 3.0, 1.0)  # Normalize confidence
                
                anomaly = CostAnomaly(
                    anomaly_id=f"stat-{entry['date']}-{int(actual_cost)}",
                    date=entry["date"],
                    actual_cost=actual_cost,
                    expected_cost=mean_cost,
                    deviation_percentage=deviation_pct * 100,
                    severity=severity,
                    confidence_score=confidence,
                    detection_method="statistical_z_score",
                    contributing_factors=["statistical_deviation"],
                    affected_resources=["bigquery"],
                    remediation_steps=[]
                )
                
                anomalies.append(anomaly)
        
        return anomalies
    
    def _detect_moving_average_anomalies(
        self, 
        time_series: List[Dict[str, Any]], 
        sensitivity: str
    ) -> List[CostAnomaly]:
        """Detect anomalies using moving average method"""
        
        if len(time_series) < 10:  # Need enough data for moving average
            return []
        
        costs = [entry["daily_cost"] for entry in time_series]
        window_size = min(7, len(costs) // 3)  # 7-day window or 1/3 of data
        
        anomalies = []
        config = self.sensitivity_config[sensitivity]
        
        for i in range(window_size, len(time_series)):
            # Calculate moving average and standard deviation
            window_costs = costs[i - window_size:i]
            moving_avg = statistics.mean(window_costs)
            moving_std = statistics.stdev(window_costs) if len(window_costs) > 1 else 0
            
            actual_cost = costs[i]
            
            # Check for anomaly
            if moving_std > 0:
                z_score = abs(actual_cost - moving_avg) / moving_std
                deviation_pct = abs(actual_cost - moving_avg) / moving_avg if moving_avg > 0 else 0
                
                if z_score >= config["z_score"] and deviation_pct >= config["deviation_threshold"]:
                    
                    severity = self._determine_severity(deviation_pct)
                    confidence = min(z_score / 3.0, 0.9)  # Slightly lower confidence for moving average
                    
                    anomaly = CostAnomaly(
                        anomaly_id=f"mavg-{time_series[i]['date']}-{int(actual_cost)}",
                        date=time_series[i]["date"],
                        actual_cost=actual_cost,
                        expected_cost=moving_avg,
                        deviation_percentage=deviation_pct * 100,
                        severity=severity,
                        confidence_score=confidence,
                        detection_method="moving_average",
                        contributing_factors=["trend_deviation"],
                        affected_resources=["bigquery"],
                        remediation_steps=[]
                    )
                    
                    anomalies.append(anomaly)
        
        return anomalies
    
    def _detect_seasonal_anomalies(
        self, 
        time_series: List[Dict[str, Any]], 
        sensitivity: str
    ) -> List[CostAnomaly]:
        """Detect anomalies based on day-of-week patterns"""
        
        # Group costs by day of week
        dow_costs = {i: [] for i in range(1, 8)}  # 1=Sunday, 7=Saturday
        
        for entry in time_series:
            dow = entry["day_of_week"]
            if 1 <= dow <= 7:
                dow_costs[dow].append(entry["daily_cost"])
        
        # Calculate expected costs for each day of week
        dow_expected = {}
        dow_std = {}
        
        for dow, costs in dow_costs.items():
            if len(costs) >= 2:  # Need at least 2 data points
                dow_expected[dow] = statistics.mean(costs)
                dow_std[dow] = statistics.stdev(costs)
        
        # Detect anomalies based on day-of-week expectations
        anomalies = []
        config = self.sensitivity_config[sensitivity]
        
        for entry in time_series:
            dow = entry["day_of_week"]
            actual_cost = entry["daily_cost"]
            
            if dow in dow_expected and dow in dow_std:
                expected = dow_expected[dow]
                std_dev = dow_std[dow]
                
                if std_dev > 0 and expected > 0:
                    z_score = abs(actual_cost - expected) / std_dev
                    deviation_pct = abs(actual_cost - expected) / expected
                    
                    if z_score >= config["z_score"] and deviation_pct >= config["deviation_threshold"]:
                        
                        severity = self._determine_severity(deviation_pct)
                        confidence = min(z_score / 3.0, 0.8)  # Seasonal patterns have moderate confidence
                        
                        day_names = ["", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
                        day_name = day_names[dow] if 1 <= dow <= 7 else "Unknown"
                        
                        anomaly = CostAnomaly(
                            anomaly_id=f"seasonal-{entry['date']}-dow{dow}",
                            date=entry["date"],
                            actual_cost=actual_cost,
                            expected_cost=expected,
                            deviation_percentage=deviation_pct * 100,
                            severity=severity,
                            confidence_score=confidence,
                            detection_method="seasonal_day_of_week",
                            contributing_factors=[f"unusual_{day_name.lower()}_pattern"],
                            affected_resources=["bigquery"],
                            remediation_steps=[]
                        )
                        
                        anomalies.append(anomaly)
        
        return anomalies
    
    def _filter_and_deduplicate_anomalies(
        self, 
        all_anomalies: List[CostAnomaly], 
        threshold: float
    ) -> List[CostAnomaly]:
        """Filter anomalies by threshold and remove duplicates"""
        
        # Filter by deviation threshold
        filtered = [
            anomaly for anomaly in all_anomalies 
            if anomaly.deviation_percentage >= threshold * 100
        ]
        
        # Group by date to handle duplicates
        date_anomalies = {}
        for anomaly in filtered:
            date = anomaly.date
            if date not in date_anomalies:
                date_anomalies[date] = []
            date_anomalies[date].append(anomaly)
        
        # For each date, keep the anomaly with highest confidence
        deduplicated = []
        for date, anomalies in date_anomalies.items():
            if len(anomalies) == 1:
                deduplicated.append(anomalies[0])
            else:
                # Merge multiple detections for same date
                best_anomaly = max(anomalies, key=lambda x: x.confidence_score)
                
                # Combine detection methods and factors
                all_methods = list(set(a.detection_method for a in anomalies))
                all_factors = list(set(factor for a in anomalies for factor in a.contributing_factors))
                
                best_anomaly.detection_method = "+".join(all_methods)
                best_anomaly.contributing_factors = all_factors
                best_anomaly.anomaly_id = f"combined-{date}"
                
                deduplicated.append(best_anomaly)
        
        # Sort by severity and deviation
        deduplicated.sort(key=lambda x: (
            self._severity_to_score(x.severity),
            x.deviation_percentage
        ), reverse=True)
        
        return deduplicated
    
    async def _enrich_anomaly_with_context(
        self, 
        anomaly: CostAnomaly, 
        time_series: List[Dict[str, Any]]
    ) -> CostAnomaly:
        """Enrich anomaly with detailed context and root cause analysis"""
        
        anomaly_date = anomaly.date
        
        # Get detailed breakdown for the anomaly date
        breakdown = await self._get_anomaly_date_breakdown(anomaly_date)
        
        # Update contributing factors based on detailed analysis
        additional_factors = []
        remediation_steps = []
        affected_resources = ["bigquery"]
        
        if breakdown:
            # Check for high-cost users
            if breakdown.get("top_user_cost", 0) > 50:
                additional_factors.append("high_cost_single_user")
                remediation_steps.append(f"Review queries from top user: {breakdown.get('top_user', 'unknown')}")
            
            # Check for unusual query volume
            avg_query_count = statistics.mean([entry["query_count"] for entry in time_series])
            if breakdown.get("query_count", 0) > avg_query_count * 2:
                additional_factors.append("unusual_query_volume")
                remediation_steps.append("Investigate increased query volume - possible automated process")
            
            # Check for expensive single queries
            if breakdown.get("max_query_cost", 0) > 25:
                additional_factors.append("expensive_single_query")
                remediation_steps.append("Optimize expensive individual queries")
            
            # Check for dataset concentration
            if breakdown.get("dataset_concentration", 0) > 0.8:
                additional_factors.append("dataset_concentration")
                affected_resources.extend(breakdown.get("top_datasets", []))
                remediation_steps.append("Review queries on concentrated datasets")
        
        # Add default remediation steps if none found
        if not remediation_steps:
            remediation_steps = [
                "Review BigQuery audit logs for the anomaly date",
                "Check for scheduled batch jobs or ETL processes",
                "Verify if data volume changes occurred in source tables",
                "Contact users with high usage on the anomaly date"
            ]
        
        # Update anomaly with enriched information
        anomaly.contributing_factors.extend(additional_factors)
        anomaly.affected_resources = affected_resources
        anomaly.remediation_steps = remediation_steps
        
        return anomaly
    
    async def _get_anomaly_date_breakdown(self, anomaly_date: str) -> Optional[Dict[str, Any]]:
        """Get detailed cost breakdown for a specific anomaly date"""
        
        query = f"""
        WITH date_breakdown AS (
            SELECT 
                user_email,
                destination_table.dataset_id as dataset_id,
                SUM(total_bytes_processed) / POW(10, 12) * 6.25 as user_cost,
                COUNT(*) as user_query_count,
                MAX(total_bytes_processed) / POW(10, 12) * 6.25 as max_query_cost
            FROM `{self.project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
            WHERE DATE(creation_time) = '{anomaly_date}'
                AND job_type = 'QUERY'
                AND state = 'DONE'
                AND error_result IS NULL
                AND total_bytes_processed IS NOT NULL
            GROUP BY user_email, destination_table.dataset_id
        )
        SELECT * FROM date_breakdown
        ORDER BY user_cost DESC
        LIMIT 20
        """
        
        try:
            results = list(self.bq_client.query(query))
            
            if not results:
                return None
            
            # Analyze breakdown
            user_costs = {}
            dataset_costs = {}
            total_cost = 0
            max_query_cost = 0
            total_queries = 0
            
            for row in results:
                user = row.user_email
                dataset = row.dataset_id
                cost = float(row.user_cost or 0)
                queries = int(row.user_query_count)
                max_cost = float(row.max_query_cost or 0)
                
                # Aggregate by user
                if user not in user_costs:
                    user_costs[user] = 0
                user_costs[user] += cost
                
                # Aggregate by dataset
                if dataset and dataset not in dataset_costs:
                    dataset_costs[dataset] = 0
                if dataset:
                    dataset_costs[dataset] += cost
                
                total_cost += cost
                total_queries += queries
                max_query_cost = max(max_query_cost, max_cost)
            
            # Find top contributors
            top_user = max(user_costs.items(), key=lambda x: x[1]) if user_costs else ("unknown", 0)
            top_datasets = sorted(dataset_costs.items(), key=lambda x: x[1], reverse=True)[:3]
            
            # Calculate concentration metrics
            dataset_concentration = top_datasets[0][1] / total_cost if total_cost > 0 and top_datasets else 0
            
            return {
                "total_cost": total_cost,
                "query_count": total_queries,
                "top_user": top_user[0],
                "top_user_cost": top_user[1],
                "max_query_cost": max_query_cost,
                "top_datasets": [dataset for dataset, _ in top_datasets],
                "dataset_concentration": dataset_concentration
            }
            
        except Exception as e:
            logger.error(f"Failed to get anomaly date breakdown: {e}")
            return None
    
    def _determine_severity(self, deviation_percentage: float) -> str:
        """Determine anomaly severity based on deviation percentage"""
        
        for severity, (min_threshold, max_threshold) in self.severity_thresholds.items():
            if min_threshold <= deviation_percentage < max_threshold:
                return severity
        
        return "CRITICAL"  # Default for very high deviations
    
    def _severity_to_score(self, severity: str) -> int:
        """Convert severity to numerical score for sorting"""
        scores = {"LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}
        return scores.get(severity, 1)
    
    def _generate_anomaly_summary(
        self, 
        anomalies: List[CostAnomaly], 
        time_series: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Generate summary statistics and insights for detected anomalies"""
        
        if not anomalies:
            return {
                "total_anomalies": 0,
                "anomaly_rate": 0,
                "total_excess_cost": 0,
                "insights": ["No significant cost anomalies detected in the analysis period"]
            }
        
        # Calculate summary metrics
        total_anomalies = len(anomalies)
        anomaly_rate = total_anomalies / len(time_series) if time_series else 0
        
        # Calculate excess costs
        total_excess_cost = sum(
            max(0, anomaly.actual_cost - anomaly.expected_cost)
            for anomaly in anomalies
        )
        
        # Severity breakdown
        severity_counts = {}
        for anomaly in anomalies:
            severity = anomaly.severity
            severity_counts[severity] = severity_counts.get(severity, 0) + 1
        
        # Most common contributing factors
        all_factors = []
        for anomaly in anomalies:
            all_factors.extend(anomaly.contributing_factors)
        
        factor_counts = {}
        for factor in all_factors:
            factor_counts[factor] = factor_counts.get(factor, 0) + 1
        
        common_factors = sorted(factor_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        
        # Generate insights
        insights = []
        insights.append(f"Detected {total_anomalies} cost anomalies over the analysis period")
        
        if total_excess_cost > 100:
            insights.append(f"Anomalies resulted in approximately ${total_excess_cost:.2f} in excess costs")
        
        if anomaly_rate > 0.2:  # > 20% of days had anomalies
            insights.append("High anomaly rate suggests systematic cost control issues")
        
        if common_factors:
            top_factor = common_factors[0][0]
            insights.append(f"Most common contributing factor: {top_factor}")
        
        return {
            "total_anomalies": total_anomalies,
            "anomaly_rate": anomaly_rate,
            "total_excess_cost": total_excess_cost,
            "severity_breakdown": severity_counts,
            "most_common_factors": [factor for factor, count in common_factors],
            "date_range": {
                "first_anomaly": min(anomaly.date for anomaly in anomalies),
                "last_anomaly": max(anomaly.date for anomaly in anomalies)
            },
            "insights": insights
        }
    
    def _assess_future_risk(
        self, 
        time_series: List[Dict[str, Any]], 
        anomalies: List[CostAnomaly]
    ) -> Dict[str, Any]:
        """Assess risk of future cost anomalies"""
        
        # Calculate recent trend
        recent_costs = [entry["daily_cost"] for entry in time_series[-7:]]  # Last 7 days
        overall_costs = [entry["daily_cost"] for entry in time_series]
        
        recent_avg = statistics.mean(recent_costs) if recent_costs else 0
        overall_avg = statistics.mean(overall_costs) if overall_costs else 0
        
        # Risk factors
        risk_factors = []
        risk_score = 0
        
        # Increasing trend risk
        if recent_avg > overall_avg * 1.2:  # 20% above average
            risk_factors.append("costs_trending_upward")
            risk_score += 2
        
        # High anomaly frequency risk
        recent_anomaly_count = sum(1 for a in anomalies if a.date in [entry["date"] for entry in time_series[-7:]])
        if recent_anomaly_count > 2:
            risk_factors.append("recent_anomaly_frequency")
            risk_score += 3
        
        # Volatility risk
        if len(overall_costs) > 1:
            volatility = statistics.stdev(overall_costs) / overall_avg if overall_avg > 0 else 0
            if volatility > 0.4:
                risk_factors.append("high_cost_volatility")
                risk_score += 2
        
        # Determine risk level
        if risk_score >= 5:
            risk_level = "HIGH"
        elif risk_score >= 3:
            risk_level = "MEDIUM"
        else:
            risk_level = "LOW"
        
        # Generate recommendations
        recommendations = []
        if risk_level in ["MEDIUM", "HIGH"]:
            recommendations.extend([
                "Implement proactive cost monitoring and alerts",
                "Review recent changes in data processing workflows",
                "Consider setting up automated cost controls"
            ])
        
        if "costs_trending_upward" in risk_factors:
            recommendations.append("Investigate root causes of cost increases")
        
        if "high_cost_volatility" in risk_factors:
            recommendations.append("Implement more predictable query scheduling")
        
        return {
            "risk_level": risk_level,
            "risk_score": risk_score,
            "risk_factors": risk_factors,
            "recommendations": recommendations,
            "predicted_weekly_cost": recent_avg * 7,
            "cost_trend": "increasing" if recent_avg > overall_avg * 1.1 else "stable"
        }
    
    async def _send_anomaly_alerts(self, anomalies: List[CostAnomaly]) -> Dict[str, Any]:
        """Send anomaly alerts (placeholder for Slack integration)"""
        
        # This would integrate with the SlackIntegrationTool
        critical_anomalies = [a for a in anomalies if a.severity == "CRITICAL"]
        high_anomalies = [a for a in anomalies if a.severity == "HIGH"]
        
        logger.info(f"Would send alerts for {len(critical_anomalies)} critical and {len(high_anomalies)} high severity anomalies")
        
        return {
            "alerts_sent": len(anomalies),
            "critical_alerts": len(critical_anomalies),
            "high_priority_alerts": len(high_anomalies),
            "integration_note": "Slack integration required for actual alert delivery"
        }
    
    async def health_check(self) -> bool:
        """Verify anomaly detection functionality"""
        try:
            # Test data fetching
            test_data = await self._fetch_cost_time_series(7)
            return len(test_data) > 0
        except Exception as e:
            logger.error(f"Anomaly detection health check failed: {e}")
            return False
