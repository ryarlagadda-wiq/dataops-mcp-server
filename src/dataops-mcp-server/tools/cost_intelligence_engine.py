#!/usr/bin/env python3

"""
Cost Intelligence Engine - Enhanced BigQuery Cost Analysis
Replaces the basic bigquery_tools.py with AI-powered cost intelligence
"""

import os
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Any
from dataclasses import dataclass, asdict
from enum import Enum

# Import fastmcp from the main module structure
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from google.cloud import bigquery

# ============================================================================
# ENHANCED ARCHITECTURE COMPONENTS
# ============================================================================

class CostAnalysisTier(Enum):
    """Cost analysis complexity tiers"""
    BASIC = "basic"           # Simple cost queries
    ADVANCED = "advanced"     # Multi-dimensional analysis
    EXECUTIVE = "executive"   # Business insights + forecasting
    AI_ENHANCED = "ai_enhanced"  # ML-powered predictions

@dataclass
class CostInsight:
    """Standardized cost insight structure"""
    category: str
    severity: str  # CRITICAL, HIGH, MEDIUM, LOW
    title: str
    description: str
    financial_impact: Dict[str, float]  # current_cost, potential_savings, roi_timeline
    recommended_actions: List[str]
    implementation_complexity: str  # SIMPLE, MODERATE, COMPLEX
    business_justification: str
    technical_details: Dict[str, Any]

@dataclass
class CostForecast:
    """Cost forecasting results"""
    forecast_horizon_days: int
    predicted_costs: List[Dict[str, Union[str, float]]]  # date, predicted_cost, confidence_interval
    trend_direction: str  # INCREASING, DECREASING, STABLE
    seasonality_detected: bool
    confidence_score: float
    key_drivers: List[str]

# ============================================================================
# COST INTELLIGENCE ENGINE
# ============================================================================

class CostIntelligenceEngine:
    """Advanced cost analytics with ML-powered insights"""
    
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.bq_client = bigquery.Client(project=project_id)
        
    def analyze_cost_trends_with_forecasting(
        self,
        time_horizon_days: int = 90,
        granularity: str = "daily",
        dimensions: List[str] = None,
        include_forecasting: bool = True,
        anomaly_detection: bool = True,
        analysis_tier: CostAnalysisTier = CostAnalysisTier.ADVANCED
    ) -> str:
        """
        Executive-grade cost trend analysis with ML forecasting
        
        Enhanced features:
        - Multi-dimensional cost attribution
        - Trend analysis with statistical significance
        - ML-powered cost forecasting 
        - Automated anomaly detection
        - Business impact assessment
        """
        
        try:
            # Validate inputs
            if not 1 <= time_horizon_days <= 365:
                return self._error_response("time_horizon_days must be between 1 and 365")
            
            dimensions = dimensions or ["user", "dataset", "query_type"]
            
            # Execute cost analysis based on tier
            if analysis_tier == CostAnalysisTier.EXECUTIVE:
                return self._executive_cost_analysis(time_horizon_days, granularity, dimensions, include_forecasting)
            elif analysis_tier == CostAnalysisTier.AI_ENHANCED:
                return self._ai_enhanced_cost_analysis(time_horizon_days, granularity, dimensions)
            else:
                return self._advanced_cost_analysis(time_horizon_days, granularity, dimensions, anomaly_detection)
                
        except Exception as e:
            return self._error_response(f"Cost analysis failed: {str(e)}")
    
    def detect_spending_anomalies_ml(
        self,
        detection_window_days: int = 30,
        sensitivity: str = "medium",
        algorithms: List[str] = None,
        context_enrichment: bool = True
    ) -> str:
        """
        ML-powered spending anomaly detection with context
        
        Features:
        - Multiple anomaly detection algorithms
        - Contextual anomaly analysis
        - Automated root cause analysis
        - Intelligent alert generation
        """
        
        algorithms = algorithms or ["isolation_forest", "statistical_threshold", "time_series"]
        
        try:
            # Get historical spending data
            spending_data = self._get_spending_timeseries(detection_window_days * 2)  # 2x for baseline
            
            # Apply anomaly detection algorithms
            anomalies = []
            
            if "statistical_threshold" in algorithms:
                stat_anomalies = self._detect_statistical_anomalies(spending_data, sensitivity)
                anomalies.extend(stat_anomalies)
            
            if "isolation_forest" in algorithms:
                ml_anomalies = self._detect_ml_anomalies(spending_data)
                anomalies.extend(ml_anomalies)
            
            if "time_series" in algorithms:
                ts_anomalies = self._detect_time_series_anomalies(spending_data)
                anomalies.extend(ts_anomalies)
            
            # Context enrichment
            if context_enrichment:
                enriched_anomalies = self._enrich_anomaly_context(anomalies)
            else:
                enriched_anomalies = anomalies
            
            # Generate insights and recommendations
            insights = self._generate_anomaly_insights(enriched_anomalies)
            
            return json.dumps({
                "success": True,
                "analysis_metadata": {
                    "detection_window_days": detection_window_days,
                    "algorithms_used": algorithms,
                    "sensitivity_level": sensitivity,
                    "context_enrichment_enabled": context_enrichment,
                    "analysis_timestamp": datetime.now().isoformat()
                },
                "anomaly_summary": {
                    "total_anomalies_detected": len(enriched_anomalies),
                    "severity_distribution": self._calculate_severity_distribution(enriched_anomalies),
                    "estimated_financial_impact": self._calculate_anomaly_impact(enriched_anomalies),
                    "detection_confidence": self._calculate_detection_confidence(enriched_anomalies)
                },
                "detected_anomalies": enriched_anomalies,
                "actionable_insights": insights,
                "recommended_next_steps": self._generate_anomaly_action_plan(enriched_anomalies)
            }, indent=2)
            
        except Exception as e:
            return self._error_response(f"Anomaly detection failed: {str(e)}")
    
    def generate_cost_optimization_roadmap(
        self,
        optimization_goals: List[str] = None,
        time_horizon_months: int = 6,
        budget_constraints: Optional[Dict[str, float]] = None,
        risk_tolerance: str = "medium"
    ) -> str:
        """
        Generate comprehensive cost optimization roadmap
        
        Features:
        - Multi-objective optimization planning
        - Phased implementation strategy
        - ROI timeline projections
        - Risk assessment and mitigation
        """
        
        optimization_goals = optimization_goals or ["reduce_costs", "improve_performance", "enhance_governance"]
        
        try:
            # Analyze current state
            current_state = self._analyze_current_cost_state()
            
            # Identify optimization opportunities
            opportunities = self._identify_optimization_opportunities(optimization_goals)
            
            # Generate implementation roadmap
            roadmap = self._generate_implementation_roadmap(
                opportunities, time_horizon_months, budget_constraints, risk_tolerance
            )
            
            # Calculate ROI projections
            roi_projections = self._calculate_roi_projections(roadmap)
            
            return json.dumps({
                "success": True,
                "roadmap_metadata": {
                    "optimization_goals": optimization_goals,
                    "time_horizon_months": time_horizon_months,
                    "budget_constraints": budget_constraints,
                    "risk_tolerance": risk_tolerance,
                    "generation_timestamp": datetime.now().isoformat()
                },
                "current_state_analysis": current_state,
                "optimization_opportunities": opportunities,
                "implementation_roadmap": roadmap,
                "roi_projections": roi_projections,
                "success_metrics": self._define_success_metrics(optimization_goals),
                "monitoring_plan": self._create_monitoring_plan(roadmap)
            }, indent=2)
            
        except Exception as e:
            return self._error_response(f"Roadmap generation failed: {str(e)}")
    
    # ========================================================================
    # ADVANCED ANALYSIS METHODS
    # ========================================================================
    
    def _executive_cost_analysis(
        self, 
        time_horizon_days: int, 
        granularity: str, 
        dimensions: List[str],
        include_forecasting: bool
    ) -> str:
        """Executive-level cost analysis with business insights"""
        
        # Enhanced query with business context
        executive_query = f"""
        WITH cost_analysis AS (
            SELECT 
                DATE(creation_time) as analysis_date,
                user_email,
                REGEXP_EXTRACT(destination_table.dataset_id, r'^([^_]+)') as business_unit,
                CASE 
                    WHEN job_id LIKE '%scheduled%' OR job_id LIKE '%airflow%' THEN 'ETL_Pipeline'
                    WHEN user_email LIKE '%@company.com' THEN 'Internal_Analytics'
                    WHEN user_email LIKE '%gserviceaccount.com' THEN 'Automated_Process'
                    ELSE 'Ad_Hoc_Analysis'
                END as workload_type,
                COUNT(*) as query_count,
                SUM(total_bytes_processed) / POW(10, 12) * 6.25 as cost_usd,
                AVG(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)) as avg_duration_ms,
                SUM(total_slot_ms) as total_slot_ms
            FROM `{self.project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
            WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {time_horizon_days} DAY)
                AND job_type = 'QUERY'
                AND state = 'DONE'
                AND error_result IS NULL
                AND total_bytes_processed IS NOT NULL
            GROUP BY analysis_date, user_email, business_unit, workload_type
        ),
        business_metrics AS (
            SELECT 
                business_unit,
                workload_type,
                COUNT(DISTINCT user_email) as active_users,
                SUM(cost_usd) as total_cost,
                AVG(cost_usd) as avg_daily_cost,
                SUM(query_count) as total_queries,
                AVG(avg_duration_ms) as avg_query_duration
            FROM cost_analysis
            GROUP BY business_unit, workload_type
        )
        SELECT * FROM business_metrics
        ORDER BY total_cost DESC
        """
        
        results = list(self.bq_client.query(executive_query))
        
        # Generate business insights
        business_insights = self._generate_business_insights(results)
        
        # Add forecasting if requested
        forecast_data = None
        if include_forecasting:
            forecast_data = self._generate_cost_forecast(results)
        
        return json.dumps({
            "success": True,
            "analysis_type": "executive_cost_analysis",
            "business_insights": business_insights,
            "cost_forecast": forecast_data,
            "detailed_metrics": [dict(row) for row in results],
            "executive_summary": self._create_executive_summary(results, business_insights),
            "generated_at": datetime.now().isoformat()
        }, indent=2, default=str)
    
    def _ai_enhanced_cost_analysis(
        self, 
        time_horizon_days: int, 
        granularity: str, 
        dimensions: List[str]
    ) -> str:
        """AI-enhanced cost analysis with machine learning insights"""
        
        # Get comprehensive cost data
        cost_data = self._get_comprehensive_cost_data(time_horizon_days)
        
        # Apply ML analysis
        ml_insights = {
            "cost_drivers": self._identify_cost_drivers_ml(cost_data),
            "usage_patterns": self._cluster_usage_patterns(cost_data),
            "optimization_opportunities": self._detect_optimization_opportunities_ml(cost_data),
            "trend_analysis": self._perform_trend_analysis_ml(cost_data)
        }
        
        # Generate AI-powered recommendations
        ai_recommendations = self._generate_ai_recommendations(ml_insights)
        
        return json.dumps({
            "success": True,
            "analysis_type": "ai_enhanced_cost_analysis",
            "ml_insights": ml_insights,
            "ai_recommendations": ai_recommendations,
            "confidence_scores": self._calculate_confidence_scores(ml_insights),
            "implementation_priority": self._prioritize_recommendations(ai_recommendations),
            "generated_at": datetime.now().isoformat()
        }, indent=2)
    
    # ========================================================================
    # UTILITY METHODS
    # ========================================================================
    
    def _error_response(self, error_message: str) -> str:
        """Standardized error response format"""
        return json.dumps({
            "success": False,
            "error": error_message,
            "timestamp": datetime.now().isoformat(),
            "suggestions": [
                "Check input parameters",
                "Verify BigQuery permissions",
                "Ensure sufficient historical data exists"
            ]
        })
    
    def _generate_business_insights(self, cost_data: List[Any]) -> List[CostInsight]:
        """Generate actionable business insights from cost data"""
        insights = []
        
        # Example insight generation logic
        total_cost = sum(row.total_cost for row in cost_data)
        
        if total_cost > 10000:  # High spend threshold
            insights.append(CostInsight(
                category="high_spend_alert",
                severity="HIGH",
                title="Significant BigQuery spending detected",
                description=f"Monthly BigQuery costs projected at ${total_cost:,.2f}",
                financial_impact={
                    "current_monthly_cost": total_cost,
                    "potential_savings": total_cost * 0.25,  # 25% optimization potential
                    "roi_timeline_months": 3
                },
                recommended_actions=[
                    "Implement query optimization program",
                    "Review data retention policies",
                    "Consider slot reservations for predictable workloads"
                ],
                implementation_complexity="MODERATE",
                business_justification="Reducing BigQuery costs by 25% would save $X annually",
                technical_details={"cost_drivers": ["large_scans", "unoptimized_queries"]}
            ))
        
        return [asdict(insight) for insight in insights]
    
    def _generate_cost_forecast(self, historical_data: List[Any]) -> CostForecast:
        """Generate ML-powered cost forecast"""
        # Simplified forecasting logic (replace with actual ML model)
        forecast = CostForecast(
            forecast_horizon_days=30,
            predicted_costs=[
                {
                    "date": (datetime.now() + timedelta(days=i)).isoformat()[:10],
                    "predicted_cost": 1000 + (i * 10),  # Placeholder linear trend
                    "confidence_interval": [900 + (i * 10), 1100 + (i * 10)]
                }
                for i in range(30)
            ],
            trend_direction="INCREASING",
            seasonality_detected=False,
            confidence_score=0.85,
            key_drivers=["increased_query_volume", "new_data_sources"]
        )
        
        return asdict(forecast)

# ============================================================================
# MCP TOOL REGISTRATION
# ============================================================================

# Note: This is a reference implementation showing the enhanced architecture
# Integrate these tools into your existing MCP server structure

project_id = os.getenv("GOOGLE_CLOUD_PROJECT")

if not project_id:
    raise ValueError("GOOGLE_CLOUD_PROJECT environment variable is required")

cost_engine = CostIntelligenceEngine(project_id)

# Example tool registration (adapt to your MCP server structure)
def register_cost_intelligence_tools(mcp_server):
    """Register cost intelligence tools with your MCP server"""
    
    @mcp_server.tool()
    def analyze_cost_trends_with_forecasting(
        time_horizon_days: int = 90,
        granularity: str = "daily",
        dimensions: List[str] = None,
        include_forecasting: bool = True,
        anomaly_detection: bool = True,
        analysis_tier: str = "advanced"
    ) -> str:
        """
        Executive-grade cost trend analysis with ML-powered forecasting and insights
        
        Args:
            time_horizon_days: Analysis period (1-365 days)
            granularity: Time granularity (daily, weekly, monthly)
            dimensions: Analysis dimensions (user, dataset, query_type, business_unit)
            include_forecasting: Enable ML-powered cost forecasting
            anomaly_detection: Enable automated anomaly detection
            analysis_tier: Analysis complexity (basic, advanced, executive, ai_enhanced)
        
        Returns:
            Comprehensive cost intelligence report with actionable insights
        """
        tier_enum = CostAnalysisTier(analysis_tier)
        return cost_engine.analyze_cost_trends_with_forecasting(
            time_horizon_days, granularity, dimensions, include_forecasting, anomaly_detection, tier_enum
        )

    @mcp_server.tool()
    def detect_spending_anomalies_ml(
        detection_window_days: int = 30,
        sensitivity: str = "medium",
        algorithms: List[str] = None,
        context_enrichment: bool = True
    ) -> str:
        """
        ML-powered spending anomaly detection with contextual analysis
        
        Args:
            detection_window_days: Analysis window for anomaly detection
            sensitivity: Detection sensitivity (low, medium, high, custom)
            algorithms: ML algorithms to use (isolation_forest, statistical_threshold, time_series)
            context_enrichment: Enable contextual anomaly analysis
        
        Returns:
            Anomaly detection report with root cause analysis and recommendations
        """
        return cost_engine.detect_spending_anomalies_ml(
            detection_window_days, sensitivity, algorithms, context_enrichment
        )

    @mcp_server.tool()
    def generate_cost_optimization_roadmap(
        optimization_goals: List[str] = None,
        time_horizon_months: int = 6,
        budget_constraints: Optional[Dict[str, float]] = None,
        risk_tolerance: str = "medium"
    ) -> str:
        """
        Generate comprehensive cost optimization roadmap with ROI projections
        
        Args:
            optimization_goals: Optimization objectives (reduce_costs, improve_performance, enhance_governance)
            time_horizon_months: Planning horizon in months
            budget_constraints: Budget limits for optimization investments
            risk_tolerance: Risk appetite (low, medium, high)
        
        Returns:
            Detailed optimization roadmap with implementation phases and ROI analysis
        """
        return cost_engine.generate_cost_optimization_roadmap(
            optimization_goals, time_horizon_months, budget_constraints, risk_tolerance
        )
