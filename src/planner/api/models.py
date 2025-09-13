#!/usr/bin/env python3
"""
Pydantic models for Immigrally Planner API
Request and response schemas
"""

from typing import Dict, List, Any, Optional
from pydantic import BaseModel


class RoadmapRequest(BaseModel):
    """Request model for roadmap generation."""
    user_id: str


class SolutionResponse(BaseModel):
    """Solution data in API response."""
    solution_id: str
    solution_name: str
    solution_description: str
    strategy_ranking: int
    user_rationale: str
    assessed_claims_count: int


class GoalResponse(BaseModel):
    """Goal data in API response."""
    goal_id: str
    goal_name: str
    goal_phase: str
    goal_description: str
    solutions: List[SolutionResponse]


class RoadmapResponse(BaseModel):
    """Complete roadmap response."""
    user_id: str
    generated_at: str
    total_goals: int
    goals: List[GoalResponse]


class ErrorResponse(BaseModel):
    """Error response model."""
    error: str
    detail: Optional[str] = None
    error_code: Optional[str] = None