#!/usr/bin/env python3
"""
API endpoints for Immigrally Planner
"""

from fastapi import APIRouter, HTTPException
from typing import Optional, List
import os
import sys
from datetime import datetime

# Add parent directories to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))))

from src.planner.api.models import RoadmapRequest, RoadmapResponse, GoalResponse, SolutionResponse, ErrorResponse
from src.planner.planner_core import PlannerCore
from src.infrastructure.firebase_client import FirebaseClient
from src.infrastructure.user_state import UserStateRepository

# Create router
router = APIRouter(prefix="/api/v1", tags=["planner"])

# Initialize dependencies (singleton pattern)
_planner = None

def get_planner():
    """Get or create planner instance"""
    global _planner
    if _planner is None:
        _planner = PlannerCore()
    return _planner


@router.get("/roadmap/{user_id}", response_model=RoadmapResponse)
async def get_roadmap(user_id: str):
    """
    Generate a personalized financial roadmap for a user.

    This endpoint:
    1. Fetches user state from Firebase
    2. Queries Neo4j for relevant goals and solutions
    3. Filters based on user's scopes and facts
    4. Returns prioritized goals with viable solutions
    """
    try:
        planner = get_planner()

        # Get user state from Firebase
        user_state = planner.user_repo.get_user_state(user_id)
        if not user_state:
            raise HTTPException(status_code=404, detail=f"User {user_id} not found")

        # Call the core roadmap function
        result = planner.roadmap(user_state)

        # Transform to API response format
        goals = []
        for goal_data in result.get("goals", []):
            solutions = []
            for sol in goal_data.get("solutions", []):
                solutions.append(SolutionResponse(
                    solution_id=sol["solution_id"],
                    solution_name=sol["solution_name"],
                    solution_description=sol.get("solution_description", ""),
                    strategy_ranking=sol.get("strategy_ranking", 999),
                    user_rationale=sol.get("user_rationale", ""),
                    assessed_claims_count=len(sol.get("viable_assessed_claims", []))
                ))

            goals.append(GoalResponse(
                goal_id=goal_data["goal_id"],
                goal_name=goal_data["goal_name"],
                goal_phase=goal_data.get("goal_phase", ""),
                goal_description=goal_data.get("goal_description", ""),
                solutions=solutions
            ))

        return RoadmapResponse(
            user_id=user_id,
            generated_at=datetime.utcnow().isoformat(),
            total_goals=len(goals),
            goals=goals
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))