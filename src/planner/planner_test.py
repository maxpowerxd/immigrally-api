#!/usr/bin/env python3
"""
Planner Test Suite - Comprehensive tests for all planner components
Following ACTION_PLAN_PLANNER.md

Tests:
1. Individual component tests (utils, neo4j, core)
2. Integration tests (full roadmap generation)
3. Edge cases and error conditions
4. Performance and data validation
"""

import sys
import os
import unittest
from unittest.mock import Mock, patch
from typing import Dict, List, Any

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from src.infrastructure.user_state import UserState, create_sample_user_state
from src.planner.planner_core import PlannerCore, SolutionData, GoalData
from src.planner.planner_neo4j import PlannerNeo4j
from src.planner.planner_utils import ScopeValidator, RequirementChecker, PlannerValidationError


class TestScopeValidator(unittest.TestCase):
    """Test ScopeValidator functionality."""
    
    def setUp(self):
        self.validator = ScopeValidator()
        self.sample_user_scopes = {
            "state": "CA",
            "nationality": "CH",
            "visa_type": "H-1B",
            "age": "21_65",
            "credit_score": "no_credit",
            "asset_band": "100k_1m",
            "previous_residence": "CH"
        }
    
    def test_no_constraints_passes(self):
        """Test that no scope constraints allows any user."""
        result = self.validator.is_viable(self.sample_user_scopes, [])
        self.assertTrue(result)
    
    def test_matching_constraints_passes(self):
        """Test that matching scope constraints pass validation."""
        claim_scopes = [
            {"scope_type": "state", "value": "CA"},
            {"scope_type": "visa_type", "value": "H-1B"}
        ]
        result = self.validator.is_viable(self.sample_user_scopes, claim_scopes)
        self.assertTrue(result)
    
    def test_non_matching_constraints_fails(self):
        """Test that non-matching constraints fail validation."""
        claim_scopes = [
            {"scope_type": "state", "value": "NY"}  # User has CA, claim requires NY
        ]
        result = self.validator.is_viable(self.sample_user_scopes, claim_scopes)
        self.assertFalse(result)
    
    def test_provider_scopes_are_optional(self):
        """Test that provider scopes don't block validation."""
        claim_scopes = [
            {"scope_type": "provider", "value": "Chase"}  # Provider is optional
        ]
        result = self.validator.is_viable(self.sample_user_scopes, claim_scopes)
        self.assertTrue(result)
    
    def test_missing_user_scope_fails(self):
        """Test that missing required user scope fails validation."""
        incomplete_scopes = {"state": "CA"}  # Missing nationality
        claim_scopes = [
            {"scope_type": "nationality", "value": "CH"}
        ]
        result = self.validator.is_viable(incomplete_scopes, claim_scopes)
        self.assertFalse(result)
    
    def test_get_missing_scopes(self):
        """Test missing scopes detection."""
        claim_scopes = [
            {"scope_type": "state", "value": "NY"},  # User has CA
            {"scope_type": "visa_type", "value": "H-1B"},  # Matches
            {"scope_type": "provider", "value": "Chase"}  # Optional, ignored
        ]
        missing = self.validator.get_missing_scopes(self.sample_user_scopes, claim_scopes)
        
        self.assertEqual(len(missing), 1)
        self.assertEqual(missing[0]["scope_type"], "state")
        self.assertEqual(missing[0]["required_value"], "NY")
        self.assertEqual(missing[0]["user_value"], "CA")


class TestRequirementChecker(unittest.TestCase):
    """Test RequirementChecker functionality."""
    
    def setUp(self):
        self.checker = RequirementChecker()
        self.sample_user_facts = {
            "req_ssn": "have",
            "req_address_proof": "have",
            "req_passport": "have",
            "req_itin": "need",
            "req_credit_history": "blocked"
        }
    
    def test_no_requirements_passes(self):
        """Test that no requirements always pass."""
        result = self.checker.is_viable(self.sample_user_facts, [])
        self.assertTrue(result)
    
    def test_satisfied_requirements_pass(self):
        """Test that satisfied requirements pass validation."""
        requirements = [
            {"id": "req_ssn", "name": "Social Security Number"},
            {"id": "req_address_proof", "name": "Proof of Address"}
        ]
        result = self.checker.is_viable(self.sample_user_facts, requirements)
        self.assertTrue(result)
    
    def test_unsatisfied_requirements_fail(self):
        """Test that unsatisfied requirements fail validation."""
        requirements = [
            {"id": "req_ssn", "name": "Social Security Number"},  # Have
            {"id": "req_itin", "name": "ITIN"}  # Need - should fail
        ]
        result = self.checker.is_viable(self.sample_user_facts, requirements)
        self.assertFalse(result)
    
    def test_blocked_requirements_fail(self):
        """Test that blocked requirements fail validation."""
        requirements = [
            {"id": "req_credit_history", "name": "Credit History"}  # Blocked
        ]
        result = self.checker.is_viable(self.sample_user_facts, requirements)
        self.assertFalse(result)
    
    def test_untracked_requirements_raise_error(self):
        """Test that untracked requirements raise exceptions."""
        requirements = [
            {"id": "req_unknown", "name": "Unknown Requirement"}
        ]
        with self.assertRaises(Exception) as context:
            self.checker.is_viable(self.sample_user_facts, requirements)
        
        self.assertIn("not tracked in user facts", str(context.exception))
    
    def test_get_missing_requirements(self):
        """Test missing requirements detection."""
        requirements = [
            {"id": "req_ssn", "name": "SSN"},  # Have
            {"id": "req_itin", "name": "ITIN"},  # Need
            {"id": "req_credit_history", "name": "Credit"}  # Blocked
        ]
        missing = self.checker.get_missing_requirements(self.sample_user_facts, requirements)
        
        self.assertEqual(len(missing), 2)
        missing_ids = [req["req_id"] for req in missing]
        self.assertIn("req_itin", missing_ids)
        self.assertIn("req_credit_history", missing_ids)
    
    def test_get_blocked_requirements(self):
        """Test blocked requirements detection."""
        blocked = self.checker.get_blocked_requirements(self.sample_user_facts)
        
        self.assertEqual(len(blocked), 1)
        self.assertEqual(blocked[0], "req_credit_history")


class TestPlannerNeo4j(unittest.TestCase):
    """Test PlannerNeo4j functionality with mocked database."""
    
    def setUp(self):
        # Mock Neo4j connection to avoid requiring live database in tests
        self.mock_driver = Mock()
        self.mock_session = Mock()
        self.mock_driver.session.return_value.__enter__.return_value = self.mock_session
        
    @patch.dict(os.environ, {
        'NEO4J_URI': 'bolt://localhost:7687',
        'NEO4J_USER': 'neo4j',  
        'NEO4J_PASSWORD': 'testpassword'
    })
    @patch('planner.planner_neo4j.GraphDatabase.driver')
    def test_neo4j_initialization(self, mock_graph_db):
        """Test Neo4j connection initialization."""
        mock_graph_db.return_value = self.mock_driver
        
        neo4j = PlannerNeo4j()
        
        mock_graph_db.assert_called_once_with(
            'bolt://localhost:7687',
            auth=('neo4j', 'testpassword')
        )
        self.mock_driver.verify_connectivity.assert_called_once()
    
    @patch.dict(os.environ, {}, clear=True)
    def test_missing_environment_variables_raises_error(self):
        """Test that missing environment variables raise exception."""
        with self.assertRaises(Exception) as context:
            PlannerNeo4j()
        
        self.assertIn("Missing Neo4j environment variables", str(context.exception))
    
    def test_extract_requirement_ids_from_logic_tree(self):
        """Test requirement ID extraction from JSON logic trees."""
        # Create a mock Neo4j instance to access the private method
        with patch.dict(os.environ, {
            'NEO4J_URI': 'bolt://localhost:7687',
            'NEO4J_USER': 'neo4j',
            'NEO4J_PASSWORD': 'testpassword'
        }), patch('planner.planner_neo4j.GraphDatabase.driver') as mock_graph_db:
            mock_graph_db.return_value = self.mock_driver
            neo4j = PlannerNeo4j()
            
            # Test simple logic tree
            simple_tree = '{"op": "has", "id": "req_ssn"}'
            result = neo4j._extract_requirement_ids_from_logic_tree(simple_tree)
            self.assertEqual(result, ["req_ssn"])
            
            # Test complex logic tree
            complex_tree = '''
            {
                "op": "AND",
                "children": [
                    {"op": "has", "id": "req_ssn"},
                    {"op": "OR", "children": [
                        {"op": "has", "id": "req_passport"},
                        {"op": "has", "id": "req_driver_license"}
                    ]}
                ]
            }
            '''
            result = neo4j._extract_requirement_ids_from_logic_tree(complex_tree)
            expected = ["req_ssn", "req_passport", "req_driver_license"]
            self.assertEqual(sorted(result), sorted(expected))
            
            # Test empty/invalid logic tree
            result = neo4j._extract_requirement_ids_from_logic_tree("")
            self.assertEqual(result, [])
            
            result = neo4j._extract_requirement_ids_from_logic_tree("invalid json")
            self.assertEqual(result, [])


class TestPlannerCore(unittest.TestCase):
    """Test PlannerCore functionality with mocked dependencies."""
    
    def setUp(self):
        self.sample_user_state = create_sample_user_state()
    
    @patch('planner.planner_core.PlannerNeo4j')
    @patch('planner.planner_core.FirebaseClient')
    @patch('planner.planner_core.UserStateRepository')
    def test_roadmap_with_no_goals_raises_error(self, mock_user_repo, mock_firebase, mock_neo4j):
        """Test that missing goals raise appropriate error."""
        # Setup mocks
        mock_neo4j_instance = Mock()
        mock_neo4j_instance.get_goals_by_phase.return_value = []
        mock_neo4j.return_value = mock_neo4j_instance
        
        planner = PlannerCore()
        
        with self.assertRaises(Exception) as context:
            planner.roadmap(self.sample_user_state)
        
        self.assertIn("No goals found in database", str(context.exception))
    
    @patch('planner.planner_core.PlannerNeo4j')
    @patch('planner.planner_core.FirebaseClient')
    @patch('planner.planner_core.UserStateRepository')
    def test_roadmap_filters_non_viable_solutions(self, mock_user_repo, mock_firebase, mock_neo4j):
        """Test that roadmap correctly filters out non-viable solutions."""
        # Setup mock data
        mock_goals = [{"id": "goal_1", "name": "Test Goal", "phase": "ARRIVE", "description": "Test"}]
        mock_solutions = [{"id": "sol_1", "name": "Test Solution", "description": "Test"}]
        mock_claims = ["claim_1"]
        mock_graphlet = {
            "assessed_claim": {"id": "claim_1", "outcome": "consensus"},
            "scopes": [{"scope_type": "state", "value": "NY"}],  # User has CA, this requires NY
            "requirements": [],
            "qualifiers": [],
            "clauses": []
        }
        mock_strategy = {"ranking_rules": ["sol_1"], "user_rationale": "Test strategy"}
        
        # Setup Neo4j mock
        mock_neo4j_instance = Mock()
        mock_neo4j_instance.get_goals_by_phase.return_value = mock_goals
        mock_neo4j_instance.get_solutions_for_goal.return_value = mock_solutions
        mock_neo4j_instance.get_assessed_claims_for_solution.return_value = mock_claims
        mock_neo4j_instance.get_complete_graphlet.return_value = mock_graphlet
        mock_neo4j_instance.get_strategy_for_goal.return_value = mock_strategy
        mock_neo4j.return_value = mock_neo4j_instance
        
        planner = PlannerCore()
        roadmap = planner.roadmap(self.sample_user_state)
        
        # Should have 0 goals because solution was filtered out due to scope mismatch
        self.assertEqual(roadmap["total_goals"], 0)
        self.assertEqual(len(roadmap["goals"]), 0)


class TestPlannerIntegration(unittest.TestCase):
    """Integration tests requiring live database connection."""
    
    def setUp(self):
        self.sample_user_state = create_sample_user_state()
    
    def test_full_planner_integration(self):
        """
        Full integration test with live database.
        
        Note: This test requires:
        - Neo4j running with test data
        - Firebase configured
        - Environment variables set
        
        Skip if not in integration test environment.
        """
        # Check if we're in integration test mode
        if not os.getenv("RUN_INTEGRATION_TESTS"):
            self.skipTest("Integration tests disabled - set RUN_INTEGRATION_TESTS=1 to enable")
        
        try:
            planner = PlannerCore()
            
            try:
                roadmap = planner.roadmap(self.sample_user_state)
                
                # Validate roadmap structure
                self.assertIsInstance(roadmap, dict)
                self.assertIn("user_id", roadmap)
                self.assertIn("total_goals", roadmap)
                self.assertIn("goals", roadmap)
                self.assertIsInstance(roadmap["goals"], list)
                
                # Validate goal structure if any goals exist
                if roadmap["goals"]:
                    goal = roadmap["goals"][0]
                    required_keys = ["goal_id", "goal_name", "goal_phase", "solutions"]
                    for key in required_keys:
                        self.assertIn(key, goal)
                    
                    # Validate solution structure if any solutions exist
                    if goal["solutions"]:
                        solution = goal["solutions"][0]
                        required_solution_keys = [
                            "solution_id", "solution_name", "strategy_ranking", 
                            "user_rationale", "assessed_claims_count"
                        ]
                        for key in required_solution_keys:
                            self.assertIn(key, solution)
                
                print(f"✅ Integration test passed: {roadmap['total_goals']} goals generated")
                
            finally:
                planner.close()
                
        except Exception as e:
            self.fail(f"Integration test failed: {e}")


def run_unit_tests():
    """Run unit tests only (no database required)."""
    print("🧪 Running Planner Unit Tests...")
    
    # Create test suite with unit tests only
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add unit test classes
    suite.addTests(loader.loadTestsFromTestCase(TestScopeValidator))
    suite.addTests(loader.loadTestsFromTestCase(TestRequirementChecker))
    suite.addTests(loader.loadTestsFromTestCase(TestPlannerNeo4j))
    suite.addTests(loader.loadTestsFromTestCase(TestPlannerCore))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()


def run_integration_tests():
    """Run integration tests (requires live database)."""
    print("🧪 Running Planner Integration Tests...")
    
    # Set integration test flag
    os.environ["RUN_INTEGRATION_TESTS"] = "1"
    
    try:
        loader = unittest.TestLoader()
        suite = unittest.TestSuite()
        
        # Add integration test class
        suite.addTests(loader.loadTestsFromTestCase(TestPlannerIntegration))
        
        # Run tests
        runner = unittest.TextTestRunner(verbosity=2)
        result = runner.run(suite)
        
        return result.wasSuccessful()
        
    finally:
        # Clean up environment
        os.environ.pop("RUN_INTEGRATION_TESTS", None)


def run_all_tests():
    """Run both unit and integration tests."""
    print("🚀 Running All Planner Tests...")
    
    unit_success = run_unit_tests()
    integration_success = run_integration_tests()
    
    overall_success = unit_success and integration_success
    
    print(f"\n📊 Test Results:")
    print(f"   Unit Tests: {'✅ PASSED' if unit_success else '❌ FAILED'}")
    print(f"   Integration Tests: {'✅ PASSED' if integration_success else '❌ FAILED'}")
    print(f"   Overall: {'✅ ALL TESTS PASSED' if overall_success else '❌ SOME TESTS FAILED'}")
    
    return overall_success


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run planner tests")
    parser.add_argument("--unit", action="store_true", help="Run unit tests only")
    parser.add_argument("--integration", action="store_true", help="Run integration tests only")
    args = parser.parse_args()
    
    if args.unit:
        success = run_unit_tests()
    elif args.integration:
        success = run_integration_tests()
    else:
        success = run_all_tests()
    
    sys.exit(0 if success else 1)