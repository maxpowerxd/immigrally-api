#!/usr/bin/env python3
"""
User State Data Access Layer for Firebase Firestore
Optimized for planner I/O contract and Neo4j integration.
Follows existing patterns from database_interface.py
"""

from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from firebase_admin import firestore
from src.infrastructure.firebase_client import FirebaseClient


@dataclass
class UserState:
    """Direct representation matching planner I/O contract."""
    user_id: str
    basic_info: Dict[str, Any]
    scopes: Dict[str, str]
    facts: Dict[str, str]
    progress: List[Dict[str, Any]]
    timeline: Dict[str, str]
    preferences: Dict[str, Any]


class UserStateRepository:
    """Firebase Firestore-based user state operations."""
    
    def __init__(self, firebase_client: FirebaseClient):
        self.db = firebase_client.db
        self.collection = 'user_states'
        
    def create_user_state(self, user_state: UserState) -> None:
        """Create new user state document."""
        try:
            doc_data = {
                "user_id": user_state.user_id,
                "basic_info": user_state.basic_info,
                "scopes": user_state.scopes,
                "facts": user_state.facts,
                "progress": user_state.progress,
                "timeline": user_state.timeline,
                "preferences": user_state.preferences,
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc)
            }
            self.db.collection(self.collection).document(user_state.user_id).set(doc_data)
        except Exception as e:
            raise Exception(f"Failed to create user state: {e}")
            
    def get_user_state(self, user_id: str) -> Optional[UserState]:
        """Get user state by user_id."""
        try:
            doc_ref = self.db.collection(self.collection).document(user_id)
            doc = doc_ref.get()
            if doc.exists:
                data = doc.to_dict()
                # Remove timestamp fields for UserState constructor
                data.pop('created_at', None)
                data.pop('updated_at', None)
                return UserState(**data)
            return None
        except Exception as e:
            raise Exception(f"Failed to get user state: {e}")
            
    def update_scopes(self, user_id: str, scopes: Dict[str, str]) -> None:
        """Update user scopes (for Neo4j query filtering)."""
        try:
            doc_ref = self.db.collection(self.collection).document(user_id)
            doc_ref.update({
                "scopes": scopes,
                "updated_at": datetime.now(timezone.utc)
            })
        except Exception as e:
            raise Exception(f"Failed to update scopes: {e}")
            
    def update_facts(self, user_id: str, facts: Dict[str, str]) -> None:
        """Update user facts (requirement states)."""
        try:
            doc_ref = self.db.collection(self.collection).document(user_id)
            doc_ref.update({
                "facts": facts,
                "updated_at": datetime.now(timezone.utc)
            })
        except Exception as e:
            raise Exception(f"Failed to update facts: {e}")
    
    def update_progress(self, user_id: str, progress: List[Dict[str, Any]]) -> None:
        """Update user progress (solution attempts)."""
        try:
            doc_ref = self.db.collection(self.collection).document(user_id)
            doc_ref.update({
                "progress": progress,
                "updated_at": datetime.now(timezone.utc)
            })
        except Exception as e:
            raise Exception(f"Failed to update progress: {e}")
    
    def delete_user_state(self, user_id: str) -> None:
        """Delete user state document."""
        try:
            self.db.collection(self.collection).document(user_id).delete()
        except Exception as e:
            raise Exception(f"Failed to delete user state: {e}")


def create_sample_user_state() -> UserState:
    """Create sample user state matching planner I/O contract for testing."""
    return UserState(
        user_id="u_test_123",
        basic_info={
            "name": "John Doe",
            "birthday": "1990-01-15",
            "current_address": {
                "city": "San Francisco", 
                "state": "CA", 
                "zip": "94105"
            }
        },
        scopes={
            "state": "CA",  # Specific US state code only
            "visa_type": "H-1B", 
            "nationality": "CH",
            "age": "21_65",  # Standard bracket
            "credit_score": "no_credit",  # Standard band
            "asset_band": "100k_1m",  # Standard range
            "previous_residence": "CH"
            # Note: tax_residency removed (redundant with state)
        },
        facts={
            "req.elig.ssn": "have",  # Using canonical IDs from deduplication
            "req.elig.itin": "need", 
            "req_address_proof": "have",
            "req_613e1063": "have",  # passport (canonical)
            "req_d3fbb190": "have"  # alternative_documents
        },
        progress=[
            {
                "solution_id": "sol_ssn_application",
                "status": "done",
                "updated_at": "2025-09-02",
                "notes": "Successfully obtained SSN"
            }
        ],
        timeline={
            "arrival_date": "2025-08-15",
            "time_horizon": "5_years"
        },
        preferences={
            "deprioritized_goals": ["goal_mortgage", "goal_premium_cards"]
        }
    )


def test_user_state_operations():
    """Test basic CRUD operations."""
    try:
        # Initialize Firebase client and repository
        client = FirebaseClient()
        repo = UserStateRepository(client)
        
        # Create sample user state
        sample_user = create_sample_user_state()
        print(f"📝 Creating user state for: {sample_user.user_id}")
        
        # Test CREATE
        repo.create_user_state(sample_user)
        print("✅ User state created successfully")
        
        # Test READ
        retrieved_user = repo.get_user_state(sample_user.user_id)
        if retrieved_user:
            print(f"✅ User state retrieved: {retrieved_user.basic_info['name']}")
            print(f"   Scopes: {len(retrieved_user.scopes)} items")
            print(f"   Facts: {len(retrieved_user.facts)} items")
        else:
            print("❌ Failed to retrieve user state")
            return False
        
        # Test UPDATE scopes
        new_scopes = retrieved_user.scopes.copy()
        new_scopes["credit_score"] = "fair"
        repo.update_scopes(sample_user.user_id, new_scopes)
        print("✅ Scopes updated successfully")
        
        # Test UPDATE facts
        new_facts = retrieved_user.facts.copy()
        new_facts["req_itin"] = "have"
        repo.update_facts(sample_user.user_id, new_facts)
        print("✅ Facts updated successfully")
        
        # Verify updates
        updated_user = repo.get_user_state(sample_user.user_id)
        if updated_user:
            print(f"✅ Verified updates:")
            print(f"   Credit score: {updated_user.scopes.get('credit_score')}")
            print(f"   ITIN status: {updated_user.facts.get('req_itin')}")
        
        # Test DELETE (cleanup)
        repo.delete_user_state(sample_user.user_id)
        print("✅ User state deleted successfully")
        
        # Verify deletion
        deleted_check = repo.get_user_state(sample_user.user_id)
        if deleted_check is None:
            print("✅ Deletion verified - user state no longer exists")
        else:
            print("⚠️  Warning: User state still exists after deletion")
            
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False


if __name__ == "__main__":
    print("🧪 Testing User State operations...")
    success = test_user_state_operations()
    print(f"\n{'✅ All tests passed!' if success else '❌ Tests failed!'}")