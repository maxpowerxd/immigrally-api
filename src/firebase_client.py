#!/usr/bin/env python3
"""
Firebase client wrapper following existing patterns from database_interface.py
Keeps it simple - uses service account key file for authentication.
"""

import os
import firebase_admin
from firebase_admin import credentials, firestore, auth
from dotenv import load_dotenv

load_dotenv()


class FirebaseClient:
    """Simple Firebase client wrapper."""
    
    def __init__(self):
        # Initialize Firebase Admin SDK with environment variables
        if not firebase_admin._apps:
            # Build service account dictionary from environment variables
            cred_dict = {
                "type": "service_account",
                "project_id": os.getenv("FIREBASE_PROJECT_ID"),
                "private_key_id": os.getenv("FIREBASE_PRIVATE_KEY_ID"),
                "private_key": os.getenv("FIREBASE_PRIVATE_KEY").replace('\\n', '\n') if os.getenv("FIREBASE_PRIVATE_KEY") else None,
                "client_email": os.getenv("FIREBASE_CLIENT_EMAIL"),
                "client_id": os.getenv("FIREBASE_CLIENT_ID"),
                "auth_uri": os.getenv("FIREBASE_AUTH_URI"),
                "token_uri": os.getenv("FIREBASE_TOKEN_URI"),
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{os.getenv('FIREBASE_CLIENT_EMAIL').replace('@', '%40')}" if os.getenv('FIREBASE_CLIENT_EMAIL') else None,
                "universe_domain": "googleapis.com"
            }
            
            # Verify required fields
            required_fields = ["project_id", "private_key", "client_email"]
            missing_fields = [field for field in required_fields if not cred_dict.get(field)]
            if missing_fields:
                raise ValueError(f"Missing required Firebase environment variables: {missing_fields}")
            
            cred = credentials.Certificate(cred_dict)
            firebase_admin.initialize_app(cred)
        
        self.db = firestore.client()
        self.auth = auth


def test_connection():
    """Test Firebase connection."""
    try:
        client = FirebaseClient()
        
        # Test Firestore connection
        collections = list(client.db.collections())
        print(f"✅ Firestore connected successfully. Collections: {len(collections)}")
        
        # Test Auth connection (list users - will be empty but confirms connection)
        try:
            users_page = client.auth.list_users(max_results=1)
            print(f"✅ Firebase Auth connected successfully")
        except Exception as e:
            print(f"⚠️  Firebase Auth warning: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Firebase connection failed: {e}")
        return False


if __name__ == "__main__":
    test_connection()