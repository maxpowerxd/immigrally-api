#!/usr/bin/env python3
"""
Direct database interface for Neon PostgreSQL research_documents table.
Uses database terminology and structure as the standard.

Schema expectation (research_documents):
- id (uuid)
- source_url (text)
- document_title (text)
- source_name (text)
- reliability_code (text, 1 char)
- reliability_notes (text, nullable)
- content (text)              # cleaned text stored here
- document_date (text, nullable)
- created_at (timestamp)
- faq_source (text, nullable)
- cleaned_ratio (double precision, nullable)
"""

import os
from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv
from typing import List, Dict, Optional
from dataclasses import dataclass

load_dotenv()


@dataclass
class ResearchDocument:
    """Direct representation of database research_documents record."""
    id: str
    source_url: str
    document_title: str
    source_name: str
    reliability_code: str
    reliability_notes: Optional[str]
    content: str                   # cleaned text
    document_date: Optional[str]
    created_at: str
    faq_source: Optional[str]
    cleaned_ratio: Optional[float] = None


class DocumentRepository:
    """Direct database interface using database terminology."""
    
    def __init__(self, connection_string: Optional[str] = None):
        """Initialize with database connection."""
        self.connection_string = connection_string or os.getenv('DATABASE_URL_NEON_POOLED')
        if not self.connection_string:
            raise ValueError("DATABASE_URL_NEON_POOLED environment variable required")
        self.engine = create_engine(self.connection_string)

    def _rows_to_docs(self, result) -> List[ResearchDocument]:
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        docs: List[ResearchDocument] = []
        for _, row in df.iterrows():
            docs.append(ResearchDocument(**row.to_dict()))
        return docs
    
    def get_all_documents(self) -> List[ResearchDocument]:
        """Get all research documents ordered by creation date (desc)."""
        query = """
        SELECT id, source_url, document_title, source_name,
               reliability_code, reliability_notes, content,
               document_date, created_at, faq_source, cleaned_ratio
        FROM research_documents
        ORDER BY created_at DESC
        """
        with self.engine.connect() as connection:
            result = connection.execute(text(query))
            return self._rows_to_docs(result)
    
    def get_documents_by_reliability(self, min_code: str = 'C') -> List[ResearchDocument]:
        """
        Get documents with minimum reliability code (A, B, C, D, E).
        A=5, B=4, C=3, D=2, E=1.
        """
        reliability_order = {'A': 5, 'B': 4, 'C': 3, 'D': 2, 'E': 1}
        min_score = reliability_order.get(min_code.upper(), 3)
        
        query = """
        SELECT id, source_url, document_title, source_name, 
               reliability_code, reliability_notes, content,
               document_date, created_at, faq_source, cleaned_ratio
        FROM research_documents 
        WHERE CASE reliability_code 
                WHEN 'A' THEN 5 
                WHEN 'B' THEN 4 
                WHEN 'C' THEN 3 
                WHEN 'D' THEN 2 
                WHEN 'E' THEN 1 
                ELSE 0 
              END >= :min_score
        ORDER BY 
            CASE reliability_code 
                WHEN 'A' THEN 5 
                WHEN 'B' THEN 4 
                WHEN 'C' THEN 3 
                WHEN 'D' THEN 2 
                WHEN 'E' THEN 1 
                ELSE 0 
            END DESC,
            created_at DESC
        """
        with self.engine.connect() as connection:
            result = connection.execute(text(query), {'min_score': min_score})
            return self._rows_to_docs(result)
    
    def search_documents(self, search_term: str, search_content: bool = True) -> List[ResearchDocument]:
        """
        Search documents by title or content.
        
        Args:
            search_term: Term to search for
            search_content: If True, search both title and content; if False, title only
        """
        if search_content:
            query = """
            SELECT id, source_url, document_title, source_name, 
                   reliability_code, reliability_notes, content,
                   document_date, created_at, faq_source, cleaned_ratio
            FROM research_documents 
            WHERE document_title ILIKE :q OR content ILIKE :q
            ORDER BY 
                CASE reliability_code 
                    WHEN 'A' THEN 5 
                    WHEN 'B' THEN 4 
                    WHEN 'C' THEN 3 
                    WHEN 'D' THEN 2 
                    WHEN 'E' THEN 1 
                    ELSE 0 
                END DESC,
                created_at DESC
            """
        else:
            query = """
            SELECT id, source_url, document_title, source_name, 
                   reliability_code, reliability_notes, content,
                   document_date, created_at, faq_source, cleaned_ratio
            FROM research_documents 
            WHERE document_title ILIKE :q
            ORDER BY 
                CASE reliability_code 
                    WHEN 'A' THEN 5 
                    WHEN 'B' THEN 4 
                    WHEN 'C' THEN 3 
                    WHEN 'D' THEN 2 
                    WHEN 'E' THEN 1 
                    ELSE 0 
                END DESC,
                created_at DESC
            """
        with self.engine.connect() as connection:
            result = connection.execute(text(query), {'q': f'%{search_term}%'})
            return self._rows_to_docs(result)
    
    def get_documents_by_faq(self, faq_source: str) -> List[ResearchDocument]:
        """Get documents for specific FAQ source (ILIKE match)."""
        query = """
        SELECT id, source_url, document_title, source_name, 
               reliability_code, reliability_notes, content,
               document_date, created_at, faq_source, cleaned_ratio
        FROM research_documents 
        WHERE faq_source ILIKE :faq_param
        ORDER BY 
            CASE reliability_code 
                WHEN 'A' THEN 5 
                WHEN 'B' THEN 4 
                WHEN 'C' THEN 3 
                WHEN 'D' THEN 2 
                WHEN 'E' THEN 1 
                ELSE 0 
            END DESC,
            created_at DESC
        """
        with self.engine.connect() as connection:
            result = connection.execute(text(query), {'faq_param': f'%{faq_source}%'})
            return self._rows_to_docs(result)
    
    def get_documents_by_source(self, source_name: str) -> List[ResearchDocument]:
        """Get documents from specific source (ILIKE match)."""
        query = """
        SELECT id, source_url, document_title, source_name, 
               reliability_code, reliability_notes, content,
               document_date, created_at, faq_source, cleaned_ratio
        FROM research_documents 
        WHERE source_name ILIKE :source_param
        ORDER BY created_at DESC
        """
        with self.engine.connect() as connection:
            result = connection.execute(text(query), {'source_param': f'%{source_name}%'})
            return self._rows_to_docs(result)
    
    def get_document_by_id(self, document_id: str) -> Optional[ResearchDocument]:
        """Get specific document by ID."""
        query = """
        SELECT id, source_url, document_title, source_name, 
               reliability_code, reliability_notes, content,
               document_date, created_at, faq_source, cleaned_ratio
        FROM research_documents 
        WHERE id = :doc_id
        """
        with self.engine.connect() as connection:
            result = connection.execute(text(query), {'doc_id': document_id})
            row = result.fetchone()
            if not row:
                return None
            return ResearchDocument(**dict(row._mapping))
    
    def get_database_stats(self) -> Dict[str, any]:
        """Get database statistics."""
        with self.engine.connect() as connection:
            # Total count
            result = connection.execute(text("SELECT COUNT(*) FROM research_documents"))
            total_count = result.scalar()
            
            # By reliability code
            result = connection.execute(text("""
                SELECT reliability_code, COUNT(*) as count 
                FROM research_documents 
                GROUP BY reliability_code 
                ORDER BY count DESC
            """))
            reliability_stats = {row[0] or 'None': row[1] for row in result}
            
            # By source
            result = connection.execute(text("""
                SELECT source_name, COUNT(*) as count 
                FROM research_documents 
                GROUP BY source_name 
                ORDER BY count DESC
                LIMIT 10
            """))
            source_stats = {row[0]: row[1] for row in result}
            
            # By FAQ
            result = connection.execute(text("""
                SELECT faq_source, COUNT(*) as count 
                FROM research_documents 
                WHERE faq_source IS NOT NULL
                GROUP BY faq_source 
                ORDER BY count DESC
                LIMIT 10
            """))
            faq_stats = {row[0]: row[1] for row in result}
            
            return {
                'total_documents': total_count,
                'by_reliability_code': reliability_stats,
                'top_sources': source_stats,
                'top_faq_sources': faq_stats
            }


# Convenience functions
def get_high_quality_documents(min_reliability: str = 'B') -> List[ResearchDocument]:
    repo = DocumentRepository()
    return repo.get_documents_by_reliability(min_reliability)

def search_for_documents(search_term: str) -> List[ResearchDocument]:
    repo = DocumentRepository()
    return repo.search_documents(search_term)


if __name__ == "__main__":
    print("=== DATABASE INTERFACE TEST ===\n")
    try:
        repo = DocumentRepository()
        
        # 1. Load all
        print("1. Loading all documents...")
        docs = repo.get_all_documents()
        print(f"   Found {len(docs)} documents")
        if docs:
            d = docs[0]
            print(f"   Title: {d.document_title}")
            print(f"   Source: {d.source_name}")
            print(f"   Reliability: {d.reliability_code}")
            print(f"   Content length: {len(d.content)}")
            print(f"   cleaned_ratio: {d.cleaned_ratio}")
        
        # 2. High reliability
        print("\n2. Loading high-reliability documents (A-B)...")
        high_docs = repo.get_documents_by_reliability('B')
        print(f"   Found {len(high_docs)}")
        
        # 3. Search by title only
        print("\n3. Searching title for 'credit'...")
        credit_docs = repo.search_documents('credit', search_content=False)
        print(f"   Found {len(credit_docs)}")
        
        # 4. FAQ filter
        print("\n4. Documents for credit history FAQ...")
        faq_docs = repo.get_documents_by_faq('credit history')
        print(f"   Found {len(faq_docs)}")
        
        # 5. Stats
        print("\n5. Database statistics:")
        stats = repo.get_database_stats()
        print(f"   Total documents: {stats['total_documents']}")
        print(f"   Reliability breakdown: {stats['by_reliability_code']}")
        print(f"   Top sources: {list(stats['top_sources'].keys())[:3]}")
        print(f"   Top FAQ sources: {list(stats['top_faq_sources'].keys())[:3]}")
        
        print("\n✅ Database interface test completed successfully!")
    except Exception as e:
        print(f"❌ Database interface test failed: {e}")
        import traceback
        traceback.print_exc()
