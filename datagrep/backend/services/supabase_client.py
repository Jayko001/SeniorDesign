"""
Supabase Client Service
Handles Supabase connection and operations
"""

from supabase import create_client, Client
import os
from typing import Optional


_supabase_client: Optional[Client] = None


def get_supabase_client() -> Optional[Client]:
    """
    Get or create Supabase client instance
    """
    global _supabase_client
    
    if _supabase_client is None:
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")
        
        if supabase_url and supabase_key:
            _supabase_client = create_client(supabase_url, supabase_key)
    
    return _supabase_client


def init_supabase(url: str, key: str):
    """
    Initialize Supabase client with credentials
    """
    global _supabase_client
    _supabase_client = create_client(url, key)
    return _supabase_client

