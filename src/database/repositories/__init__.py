"""
Database repositories package.

Provides repository pattern implementation for database operations
with type safety and query optimization.
"""

from .base import BaseRepository
from .opportunity_repository import OpportunityRepository

__all__ = [
    "BaseRepository",
    "OpportunityRepository"
] 