"""
Base repository class.

Provides common CRUD operations and query utilities for all repositories.
"""

import logging
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar, Union
from uuid import UUID

from sqlalchemy import and_, or_, select, update, delete
from sqlalchemy.orm import Session
from sqlalchemy.sql import Select

from src.database.models.base import Base

logger = logging.getLogger(__name__)

# Type variable for model classes
ModelType = TypeVar("ModelType", bound=Base)


class BaseRepository(Generic[ModelType]):
    """Base repository with common CRUD operations."""
    
    def __init__(self, model: Type[ModelType], session: Session):
        """Initialize repository with model and session."""
        self.model = model
        self.session = session
    
    def create(self, **kwargs) -> ModelType:
        """Create a new model instance."""
        try:
            instance = self.model(**kwargs)
            self.session.add(instance)
            self.session.flush()  # Get the ID without committing
            logger.debug(f"Created {self.model.__name__} with ID: {instance.id}")
            return instance
        except Exception as e:
            logger.error(f"Error creating {self.model.__name__}: {e}")
            raise
    
    def get_by_id(self, id: UUID) -> Optional[ModelType]:
        """Get model instance by ID."""
        try:
            return self.session.get(self.model, id)
        except Exception as e:
            logger.error(f"Error getting {self.model.__name__} by ID {id}: {e}")
            raise
    
    def get_by_ids(self, ids: List[UUID]) -> List[ModelType]:
        """Get multiple model instances by IDs."""
        try:
            stmt = select(self.model).where(self.model.id.in_(ids))
            result = self.session.execute(stmt)
            return list(result.scalars().all())
        except Exception as e:
            logger.error(f"Error getting {self.model.__name__} by IDs: {e}")
            raise
    
    def get_all(self, limit: Optional[int] = None, offset: Optional[int] = None) -> List[ModelType]:
        """Get all model instances with optional pagination."""
        try:
            stmt = select(self.model)
            
            if offset:
                stmt = stmt.offset(offset)
            if limit:
                stmt = stmt.limit(limit)
            
            result = self.session.execute(stmt)
            return list(result.scalars().all())
        except Exception as e:
            logger.error(f"Error getting all {self.model.__name__}: {e}")
            raise
    
    def count(self) -> int:
        """Count total number of model instances."""
        try:
            stmt = select(self.model)
            result = self.session.execute(stmt)
            return len(result.scalars().all())
        except Exception as e:
            logger.error(f"Error counting {self.model.__name__}: {e}")
            raise
    
    def update(self, id: UUID, **kwargs) -> Optional[ModelType]:
        """Update model instance by ID."""
        try:
            instance = self.get_by_id(id)
            if instance:
                for key, value in kwargs.items():
                    if hasattr(instance, key):
                        setattr(instance, key, value)
                self.session.flush()
                logger.debug(f"Updated {self.model.__name__} with ID: {id}")
            return instance
        except Exception as e:
            logger.error(f"Error updating {self.model.__name__} with ID {id}: {e}")
            raise
    
    def delete(self, id: UUID) -> bool:
        """Delete model instance by ID."""
        try:
            instance = self.get_by_id(id)
            if instance:
                self.session.delete(instance)
                self.session.flush()
                logger.debug(f"Deleted {self.model.__name__} with ID: {id}")
                return True
            return False
        except Exception as e:
            logger.error(f"Error deleting {self.model.__name__} with ID {id}: {e}")
            raise
    
    def delete_many(self, ids: List[UUID]) -> int:
        """Delete multiple model instances by IDs."""
        try:
            stmt = delete(self.model).where(self.model.id.in_(ids))
            result = self.session.execute(stmt)
            deleted_count = result.rowcount
            logger.debug(f"Deleted {deleted_count} {self.model.__name__} instances")
            return deleted_count
        except Exception as e:
            logger.error(f"Error deleting multiple {self.model.__name__}: {e}")
            raise
    
    def exists(self, id: UUID) -> bool:
        """Check if model instance exists by ID."""
        try:
            instance = self.get_by_id(id)
            return instance is not None
        except Exception as e:
            logger.error(f"Error checking existence of {self.model.__name__} with ID {id}: {e}")
            raise
    
    def find_by(self, **kwargs) -> List[ModelType]:
        """Find model instances by field values."""
        try:
            conditions = []
            for key, value in kwargs.items():
                if hasattr(self.model, key):
                    conditions.append(getattr(self.model, key) == value)
            
            if not conditions:
                return []
            
            stmt = select(self.model).where(and_(*conditions))
            result = self.session.execute(stmt)
            return list(result.scalars().all())
        except Exception as e:
            logger.error(f"Error finding {self.model.__name__} by criteria: {e}")
            raise
    
    def find_one_by(self, **kwargs) -> Optional[ModelType]:
        """Find single model instance by field values."""
        try:
            instances = self.find_by(**kwargs)
            return instances[0] if instances else None
        except Exception as e:
            logger.error(f"Error finding one {self.model.__name__} by criteria: {e}")
            raise
    
    def find_by_condition(self, condition: Any) -> List[ModelType]:
        """Find model instances by custom condition."""
        try:
            stmt = select(self.model).where(condition)
            result = self.session.execute(stmt)
            return list(result.scalars().all())
        except Exception as e:
            logger.error(f"Error finding {self.model.__name__} by condition: {e}")
            raise
    
    def find_one_by_condition(self, condition: Any) -> Optional[ModelType]:
        """Find single model instance by custom condition."""
        try:
            instances = self.find_by_condition(condition)
            return instances[0] if instances else None
        except Exception as e:
            logger.error(f"Error finding one {self.model.__name__} by condition: {e}")
            raise
    
    def bulk_create(self, instances: List[Dict[str, Any]]) -> List[ModelType]:
        """Create multiple model instances in bulk."""
        try:
            created_instances = []
            for instance_data in instances:
                instance = self.model(**instance_data)
                self.session.add(instance)
                created_instances.append(instance)
            
            self.session.flush()
            logger.debug(f"Bulk created {len(created_instances)} {self.model.__name__} instances")
            return created_instances
        except Exception as e:
            logger.error(f"Error bulk creating {self.model.__name__}: {e}")
            raise
    
    def bulk_update(self, updates: List[Dict[str, Any]]) -> int:
        """Update multiple model instances in bulk."""
        try:
            updated_count = 0
            for update_data in updates:
                id = update_data.pop('id', None)
                if id:
                    instance = self.get_by_id(id)
                    if instance:
                        for key, value in update_data.items():
                            if hasattr(instance, key):
                                setattr(instance, key, value)
                        updated_count += 1
            
            self.session.flush()
            logger.debug(f"Bulk updated {updated_count} {self.model.__name__} instances")
            return updated_count
        except Exception as e:
            logger.error(f"Error bulk updating {self.model.__name__}: {e}")
            raise
    
    def search(self, search_term: str, fields: List[str]) -> List[ModelType]:
        """Search model instances by text in specified fields."""
        try:
            conditions = []
            for field in fields:
                if hasattr(self.model, field):
                    field_attr = getattr(self.model, field)
                    if hasattr(field_attr, 'ilike'):
                        conditions.append(field_attr.ilike(f"%{search_term}%"))
            
            if not conditions:
                return []
            
            stmt = select(self.model).where(or_(*conditions))
            result = self.session.execute(stmt)
            return list(result.scalars().all())
        except Exception as e:
            logger.error(f"Error searching {self.model.__name__}: {e}")
            raise
    
    def paginate(self, page: int = 1, page_size: int = 20) -> Dict[str, Any]:
        """Paginate model instances."""
        try:
            offset = (page - 1) * page_size
            
            # Get total count
            total_count = self.count()
            
            # Get page data
            instances = self.get_all(limit=page_size, offset=offset)
            
            # Calculate pagination info
            total_pages = (total_count + page_size - 1) // page_size
            has_next = page < total_pages
            has_prev = page > 1
            
            return {
                "items": instances,
                "total_count": total_count,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "has_next": has_next,
                "has_prev": has_prev
            }
        except Exception as e:
            logger.error(f"Error paginating {self.model.__name__}: {e}")
            raise
    
    def execute_query(self, query: Select) -> List[ModelType]:
        """Execute a custom query."""
        try:
            result = self.session.execute(query)
            return list(result.scalars().all())
        except Exception as e:
            logger.error(f"Error executing query for {self.model.__name__}: {e}")
            raise
    
    def execute_raw_sql(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute raw SQL query."""
        try:
            result = self.session.execute(sql, params or {})
            return [dict(row._mapping) for row in result]
        except Exception as e:
            logger.error(f"Error executing raw SQL for {self.model.__name__}: {e}")
            raise 