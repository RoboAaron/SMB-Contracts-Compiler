"""
Opportunity Repository

Provides data access layer for opportunities with both database and mock data support.
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from uuid import UUID, uuid4

from sqlalchemy.orm import Session
from sqlalchemy import text, and_, or_

from .base import BaseRepository
from ..models.opportunity import Opportunity
from ..connection import get_db, test_database_connection

logger = logging.getLogger(__name__)


class OpportunityRepository(BaseRepository[Opportunity]):
    """Repository for opportunity data access with fallback to mock data."""
    
    def __init__(self, session: Optional[Session] = None):
        """Initialize repository with optional session."""
        self.use_database = False
        self.session = None
        
        # Try to initialize with database
        if session:
            self.session = session
            self.use_database = True
        else:
            # Test database connection
            try:
                if test_database_connection():
                    with get_db() as db_session:
                        self.session = db_session
                        self.use_database = True
                        logger.info("OpportunityRepository initialized with database")
                else:
                    logger.warning("Database not available, using mock data")
            except Exception as e:
                logger.warning(f"Database connection failed, using mock data: {e}")
        
        if self.use_database and self.session:
            super().__init__(Opportunity, self.session)
    
    def get_opportunities(
        self,
        portal: Optional[str] = None,
        min_score: Optional[float] = None,
        max_score: Optional[float] = None,
        division: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Get filtered opportunities."""
        if self.use_database:
            return self._get_opportunities_from_db(portal, min_score, max_score, division, limit, offset)
        else:
            return self._get_mock_opportunities(portal, min_score, max_score, division, limit, offset)
    
    def _get_opportunities_from_db(
        self,
        portal: Optional[str] = None,
        min_score: Optional[float] = None,
        max_score: Optional[float] = None,
        division: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Get opportunities from database."""
        try:
            query = self.session.query(Opportunity)
            
            # Apply filters
            if portal:
                query = query.filter(Opportunity.source_portal == portal)
            
            if min_score is not None:
                query = query.filter(Opportunity.advantage_score >= min_score)
            
            if max_score is not None:
                query = query.filter(Opportunity.advantage_score <= max_score)
            
            if division:
                # Filter by division (assuming we have a division field or can match keywords)
                query = query.filter(Opportunity.description.ilike(f"%{division}%"))
            
            # Apply pagination
            query = query.offset(offset).limit(limit)
            
            # Execute query
            opportunities = query.all()
            
            # Convert to dictionaries
            return [self._opportunity_to_dict(opp) for opp in opportunities]
            
        except Exception as e:
            logger.error(f"Error querying opportunities from database: {e}")
            # Fallback to mock data
            return self._get_mock_opportunities(portal, min_score, max_score, division, limit, offset)
    
    def _get_mock_opportunities(
        self,
        portal: Optional[str] = None,
        min_score: Optional[float] = None,
        max_score: Optional[float] = None,
        division: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Generate mock opportunities for testing."""
        # Business divisions for mock data
        business_divisions = {
            "hvac_contractor": {
                "name": "HVAC Contracting",
                "description": "Installation, maintenance, and repair of HVAC systems",
                "keywords": ["HVAC", "heating", "cooling", "ventilation", "air conditioning", "contractor"],
                "contract_value_range": [25000, 2000000]
            },
            "plumbing_contractor": {
                "name": "Plumbing Contracting",
                "description": "Professional plumbing installation and repair services",
                "keywords": ["plumbing", "plumber", "pipes", "water", "contractor"],
                "contract_value_range": [15000, 1500000]
            },
            "safety_supplies": {
                "name": "Safety Equipment & Supplies",
                "description": "Industrial safety equipment and PPE supplies",
                "keywords": ["safety equipment", "PPE", "supplies", "protective gear"],
                "contract_value_range": [5000, 200000]
            },
            "uniforms": {
                "name": "Uniforms & Apparel",
                "description": "Uniform and apparel supply services",
                "keywords": ["uniforms", "apparel", "clothing", "workwear"],
                "contract_value_range": [3000, 150000]
            }
        }
        
        opportunities = []
        portals = ["esbd", "houston", "san_antonio"]
        divisions = list(business_divisions.keys())
        
        for i in range(limit):
            opp_id = offset + i + 1
            selected_portal = portal if portal else portals[i % len(portals)]
            selected_division = division if division else divisions[i % len(divisions)]
            
            # Apply filters
            if portal and selected_portal != portal:
                continue
            
            advantage_score = 0.3 + (i % 7) * 0.1
            if min_score is not None and advantage_score < min_score:
                continue
            if max_score is not None and advantage_score > max_score:
                continue
            
            opportunities.append({
                "id": f"opp_{opp_id:05d}",
                "title": f"Opportunity {opp_id}: {business_divisions[selected_division]['name']} Services",
                "description": f"Mock opportunity {opp_id} for {business_divisions[selected_division]['description']}",
                "portal": selected_portal,
                "issuing_entity": f"City of Austin Department {i % 5 + 1}",
                "estimated_value": 50000 + (i * 10000),
                "due_date": (datetime.now() + timedelta(days=30 + i)).isoformat(),
                "advantage_score": advantage_score,
                "best_division": selected_division,
                "scraped_at": (datetime.now() - timedelta(hours=i)).isoformat(),
                "nigp_codes": ["031-00", "451-10"] if i % 3 == 0 else [],
                "wbe_advantage": i % 4 == 0,
                "keywords_matched": business_divisions[selected_division]["keywords"][:3],
                "source_portal": selected_portal,
                "status": "active"
            })
        
        return opportunities
    
    def _opportunity_to_dict(self, opportunity: Opportunity) -> Dict[str, Any]:
        """Convert Opportunity model to dictionary."""
        return {
            "id": str(opportunity.id),
            "title": opportunity.title,
            "description": opportunity.description_short or opportunity.description_full,
            "portal": opportunity.source_portal,
            "issuing_entity": opportunity.issuing_entity_name,
            "estimated_value": float(opportunity.estimated_value) if opportunity.estimated_value else None,
            "due_date": opportunity.due_date.isoformat() if opportunity.due_date else None,
            "advantage_score": float(opportunity.advantage_score) if opportunity.advantage_score else 0.0,
            "scraped_at": opportunity.last_scraped_at.isoformat() if opportunity.last_scraped_at else None,
            "nigp_codes": [code.nigp_class_code for code in opportunity.nigp_codes] if opportunity.nigp_codes else [],
            "source_portal": opportunity.source_portal,
            "status": opportunity.status,
            "external_id": opportunity.external_id
        }
    
    def store_opportunities(self, opportunities: List[Dict[str, Any]], portal_name: str) -> int:
        """Store opportunities in database or log if using mock data."""
        if not self.use_database:
            logger.info(f"Mock mode: Would store {len(opportunities)} opportunities from {portal_name}")
            return len(opportunities)
        
        try:
            stored_count = 0
            for opp_data in opportunities:
                # Check if opportunity already exists
                existing = self.session.query(Opportunity).filter(
                    Opportunity.external_id == opp_data.get('external_id'),
                    Opportunity.source_portal == portal_name
                ).first()
                
                if not existing:
                    # Create new opportunity (advantage_score is stored in analysis_results table)
                    opportunity = Opportunity(
                        id=uuid4(),
                        title=opp_data.get('title', ''),
                        description_short=opp_data.get('description', ''),
                        external_id=opp_data.get('external_id', ''),
                        source_portal=portal_name,
                        issuing_entity_name=opp_data.get('issuing_entity', ''),
                        estimated_value=opp_data.get('estimated_value'),
                        due_date=datetime.fromisoformat(opp_data['due_date']) if opp_data.get('due_date') else None,
                        last_scraped_at=datetime.now(),
                        status='Open'
                    )
                    self.session.add(opportunity)
                    stored_count += 1
            
            self.session.commit()
            logger.info(f"Stored {stored_count} new opportunities from {portal_name}")
            return stored_count
            
        except Exception as e:
            logger.error(f"Error storing opportunities: {e}")
            self.session.rollback()
            return 0
    
    def get_opportunity_stats(self) -> Dict[str, Any]:
        """Get opportunity statistics."""
        if not self.use_database:
            return {
                "total_opportunities": 150,
                "active_opportunities": 120,
                "recent_opportunities": 25,
                "portals": {
                    "esbd": 50,
                    "houston": 60,
                    "san_antonio": 40
                },
                "mock_data": True
            }
        
        try:
            total = self.session.query(Opportunity).count()
            active = self.session.query(Opportunity).filter(Opportunity.status == 'Open').count()
            
            # Recent opportunities (last 7 days)
            week_ago = datetime.now() - timedelta(days=7)
            recent = self.session.query(Opportunity).filter(
                Opportunity.last_scraped_at >= week_ago
            ).count()
            
            # Portal breakdown
            portal_stats = {}
            for portal in ['esbd', 'houston', 'san_antonio']:
                count = self.session.query(Opportunity).filter(
                    Opportunity.source_portal == portal
                ).count()
                portal_stats[portal] = count
            
            return {
                "total_opportunities": total,
                "active_opportunities": active,
                "recent_opportunities": recent,
                "portals": portal_stats,
                "mock_data": False
            }
            
        except Exception as e:
            logger.error(f"Error getting opportunity stats: {e}")
            # Return mock stats as fallback
            return {
                "total_opportunities": 0,
                "active_opportunities": 0,
                "recent_opportunities": 0,
                "portals": {},
                "error": str(e)
            } 