"""
Opportunity model for procurement opportunities.

Represents procurement opportunities from various government portals
with metadata, status tracking, and relationships to documents and analysis.
"""

from datetime import datetime
from decimal import Decimal
from typing import List, Optional
from uuid import UUID

from sqlalchemy import CheckConstraint, String, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB, NUMERIC
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base


class Opportunity(Base):
    """Model for procurement opportunities."""
    
    __tablename__ = "opportunities"
    
    # External identification
    external_id: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        index=True,
        comment="ID from the source portal (ESBD, BeaconBid, etc.)"
    )
    
    source_portal: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        index=True,
        comment="Source system identifier"
    )
    
    # Basic information
    title: Mapped[str] = mapped_column(
        String(500),
        nullable=False,
        comment="Opportunity title/name"
    )
    
    description_short: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Brief description for quick review"
    )
    
    description_full: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Complete opportunity description"
    )
    
    # Entity information
    issuing_entity_name: Mapped[Optional[str]] = mapped_column(
        String(200),
        nullable=True,
        comment="Government entity name"
    )
    
    issuing_entity_id: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True,
        comment="Entity identifier"
    )
    
    # Status and dates
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        server_default="Open",
        index=True,
        comment="Opportunity status"
    )
    
    post_date: Mapped[Optional[datetime]] = mapped_column(
        index=True,
        comment="When the opportunity was posted"
    )
    
    due_date: Mapped[Optional[datetime]] = mapped_column(
        index=True,
        comment="Submission deadline"
    )
    
    # URLs and values
    opportunity_url: Mapped[Optional[str]] = mapped_column(
        String(500),
        nullable=True,
        comment="Direct link to the opportunity"
    )
    
    estimated_value: Mapped[Optional[Decimal]] = mapped_column(
        NUMERIC(15, 2),
        nullable=True,
        comment="Estimated contract value"
    )
    
    # Additional data
    contact_info: Mapped[Optional[dict]] = mapped_column(
        JSONB,
        nullable=True,
        comment="Contact details and requirements"
    )
    
    # Note: Advantage scores are stored in the analysis_results table
    
    last_scraped_at: Mapped[Optional[datetime]] = mapped_column(
        comment="Last successful scraping timestamp"
    )
    
    # Relationships
    documents: Mapped[List["Document"]] = relationship(
        "Document",
        back_populates="opportunity",
        cascade="all, delete-orphan",
        lazy="selectin"
    )
    
    analysis_result: Mapped[Optional["AnalysisResult"]] = relationship(
        "AnalysisResult",
        back_populates="opportunity",
        uselist=False,
        cascade="all, delete-orphan",
        lazy="selectin"
    )
    
    nigp_codes: Mapped[List["NIGPCode"]] = relationship(
        "NIGPCode",
        back_populates="opportunity",
        cascade="all, delete-orphan",
        lazy="selectin"
    )
    
    # Constraints
    __table_args__ = (
        UniqueConstraint(
            "external_id", 
            "source_portal", 
            name="opportunities_external_source_unique"
        ),
        CheckConstraint(
            "estimated_value IS NULL OR estimated_value > 0",
            name="opportunities_positive_value"
        ),
        CheckConstraint(
            "advantage_score IS NULL OR (advantage_score >= 0 AND advantage_score <= 1)",
            name="opportunities_valid_advantage_score"
        ),
        CheckConstraint(
            "post_date IS NULL OR due_date IS NULL OR post_date <= due_date",
            name="opportunities_valid_dates"
        ),
        CheckConstraint(
            "status IN ('Open', 'Closed', 'Awarded', 'Cancelled')",
            name="opportunities_valid_status"
        ),
    )
    
    # Indexes (defined in migration)
    __table_args__ += (
        # These will be created by Alembic migrations
        # Index('idx_opportunities_source_portal', 'source_portal'),
        # Index('idx_opportunities_status', 'status'),
        # Index('idx_opportunities_due_date', 'due_date'),
        # Index('idx_opportunities_post_date', 'post_date'),
        # Index('idx_opportunities_source_status_due', 'source_portal', 'status', 'due_date'),
        # Index('idx_opportunities_status_due', 'status', 'due_date', 
        #       postgresql_where=text("status = 'Open'")),
    )
    
    def __init__(self, **kwargs):
        """Initialize opportunity with validation."""
        # Set default status if not provided
        if 'status' not in kwargs:
            kwargs['status'] = 'Open'
        if 'id' not in kwargs:
            from uuid import uuid4
            kwargs['id'] = uuid4()
        super().__init__(**kwargs)
        self._validate_status()
        self._validate_dates()
        self._validate_value()
    
    def _validate_status(self) -> None:
        """Validate status field."""
        valid_statuses = {'Open', 'Closed', 'Awarded', 'Cancelled'}
        if self.status and self.status not in valid_statuses:
            raise ValueError(f"Status must be one of: {valid_statuses}")
    
    def _validate_dates(self) -> None:
        """Validate date relationships."""
        if self.post_date and self.due_date and self.post_date > self.due_date:
            raise ValueError("Post date cannot be after due date")
    
    def _validate_value(self) -> None:
        """Validate estimated value."""
        if self.estimated_value is not None and self.estimated_value <= 0:
            raise ValueError("Estimated value must be positive")
    
    @property
    def is_open(self) -> bool:
        """Check if opportunity is currently open."""
        return self.status == "Open"
    
    @property
    def is_overdue(self) -> bool:
        """Check if opportunity is overdue."""
        if not self.due_date or self.status != "Open":
            return False
        return datetime.now(self.due_date.tzinfo) > self.due_date
    
    @property
    def days_until_due(self) -> Optional[int]:
        """Calculate days until due date."""
        if not self.due_date:
            return None
        delta = self.due_date - datetime.now(self.due_date.tzinfo)
        return delta.days
    
    def add_document(self, document: "Document") -> None:
        """Add a document to this opportunity, avoiding duplicates by id."""
        # Check if document is already in the list by id
        if not any(doc.id == document.id for doc in self.documents):
            # Add to the documents list - SQLAlchemy will handle the relationship
            self.documents.append(document)
    
    def get_documents_by_status(self, status: str) -> List["Document"]:
        """Get documents by processing status."""
        return [doc for doc in self.documents if doc.processing_status == status]
    
    def get_pending_documents(self) -> List["Document"]:
        """Get documents pending processing."""
        return self.get_documents_by_status("Pending")
    
    def get_completed_documents(self) -> List["Document"]:
        """Get successfully processed documents."""
        return self.get_documents_by_status("Completed")
    
    def has_analysis(self) -> bool:
        """Check if opportunity has been analyzed."""
        return self.analysis_result is not None
    
    def is_relevant(self) -> Optional[bool]:
        """Check if opportunity is relevant to business lines."""
        if not self.analysis_result:
            return None
        return self.analysis_result.is_relevant
    
    def get_advantage_score(self) -> Optional[Decimal]:
        """Get the final advantage score."""
        if not self.analysis_result:
            return None
        return self.analysis_result.final_advantage_score
    
    def get_primary_nigp_codes(self) -> List["NIGPCode"]:
        """Get NIGP codes with relevance tier 1 (perfect matches)."""
        return [code for code in self.nigp_codes if code.relevance_tier == 1]
    
    def get_related_nigp_codes(self) -> List["NIGPCode"]:
        """Get NIGP codes with relevance tier 2 (related)."""
        return [code for code in self.nigp_codes if code.relevance_tier == 2]
    
    def update_scraping_timestamp(self) -> None:
        """Update the last scraping timestamp."""
        self.last_scraped_at = datetime.now()
    
    def __repr__(self) -> str:
        """String representation."""
        return (
            f"<Opportunity(id={self.id}, "
            f"external_id='{self.external_id}', "
            f"source_portal='{self.source_portal}', "
            f"title='{self.title[:50]}...', "
            f"status='{self.status}')>"
        ) 