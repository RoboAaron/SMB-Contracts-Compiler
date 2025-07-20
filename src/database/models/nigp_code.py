"""
NIGPCode model for product classification.

Represents the many-to-many relationship between opportunities and NIGP
(National Institute of Government Purchasing) classification codes
for product line matching and relevance assessment.
"""

from typing import Optional
from uuid import UUID

from sqlalchemy import CheckConstraint, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base


class NIGPCode(Base):
    """Model for NIGP codes associated with opportunities."""
    
    __tablename__ = "nigp_codes"
    
    # Foreign key to opportunity
    opportunity_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("opportunities.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Reference to opportunities table"
    )
    
    # NIGP classification codes
    nigp_class_code: Mapped[str] = mapped_column(
        String(10),
        nullable=False,
        index=True,
        comment="NIGP class code (2-6 digits)"
    )
    
    nigp_item_code: Mapped[Optional[str]] = mapped_column(
        String(10),
        nullable=True,
        index=True,
        comment="NIGP item code (2-6 digits)"
    )
    
    description: Mapped[Optional[str]] = mapped_column(
        String(500),
        nullable=True,
        comment="NIGP code description"
    )
    
    relevance_tier: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default="3",
        index=True,
        comment="1=Perfect match, 2=Related, 3=Ancillary"
    )
    
    # Relationships
    opportunity: Mapped["Opportunity"] = relationship(
        "Opportunity",
        back_populates="nigp_codes",
        lazy="selectin"
    )
    
    # Constraints
    __table_args__ = (
        CheckConstraint(
            "nigp_class_code ~ '^[0-9]{2,6}$'",
            name="nigp_codes_valid_format"
        ),
        CheckConstraint(
            "nigp_item_code IS NULL OR nigp_item_code ~ '^[0-9]{2,6}$'",
            name="nigp_codes_item_format"
        ),
        CheckConstraint(
            "relevance_tier IN (1, 2, 3)",
            name="nigp_codes_valid_relevance_tier"
        ),
    )
    
    def __init__(self, **kwargs):
        """Initialize NIGP code with validation."""
        super().__init__(**kwargs)
        self._validate_class_code()
        self._validate_item_code()
        self._validate_relevance_tier()
    
    def _validate_class_code(self) -> None:
        """Validate NIGP class code format."""
        if self.nigp_class_code:
            import re
            if not re.match(r'^[0-9]{2,6}$', self.nigp_class_code):
                raise ValueError("NIGP class code must be 2-6 digits")
        else:
            raise ValueError("NIGP class code is required")
    
    def _validate_item_code(self) -> None:
        """Validate NIGP item code format."""
        if self.nigp_item_code:
            import re
            if not re.match(r'^[0-9]{2,6}$', self.nigp_item_code):
                raise ValueError("NIGP item code must be 2-6 digits")
    
    def _validate_relevance_tier(self) -> None:
        """Validate relevance tier value."""
        valid_tiers = {1, 2, 3}
        if self.relevance_tier and self.relevance_tier not in valid_tiers:
            raise ValueError(f"Relevance tier must be one of: {valid_tiers}")
    
    @property
    def is_perfect_match(self) -> bool:
        """Check if this is a perfect match (tier 1)."""
        return self.relevance_tier == 1
    
    @property
    def is_related(self) -> bool:
        """Check if this is related (tier 2)."""
        return self.relevance_tier == 2
    
    @property
    def is_ancillary(self) -> bool:
        """Check if this is ancillary (tier 3)."""
        return self.relevance_tier == 3
    
    @property
    def full_code(self) -> str:
        """Get the full NIGP code (class + item if available)."""
        if self.nigp_item_code:
            return f"{self.nigp_class_code}-{self.nigp_item_code}"
        return self.nigp_class_code
    
    @property
    def relevance_description(self) -> str:
        """Get human-readable relevance description."""
        descriptions = {
            1: "Perfect Match",
            2: "Related",
            3: "Ancillary"
        }
        return descriptions.get(self.relevance_tier, "Unknown")
    
    @property
    def class_code_length(self) -> int:
        """Get the length of the class code."""
        return len(self.nigp_class_code)
    
    @property
    def item_code_length(self) -> Optional[int]:
        """Get the length of the item code if present."""
        if self.nigp_item_code:
            return len(self.nigp_item_code)
        return None
    
    def get_class_code_prefix(self, length: int) -> str:
        """Get a prefix of the class code."""
        if length <= 0 or length > len(self.nigp_class_code):
            raise ValueError(f"Invalid prefix length: {length}")
        return self.nigp_class_code[:length]
    
    def get_item_code_prefix(self, length: int) -> Optional[str]:
        """Get a prefix of the item code if present."""
        if not self.nigp_item_code:
            return None
        
        if length <= 0 or length > len(self.nigp_item_code):
            raise ValueError(f"Invalid prefix length: {length}")
        return self.nigp_item_code[:length]
    
    def matches_class_code(self, target_code: str) -> bool:
        """Check if this matches a target class code."""
        return self.nigp_class_code == target_code
    
    def matches_class_code_prefix(self, prefix: str) -> bool:
        """Check if class code starts with the given prefix."""
        return self.nigp_class_code.startswith(prefix)
    
    def matches_item_code(self, target_code: str) -> bool:
        """Check if this matches a target item code."""
        return self.nigp_item_code == target_code
    
    def matches_item_code_prefix(self, prefix: str) -> bool:
        """Check if item code starts with the given prefix."""
        if not self.nigp_item_code:
            return False
        return self.nigp_item_code.startswith(prefix)
    
    def get_description_preview(self, max_length: int = 100) -> Optional[str]:
        """Get a preview of the description."""
        if not self.description:
            return None
        
        description = self.description.strip()
        if len(description) <= max_length:
            return description
        
        return description[:max_length] + "..."
    
    def to_dict(self) -> dict:
        """Convert to dictionary with additional computed fields."""
        base_dict = super().to_dict()
        base_dict.update({
            'full_code': self.full_code,
            'relevance_description': self.relevance_description,
            'is_perfect_match': self.is_perfect_match,
            'is_related': self.is_related,
            'is_ancillary': self.is_ancillary,
            'class_code_length': self.class_code_length,
            'item_code_length': self.item_code_length
        })
        return base_dict
    
    @classmethod
    def create_perfect_match(cls, opportunity_id: UUID, class_code: str, 
                           item_code: Optional[str] = None, 
                           description: Optional[str] = None) -> "NIGPCode":
        """Create a perfect match NIGP code."""
        return cls(
            opportunity_id=opportunity_id,
            nigp_class_code=class_code,
            nigp_item_code=item_code,
            description=description,
            relevance_tier=1
        )
    
    @classmethod
    def create_related(cls, opportunity_id: UUID, class_code: str,
                      item_code: Optional[str] = None,
                      description: Optional[str] = None) -> "NIGPCode":
        """Create a related NIGP code."""
        return cls(
            opportunity_id=opportunity_id,
            nigp_class_code=class_code,
            nigp_item_code=item_code,
            description=description,
            relevance_tier=2
        )
    
    @classmethod
    def create_ancillary(cls, opportunity_id: UUID, class_code: str,
                        item_code: Optional[str] = None,
                        description: Optional[str] = None) -> "NIGPCode":
        """Create an ancillary NIGP code."""
        return cls(
            opportunity_id=opportunity_id,
            nigp_class_code=class_code,
            nigp_item_code=item_code,
            description=description,
            relevance_tier=3
        )
    
    def __repr__(self) -> str:
        """String representation."""
        return (
            f"<NIGPCode(id={self.id}, "
            f"opportunity_id={self.opportunity_id}, "
            f"code='{self.full_code}', "
            f"tier={self.relevance_tier})>"
        ) 