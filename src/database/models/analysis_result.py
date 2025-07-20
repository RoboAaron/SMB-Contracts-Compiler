"""
AnalysisResult model for AI analysis outcomes.

Stores the results of AI analysis including relevance assessment,
advantage detection, and proprietary scoring for procurement opportunities.
"""

from datetime import datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID

from sqlalchemy import Boolean, CheckConstraint, ForeignKey, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, NUMERIC, UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base


class AnalysisResult(Base):
    """Model for AI analysis results of opportunities."""
    
    __tablename__ = "analysis_results"
    
    # Foreign key to opportunity
    opportunity_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("opportunities.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Reference to opportunities table"
    )
    
    # Relevance assessment
    is_relevant: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        index=True,
        comment="Whether opportunity matches business lines"
    )
    
    primary_category: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True,
        comment="Main product category match"
    )
    
    relevance_score: Mapped[Optional[Decimal]] = mapped_column(
        NUMERIC(3, 2),
        nullable=True,
        comment="0.0-1.0 relevance score"
    )
    
    # Advantage detection
    advantage_opportunity_found: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default="false",
        index=True,
        comment="Whether WBE/HUB advantages detected"
    )
    
    advantage_type: Mapped[Optional[str]] = mapped_column(
        String(50),
        nullable=True,
        comment="Type of advantage (HUB, MWBE, SCTRCA, etc.)"
    )
    
    goal_percentage: Mapped[Optional[Decimal]] = mapped_column(
        NUMERIC(5, 2),
        nullable=True,
        comment="M/WBE participation goal percentage"
    )
    
    # Analysis summaries
    advantage_summary: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Detailed advantage analysis"
    )
    
    final_briefing_summary: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Executive summary for decision making"
    )
    
    actionable_insight: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Specific action items and recommendations"
    )
    
    # Final scoring
    final_advantage_score: Mapped[Optional[Decimal]] = mapped_column(
        NUMERIC(3, 2),
        nullable=True,
        index=True,
        comment="0.0-1.0 proprietary advantage score"
    )
    
    # Metadata
    analysis_metadata: Mapped[Optional[dict]] = mapped_column(
        JSONB,
        nullable=True,
        comment="AI model parameters and processing details"
    )
    
    analysis_timestamp: Mapped[datetime] = mapped_column(
        server_default="now()",
        nullable=False,
        comment="When analysis was performed"
    )
    
    # Relationships
    opportunity: Mapped["Opportunity"] = relationship(
        "Opportunity",
        back_populates="analysis_result",
        lazy="selectin"
    )
    
    # Constraints
    __table_args__ = (
        UniqueConstraint(
            "opportunity_id",
            name="analysis_results_one_per_opportunity"
        ),
        CheckConstraint(
            "relevance_score IS NULL OR (relevance_score >= 0.0 AND relevance_score <= 1.0)",
            name="analysis_results_valid_relevance_score"
        ),
        CheckConstraint(
            "goal_percentage IS NULL OR (goal_percentage >= 0.0 AND goal_percentage <= 100.0)",
            name="analysis_results_valid_goal_percentage"
        ),
        CheckConstraint(
            "final_advantage_score IS NULL OR (final_advantage_score >= 0.0 AND final_advantage_score <= 1.0)",
            name="analysis_results_valid_advantage_score"
        ),
    )
    
    def __init__(self, **kwargs):
        """Initialize analysis result with validation."""
        super().__init__(**kwargs)
        self._validate_scores()
        self._validate_goal_percentage()
    
    def _validate_scores(self) -> None:
        """Validate score ranges."""
        if self.relevance_score is not None:
            if not (0.0 <= self.relevance_score <= 1.0):
                raise ValueError("Relevance score must be between 0.0 and 1.0")
        
        if self.final_advantage_score is not None:
            if not (0.0 <= self.final_advantage_score <= 1.0):
                raise ValueError("Final advantage score must be between 0.0 and 1.0")
    
    def _validate_goal_percentage(self) -> None:
        """Validate goal percentage range."""
        if self.goal_percentage is not None:
            if not (0.0 <= self.goal_percentage <= 100.0):
                raise ValueError("Goal percentage must be between 0.0 and 100.0")
    
    @property
    def has_high_relevance(self) -> bool:
        """Check if opportunity has high relevance (score >= 0.7)."""
        if self.relevance_score is None:
            return False
        return self.relevance_score >= 0.7
    
    @property
    def has_medium_relevance(self) -> bool:
        """Check if opportunity has medium relevance (0.4 <= score < 0.7)."""
        if self.relevance_score is None:
            return False
        return 0.4 <= self.relevance_score < 0.7
    
    @property
    def has_low_relevance(self) -> bool:
        """Check if opportunity has low relevance (score < 0.4)."""
        if self.relevance_score is None:
            return False
        return self.relevance_score < 0.4
    
    @property
    def has_high_advantage(self) -> bool:
        """Check if opportunity has high advantage (score >= 0.8)."""
        if self.final_advantage_score is None:
            return False
        return self.final_advantage_score >= 0.8
    
    @property
    def has_medium_advantage(self) -> bool:
        """Check if opportunity has medium advantage (0.5 <= score < 0.8)."""
        if self.final_advantage_score is None:
            return False
        return 0.5 <= self.final_advantage_score < 0.8
    
    @property
    def has_low_advantage(self) -> bool:
        """Check if opportunity has low advantage (score < 0.5)."""
        if self.final_advantage_score is None:
            return False
        return self.final_advantage_score < 0.5
    
    @property
    def is_high_priority(self) -> bool:
        """Check if opportunity is high priority (relevant and high advantage)."""
        return self.is_relevant and self.has_high_advantage
    
    @property
    def is_medium_priority(self) -> bool:
        """Check if opportunity is medium priority (relevant and medium advantage)."""
        return self.is_relevant and self.has_medium_advantage
    
    @property
    def is_low_priority(self) -> bool:
        """Check if opportunity is low priority (relevant and low advantage)."""
        return self.is_relevant and self.has_low_advantage
    
    @property
    def priority_level(self) -> str:
        """Get priority level as string."""
        if not self.is_relevant:
            return "Not Relevant"
        elif self.has_high_advantage:
            return "High Priority"
        elif self.has_medium_advantage:
            return "Medium Priority"
        elif self.has_low_advantage:
            return "Low Priority"
        else:
            return "No Advantage"
    
    def get_advantage_types(self) -> list:
        """Get list of advantage types from metadata."""
        if not self.analysis_metadata:
            return []
        
        advantage_types = self.analysis_metadata.get('advantage_types', [])
        if isinstance(advantage_types, list):
            return advantage_types
        return []
    
    def get_confidence_score(self) -> Optional[Decimal]:
        """Get confidence score from metadata."""
        if not self.analysis_metadata:
            return None
        
        confidence = self.analysis_metadata.get('confidence_score')
        if confidence is not None:
            try:
                return Decimal(str(confidence))
            except (ValueError, TypeError):
                return None
        return None
    
    def get_analysis_duration(self) -> Optional[float]:
        """Get analysis duration in seconds from metadata."""
        if not self.analysis_metadata:
            return None
        
        return self.analysis_metadata.get('analysis_duration_seconds')
    
    def get_model_version(self) -> Optional[str]:
        """Get AI model version from metadata."""
        if not self.analysis_metadata:
            return None
        
        return self.analysis_metadata.get('model_version')
    
    def update_analysis_metadata(self, metadata: dict) -> None:
        """Update analysis metadata."""
        if self.analysis_metadata:
            self.analysis_metadata.update(metadata)
        else:
            self.analysis_metadata = metadata
    
    def set_advantage_types(self, advantage_types: list) -> None:
        """Set advantage types in metadata."""
        if not self.analysis_metadata:
            self.analysis_metadata = {}
        self.analysis_metadata['advantage_types'] = advantage_types
    
    def set_confidence_score(self, confidence: float) -> None:
        """Set confidence score in metadata."""
        if not self.analysis_metadata:
            self.analysis_metadata = {}
        self.analysis_metadata['confidence_score'] = confidence
    
    def set_analysis_duration(self, duration_seconds: float) -> None:
        """Set analysis duration in metadata."""
        if not self.analysis_metadata:
            self.analysis_metadata = {}
        self.analysis_metadata['analysis_duration_seconds'] = duration_seconds
    
    def set_model_version(self, version: str) -> None:
        """Set AI model version in metadata."""
        if not self.analysis_metadata:
            self.analysis_metadata = {}
        self.analysis_metadata['model_version'] = version
    
    def get_summary_preview(self, max_length: int = 200) -> Optional[str]:
        """Get a preview of the final briefing summary."""
        if not self.final_briefing_summary:
            return None
        
        summary = self.final_briefing_summary.strip()
        if len(summary) <= max_length:
            return summary
        
        return summary[:max_length] + "..."
    
    def get_action_items_preview(self, max_length: int = 200) -> Optional[str]:
        """Get a preview of actionable insights."""
        if not self.actionable_insight:
            return None
        
        insight = self.actionable_insight.strip()
        if len(insight) <= max_length:
            return insight
        
        return insight[:max_length] + "..."
    
    def __repr__(self) -> str:
        """String representation."""
        return (
            f"<AnalysisResult(id={self.id}, "
            f"opportunity_id={self.opportunity_id}, "
            f"is_relevant={self.is_relevant}, "
            f"advantage_found={self.advantage_opportunity_found}, "
            f"advantage_score={self.final_advantage_score})>"
        ) 