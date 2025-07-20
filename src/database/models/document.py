"""
Document model for files associated with opportunities.

Tracks document metadata, processing status, and extracted content
for AI analysis and full-text search capabilities.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from sqlalchemy import BigInteger, CheckConstraint, ForeignKey, String, Text
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base


class Document(Base):
    """Model for documents associated with opportunities."""
    
    __tablename__ = "documents"
    
    # Foreign key to opportunity
    opportunity_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("opportunities.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Reference to opportunities table"
    )
    
    # Document metadata
    document_name: Mapped[str] = mapped_column(
        String(200),
        nullable=False,
        comment="Original filename"
    )
    
    document_url: Mapped[Optional[str]] = mapped_column(
        String(500),
        nullable=True,
        comment="Source URL for the document"
    )
    
    document_type: Mapped[Optional[str]] = mapped_column(
        String(20),
        nullable=True,
        index=True,
        comment="File type (PDF, DOCX, etc.)"
    )
    
    file_size: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        nullable=True,
        comment="File size in bytes"
    )
    
    storage_path: Mapped[Optional[str]] = mapped_column(
        String(500),
        nullable=True,
        comment="Path in cloud/blob storage"
    )
    
    # Content and processing
    raw_text_content: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Extracted text for AI analysis"
    )
    
    content_hash: Mapped[Optional[str]] = mapped_column(
        String(64),
        nullable=True,
        comment="SHA-256 hash for change detection"
    )
    
    processing_status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        server_default="Pending",
        index=True,
        comment="Document processing status"
    )
    
    last_processed_at: Mapped[Optional[datetime]] = mapped_column(
        comment="Last processing attempt"
    )
    
    # Relationships
    opportunity: Mapped["Opportunity"] = relationship(
        "Opportunity",
        back_populates="documents",
        lazy="selectin"
    )
    
    # Constraints
    __table_args__ = (
        CheckConstraint(
            "file_size IS NULL OR file_size > 0",
            name="documents_positive_size"
        ),
        CheckConstraint(
            "content_hash IS NULL OR length(content_hash) = 64",
            name="documents_valid_hash"
        ),
        CheckConstraint(
            "processing_status IN ('Pending', 'Processing', 'Completed', 'Failed')",
            name="documents_valid_status"
        ),
    )
    
    def __init__(self, **kwargs):
        """Initialize document with validation."""
        # Set default processing status if not provided
        if 'processing_status' not in kwargs:
            kwargs['processing_status'] = 'Pending'
        if 'id' not in kwargs:
            from uuid import uuid4
            kwargs['id'] = uuid4()
        super().__init__(**kwargs)
        self._validate_status()
        self._validate_file_size()
        self._validate_content_hash()
    
    def _validate_status(self) -> None:
        """Validate processing status."""
        valid_statuses = {'Pending', 'Processing', 'Completed', 'Failed'}
        if self.processing_status and self.processing_status not in valid_statuses:
            raise ValueError(f"Processing status must be one of: {valid_statuses}")
    
    def _validate_file_size(self) -> None:
        """Validate file size."""
        if self.file_size is not None and self.file_size <= 0:
            raise ValueError("File size must be positive")
    
    def _validate_content_hash(self) -> None:
        """Validate content hash format."""
        if self.content_hash and len(self.content_hash) != 64:
            raise ValueError("Content hash must be exactly 64 characters (SHA-256)")
    
    @property
    def is_pending(self) -> bool:
        """Check if document is pending processing."""
        return self.processing_status == "Pending"
    
    @property
    def is_processing(self) -> bool:
        """Check if document is currently being processed."""
        return self.processing_status == "Processing"
    
    @property
    def is_completed(self) -> bool:
        """Check if document processing is completed."""
        return self.processing_status == "Completed"
    
    @property
    def is_failed(self) -> bool:
        """Check if document processing failed."""
        return self.processing_status == "Failed"
    
    @property
    def has_content(self) -> bool:
        """Check if document has extracted text content."""
        return bool(self.raw_text_content and self.raw_text_content.strip())
    
    @property
    def file_extension(self) -> Optional[str]:
        """Get file extension from document name."""
        if not self.document_name:
            return None
        parts = self.document_name.split('.')
        return parts[-1].lower() if len(parts) > 1 else None
    
    @property
    def is_pdf(self) -> bool:
        """Check if document is a PDF."""
        return self.file_extension == 'pdf' or self.document_type == 'PDF'
    
    @property
    def is_word_document(self) -> bool:
        """Check if document is a Word document."""
        word_extensions = {'doc', 'docx'}
        return (self.file_extension in word_extensions or 
                self.document_type in {'DOC', 'DOCX'})
    
    def update_processing_status(self, status: str, error_message: Optional[str] = None) -> None:
        """Update processing status and timestamp."""
        self.processing_status = status
        self.last_processed_at = datetime.now()
        
        if status == "Failed" and error_message:
            # Store error message in a way that doesn't break the model
            # This could be extended to store in a separate error_log table
            pass
    
    def set_content(self, content: str, content_hash: Optional[str] = None) -> None:
        """Set extracted text content and optional hash."""
        self.raw_text_content = content
        if content_hash:
            self.content_hash = content_hash
        self.processing_status = "Completed"
        self.last_processed_at = datetime.now()
    
    def mark_as_failed(self, error_message: Optional[str] = None) -> None:
        """Mark document processing as failed."""
        self.processing_status = "Failed"
        self.last_processed_at = datetime.now()
    
    def get_content_preview(self, max_length: int = 200) -> Optional[str]:
        """Get a preview of the document content."""
        if not self.raw_text_content:
            return None
        
        content = self.raw_text_content.strip()
        if len(content) <= max_length:
            return content
        
        return content[:max_length] + "..."
    
    def calculate_content_hash(self) -> str:
        """Calculate SHA-256 hash of content."""
        import hashlib
        
        if not self.raw_text_content:
            return ""
        
        return hashlib.sha256(self.raw_text_content.encode('utf-8')).hexdigest()
    
    def content_has_changed(self) -> bool:
        """Check if content has changed since last hash calculation."""
        if not self.content_hash:
            return True
        
        current_hash = self.calculate_content_hash()
        return current_hash != self.content_hash
    
    def __repr__(self) -> str:
        """String representation."""
        return (
            f"<Document(id={self.id}, "
            f"opportunity_id={self.opportunity_id}, "
            f"name='{self.document_name}', "
            f"status='{self.processing_status}')>"
        ) 