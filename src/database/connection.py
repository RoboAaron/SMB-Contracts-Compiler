"""
Database connection management for the Texas WBE Opportunity Discovery Engine.

Provides unified connection management for both synchronous and asynchronous
database operations using SQLAlchemy with PostgreSQL.
"""

import logging
from contextlib import contextmanager, asynccontextmanager
from typing import Optional, Generator, AsyncGenerator, Dict, Any

from sqlalchemy import create_engine, event, Engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from ..config import get_config

logger = logging.getLogger(__name__)

# Global engine and session factory
_engine: Optional[Engine] = None
_SessionLocal: Optional[sessionmaker] = None

# Async engine and session factory
_async_engine: Optional[Engine] = None
_AsyncSessionLocal: Optional[async_sessionmaker] = None


def get_database_url(config: Dict[str, Any] = None) -> str:
    """Get database URL from settings."""
    if config is None:
        config = get_config()
    
    # Handle both dict and object configs
    if isinstance(config, dict):
        db_config = config['database']
    else:
        db_config = config.database
    
    # Construct database URL
    if db_config.get('password') if isinstance(db_config, dict) else db_config.password:
        password = db_config.get('password') if isinstance(db_config, dict) else db_config.password
        return f"postgresql://{db_config.get('user') if isinstance(db_config, dict) else db_config.user}:{password}@{db_config.get('host') if isinstance(db_config, dict) else db_config.host}:{db_config.get('port') if isinstance(db_config, dict) else db_config.port}/{db_config.get('name') if isinstance(db_config, dict) else db_config.name}"
    else:
        return f"postgresql://{db_config.get('user') if isinstance(db_config, dict) else db_config.user}@{db_config.get('host') if isinstance(db_config, dict) else db_config.host}:{db_config.get('port') if isinstance(db_config, dict) else db_config.port}/{db_config.get('name') if isinstance(db_config, dict) else db_config.name}"


def create_database_engine() -> Engine:
    """Create and configure database engine with connection pooling."""
    config = get_config()
    database_url = get_database_url()
    
    # Handle both dict and object configs
    if isinstance(config, dict):
        db_config = config['database']
        pool_size = db_config.get('pool_size', 10)
        max_overflow = db_config.get('max_overflow', 20)
        echo = db_config.get('echo', False)
    else:
        db_config = config.database
        pool_size = db_config.pool_size
        max_overflow = db_config.max_overflow
        echo = db_config.echo
    
    # Engine configuration
    engine_config = {
        "poolclass": QueuePool,
        "pool_size": pool_size,
        "max_overflow": max_overflow,
        "pool_pre_ping": True,  # Validate connections before use
        "pool_recycle": 3600,  # Recycle connections after 1 hour
        "echo": echo,  # Set to True for SQL logging
        "echo_pool": False,  # Set to True for pool logging
    }
    
    engine = create_engine(database_url, **engine_config)
    
    # Add event listeners for connection management
    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        """Set SQLite pragmas for better performance."""
        if "sqlite" in database_url:
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA cache_size=10000")
            cursor.execute("PRAGMA temp_store=MEMORY")
            cursor.close()
    
    @event.listens_for(engine, "checkout")
    def receive_checkout(dbapi_connection, connection_record, connection_proxy):
        """Log connection checkout."""
        logger.debug("Database connection checked out")
    
    @event.listens_for(engine, "checkin")
    def receive_checkin(dbapi_connection, connection_record):
        """Log connection checkin."""
        logger.debug("Database connection checked in")
    
    return engine


def get_engine() -> Engine:
    """Get the database engine, creating it if necessary."""
    global _engine
    if _engine is None:
        _engine = create_database_engine()
        logger.info("Database engine created")
    return _engine


def create_session_factory() -> sessionmaker:
    """Create session factory with engine."""
    engine = get_engine()
    return sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine,
        expire_on_commit=False
    )


def get_session_factory() -> sessionmaker:
    """Get the session factory, creating it if necessary."""
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = create_session_factory()
        logger.info("Database session factory created")
    return _SessionLocal


def get_db_session() -> Session:
    """Get a new database session."""
    session_factory = get_session_factory()
    return session_factory()


@contextmanager
def get_db() -> Generator[Session, None, None]:
    """Context manager for database sessions with automatic cleanup."""
    session = get_db_session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def close_database_connections():
    """Close all database connections and cleanup resources."""
    global _engine, _SessionLocal
    
    if _engine:
        _engine.dispose()
        _engine = None
        logger.info("Database engine disposed")
    
    if _SessionLocal:
        _SessionLocal.close_all()
        _SessionLocal = None
        logger.info("Database session factory closed")


def test_database_connection() -> bool:
    """Test database connection and return success status."""
    try:
        with get_db() as session:
            # Execute a simple query to test connection
            session.execute(text("SELECT 1"))
            logger.info("Database connection test successful")
            return True
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return False


def get_connection_info() -> dict:
    """Get database connection information."""
    engine = get_engine()
    return {
        "driver": engine.driver,
        "pool_size": engine.pool.size(),
        "pool_checked_in": engine.pool.checkedin(),
        "pool_checked_out": engine.pool.checkedout(),
        "pool_overflow": engine.pool.overflow(),
        "pool_invalid": engine.pool.invalid(),
    }


class DatabaseHealthCheck:
    """Database health check utility."""
    
    @staticmethod
    def check_connection() -> bool:
        """Check if database connection is healthy."""
        return test_database_connection()
    
    @staticmethod
    def check_pool_status() -> dict:
        """Check connection pool status."""
        return get_connection_info()
    
    @staticmethod
    def get_health_status() -> dict:
        """Get comprehensive health status."""
        connection_healthy = test_database_connection()
        pool_info = get_connection_info()
        
        return {
            "connection_healthy": connection_healthy,
            "pool_status": pool_info,
            "overall_healthy": connection_healthy and pool_info["pool_invalid"] == 0
        }


# Async Database Functions

def get_async_database_url(config: Dict[str, Any] = None) -> str:
    """Get async database URL from settings."""
    if config is None:
        config = get_config()
    
    # Handle both dict and object configs
    if isinstance(config, dict):
        db_config = config['database']
    else:
        db_config = config.database
    
    # Construct async database URL
    if db_config.get('password') if isinstance(db_config, dict) else db_config.password:
        password = db_config.get('password') if isinstance(db_config, dict) else db_config.password
        return f"postgresql+asyncpg://{db_config.get('user') if isinstance(db_config, dict) else db_config.user}:{password}@{db_config.get('host') if isinstance(db_config, dict) else db_config.host}:{db_config.get('port') if isinstance(db_config, dict) else db_config.port}/{db_config.get('name') if isinstance(db_config, dict) else db_config.name}"
    else:
        return f"postgresql+asyncpg://{db_config.get('user') if isinstance(db_config, dict) else db_config.user}@{db_config.get('host') if isinstance(db_config, dict) else db_config.host}:{db_config.get('port') if isinstance(db_config, dict) else db_config.port}/{db_config.get('name') if isinstance(db_config, dict) else db_config.name}"


def create_async_database_engine() -> Engine:
    """Create and configure async database engine with connection pooling."""
    config = get_config()
    
    # Handle both dict and object configs
    if isinstance(config, dict):
        db_config = config['database']
        user = db_config.get('user')
        password = db_config.get('password')
        host = db_config.get('host')
        port = db_config.get('port')
        name = db_config.get('name')
        echo = db_config.get('echo', False)
        pool_size = db_config.get('pool_size', 10)
        max_overflow = db_config.get('max_overflow', 20)
    else:
        db_config = config.database
        user = db_config.user
        password = db_config.password
        host = db_config.host
        port = db_config.port
        name = db_config.name
        echo = db_config.echo
        pool_size = db_config.pool_size
        max_overflow = db_config.max_overflow
    
    engine = create_async_engine(
        f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{name}",
        echo=echo,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_pre_ping=True,
        pool_recycle=3600,
    )
    return engine


def get_async_engine() -> Engine:
    """Get the async database engine, creating it if necessary."""
    global _async_engine
    if _async_engine is None:
        _async_engine = create_async_database_engine()
        logger.info("Async database engine created")
    return _async_engine


def create_async_session_factory() -> async_sessionmaker:
    """Create async session factory with engine."""
    engine = get_async_engine()
    return async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False
    )


def get_async_session_factory() -> async_sessionmaker:
    """Get the async session factory, creating it if necessary."""
    global _AsyncSessionLocal
    if _AsyncSessionLocal is None:
        _AsyncSessionLocal = create_async_session_factory()
        logger.info("Async database session factory created")
    return _AsyncSessionLocal


def get_async_db_session() -> AsyncSession:
    """Get a new async database session."""
    session_factory = get_async_session_factory()
    return session_factory()


@asynccontextmanager
async def get_async_session(session: Optional[AsyncSession] = None) -> AsyncGenerator[AsyncSession, None]:
    """Context manager for async database sessions with automatic cleanup."""
    if session is not None:
        yield session
    else:
        session = get_async_db_session()
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def close_async_database_connections():
    """Close all async database connections and cleanup resources."""
    global _async_engine, _AsyncSessionLocal
    
    if _async_engine:
        await _async_engine.dispose()
        _async_engine = None
        logger.info("Async database engine disposed")
    
    if _AsyncSessionLocal:
        await _AsyncSessionLocal.close_all()
        _AsyncSessionLocal = None
        logger.info("Async database session factory closed")


async def test_async_database_connection() -> bool:
    """Test async database connection and return success status."""
    try:
        async with get_async_session() as session:
            # Execute a simple query to test connection
            await session.execute("SELECT 1")
            logger.info("Async database connection test successful")
            return True
    except Exception as e:
        logger.error(f"Async database connection test failed: {e}")
        return False 