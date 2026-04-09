"""
db/models.py — схема базы данных (SQLAlchemy).

Таблицы:
  articles     — статьи со всеми полями включая метки классификации
  scraping_log — журнал каждого запуска парсера
"""

from datetime import datetime

from sqlalchemy import (
    Boolean, Column, DateTime, Float, Index,
    Integer, SmallInteger, String, Text, UniqueConstraint, create_engine,
)
from sqlalchemy.orm import DeclarativeBase, Session


class Base(DeclarativeBase):
    pass


class Article(Base):
    __tablename__ = "articles"
    __table_args__ = (
        UniqueConstraint("url", name="uq_articles_url"),
        Index("ix_articles_source",   "source_name"),
        Index("ix_articles_relevant", "is_relevant"),
    )

    id           = Column(Integer, primary_key=True, autoincrement=True)

    # Источник
    source_name  = Column(String(64),   nullable=False)
    url          = Column(String(2048), nullable=False, unique=True)

    # Содержимое
    title        = Column(String(512))
    abstract     = Column(Text)   # аннотация — короткий «паспорт» статьи
    text         = Column(Text)   # полный текст — для чанкинга при BERT
    language     = Column(String(8))    # "ru" | "en"
    published_at = Column(DateTime)
    scraped_at   = Column(DateTime, default=datetime.utcnow)

    # Разметка (заполняется позже через labeler.py)
    is_relevant      = Column(Boolean)        # None = ещё не размечена
    label_source     = Column(String(16))     # "human" | "knn" | "bert"
    relevance_score  = Column(Float)          # итоговая вероятность 0–1
    knn_score        = Column(Float)
    bert_score       = Column(Float)

    def __repr__(self):
        return (
            f"<Article id={self.id} source={self.source_name!r} "
            f"relevant={self.is_relevant} title={str(self.title)[:50]!r}>"
        )


class ScrapingLog(Base):
    __tablename__ = "scraping_log"

    id            = Column(Integer,     primary_key=True, autoincrement=True)
    source_name   = Column(String(64),  nullable=False)
    started_at    = Column(DateTime,    default=datetime.utcnow)
    finished_at   = Column(DateTime)
    total_fetched = Column(Integer,     default=0)
    total_new     = Column(Integer,     default=0)
    total_skipped = Column(Integer,     default=0)
    total_errors  = Column(Integer,     default=0)
    error_details = Column(Text)


# ── Engine helpers ─────────────────────────────────────────────────────────────

_engine = None


def get_engine(db_url: str):
    global _engine
    if _engine is None:
        _engine = create_engine(db_url, echo=False)
    return _engine


def init_db(db_url: str):
    """Создать все таблицы если их ещё нет (идемпотентно)."""
    engine = get_engine(db_url)
    Base.metadata.create_all(engine)
    return engine


def get_session(db_url: str) -> Session:
    return Session(get_engine(db_url))
