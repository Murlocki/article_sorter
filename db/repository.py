"""
db/repository.py — все операции записи/чтения в одном месте.
Скраперы не знают про SQLAlchemy — они отдают ArticleData, репозиторий решает остальное.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from sqlalchemy.exc import IntegrityError

from config import DATABASE_URL
from db.models import Article, ScrapingLog, get_session, init_db

logger = logging.getLogger(__name__)


@dataclass
class ArticleData:
    """Структура данных которую возвращает любой скрапер."""
    source_name:  str
    url:          str
    title:        str
    text:         str
    abstract:     str = ""
    language:     str = "en"
    published_at: Optional[datetime] = None


class ArticleRepository:

    def __init__(self, db_url: str = DATABASE_URL):
        self.db_url = db_url
        init_db(db_url)   # создать таблицы при первом запуске

    # ── Запись ─────────────────────────────────────────────────────────────────

    def save(self, data: ArticleData) -> tuple[bool, str]:
        """
        Сохранить статью.
        Возвращает (success, reason):
          reason ∈ "saved" | "duplicate" | "too_short" | "error"
        """
        if not data.text or not data.text.strip():
            return False, "too_short"

        article = Article(
            source_name  = data.source_name,
            url          = data.url,
            title        = data.title,
            abstract     = data.abstract,
            text         = data.text,
            language     = data.language,
            published_at = data.published_at,
        )

        try:
            with get_session(self.db_url) as s:
                s.add(article)
                s.commit()
            return True, "saved"
        except IntegrityError:
            return False, "duplicate"
        except Exception as exc:
            logger.error("DB save error for %s: %s", data.url, exc)
            return False, "error"

    # ── Лог ────────────────────────────────────────────────────────────────────

    def start_log(self, source_name: str) -> int:
        with get_session(self.db_url) as s:
            log = ScrapingLog(source_name=source_name)
            s.add(log)
            s.commit()
            return log.id

    def finish_log(self, log_id: int, **kwargs):
        with get_session(self.db_url) as s:
            log = s.get(ScrapingLog, log_id)
            if log:
                log.finished_at = datetime.utcnow()
                for k, v in kwargs.items():
                    setattr(log, k, v)
                s.commit()

    # ── Чтение ─────────────────────────────────────────────────────────────────

    def get_articles(
        self,
        source_name: str | None = None,
        language: str | None = None,
        is_relevant: bool | None = None,
        date_from=None,
        date_to=None,
        search: str | None = None,
        limit: int = 500,
    ):
        """Вернуть статьи с фильтрами."""
        with get_session(self.db_url) as s:
            q = s.query(Article)
            if source_name:
                q = q.filter(Article.source_name == source_name)
            if language:
                q = q.filter(Article.language == language)
            if is_relevant is not None:
                q = q.filter(Article.is_relevant == is_relevant)
            if date_from:
                q = q.filter(Article.scraped_at >= date_from)
            if date_to:
                q = q.filter(Article.scraped_at <= date_to)
            if search:
                like = f"%{search}%"
                from sqlalchemy import or_
                q = q.filter(or_(Article.title.ilike(like), Article.abstract.ilike(like)))
            articles = q.order_by(Article.scraped_at.desc()).limit(limit).all()
            s.expunge_all()
            return articles

    def get_scraping_logs(self, limit: int = 50):
        with get_session(self.db_url) as s:
            logs = (
                s.query(ScrapingLog)
                 .order_by(ScrapingLog.started_at.desc())
                 .limit(limit)
                 .all()
            )
            s.expunge_all()
            return logs

    # ── Статистика ─────────────────────────────────────────────────────────────

    def stats(self) -> dict:
        with get_session(self.db_url) as s:
            total    = s.query(Article).count()
            labeled  = s.query(Article).filter(Article.is_relevant.isnot(None)).count()
            relevant = s.query(Article).filter_by(is_relevant=True).count()

            # По источникам
            from sqlalchemy import func
            by_source = dict(
                s.query(Article.source_name, func.count(Article.id))
                 .group_by(Article.source_name)
                 .all()
            )

        return {
            "total":     total,
            "labeled":   labeled,
            "relevant":  relevant,
            "unlabeled": total - labeled,
            "by_source": by_source,
        }

    def update_article(self, article_id: int, **fields) -> bool:
        """
        Обновить поля статьи по id.
        Допустимые поля: title, abstract, text, language,
                         is_relevant, relevance_score, label_source, published_at
        """
        ALLOWED = {
            "title", "abstract", "text", "language",
            "is_relevant", "relevance_score", "label_source", "published_at",
        }
        updates = {k: v for k, v in fields.items() if k in ALLOWED}
        if not updates:
            return False
        try:
            with get_session(self.db_url) as s:
                article = s.get(Article, article_id)
                if article is None:
                    return False
                for k, v in updates.items():
                    setattr(article, k, v)
                s.commit()
            return True
        except Exception as exc:
            logger.error("update_article error id=%s: %s", article_id, exc)
            return False

    def delete_article(self, article_id: int) -> bool:
        """Удалить статью по id."""
        try:
            with get_session(self.db_url) as s:
                article = s.get(Article, article_id)
                if article is None:
                    return False
                s.delete(article)
                s.commit()
            return True
        except Exception as exc:
            logger.error("delete_article error id=%s: %s", article_id, exc)
            return False