"""
scrapers/habr.py — Habr через RSS поиска + RSS хабов.

Стратегия в три этапа:
  1. RSS поиска по релевантности — самые подходящие статьи за всё время
  2. RSS поиска по дате — свежие статьи по теме
  3. RSS хабов — последние статьи тематических разделов

RSS поиска (реальный макс: 100 на запрос):
  https://habr.com/ru/rss/search/?q=<запрос>&target_type=posts&order=relevance&limit=100
  https://habr.com/ru/rss/search/?q=<запрос>&target_type=posts&order=date&limit=100

RSS хабов (реальный макс: 100, пагинации нет):
  https://habr.com/ru/rss/hub/<hub>/articles/?limit=100
"""

from __future__ import annotations

import logging
import urllib.parse
from datetime import datetime
from typing import Iterator

import feedparser
from bs4 import BeautifulSoup

from db.repository import ArticleData
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

SEARCH_RSS = "https://habr.com/ru/rss/search/"
HUB_RSS    = "https://habr.com/ru/rss/hub/{hub}/articles/"

# Поисковые запросы — русский и английский
SEARCH_QUERIES = [
    "извлечение данных pdf",
    "парсинг медицинских документов",
    "распознавание таблиц ocr",
    "обработка медицинских анализов",
    "nlp медицина клинические данные",
    "pdf table extraction",
    "medical document parsing",
    "laboratory results nlp",
]

# Хабы — проверено, возвращают 200
HUBS = [
    "machine_learning",
    "data_mining",
    "python",
    "image_processing",
    "artificial_intelligence",
    "data_analysis",
    "programming",
    "open_source",
]


class HabrScraper(BaseScraper):
    source_name = "habr"
    language    = "ru"

    # ── RSS helpers ───────────────────────────────────────────────────────────

    def _fetch_feed(self, url: str) -> list:
        """Загрузить RSS и вернуть список entries."""
        resp = self.get(url)
        if resp is None:
            return []
        feed = feedparser.parse(resp.text)
        if feed.bozo and not feed.entries:
            logger.warning("[habr] broken feed %s: %s", url, feed.bozo_exception)
            return []
        return feed.entries

    def _search_rss(self, query: str, order: str, limit: int) -> list:
        """
        RSS поиска Habr.
        order = "relevance" | "date"
        limit = до 200
        """
        # Habr RSS поиска требует + между словами (не %20)
        url = (
            f"{SEARCH_RSS}"
            f"?q={urllib.parse.quote_plus(query)}"
            f"&target_type=posts"
            f"&order={order}"
            f"&limit={min(limit, 100)}"
        )
        entries = self._fetch_feed(url)
        logger.info("[habr] поиск=%r order=%s → %d статей", query, order, len(entries))
        return entries

    def _hub_rss(self, hub: str, limit: int) -> list:
        """RSS хаба с расширенным лимитом."""
        url = f"{HUB_RSS.format(hub=hub)}?limit={min(limit, 100)}"
        entries = self._fetch_feed(url)
        logger.info("[habr] hub=%s → %d статей", hub, len(entries))
        return entries

    # ── Парсинг записи ────────────────────────────────────────────────────────

    def _entry_to_article(self, entry) -> ArticleData | None:
        url   = entry.get("link", "").strip()
        title = entry.get("title", "").strip()
        if not url or not title:
            return None

        content_html = ""
        if entry.get("content"):
            content_html = entry["content"][0].get("value", "")
        if not content_html:
            content_html = entry.get("summary", "")
        if not content_html:
            return None

        soup     = BeautifulSoup(content_html, "lxml")
        first_p  = soup.find("p")
        abstract = first_p.get_text(strip=True) if first_p else ""
        text     = self.extract_text(soup)

        if len(text) < 150:
            return None

        # Добавляем теги в текст — полезно для классификатора
        tags = [t.term for t in entry.get("tags", [])]
        if tags:
            text += "\nТеги: " + ", ".join(tags)

        pub_date = None
        ts = entry.get("published_parsed") or entry.get("updated_parsed")
        if ts:
            try:
                pub_date = datetime(*ts[:6])
            except Exception:
                pass

        return ArticleData(
            source_name  = self.source_name,
            url          = url,
            title        = title,
            abstract     = abstract,
            text         = text,
            language     = self.language,
            published_at = pub_date,
        )

    # ── Публичный интерфейс ───────────────────────────────────────────────────

    def iter_articles(self, max_articles: int = 50, custom_query: str = "") -> Iterator[ArticleData]:
        seen_urls: set[str] = set()
        yielded   = 0

        def _yield_entries(entries: list) -> Iterator[ArticleData]:
            nonlocal yielded
            for entry in entries:
                article = self._entry_to_article(entry)
                if article is None or article.url in seen_urls:
                    continue
                seen_urls.add(article.url)
                yield article
                yielded += 1

        queries = [custom_query] if custom_query else SEARCH_QUERIES

        # Этап 1: поиск по релевантности
        logger.info("[habr] Этап 1: relevance (%d запросов)", len(queries))
        for query in queries:
            entries = self._search_rss(query, order="relevance", limit=100)
            yield from _yield_entries(entries)

        # Этап 2: поиск по дате
        logger.info("[habr] Этап 2: date (%d запросов)", len(queries))
        for query in queries:
            entries = self._search_rss(query, order="date", limit=100)
            yield from _yield_entries(entries)

        # Этап 3: RSS хабов (только если нет кастомного запроса)
        if not custom_query:
            logger.info("[habr] Этап 3: хабы (%d штук)", len(HUBS))
            for hub in HUBS:
                entries = self._hub_rss(hub, limit=100)
                yield from _yield_entries(entries)

        logger.info("[habr] итого отдано: %d статей", yielded)