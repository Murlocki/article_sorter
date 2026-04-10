"""
scrapers/towards_ds.py — Towards Data Science + Medium теги.

Стратегия:
  - Список статей берём из RSS (единственный способ — страницы не отдают список)
  - TDS статьи (towardsdatascience.com): загружаем страницу, берём полный текст
  - Medium статьи: 403 при прямом запросе, используем только RSS сниппет

TDS структура страницы (проверено):
  <main>
    <div class='wp-block-group is-layout-flow'>  ← основной контент
      <h1>  ← заголовок
      <p>   ← абзацы
      ...

Medium RSS: только сниппет (~700 символов), полный текст недоступен (403).
"""

from __future__ import annotations

import logging
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Iterator

import feedparser
from bs4 import BeautifulSoup

from db.repository import ArticleData
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# RSS ленты — только проверенные (10-20 статей каждая)
RSS_FEEDS = [
    # TDS — полный текст доступен
    ("https://towardsdatascience.com/feed",              "tds"),
    # Medium теги — только сниппет
    ("https://medium.com/feed/tag/ocr",                  "medium"),
    ("https://medium.com/feed/tag/pdf-parsing",          "medium"),
    ("https://medium.com/feed/tag/nlp",                  "medium"),
    ("https://medium.com/feed/tag/healthcare-ai",        "medium"),
    ("https://medium.com/feed/tag/document-understanding","medium"),
    ("https://medium.com/feed/tag/machine-learning",     "medium"),
    ("https://medium.com/feed/tag/data-science",         "medium"),
    ("https://medium.com/feed/tag/medical-imaging",      "medium"),
    ("https://medium.com/feed/tag/deep-learning",        "medium"),
    ("https://medium.com/feed/tag/artificial-intelligence","medium"),
    ("https://medium.com/feed/tag/information-extraction","medium"),
    ("https://medium.com/feed/tag/computer-vision",      "medium"),
    ("https://medium.com/feed/tag/text-mining",          "medium"),
]


class TowardsDSScraper(BaseScraper):
    source_name = "towards_ds"
    language    = "en"

    # ── RSS ──────────────────────────────────────────────────────────────────

    def _fetch_feed(self, url: str) -> list:
        resp = self.get(url)
        if resp is None:
            return []
        feed = feedparser.parse(resp.text)
        if feed.bozo and not feed.entries:
            logger.warning("[towards_ds] broken feed %s", url)
            return []
        tag = url.split("/tag/")[-1] if "/tag/" in url else "tds"
        logger.info("[towards_ds] %s → %d записей", tag, len(feed.entries))
        return feed.entries

    # ── Полный текст TDS страницы ─────────────────────────────────────────

    def _fetch_tds_text(self, url: str) -> tuple[str, str]:
        """
        Загрузить полный текст статьи TDS.
        Возвращает (abstract, full_text).
        """
        page = self.soup(url)
        if page is None:
            return "", ""

        # Основной контент — второй div с классом is-layout-flow
        # (первый содержит мета-инфо: автор, дата)
        content_divs = page.select("div.wp-block-group.is-layout-flow")
        main_div = None
        for div in content_divs:
            text = div.get_text(strip=True)
            if len(text) > 500:  # берём первый достаточно длинный
                main_div = div
                break

        if main_div is None:
            # Fallback — весь <main>
            main_div = page.find("main")

        if main_div is None:
            return "", ""

        # Убираем навигацию и служебные блоки
        for tag in main_div.find_all(["nav", "aside", "footer",
                                       "script", "style",
                                       "div.tds-cta-box"]):
            tag.decompose()

        full_text = self.extract_text(main_div)

        # Abstract — первый длинный параграф
        abstract = ""
        for p in main_div.find_all("p"):
            t = p.get_text(strip=True)
            if len(t) > 80:
                abstract = t[:500]
                break

        return abstract, full_text

    # ── Парсинг RSS записи ────────────────────────────────────────────────

    def _entry_to_article(self, entry, source_type: str) -> ArticleData | None:
        url   = entry.get("link", "").strip()
        title = entry.get("title", "").strip()
        if not url or not title:
            return None

        # Дата
        pub_date = None
        date_str = entry.get("published") or entry.get("updated")
        if date_str:
            try:
                pub_date = parsedate_to_datetime(date_str).replace(tzinfo=None)
            except Exception:
                pass

        # Для TDS — загружаем полный текст со страницы
        if source_type == "tds" and "towardsdatascience.com" in url:
            abstract, text = self._fetch_tds_text(url)
            if not text:
                # Fallback на RSS сниппет если страница не загрузилась
                text = self._rss_snippet(entry)
            if not abstract and text:
                lines = [l for l in text.splitlines() if len(l) > 80]
                abstract = lines[0][:500] if lines else text[:300]
        else:
            # Medium — только RSS сниппет (403 на страницах)
            snippet  = self._rss_snippet(entry)
            abstract = snippet[:300] if snippet else ""
            # Дополняем заголовком чтобы пройти MIN_TEXT_LENGTH=150
            text = (title + "\n\n" + snippet) if snippet else title

        if len(text) < 50:
            return None

        return ArticleData(
            source_name  = self.source_name,
            url          = url,
            title        = title,
            abstract     = abstract,
            text         = text,
            language     = self.language,
            published_at = pub_date,
        )

    @staticmethod
    def _rss_snippet(entry) -> str:
        """Извлечь текст из RSS content/summary."""
        content_html = ""
        if entry.get("content"):
            content_html = entry["content"][0].get("value", "")
        if not content_html:
            content_html = entry.get("summary", "")
        if not content_html:
            return ""
        soup = BeautifulSoup(content_html, "lxml")
        # medium-feed-snippet — это единственный полезный элемент
        snippet = soup.find(class_="medium-feed-snippet")
        if snippet:
            return snippet.get_text(strip=True)
        return soup.get_text(separator=" ", strip=True)

    # ── Публичный интерфейс ───────────────────────────────────────────────

    def iter_articles(self, max_articles: int = 50, custom_query: str = "") -> Iterator[ArticleData]:
        seen_urls: set[str] = set()
        yielded = 0

        if custom_query:
            logger.info("[towards_ds] кастомный запрос %r игнорируется — "
                        "TDS/Medium не поддерживают RSS поиска", custom_query)

        for feed_url, source_type in RSS_FEEDS:
            for entry in self._fetch_feed(feed_url):
                article = self._entry_to_article(entry, source_type)
                if article is None or article.url in seen_urls:
                    continue
                seen_urls.add(article.url)
                yield article
                yielded += 1

        logger.info("[towards_ds] итого отдано: %d статей", yielded)