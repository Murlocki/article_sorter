"""
scrapers/doaj.py — DOAJ (Directory of Open Access Journals).

Endpoint: GET /api/search/articles/{search_query}

Важно: слова через + (не %20, не пробел).
Поиск по полям (bibjson.abstract:) не работает.
Оптимальные запросы: 2-3 тематических слова.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterator

from db.repository import ArticleData
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

API_BASE = "https://doaj.org/api/search/articles"

SEARCH_QUERIES = [
    # Проверенные — дают результаты
    "ocr+medical",
    "medical+table+extraction",
    "laboratory+pdf+results",
    "pdf+medical+extraction",
    "clinical+document+parsing",
    "medical+document+information",
    "laboratory+results+extraction",
    "clinical+data+extraction",
    # Расширенные — извлечение данных
    "pdf+data+extraction",
    "document+information+extraction",
    "text+extraction+medical",
    "data+extraction+clinical",
    "structured+data+medical",
    "unstructured+data+extraction",
    "named+entity+recognition+medical",
    "information+retrieval+clinical",
    # Таблицы и документы
    "table+extraction+document",
    "table+detection+recognition",
    "pdf+table+parsing",
    "document+layout+analysis",
    "document+understanding+deep+learning",
    "form+extraction+document",
    # OCR и распознавание
    "optical+character+recognition+medical",
    "ocr+document+processing",
    "ocr+table+recognition",
    "text+recognition+document",
    "handwriting+recognition+medical",
    # Медицинские записи
    "electronic+health+record+extraction",
    "ehr+information+extraction",
    "clinical+notes+nlp",
    "medical+record+parsing",
    "patient+data+extraction",
    "radiology+report+extraction",
    "pathology+report+nlp",
    "discharge+summary+extraction",
    "medical+report+parsing",
    # Лабораторные данные
    "laboratory+test+results",
    "lab+results+extraction",
    "blood+test+data+extraction",
    "clinical+laboratory+nlp",
    "laboratory+findings+extraction",
    "diagnostic+report+extraction",
    # NLP в медицине
    "natural+language+processing+clinical",
    "nlp+healthcare",
    "biomedical+text+mining",
    "clinical+text+mining",
    "medical+nlp+deep+learning",
    "biomedical+information+extraction",
]


class DOAJScraper(BaseScraper):
    source_name = "doaj"
    language    = "en"

    def __init__(self, **kwargs):
        super().__init__(delay=2.0, **kwargs)

    def _search(self, query: str, page_size: int) -> list[dict]:
        """
        Поиск с пагинацией — собираем несколько страниц если нужно больше 10 статей.
        query содержит слова через + (единственный рабочий формат DOAJ).
        """
        url      = f"{API_BASE}/{query}"
        per_page = min(page_size, 100)
        results  = []
        page     = 1

        while len(results) < page_size:
            resp = self.get(url, params={"pageSize": per_page, "page": page})
            if resp is None:
                break

            text = (resp.text or "").strip()
            if not text.startswith("{"):
                logger.warning("[doaj] не-JSON (status=%s)", resp.status_code)
                break

            try:
                data    = resp.json()
                batch   = data.get("results", [])
                total   = data.get("total", 0)
            except Exception as e:
                logger.warning("[doaj] JSON error: %s", e)
                break

            if not batch:
                break

            results.extend(batch)
            logger.info("[doaj] %r page=%d → %d (+%d), total=%d",
                        query, page, len(results), len(batch), total)

            # Если забрали всё что есть — выходим
            if len(results) >= total or len(batch) < per_page:
                break
            page += 1

        return results[:page_size]

    def _to_article(self, result: dict) -> ArticleData | None:
        bibjson = result.get("bibjson", {})

        title    = (bibjson.get("title")    or "").strip()
        abstract = (bibjson.get("abstract") or "").strip()

        if not title or not abstract:
            return None

        # URL: fulltext ссылка
        url = ""
        for link in bibjson.get("link", []):
            href = link.get("url", "")
            if href:
                url = href
                # Предпочитаем HTML над PDF
                if link.get("content_type", "").lower() == "text/html":
                    break
        if not url:
            for ident in bibjson.get("identifier", []):
                if ident.get("type") == "doi":
                    url = f"https://doi.org/{ident['id']}"
                    break
        if not url:
            logger.debug("[doaj] нет URL для: %s", title[:60])
            return None

        # Язык
        lang_list = bibjson.get("language", ["EN"])
        lang = lang_list[0].lower() if lang_list else "en"
        if lang not in ("ru", "en"):
            lang = "en"

        # Дата
        pub_date = None
        year  = bibjson.get("year")
        month = bibjson.get("month")
        if year:
            try:
                pub_date = datetime(int(year), int(month) if month else 1, 1)
            except (ValueError, TypeError):
                pass

        return ArticleData(
            source_name  = self.source_name,
            url          = url,
            title        = title,
            abstract     = abstract,
            text         = abstract,
            language     = lang,
            published_at = pub_date,
        )

    def iter_articles(self, max_articles: int = 50, custom_query: str = "") -> Iterator[ArticleData]:
        queries   = [custom_query.replace(" ", "+")] if custom_query else SEARCH_QUERIES
        per_query = max(10, max_articles // len(queries))
        seen_urls: set[str] = set()
        yielded = 0

        for query in queries:
            results = self._search(query, per_query)
            logger.info("[doaj] %r → %d results", query, len(results))

            for result in results:
                article = self._to_article(result)
                if article is None:
                    continue
                if article.url in seen_urls:
                    continue
                seen_urls.add(article.url)
                logger.info("[doaj] статья: %s", article.title[:70])
                yield article
                yielded += 1

        logger.info("[doaj] итого отдано: %d статей", yielded)