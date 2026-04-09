"""
scrapers/pubmed.py — PubMed Central через E-utilities API.

Улучшения на основе отладки:
  - Точные [tiab] запросы вместо широкого полнотекстового поиска
  - Правильное извлечение PMC ID: pub-id-type='pmcid' (не 'pmc')
  - usehistory=y + WebEnv для эффективного батчинга без передачи ID
  - Поддержка фильтрации по дате через mindate/maxdate
  - Два прохода: relevance + pub date (как в Habr)
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterator

from bs4 import BeautifulSoup

from db.repository import ArticleData
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
EFETCH_URL  = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

BATCH_SIZE = 10  # статей за один efetch запрос

# Запросы с [tiab] — поиск только в title+abstract, точнее чем полнотекстовый.
# Диапазон 50–1000 результатов — баланс точности и охвата.
SEARCH_QUERIES_RELEVANCE = [
    "pdf extraction[tiab] AND medical[tiab]",              # 814
    "table extraction[tiab] AND pdf[tiab]",                # 59
    "laboratory results[tiab] AND pdf[tiab]",              # 467
    "clinical data[tiab] AND pdf[tiab] AND extraction[tiab]",  # 26
    "ocr[tiab] AND medical document[tiab]",                # 15
    "pdf parsing[tiab] AND medical[tiab]",                 # 15
    "document parsing[tiab] AND medical[tiab]",            # 56
    "table detection[tiab] AND document[tiab]",            # 1148
    "electronic health record[tiab] AND extraction[tiab]", # 1103
    "structured data[tiab] AND medical[tiab] AND extraction[tiab]",  # 3647
]

# Те же запросы для прохода по дате — свежие статьи
SEARCH_QUERIES_DATE = SEARCH_QUERIES_RELEVANCE


class PubMedScraper(BaseScraper):
    source_name = "pubmed"
    language    = "en"

    def __init__(self, api_key: str = "", **kwargs):
        super().__init__(delay=0.4, **kwargs)
        self.api_key = api_key
        if api_key:
            self.delay = 0.15

    def _base_params(self, **extra) -> dict:
        p = {"db": "pmc", "retmode": "json", **extra}
        if self.api_key:
            p["api_key"] = self.api_key
        return p

    # ── ESearch с usehistory ───────────────────────────────────────────────────

    def _esearch(
        self,
        term: str,
        retmax: int,
        sort: str = "relevance",
        mindate: str = "",
        maxdate: str = "",
    ) -> tuple[str, str, int]:
        """
        Поиск через esearch с usehistory=y.
        Возвращает (WebEnv, QueryKey, total_count).
        WebEnv+QueryKey используются в efetch — не нужно передавать IDs.
        """
        params = self._base_params(
            term        = term,
            retmax      = retmax,
            sort        = sort,
            usehistory  = "y",
        )
        if mindate:
            params["datetype"] = "pdat"
            params["mindate"]  = mindate
        if maxdate:
            params["datetype"] = "pdat"
            params["maxdate"]  = maxdate

        resp = self.get(ESEARCH_URL, params=params)
        if resp is None:
            return "", "", 0

        try:
            data   = resp.json().get("esearchresult", {})
            webenv = data.get("webenv", "")
            qkey   = data.get("querykey", "")
            total  = int(data.get("count", 0))
            logger.info("[pubmed] esearch %r sort=%s → total=%d", term[:50], sort, total)
            return webenv, qkey, total
        except Exception as e:
            logger.warning("[pubmed] esearch parse error: %s", e)
            return "", "", 0

    # ── EFetch батчами через WebEnv ────────────────────────────────────────────

    def _efetch_batch(self, webenv: str, qkey: str, retstart: int) -> list[ArticleData]:
        """
        Загрузить BATCH_SIZE статей через WebEnv+QueryKey.
        retstart — смещение в результатах поиска.
        """
        params: dict = {
            "db":        "pmc",
            "query_key": qkey,
            "WebEnv":    webenv,
            "rettype":   "full",
            "retmode":   "xml",
            "retmax":    BATCH_SIZE,
            "retstart":  retstart,
        }
        if self.api_key:
            params["api_key"] = self.api_key

        resp = self.get(EFETCH_URL, params=params)
        if resp is None:
            return []

        soup    = BeautifulSoup(resp.content, "xml")
        results = []

        for art in soup.find_all("article"):
            article = self._parse_article(art)
            if article:
                results.append(article)

        return results

    # ── Парсинг одной статьи из XML ───────────────────────────────────────────

    def _parse_article(self, art) -> ArticleData | None:
        # Заголовок
        title_tag = art.find("article-title")
        title     = title_tag.get_text(strip=True) if title_tag else ""

        # Аннотация
        abstract_tag = art.find("abstract")
        abstract     = self.clean(abstract_tag.get_text(separator=" ")) if abstract_tag else ""

        # Тело статьи — берём <body> целиком, он содержит все <sec>
        body_tag = art.find("body")
        if body_tag:
            # Убираем таблицы и формулы — они дают мусор в тексте
            for tag in body_tag.find_all(["table", "table-wrap", "formula", "disp-formula"]):
                tag.decompose()
            body = self.clean(body_tag.get_text(separator=" "))
        else:
            # Fallback: собираем из <sec> если нет <body>
            sections = []
            for sec in art.find_all("sec"):
                sec_title = sec.find("title")
                sec_text  = sec.get_text(separator=" ", strip=True)
                if sec_title:
                    sections.append(f"## {sec_title.get_text(strip=True)}\n{sec_text}")
                else:
                    sections.append(sec_text)
            body = self.clean("\n\n".join(sections))

        full_text = f"{abstract}\n\n{body}".strip()
        if not full_text:
            return None

        # PMC ID — правильный тег: pub-id-type='pmcid'
        pmc_id = ""
        for id_tag in art.find_all("article-id"):
            if id_tag.get("pub-id-type") == "pmcid":
                # Значение может быть "PMC9092456" или просто "9092456"
                raw = id_tag.get_text(strip=True).replace("PMC", "")
                if raw.isdigit():
                    pmc_id = raw
                    break

        if not pmc_id:
            # Fallback: pmcaid
            tag = art.find("article-id", {"pub-id-type": "pmcaid"})
            if tag:
                pmc_id = tag.get_text(strip=True)

        if not pmc_id:
            return None

        url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/PMC{pmc_id}/"

        # Дата публикации
        pub_date = None
        year_tag = art.find("pub-date") or art.find("date")
        if year_tag:
            year  = year_tag.find("year")
            month = year_tag.find("month")
            day   = year_tag.find("day")
            try:
                pub_date = datetime(
                    int(year.text)  if year  else 2000,
                    int(month.text) if month else 1,
                    int(day.text)   if day   else 1,
                )
            except (ValueError, AttributeError):
                pass

        return ArticleData(
            source_name  = self.source_name,
            url          = url,
            title        = title,
            abstract     = abstract,
            text         = full_text,
            language     = self.language,
            published_at = pub_date,
        )

    # ── Публичный интерфейс ───────────────────────────────────────────────────

    def iter_articles(
        self,
        max_articles: int = 50,
        mindate: str = "",
        maxdate: str = "",
    ) -> Iterator[ArticleData]:
        seen_urls: set[str] = set()
        yielded   = 0
        per_query = max(BATCH_SIZE, max_articles // len(SEARCH_QUERIES_RELEVANCE) + 10)

        def _fetch_query(term: str, sort: str):
            nonlocal yielded
            if yielded >= max_articles:
                return

            webenv, qkey, total = self._esearch(
                term, retmax=per_query, sort=sort,
                mindate=mindate, maxdate=maxdate,
            )
            if not webenv:
                return

            for retstart in range(0, min(per_query, total), BATCH_SIZE):
                if yielded >= max_articles:
                    break

                articles = self._efetch_batch(webenv, qkey, retstart)
                for article in articles:
                    if yielded >= max_articles:
                        break
                    if article.url in seen_urls:
                        continue
                    seen_urls.add(article.url)
                    yield article
                    yielded += 1

        # Проход 1: по релевантности
        logger.info("[pubmed] Проход 1: relevance (%d запросов)", len(SEARCH_QUERIES_RELEVANCE))
        for term in SEARCH_QUERIES_RELEVANCE:
            if yielded >= max_articles:
                break
            yield from _fetch_query(term, sort="relevance")

        # Проход 2: по дате (свежие статьи)
        logger.info("[pubmed] Проход 2: pub+date (%d запросов)", len(SEARCH_QUERIES_DATE))
        for term in SEARCH_QUERIES_DATE:
            if yielded >= max_articles:
                break
            yield from _fetch_query(term, sort="pub+date")

        logger.info("[pubmed] итого выдано: %d статей", yielded)