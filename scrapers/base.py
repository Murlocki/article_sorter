"""
scrapers/base.py — базовый класс и общие HTTP-утилиты.
"""

from __future__ import annotations

import logging
import ssl
import time
from abc import ABC, abstractmethod
from typing import Iterator

import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup

from db.repository import ArticleData

logger = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru,en;q=0.9",
}

DEFAULT_TIMEOUT = 20
DEFAULT_DELAY   = 1.5


class TLSAdapter(HTTPAdapter):
    """Адаптер с понижением до TLS 1.2 — для серверов не поддерживающих TLS 1.3."""

    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.set_ciphers("DEFAULT@SECLEVEL=1")
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.maximum_version = ssl.TLSVersion.TLSv1_2
        kwargs["ssl_context"] = ctx
        super().init_poolmanager(*args, **kwargs)


class BaseScraper(ABC):
    source_name: str = ""
    language:    str = "en"

    def __init__(
        self,
        delay:      float = DEFAULT_DELAY,
        timeout:    int   = DEFAULT_TIMEOUT,
        tls12_only: bool  = False,   # True для серверов со старым TLS
        verify_ssl: bool  = True,
    ):
        self.delay      = delay
        self.timeout    = timeout
        self._session   = requests.Session()
        self._session.headers.update(DEFAULT_HEADERS)

        if tls12_only:
            adapter = TLSAdapter()
            self._session.mount("https://", adapter)

        if not verify_ssl:
            self._session.verify = False
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # ── GET с retry на 429/503 ────────────────────────────────────────────────

    def get(
        self,
        url: str,
        retries: int = 3,
        retry_delay: float = 5.0,
        **kwargs,
    ) -> requests.Response | None:
        """
        GET с задержкой и автоматическим retry при 429 (rate limit) и 503.
        """
        for attempt in range(retries):
            try:
                time.sleep(self.delay)
                timeout = kwargs.pop('timeout', self.timeout)
                logger.debug("[%s] GET %s", self.source_name, url)
                resp = self._session.get(url, timeout=timeout, **kwargs)

                # Rate limit — ждём и повторяем
                if resp.status_code == 429:
                    wait = float(resp.headers.get("Retry-After", retry_delay * (attempt + 1)))
                    logger.warning(
                        "[%s] 429 rate limit, ждём %.0fs (попытка %d/%d): %s",
                        self.source_name, wait, attempt + 1, retries, url,
                    )
                    time.sleep(wait)
                    continue

                # Временная недоступность
                if resp.status_code == 503:
                    wait = retry_delay * (attempt + 1)
                    logger.warning("[%s] 503, retry через %.0fs", self.source_name, wait)
                    time.sleep(wait)
                    continue

                resp.raise_for_status()
                return resp

            except requests.RequestException as exc:
                logger.warning("[%s] GET failed %s: %s", self.source_name, url, exc)
                if attempt < retries - 1:
                    time.sleep(retry_delay)

        return None

    def soup(self, url: str, **kwargs) -> BeautifulSoup | None:
        resp = self.get(url, **kwargs)
        if resp is None:
            return None
        return BeautifulSoup(resp.text, "lxml")

    # ── Текстовые утилиты ─────────────────────────────────────────────────────

    @staticmethod
    def clean(text: str) -> str:
        lines = (line.strip() for line in text.splitlines())
        return "\n".join(line for line in lines if line)

    @staticmethod
    def extract_text(tag) -> str:
        if tag is None:
            return ""
        for el in tag.find_all(["script", "style", "nav", "footer", "aside"]):
            el.decompose()
        return BaseScraper.clean(tag.get_text(separator="\n"))

    @abstractmethod
    def iter_articles(self, max_articles: int = 50) -> Iterator[ArticleData]:
        ...