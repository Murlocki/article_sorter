"""
config.py — все настройки проекта в одном месте.
"""

from pathlib import Path

# ── База данных ────────────────────────────────────────────────────────────────

# SQLite — файл создаётся рядом с pipeline.py, ничего устанавливать не нужно
DATABASE_URL = "sqlite:///articles.db"

# Для PostgreSQL раскомментируйте и заполните:
# DATABASE_URL = "postgresql+psycopg2://user:password@localhost:5432/articles"

# ── HTTP ───────────────────────────────────────────────────────────────────────

REQUEST_DELAY   = 1.5   # секунд между запросами к одному сайту
REQUEST_TIMEOUT = 15    # секунд таймаут

# ── Источники ──────────────────────────────────────────────────────────────────

# PubMed: бесплатный API-ключ можно получить на https://www.ncbi.nlm.nih.gov/account/
# Без ключа — 3 req/s, с ключом — 10 req/s
PUBMED_API_KEY = ""

# Semantic Scholar: бесплатный ключ на https://www.semanticscholar.org/product/api
# Без ключа — 100 запросов / 5 мин, с ключом — ~1 req/sec без ограничений
SEMANTIC_SCHOLAR_API_KEY = ""

# ── Минимальная длина текста статьи (символов) ────────────────────────────────

MIN_TEXT_LENGTH = 150
