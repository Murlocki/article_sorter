"""
pipeline.py — запускает скраперы и сохраняет статьи в БД.

Запуск из корня проекта (папки где лежит этот файл):

  python pipeline.py                         # все источники
  python pipeline.py --source habr           # один источник
  python pipeline.py --source pubmed --max 20
  python pipeline.py --stats                 # статистика из БД
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from typing import Type

from config import DATABASE_URL, PUBMED_API_KEY
from db.repository import ArticleRepository
from scrapers.base import BaseScraper
from scrapers.pubmed import PubMedScraper
from scrapers.doaj import DOAJScraper
from scrapers.towards_ds import TowardsDSScraper
from scrapers.habr import HabrScraper

# ── Логирование ────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("pipeline")


# ── Реестр источников ──────────────────────────────────────────────────────────

@dataclass
class SourceConfig:
    key:         str
    cls:         Type[BaseScraper]
    max_default: int
    kwargs:      dict


SOURCES: list[SourceConfig] = [
    SourceConfig("pubmed",         PubMedScraper,         60, {"api_key": PUBMED_API_KEY}),
    SourceConfig("doaj",            DOAJScraper,            50, {}),
    SourceConfig("towards_ds",     TowardsDSScraper,      60, {}),
    SourceConfig("habr",           HabrScraper,           60, {}),
]

SOURCES_BY_KEY = {s.key: s for s in SOURCES}


# ── Обработка одного источника ─────────────────────────────────────────────────

def run_source(cfg: SourceConfig, repo: ArticleRepository, max_articles: int) -> dict:
    scraper  = cfg.cls(**cfg.kwargs)
    log_id   = repo.start_log(cfg.key)
    counters = {"fetched": 0, "new": 0, "skipped": 0, "errors": 0}

    logger.info("▶ [%s] старт (max=%d)", cfg.key, max_articles)

    try:
        for article in scraper.iter_articles(max_articles=max_articles):
            counters["fetched"] += 1
            logger.info(
                "[%s] #%d получена: %s",
                cfg.key, counters["fetched"], article.title[:70],
            )

            ok, reason = repo.save(article)

            if ok:
                counters["new"] += 1
                logger.info("[%s] ✓ сохранена в БД (новых: %d/%d)",
                            cfg.key, counters["new"], max_articles)
                # Останавливаемся когда набрали нужное число НОВЫХ статей
                if counters["new"] >= max_articles:
                    logger.info("[%s] достигнут лимит новых статей (%d)",
                                cfg.key, max_articles)
                    break
            elif reason == "duplicate":
                counters["skipped"] += 1
                logger.info("[%s] — дубль (пропущено: %d)", cfg.key, counters["skipped"])
            elif reason == "too_short":
                counters["skipped"] += 1
            else:
                counters["errors"] += 1
                logger.warning("[%s] ✗ ошибка сохранения", cfg.key)

    except Exception as exc:
        logger.error("[%s] критическая ошибка: %s", cfg.key, exc, exc_info=True)
        counters["errors"] += 1

    repo.finish_log(
        log_id,
        total_fetched = counters["fetched"],
        total_new     = counters["new"],
        total_skipped = counters["skipped"],
        total_errors  = counters["errors"],
    )

    logger.info(
        "◀ [%s] готово — загружено: %d  новых: %d  дублей: %d  ошибок: %d",
        cfg.key,
        counters["fetched"], counters["new"],
        counters["skipped"], counters["errors"],
    )
    return counters


# ── Отчёт ──────────────────────────────────────────────────────────────────────

def print_stats(repo: ArticleRepository):
    s = repo.stats()
    print(f"\n{'─' * 50}")
    print(f"  Статей всего:       {s['total']}")
    print(f"  Размечено:          {s['labeled']}")
    print(f"  Релевантных:        {s['relevant']}")
    print(f"  Ожидают разметки:   {s['unlabeled']}")
    if s.get("by_source"):
        print(f"  По источникам:")
        for src, cnt in sorted(s["by_source"].items()):
            print(f"    {src:<20} {cnt}")
    print(f"{'─' * 50}\n")


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Скрапер научных и блог-статей",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--source",
        choices=list(SOURCES_BY_KEY),
        metavar="NAME",
        help="Источник: " + ", ".join(SOURCES_BY_KEY),
    )
    parser.add_argument(
        "--max", type=int, default=None,
        help="Максимум статей на источник (по умолчанию из конфига)",
    )
    parser.add_argument(
        "--stats", action="store_true",
        help="Показать статистику БД и выйти",
    )
    args = parser.parse_args()

    repo = ArticleRepository(DATABASE_URL)

    if args.stats:
        print_stats(repo)
        return

    sources = [SOURCES_BY_KEY[args.source]] if args.source else SOURCES
    total   = {"fetched": 0, "new": 0, "skipped": 0, "errors": 0}

    for cfg in sources:
        counters = run_source(cfg, repo, args.max or cfg.max_default)
        for k in total:
            total[k] += counters.get(k, 0)

    logger.info(
        "✓ Итого — загружено: %d  новых: %d  дублей: %d  ошибок: %d",
        total["fetched"], total["new"], total["skipped"], total["errors"],
    )
    print_stats(repo)


if __name__ == "__main__":
    main()