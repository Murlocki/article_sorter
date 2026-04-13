"""
classifier.py — KNN классификатор релевантности статей.

Два режима векторизации на выбор:
  1. sentence-transformers (рекомендуется) — семантические эмбеддинги,
     решает проблему разных длин текстов автоматически, понимает RU+EN
  2. TF-IDF — быстро, без зависимостей, fallback если нет GPU/интернета

Логика входного текста:
  - Основа: title + abstract (нормализованная длина у всех источников)
  - Дополнение: text[:2000] если abstract пустой или короткий (<100 символов)
  - Короткие статьи (Medium сниппеты): title + text — всё что есть

Использование:
  clf = Classifier(method="sbert")   # или "tfidf"
  clf.fit(labeled_articles)          # обучение на размеченных статьях
  scores = clf.predict(articles)     # возвращает dict {article_id: score}
"""

from __future__ import annotations

import logging
import os
import pickle
from pathlib import Path
import numpy as np

logger = logging.getLogger(__name__)

# Путь для кэша модели и индекса
CACHE_DIR  = Path("classifier_cache")
INDEX_FILE = CACHE_DIR / "knn_index.pkl"

# paraphrase-multilingual — поддерживает RU+EN, ~120MB
SBERT_MODEL = "paraphrase-multilingual-MiniLM-L12-v2"


def _build_input_text(article) -> str:
    """
    Формирует входной текст для векторизации.
    Приоритет: title + abstract > title + text[:2000]
    """
    title    = (article.title    or "").strip()
    abstract = (article.abstract or "").strip()
    text     = (article.text     or "").strip()

    # Если есть нормальный abstract (>100 символов) — используем его
    if len(abstract) >= 100:
        return f"{title}\n\n{abstract}"

    # Иначе берём начало полного текста
    supplement = text[:2000] if text else ""
    return f"{title}\n\n{supplement}".strip()


class Classifier:
    """KNN классификатор на основе косинусного сходства SBERT векторов."""

    def __init__(self, n_neighbors: int = 5):
        self.n_neighbors = n_neighbors

        # Состояние после fit()
        self._vectors:  np.ndarray | None = None
        self._labels:   np.ndarray | None = None
        self._ids:      list[int]         = []

        self._sbert_model = None
        self._is_fitted   = False

    # ── Векторизация ──────────────────────────────────────────────────────────

    def _get_sbert(self):
        if self._sbert_model is None:
            try:
                from sentence_transformers import SentenceTransformer
                logger.info("[classifier] Загружаем модель %s...", SBERT_MODEL)
                self._sbert_model = SentenceTransformer(SBERT_MODEL)
                logger.info("[classifier] Модель загружена")
            except ImportError:
                raise ImportError(
                    "Установите: pip install sentence-transformers"
                )
        return self._sbert_model

    def _vectorize_sbert(self, texts: list[str]) -> np.ndarray:
        model = self._get_sbert()
        logger.info("[classifier] SBERT векторизация %d текстов...", len(texts))
        vectors = model.encode(texts, show_progress_bar=False, batch_size=32)
        return np.array(vectors, dtype=np.float32)

    def _vectorize(self, texts: list[str]) -> np.ndarray:
        return self._vectorize_sbert(texts)

    # ── Обучение ──────────────────────────────────────────────────────────────

    def fit(self, articles: list) -> "Classifier":
        """
        Обучить на размеченных статьях.
        articles — список объектов Article с is_relevant != None.
        """
        labeled = [a for a in articles if a.is_relevant is not None]
        if len(labeled) < 2:
            raise ValueError(
                f"Нужно минимум 2 размеченных статьи, получено: {len(labeled)}"
            )

        n_pos = sum(1 for a in labeled if a.is_relevant)
        n_neg = len(labeled) - n_pos
        logger.info("[classifier] Обучение: %d статей (%d релевантных, %d нерелевантных)",
                    len(labeled), n_pos, n_neg)

        if n_pos == 0 or n_neg == 0:
            raise ValueError(
                "Нужны оба класса (релевантные И нерелевантные статьи)"
            )

        texts  = [_build_input_text(a) for a in labeled]
        labels = np.array([1 if a.is_relevant else 0 for a in labeled], dtype=np.int8)
        ids    = [a.id for a in labeled]

        vectors = self._vectorize(texts)

        # L2-нормализация для косинусного сходства через dot product
        norms = np.linalg.norm(vectors, axis=1, keepdims=True)
        norms = np.where(norms == 0, 1, norms)   # избегаем деления на 0
        vectors = vectors / norms

        self._vectors  = vectors
        self._labels   = labels
        self._ids      = ids
        self._is_fitted = True

        logger.info("[classifier] Обучение завершено, %d векторов", len(vectors))
        self._save_cache()
        return self

    # ── Предсказание ──────────────────────────────────────────────────────────

    def predict(self, articles: list) -> dict[int, float]:
        """
        Предсказать релевантность для списка статей.
        Возвращает {article_id: score} где score ∈ [0, 1].
        """
        if not self._is_fitted:
            raise RuntimeError("Сначала вызовите fit()")

        if not articles:
            return {}

        texts   = [_build_input_text(a) for a in articles]
        vectors = self._vectorize(texts)

        # Нормализуем
        norms = np.linalg.norm(vectors, axis=1, keepdims=True)
        norms = np.where(norms == 0, 1, norms)
        vectors = vectors / norms

        # Косинусное сходство = dot product нормализованных векторов
        # shape: (n_predict, n_train)
        similarities = vectors @ self._vectors.T

        scores: dict[int, float] = {}
        k = min(self.n_neighbors, len(self._labels))

        for i, article in enumerate(articles):
            sims    = similarities[i]
            top_k   = np.argsort(sims)[-k:]
            k_labels = self._labels[top_k]
            k_sims   = sims[top_k]

            # Взвешенное голосование: ближайшие соседи имеют больший вес
            weights  = k_sims + 1e-8   # избегаем нулевых весов
            score    = float(np.dot(weights, k_labels) / weights.sum())
            scores[article.id] = round(score, 4)

        return scores

    def predict_one(self, article) -> float:
        return self.predict([article]).get(article.id, 0.0)

    # ── Кэш ──────────────────────────────────────────────────────────────────

    def _save_cache(self):
        CACHE_DIR.mkdir(exist_ok=True)
        state = {
            "n_neighbors": self.n_neighbors,
            "vectors":     self._vectors,
            "labels":      self._labels,
            "ids":         self._ids,
        }
        with open(INDEX_FILE, "wb") as f:
            pickle.dump(state, f)
        logger.info("[classifier] Индекс сохранён → %s", INDEX_FILE)

    def load_cache(self) -> bool:
        """Загрузить сохранённый индекс. Возвращает True если успешно."""
        if not INDEX_FILE.exists():
            return False
        try:
            with open(INDEX_FILE, "rb") as f:
                state = pickle.load(f)
            self.n_neighbors = state["n_neighbors"]
            self._vectors    = state["vectors"]
            self._labels     = state["labels"]
            self._ids        = state["ids"]
            self._is_fitted  = True
            logger.info("[classifier] Индекс загружен: %d векторов", len(self._ids))
            return True
        except Exception as e:
            logger.warning("[classifier] Ошибка загрузки кэша: %s", e)
            return False

    @property
    def is_fitted(self) -> bool:
        return self._is_fitted

    @property
    def n_labeled(self) -> int:
        return len(self._ids) if self._ids else 0

    @property
    def n_positive(self) -> int:
        return int(self._labels.sum()) if self._labels is not None else 0

    @property
    def n_negative(self) -> int:
        return self.n_labeled - self.n_positive