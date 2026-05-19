"""
classifier.py — два классификатора релевантности статей.

KNN (метод по умолчанию):
  - Векторизация через sentence-transformers (SBERT)
  - Модель paraphrase-multilingual-MiniLM-L12-v2 (~120MB, RU+EN)
  - Никакой предобработки текста не нужно — трансформер всё делает сам
  - Кэш: classifier_cache/knn_index.pkl

Naive Bayes:
  - TF-IDF + предобработка: токенизация, удаление стоп-слов, лемматизация
  - MultinomialNB из scikit-learn
  - Работает на русском и английском (раздельные стоп-слова и лемматизаторы)
  - Кэш: classifier_cache/nb_model.pkl
"""

from __future__ import annotations

import logging
import pickle
import re
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)

# Пути кэша
CACHE_DIR      = Path("classifier_cache")
KNN_INDEX_FILE = CACHE_DIR / "knn_index.pkl"
NB_MODEL_FILE  = CACHE_DIR / "nb_model.pkl"

# Алиас для обратной совместимости
INDEX_FILE = KNN_INDEX_FILE

# SBERT модель
SBERT_MODEL = "paraphrase-multilingual-MiniLM-L12-v2"


def _build_input_text(article) -> str:
    title    = (article.title    or "").strip()
    abstract = (article.abstract or "").strip()
    text     = (article.text     or "").strip()
    if len(abstract) >= 100:
        return f"{title}\n\n{abstract}"
    supplement = text[:2000] if text else ""
    return f"{title}\n\n{supplement}".strip()


# ══════════════════════════════════════════════════════════════════════════════
#  1. KNN через SBERT-эмбеддинги
# ══════════════════════════════════════════════════════════════════════════════

class Classifier:
    """KNN классификатор на основе косинусного сходства SBERT векторов."""

    def __init__(self, n_neighbors: int = 5):
        self.n_neighbors = n_neighbors
        self._vectors:  np.ndarray | None = None
        self._labels:   np.ndarray | None = None
        self._ids:      list[int]         = []
        self._sbert_model = None
        self._is_fitted   = False

    def _get_sbert(self):
        if self._sbert_model is None:
            try:
                from sentence_transformers import SentenceTransformer
                logger.info("[knn] Загружаем модель %s...", SBERT_MODEL)
                self._sbert_model = SentenceTransformer(SBERT_MODEL)
                logger.info("[knn] Модель загружена")
            except ImportError:
                raise ImportError("Установите: pip install sentence-transformers")
        return self._sbert_model

    def _vectorize(self, texts: list[str]) -> np.ndarray:
        model = self._get_sbert()
        logger.info("[knn] SBERT векторизация %d текстов...", len(texts))
        vectors = model.encode(texts, show_progress_bar=False, batch_size=32)
        return np.array(vectors, dtype=np.float32)

    def fit(self, articles: list) -> "Classifier":
        labeled = [a for a in articles if a.is_relevant is not None]
        if len(labeled) < 2:
            raise ValueError(f"Нужно минимум 2 размеченных статьи, получено: {len(labeled)}")
        n_pos = sum(1 for a in labeled if a.is_relevant)
        n_neg = len(labeled) - n_pos
        logger.info("[knn] Обучение: %d статей (%d релевантных, %d нерелевантных)",
                    len(labeled), n_pos, n_neg)
        if n_pos == 0 or n_neg == 0:
            raise ValueError("Нужны оба класса (релевантные И нерелевантные статьи)")
        texts   = [_build_input_text(a) for a in labeled]
        labels  = np.array([1 if a.is_relevant else 0 for a in labeled], dtype=np.int8)
        ids     = [a.id for a in labeled]
        vectors = self._vectorize(texts)
        norms = np.linalg.norm(vectors, axis=1, keepdims=True)
        norms = np.where(norms == 0, 1, norms)
        vectors = vectors / norms
        self._vectors   = vectors
        self._labels    = labels
        self._ids       = ids
        self._is_fitted = True
        logger.info("[knn] Обучение завершено, %d векторов", len(vectors))
        self._save_cache()
        return self

    def predict(self, articles: list) -> dict[int, float]:
        if not self._is_fitted:
            raise RuntimeError("Сначала вызовите fit()")
        if not articles:
            return {}
        texts   = [_build_input_text(a) for a in articles]
        vectors = self._vectorize(texts)
        norms = np.linalg.norm(vectors, axis=1, keepdims=True)
        norms = np.where(norms == 0, 1, norms)
        vectors = vectors / norms
        similarities = vectors @ self._vectors.T
        scores: dict[int, float] = {}
        k = min(self.n_neighbors, len(self._labels))
        for i, article in enumerate(articles):
            sims     = similarities[i]
            top_k    = np.argsort(sims)[-k:]
            k_labels = self._labels[top_k]
            k_sims   = sims[top_k]
            weights  = k_sims + 1e-8
            score    = float(np.dot(weights, k_labels) / weights.sum())
            scores[article.id] = round(score, 4)
        return scores

    def predict_one(self, article) -> float:
        return self.predict([article]).get(article.id, 0.0)

    def _save_cache(self):
        CACHE_DIR.mkdir(exist_ok=True)
        state = {
            "n_neighbors": self.n_neighbors,
            "vectors":     self._vectors,
            "labels":      self._labels,
            "ids":         self._ids,
        }
        with open(KNN_INDEX_FILE, "wb") as f:
            pickle.dump(state, f)
        logger.info("[knn] Индекс сохранён -> %s", KNN_INDEX_FILE)

    def load_cache(self) -> bool:
        if not KNN_INDEX_FILE.exists():
            return False
        try:
            with open(KNN_INDEX_FILE, "rb") as f:
                state = pickle.load(f)
            self.n_neighbors = state["n_neighbors"]
            self._vectors    = state["vectors"]
            self._labels     = state["labels"]
            self._ids        = state["ids"]
            self._is_fitted  = True
            logger.info("[knn] Индекс загружен: %d векторов", len(self._ids))
            return True
        except Exception as e:
            logger.warning("[knn] Ошибка загрузки кэша: %s", e)
            return False

    @staticmethod
    def reset_cache():
        if KNN_INDEX_FILE.exists():
            KNN_INDEX_FILE.unlink()
            logger.info("[knn] Кэш удалён")

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


# ══════════════════════════════════════════════════════════════════════════════
#  2. Naive Bayes через TF-IDF + предобработка текста
# ══════════════════════════════════════════════════════════════════════════════

def _ensure_nltk():
    import nltk
    for resource, path in [
        ("punkt_tab",  "tokenizers/punkt_tab"),
        ("stopwords",  "corpora/stopwords"),
        ("wordnet",    "corpora/wordnet"),
    ]:
        try:
            nltk.data.find(path)
        except LookupError:
            logger.info("[nb] Загружаем NLTK ресурс: %s", resource)
            nltk.download(resource, quiet=True)


def _preprocess_text(text: str) -> str:
    """
    Предобработка текста для Naive Bayes:
      1. Нижний регистр
      2. Удаление знаков препинания и цифр
      3. Токенизация
      4. Удаление стоп-слов (EN + RU)
      5. Лемматизация (EN — WordNetLemmatizer, RU — pymorphy3/pymorphy2)

    Именно здесь предобработка имеет смысл: TF-IDF считает каждый токен
    независимо, поэтому "extract", "extraction", "extracting" — три разные
    фичи без лемматизации. Стоп-слова ("и", "в", "the", "of") засоряют
    словарь и снижают качество.
    """
    _ensure_nltk()
    from nltk.tokenize import word_tokenize
    from nltk.corpus import stopwords
    from nltk.stem import WordNetLemmatizer

    text = text.lower()
    text = re.sub(r"[^a-zа-яёA-ZА-ЯЁ\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    tokens = word_tokenize(text)

    try:
        stop_en = set(stopwords.words("english"))
    except Exception:
        stop_en = set()
    try:
        stop_ru = set(stopwords.words("russian"))
    except Exception:
        stop_ru = set()
    stop_words = stop_en | stop_ru
    tokens = [t for t in tokens if t not in stop_words and len(t) > 2]

    en_lemmatizer = WordNetLemmatizer()
    ru_analyzer = None
    for morph_lib in ("pymorphy3", "pymorphy2"):
        try:
            import importlib
            morph = importlib.import_module(morph_lib)
            ru_analyzer = morph.MorphAnalyzer()
            break
        except ImportError:
            continue

    result = []
    for token in tokens:
        is_russian = bool(re.search(r"[а-яёА-ЯЁ]", token))
        if is_russian:
            lemma = ru_analyzer.parse(token)[0].normal_form if ru_analyzer else token
        else:
            lemma = en_lemmatizer.lemmatize(token)
        result.append(lemma)

    return " ".join(result)


class NaiveBayesClassifier:
    """
    Наивный Байес на TF-IDF фичах с предобработкой текста.

    Преимущества перед KNN:
      - Молниеносное предсказание
      - Интерпретируемость: топ слов каждого класса
      - Хорошо на малой выборке
      - Предобработка реально улучшает качество (в отличие от SBERT)

    Ограничения:
      - Не понимает семантику (синонимы — разные фичи)
      - Хуже с короткими текстами без abstract
    """

    def __init__(self, max_features: int = 10000, ngram_range: tuple = (1, 2)):
        self.max_features = max_features
        self.ngram_range  = ngram_range
        self._vectorizer  = None
        self._model       = None
        self._ids: list[int] = []
        self._is_fitted   = False

    def fit(self, articles: list) -> "NaiveBayesClassifier":
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.naive_bayes import MultinomialNB

        labeled = [a for a in articles if a.is_relevant is not None]
        if len(labeled) < 2:
            raise ValueError(f"Нужно минимум 2 размеченных статьи, получено: {len(labeled)}")
        n_pos = sum(1 for a in labeled if a.is_relevant)
        n_neg = len(labeled) - n_pos
        logger.info("[nb] Обучение: %d статей (%d релевантных, %d нерелевантных)",
                    len(labeled), n_pos, n_neg)
        if n_pos == 0 or n_neg == 0:
            raise ValueError("Нужны оба класса (релевантные И нерелевантные статьи)")

        logger.info("[nb] Предобработка текстов (лемматизация + стоп-слова)...")
        raw_texts = [_build_input_text(a) for a in labeled]
        texts     = [_preprocess_text(t) for t in raw_texts]
        labels    = [1 if a.is_relevant else 0 for a in labeled]

        self._vectorizer = TfidfVectorizer(
            max_features = self.max_features,
            ngram_range  = self.ngram_range,
            sublinear_tf = True,
        )
        X = self._vectorizer.fit_transform(texts)
        self._model     = MultinomialNB(alpha=1.0)
        self._model.fit(X, labels)
        self._ids       = [a.id for a in labeled]
        self._is_fitted = True

        logger.info("[nb] Обучение завершено, словарь: %d фич",
                    len(self._vectorizer.vocabulary_))
        self._save_cache()
        return self

    def predict(self, articles: list) -> dict[int, float]:
        if not self._is_fitted:
            raise RuntimeError("Сначала вызовите fit()")
        if not articles:
            return {}
        logger.info("[nb] Предобработка %d текстов...", len(articles))
        texts  = [_preprocess_text(_build_input_text(a)) for a in articles]
        X      = self._vectorizer.transform(texts)
        probas = self._model.predict_proba(X)[:, 1]
        return {a.id: round(float(p), 4) for a, p in zip(articles, probas)}

    def predict_one(self, article) -> float:
        return self.predict([article]).get(article.id, 0.0)

    def top_features(self, n: int = 20) -> dict[str, list[tuple[str, float]]]:
        """Топ слов для каждого класса — интерпретация модели."""
        if not self._is_fitted:
            return {}
        feature_names = self._vectorizer.get_feature_names_out()
        log_probs     = self._model.feature_log_prob_
        result = {}
        for cls_idx, cls_name in [(1, "relevant"), (0, "irrelevant")]:
            top_idx = np.argsort(log_probs[cls_idx])[-n:][::-1]
            result[cls_name] = [
                (feature_names[i], round(float(log_probs[cls_idx][i]), 3))
                for i in top_idx
            ]
        return result

    def _save_cache(self):
        CACHE_DIR.mkdir(exist_ok=True)
        state = {
            "max_features": self.max_features,
            "ngram_range":  self.ngram_range,
            "vectorizer":   self._vectorizer,
            "model":        self._model,
            "ids":          self._ids,
        }
        with open(NB_MODEL_FILE, "wb") as f:
            pickle.dump(state, f)
        logger.info("[nb] Модель сохранена -> %s", NB_MODEL_FILE)

    def load_cache(self) -> bool:
        if not NB_MODEL_FILE.exists():
            return False
        try:
            with open(NB_MODEL_FILE, "rb") as f:
                state = pickle.load(f)
            self.max_features = state["max_features"]
            self.ngram_range  = state["ngram_range"]
            self._vectorizer  = state["vectorizer"]
            self._model       = state["model"]
            self._ids         = state["ids"]
            self._is_fitted   = True
            logger.info("[nb] Модель загружена: %d образцов, словарь %d фич",
                        len(self._ids), len(self._vectorizer.vocabulary_))
            return True
        except Exception as e:
            logger.warning("[nb] Ошибка загрузки кэша: %s", e)
            return False

    @staticmethod
    def reset_cache():
        if NB_MODEL_FILE.exists():
            NB_MODEL_FILE.unlink()
            logger.info("[nb] Кэш удалён")

    @property
    def is_fitted(self) -> bool:
        return self._is_fitted

    @property
    def n_labeled(self) -> int:
        return len(self._ids) if self._ids else 0

    @property
    def n_positive(self) -> int:
        if self._model is None or len(self._model.class_count_) < 2:
            return 0
        return int(self._model.class_count_[1])

    @property
    def n_negative(self) -> int:
        return self.n_labeled - self.n_positive