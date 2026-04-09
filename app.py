"""
app.py — Streamlit-интерфейс для управления скрапером и просмотра БД.
Запуск: streamlit run app.py
"""

from __future__ import annotations

from datetime import datetime, date, timedelta

import logging

import pandas as pd
import streamlit as st

from config import DATABASE_URL, PUBMED_API_KEY


# ── Logging → session_state ────────────────────────────────────────────────────
# Перехватываем все логи скраперов и пишем в session_state.scraping_log

class _StreamlitLogHandler(logging.Handler):
    """Handler который добавляет записи лога в session_state."""
    def emit(self, record):
        try:
            msg = self.format(record)
            if "scraping_log" in st.session_state:
                st.session_state.scraping_log.append(msg)
        except Exception:
            pass

# Сначала инициализируем корневой логгер
_root_logger = logging.getLogger()
_root_logger.setLevel(logging.INFO)

# Удаляем старые хэндлеры нашего типа перед добавлением новых
# (Streamlit переисполняет скрипт при rerun — без этого хэндлеры дублируются)
_root_logger.handlers = [
    h for h in _root_logger.handlers
    if not isinstance(h, (_StreamlitLogHandler,))
]

# Handler 1: все логи → session_state.scraping_log
_log_handler = _StreamlitLogHandler()
_log_handler.setFormatter(logging.Formatter("%(levelname)-8s [%(name)s] %(message)s"))
_root_logger.addHandler(_log_handler)

# Handler 2: только WARNING/ERROR → session_state.scraping_errors
class _ErrorLogHandler(logging.Handler):
    """Собирает только WARNING/ERROR для отдельного отображения."""
    def emit(self, record):
        try:
            if record.levelno >= logging.WARNING:
                msg = self.format(record)
                if "scraping_errors" in st.session_state:
                    st.session_state.scraping_errors.append(msg)
        except Exception:
            pass

_error_handler = _ErrorLogHandler()
_error_handler.setFormatter(logging.Formatter("%(levelname)s [%(name)s] %(message)s"))
_root_logger.addHandler(_error_handler)

# Handler 3: файл — добавляем только один раз
if not any(isinstance(h, logging.FileHandler) for h in _root_logger.handlers):
    _file_handler = logging.FileHandler("pipeline.log", encoding="utf-8")
    _file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
    _root_logger.addHandler(_file_handler)
from db.repository import ArticleRepository
from scrapers.pubmed import PubMedScraper
from scrapers.doaj import DOAJScraper
from scrapers.towards_ds import TowardsDSScraper
from scrapers.habr import HabrScraper

# ── Константы ──────────────────────────────────────────────────────────────────

SCRAPERS = {
    "pubmed":          (PubMedScraper,          {"api_key": PUBMED_API_KEY}),
    "doaj":             (DOAJScraper,            {}),
    "towards_ds":      (TowardsDSScraper,        {}),
    "habr":            (HabrScraper,             {}),
}

SOURCE_LABELS = {
    "pubmed":         "PubMed Central",
    "doaj":             "DOAJ",
    "towards_ds":     "Towards Data Science",
    "habr":           "Habr",
}

RELEVANCE_ICON = {True: "✅ Да", False: "❌ Нет", None: "❓ Не размечено"}

# ── Инициализация ──────────────────────────────────────────────────────────────

st.set_page_config(page_title="Article Scraper", page_icon="🔬", layout="wide")


@st.cache_resource
def get_repo():
    return ArticleRepository(DATABASE_URL)

repo = get_repo()

if "scraping_running" not in st.session_state:
    st.session_state.scraping_running = False
if "scraping_log" not in st.session_state:
    st.session_state.scraping_log = []
if "scraping_errors" not in st.session_state:
    st.session_state.scraping_errors = []
if "last_stats" not in st.session_state:
    st.session_state.last_stats = None
if "edit_id" not in st.session_state:
    st.session_state.edit_id = None

# ── Вкладки ────────────────────────────────────────────────────────────────────

tab_scrape, tab_db, tab_logs = st.tabs([
    "🚀 Скрапинг", "📚 База данных", "📋 Журнал запусков",
])


# ══════════════════════════════════════════════════════════════════════════════
# Вкладка 1 — Скрапинг
# ══════════════════════════════════════════════════════════════════════════════

with tab_scrape:
    st.header("Запуск скрапера")

    col_left, col_right = st.columns([1, 1], gap="large")

    with col_left:
        st.subheader("Источники")
        selected_sources = []
        for key, label in SOURCE_LABELS.items():
            if st.checkbox(label, value=True, key=f"src_{key}"):
                selected_sources.append(key)

        st.subheader("Количество статей")
        max_articles = st.slider(
            "Максимум на источник", min_value=5, max_value=200, value=50, step=5,
        )

    with col_right:
        st.subheader("Временной промежуток публикации")
        use_date_filter = st.toggle("Фильтровать по дате публикации", value=False)
        date_from = date_to = None

        if use_date_filter:
            c1, c2 = st.columns(2)
            with c1:
                date_from = st.date_input("С", value=date.today() - timedelta(days=365))
            with c2:
                date_to = st.date_input("По", value=date.today())
            if date_from and date_to and date_from > date_to:
                st.error("Дата начала должна быть раньше даты конца")
                use_date_filter = False

        st.subheader("Итог запроса")
        if selected_sources:
            st.info(
                f"Источников: **{len(selected_sources)}**  \n"
                f"Макс. статей итого: **{max_articles * len(selected_sources)}**  \n"
                + (f"Период: **{date_from}** — **{date_to}**" if use_date_filter
                   else "Период: **без ограничений**")
            )
        else:
            st.warning("Выберите хотя бы один источник")

    st.divider()

    run_btn = st.button(
        "▶ Запустить скрапинг", type="primary",
        disabled=st.session_state.scraping_running or not selected_sources,
        width="stretch",
    )

    progress_bar  = st.progress(0)
    status_text   = st.empty()
    live_log_placeholder = st.empty()

    if run_btn and not st.session_state.scraping_running:
        st.session_state.scraping_running = True
        st.session_state.scraping_log    = []
        st.session_state.scraping_errors = []

        dt_from = datetime.combine(date_from, datetime.min.time()) if use_date_filter and date_from else None
        dt_to   = datetime.combine(date_to,   datetime.max.time()) if use_date_filter and date_to   else None

        all_stats = []
        total_sources = len(selected_sources)

        for src_idx, source_key in enumerate(selected_sources):
            cls, kwargs = SCRAPERS[source_key]
            label   = SOURCE_LABELS[source_key]
            scraper = cls(**kwargs)
            log_id  = repo.start_log(source_key)
            counters = {"new": 0, "skipped": 0, "errors": 0, "fetched": 0}

            status_text.info(f"⏳ **{label}** ({src_idx + 1}/{total_sources})…")

            try:
                # PubMed поддерживает фильтрацию по дате на уровне API
                iter_kwargs: dict = {"max_articles": max_articles}
                if use_date_filter and source_key == "pubmed":
                    if dt_from:
                        iter_kwargs["mindate"] = dt_from.strftime("%Y/%m/%d")
                    if dt_to:
                        iter_kwargs["maxdate"] = dt_to.strftime("%Y/%m/%d")
                for article in scraper.iter_articles(**iter_kwargs):
                    counters["fetched"] += 1

                    # Фильтр по дате публикации
                    if use_date_filter and article.published_at:
                        if dt_from and article.published_at < dt_from:
                            counters["skipped"] += 1
                            continue
                        if dt_to and article.published_at > dt_to:
                            counters["skipped"] += 1
                            continue

                    ok, reason = repo.save(article)
                    if ok:
                        counters["new"] += 1
                        st.session_state.scraping_log.append(
                            f"✅ [{label}] {article.title[:80]}"
                        )
                    elif reason == "duplicate":
                        counters["skipped"] += 1
                    elif reason == "too_short":
                        counters["skipped"] += 1
                        logging.getLogger("app").debug(
                            "[%s] слишком короткий текст (%d симв): %s",
                            label, len(article.text or ""), article.url
                        )
                    else:
                        counters["errors"] += 1
                        logging.getLogger("app").warning(
                            "[%s] ошибка сохранения (reason=%s): %s",
                            label, reason, article.url
                        )

                    inner = min(counters["fetched"] / max_articles, 1.0)
                    progress_bar.progress((src_idx + inner) / total_sources)

                    # Обновляем лог в реальном времени
                    _recent = st.session_state.scraping_log[-50:]
                    live_log_placeholder.code(
                        "\n".join(_recent),
                        language=None,
                    )

            except Exception as exc:
                st.session_state.scraping_log.append(f"❌ [{label}] Ошибка: {exc}")
                counters["errors"] += 1

            repo.finish_log(log_id, total_fetched=counters["fetched"],
                            total_new=counters["new"], total_skipped=counters["skipped"],
                            total_errors=counters["errors"])

            st.session_state.scraping_log.append(
                f"— [{label}] готово: новых={counters['new']} "
                f"пропущено={counters['skipped']} ошибок={counters['errors']}"
            )
            all_stats.append({"source": label, **counters})

        progress_bar.progress(1.0)
        status_text.success("✅ Скрапинг завершён!")
        st.session_state.last_stats = all_stats
        st.session_state.scraping_running = False
        st.rerun()

    if st.session_state.scraping_log:
        st.subheader("Лог последнего запуска")

        # Фильтры лога
        lf1, lf2, lf3 = st.columns(3)
        with lf1:
            show_info    = st.checkbox("INFO",    value=True,  key="log_info")
        with lf2:
            show_warning = st.checkbox("WARNING", value=True,  key="log_warn")
        with lf3:
            show_errors  = st.checkbox("ERROR",   value=True,  key="log_err")

        all_lines = st.session_state.scraping_log
        filtered  = []
        for line in all_lines:
            if "ERROR" in line and not show_errors:
                continue
            if "WARNING" in line and not show_warning:
                continue
            if not any(lvl in line for lvl in ("ERROR","WARNING")) and not show_info:
                continue
            filtered.append(line)

        # Статистика лога
        n_errors   = sum(1 for l in all_lines if "ERROR"   in l)
        n_warnings = sum(1 for l in all_lines if "WARNING" in l)
        lm1, lm2, lm3 = st.columns(3)
        lm1.metric("Всего строк",  len(all_lines))
        lm2.metric("⚠ Warnings",   n_warnings)
        lm3.metric("❌ Errors",     n_errors)

        # Основной лог со скроллингом через st.container + height
        log_text = "\n".join(filtered[-500:])
        st.text_area(
            label        = "Лог (скроллинг)",
            value        = log_text,
            height       = 400,
            key          = "log_textarea",
            label_visibility = "collapsed",
        )

        # Ошибки отдельным блоком
        if st.session_state.scraping_errors:
            with st.expander(f"⚠ Ошибки и предупреждения ({len(st.session_state.scraping_errors)})", expanded=True):
                for err in st.session_state.scraping_errors:
                    if "ERROR" in err:
                        st.error(err)
                    else:
                        st.warning(err)

        # Скачать полный лог
        st.download_button(
            "⬇ Скачать полный лог",
            data      = "\n".join(all_lines),
            file_name = f"scraping_log_{datetime.now().strftime('%Y%m%d_%H%M')}.txt",
            mime      = "text/plain",
        )

    if st.session_state.last_stats:
        st.subheader("Результаты последнего запуска")
        df = pd.DataFrame(st.session_state.last_stats)
        df.columns = ["Источник", "Новых", "Пропущено", "Ошибок", "Загружено"]
        st.dataframe(df[["Источник", "Загружено", "Новых", "Пропущено", "Ошибок"]],
                     width="stretch", hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# Вкладка 2 — База данных
# ══════════════════════════════════════════════════════════════════════════════

with tab_db:

    # ── Панель редактирования (если выбрана статья) ───────────────────────────
    if st.session_state.edit_id is not None:
        art_id = st.session_state.edit_id
        # Загружаем свежую запись
        records = repo.get_articles(limit=10000)
        art = next((a for a in records if a.id == art_id), None)

        if art is None:
            st.warning("Статья не найдена")
            st.session_state.edit_id = None
        else:
            st.header(f"✏️ Редактирование статьи #{art.id}")

            with st.form("edit_form"):
                new_title = st.text_input("Заголовок", value=art.title or "")
                new_abstract = st.text_area("Аннотация", value=art.abstract or "", height=120)
                new_text = st.text_area("Полный текст", value=art.text or "", height=250)
                new_lang = st.selectbox(
                    "Язык", ["en", "ru"],
                    index=0 if (art.language or "en") == "en" else 1,
                )

                st.divider()
                st.markdown("**Разметка релевантности**")
                c1, c2 = st.columns(2)
                with c1:
                    rel_options = ["Не размечено", "Релевантно", "Нерелевантно"]
                    current_rel = {True: "Релевантно", False: "Нерелевантно", None: "Не размечено"}[art.is_relevant]
                    new_relevant_str = st.selectbox("Релевантность", rel_options,
                                                     index=rel_options.index(current_rel))
                with c2:
                    new_score = st.number_input(
                        "Score (0.0 – 1.0)", min_value=0.0, max_value=1.0,
                        value=float(art.relevance_score or 0.0), step=0.05,
                    )

                st.divider()
                col_save, col_del, col_cancel = st.columns([2, 1, 1])
                with col_save:
                    submitted = st.form_submit_button("💾 Сохранить", type="primary", width="stretch")
                with col_del:
                    delete_btn = st.form_submit_button("🗑 Удалить", width="stretch")
                with col_cancel:
                    cancel_btn = st.form_submit_button("✕ Отмена", width="stretch")

                if submitted:
                    rel_map = {"Не размечено": None, "Релевантно": True, "Нерелевантно": False}
                    ok = repo.update_article(
                        art_id,
                        title=new_title,
                        abstract=new_abstract,
                        text=new_text,
                        language=new_lang,
                        is_relevant=rel_map[new_relevant_str],
                        relevance_score=new_score if new_score > 0 else None,
                        label_source="human" if rel_map[new_relevant_str] is not None else None,
                    )
                    if ok:
                        st.success("Сохранено!")
                        st.session_state.edit_id = None
                        st.rerun()
                    else:
                        st.error("Ошибка при сохранении")

                if delete_btn:
                    repo.delete_article(art_id)
                    st.success(f"Статья #{art_id} удалена")
                    st.session_state.edit_id = None
                    st.rerun()

                if cancel_btn:
                    st.session_state.edit_id = None
                    st.rerun()

        st.divider()

    # ── Статистика ─────────────────────────────────────────────────────────────

    st.header("База данных")

    stats = repo.stats()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Всего статей",     stats["total"])
    c2.metric("Размечено",        stats["labeled"])
    c3.metric("Релевантных",      stats["relevant"])
    c4.metric("Ожидают разметки", stats["unlabeled"])

    if stats.get("by_source"):
        with st.expander("По источникам", expanded=False):
            src_df = pd.DataFrame(
                [(SOURCE_LABELS.get(k, k), v) for k, v in stats["by_source"].items()],
                columns=["Источник", "Статей"],
            ).sort_values("Статей", ascending=False)
            st.bar_chart(src_df.set_index("Источник"))

    st.divider()

    # ── Фильтры ───────────────────────────────────────────────────────────────

    with st.expander("🔍 Фильтры", expanded=True):
        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            f_source = st.selectbox(
                "Источник", ["Все"] + list(SOURCE_LABELS.keys()),
                format_func=lambda x: "Все" if x == "Все" else SOURCE_LABELS[x],
            )
        with fc2:
            f_lang = st.selectbox(
                "Язык", ["Все", "en", "ru"],
                format_func=lambda x: {"Все": "Все", "en": "English", "ru": "Русский"}[x],
            )
        with fc3:
            f_relevant = st.selectbox(
                "Релевантность", ["Все", "Да", "Нет", "Не размечено"],
            )

        fd1, fd2, fd3 = st.columns(3)
        with fd1:
            f_date_from = st.date_input("Загружено с", value=None, key="db_df")
        with fd2:
            f_date_to   = st.date_input("Загружено по", value=None, key="db_dt")
        with fd3:
            f_search = st.text_input("Поиск в заголовке / аннотации", placeholder="pdf extraction…")

    # ── Загрузка данных ───────────────────────────────────────────────────────

    articles = repo.get_articles(
        source_name = None if f_source == "Все" else f_source,
        language    = None if f_lang   == "Все" else f_lang,
        date_from   = datetime.combine(f_date_from, datetime.min.time()) if f_date_from else None,
        date_to     = datetime.combine(f_date_to,   datetime.max.time()) if f_date_to   else None,
        search      = f_search or None,
        limit       = 2000,
    )

    # Фильтр релевантности в Python (None нельзя передать в SQL ==)
    if f_relevant == "Да":
        articles = [a for a in articles if a.is_relevant is True]
    elif f_relevant == "Нет":
        articles = [a for a in articles if a.is_relevant is False]
    elif f_relevant == "Не размечено":
        articles = [a for a in articles if a.is_relevant is None]

    st.caption(f"Найдено: **{len(articles)}** статей")

    # ── Таблица ───────────────────────────────────────────────────────────────

    if articles:
        rows = []
        for a in articles:
            rows.append({
                "ID":          a.id,
                "Источник":    SOURCE_LABELS.get(a.source_name, a.source_name),
                "Яз":          (a.language or "—").upper(),
                "Заголовок":   a.title or "—",
                "Аннотация":   (a.abstract or "")[:100] + ("…" if len(a.abstract or "") > 100 else ""),
                "Опубл.":      a.published_at.strftime("%Y-%m-%d") if a.published_at else "—",
                "Загружено":   a.scraped_at.strftime("%Y-%m-%d %H:%M") if a.scraped_at else "—",
                "Релевантно":  RELEVANCE_ICON[a.is_relevant],
                "Score":       f"{a.relevance_score:.2f}" if a.relevance_score is not None else "—",
                "URL":         a.url,
            })

        df = pd.DataFrame(rows)
        st.dataframe(
            df, width="stretch", hide_index=True,
            column_config={
                "URL":        st.column_config.LinkColumn("URL", display_text="🔗"),
                "Заголовок":  st.column_config.TextColumn("Заголовок", width="large"),
                "Аннотация":  st.column_config.TextColumn("Аннотация", width="large"),
            },
        )

        # ── Выбор статьи для просмотра / редактирования ───────────────────────

        st.subheader("Просмотр и редактирование")
        id_map = {f"[#{a.id}] {(a.title or a.url)[:75]}": a.id for a in articles}
        chosen = st.selectbox("Выберите статью", ["— выберите —"] + list(id_map.keys()))

        if chosen != "— выберите —":
            art_id  = id_map[chosen]
            sel_art = next(a for a in articles if a.id == art_id)

            with st.expander("📄 Содержимое статьи", expanded=True):
                st.markdown(f"**{sel_art.title}**")
                st.markdown(
                    f"Источник: `{SOURCE_LABELS.get(sel_art.source_name, sel_art.source_name)}`  "
                    f"| Язык: `{sel_art.language}`  "
                    f"| Релевантность: {RELEVANCE_ICON[sel_art.is_relevant]}"
                )
                if sel_art.url:
                    st.markdown(f"[🔗 Открыть статью]({sel_art.url})")
                if sel_art.abstract:
                    st.markdown("**Аннотация:**")
                    st.write(sel_art.abstract)
                if sel_art.text:
                    st.markdown("**Текст (первые 3000 символов):**")
                    st.text(sel_art.text[:3000] + ("…" if len(sel_art.text) > 3000 else ""))

            c_edit, c_rel, c_norel = st.columns([1, 1, 1])
            with c_edit:
                if st.button("✏️ Редактировать", width="stretch"):
                    st.session_state.edit_id = art_id
                    st.rerun()
            with c_rel:
                if st.button("✅ Отметить релевантной", width="stretch",
                              disabled=sel_art.is_relevant is True):
                    repo.update_article(art_id, is_relevant=True,
                                        label_source="human", relevance_score=1.0)
                    st.success("Отмечена как релевантная")
                    st.rerun()
            with c_norel:
                if st.button("❌ Отметить нерелевантной", width="stretch",
                              disabled=sel_art.is_relevant is False):
                    repo.update_article(art_id, is_relevant=False,
                                        label_source="human", relevance_score=0.0)
                    st.success("Отмечена как нерелевантная")
                    st.rerun()

        # ── Экспорт ───────────────────────────────────────────────────────────

        st.divider()
        export_df = pd.DataFrame([{
            "id": a.id, "source": a.source_name, "language": a.language,
            "title": a.title, "abstract": a.abstract, "url": a.url,
            "published_at": str(a.published_at or ""), "scraped_at": str(a.scraped_at or ""),
            "is_relevant": a.is_relevant, "relevance_score": a.relevance_score,
        } for a in articles])

        st.download_button(
            "⬇ Скачать CSV", data=export_df.to_csv(index=False, encoding="utf-8-sig"),
            file_name=f"articles_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
        )
    else:
        st.info("Нет статей по выбранным фильтрам")


# ══════════════════════════════════════════════════════════════════════════════
# Вкладка 3 — Журнал запусков
# ══════════════════════════════════════════════════════════════════════════════

with tab_logs:
    st.header("Журнал запусков")

    if st.button("🔄 Обновить"):
        st.rerun()

    logs = repo.get_scraping_logs(limit=100)

    if logs:
        rows = []
        for log in logs:
            dur = ""
            if log.started_at and log.finished_at:
                secs = int((log.finished_at - log.started_at).total_seconds())
                dur = f"{secs // 60}м {secs % 60}с"
            rows.append({
                "Источник":      SOURCE_LABELS.get(log.source_name, log.source_name),
                "Начало":        log.started_at.strftime("%Y-%m-%d %H:%M:%S") if log.started_at else "—",
                "Длительность":  dur or "—",
                "Загружено":     log.total_fetched or 0,
                "Новых":         log.total_new     or 0,
                "Пропущено":     log.total_skipped or 0,
                "Ошибок":        log.total_errors  or 0,
                "Статус":        "✅" if not log.error_details else "❌",
            })

        st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)

        error_logs = [l for l in logs if l.error_details]
        if error_logs:
            with st.expander(f"⚠ Детали ошибок ({len(error_logs)})"):
                for l in error_logs:
                    st.error(
                        f"[{l.source_name}] "
                        f"{l.started_at.strftime('%Y-%m-%d %H:%M') if l.started_at else ''}:\n"
                        f"{l.error_details}"
                    )
    else:
        st.info("Журнал пуст — запустите скрапинг на первой вкладке")