"""
app.py — Streamlit-интерфейс для управления скрапером и просмотра БД.
Запуск: streamlit run app.py
"""

from __future__ import annotations

from datetime import datetime

import logging
import pandas as pd
import streamlit as st

from config import DATABASE_URL, PUBMED_API_KEY


# ── Logging ────────────────────────────────────────────────────────────────────

class _StreamlitLogHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            if "scraping_log" in st.session_state:
                st.session_state.scraping_log.append(msg)
        except Exception:
            pass

_root_logger = logging.getLogger()
_root_logger.setLevel(logging.INFO)
# Удаляем ВСЕ наши хэндлеры перед добавлением новых
# (Streamlit переисполняет скрипт при каждом rerun)
_root_logger.handlers = [
    h for h in _root_logger.handlers
    if not isinstance(h, (_StreamlitLogHandler, _ErrorLogHandler))
]
_log_handler = _StreamlitLogHandler()
_log_handler.setFormatter(logging.Formatter("%(levelname)-8s [%(name)s] %(message)s"))
_root_logger.addHandler(_log_handler)

class _ErrorLogHandler(logging.Handler):
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
    "pubmed":     (PubMedScraper,    {"api_key": PUBMED_API_KEY}),
    "doaj":       (DOAJScraper,      {}),
    "towards_ds": (TowardsDSScraper, {}),
    "habr":       (HabrScraper,      {}),
}

SOURCE_LABELS = {
    "pubmed":     "PubMed Central",
    "doaj":       "DOAJ",
    "towards_ds": "Towards Data Science",
    "habr":       "Habr",
}

RELEVANCE_ICON = {True: "✅ Да", False: "❌ Нет", None: "❓ Не размечено"}
REL_OPTIONS    = ["Не размечено", "Релевантно", "Нерелевантно"]
REL_MAP        = {"Не размечено": None, "Релевантно": True, "Нерелевантно": False}
REL_RMAP       = {True: "Релевантно", False: "Нерелевантно", None: "Не размечено"}

# ── Инициализация ──────────────────────────────────────────────────────────────

st.set_page_config(page_title="Article Scraper", page_icon="🔬", layout="wide")

@st.cache_resource
def get_repo():
    return ArticleRepository(DATABASE_URL)

repo = get_repo()

for key, default in [
    ("scraping_running", False),
    ("scraping_log",     []),
    ("scraping_errors",  []),
    ("last_stats",       None),
    ("edit_id",          None),
    ("new_article_ids",  []),
]:
    if key not in st.session_state:
        st.session_state[key] = default


# ── Форма редактирования ────────────────────────────────────────────────────────

def render_edit_form(art, form_key: str, show_text: bool = False):
    with st.form(form_key):
        new_title    = st.text_input("Заголовок", value=art.title or "")
        new_abstract = st.text_area("Аннотация", value=art.abstract or "", height=100)
        if show_text:
            new_text = st.text_area("Полный текст", value=art.text or "", height=200)
        else:
            new_text = art.text
        new_lang = st.selectbox("Язык", ["en", "ru"],
                                 index=0 if (art.language or "en") == "en" else 1)
        c1, c2 = st.columns(2)
        with c1:
            new_rel_str = st.selectbox("Релевантность", REL_OPTIONS,
                                        index=REL_OPTIONS.index(REL_RMAP[art.is_relevant]))
        with c2:
            new_score = st.number_input("Score (0–1)", min_value=0.0, max_value=1.0,
                                         value=float(art.relevance_score or 0.0), step=0.05)
        cs, cd, cc = st.columns([2, 1, 1])
        with cs:
            submitted = st.form_submit_button("💾 Сохранить", type="primary", width="stretch")
        with cd:
            deleted = st.form_submit_button("🗑 Удалить", width="stretch")
        with cc:
            cancelled = st.form_submit_button("✕ Отмена", width="stretch")

    if submitted:
        new_rel = REL_MAP[new_rel_str]
        repo.update_article(art.id, title=new_title, abstract=new_abstract, text=new_text,
                            language=new_lang, is_relevant=new_rel,
                            relevance_score=new_score if new_score > 0 else None,
                            label_source="human" if new_rel is not None else None)
        st.success("Сохранено!")
        return "saved"
    if deleted:
        repo.delete_article(art.id)
        st.success(f"Статья #{art.id} удалена")
        return "deleted"
    if cancelled:
        return "cancelled"
    return None


# ── Вкладки ────────────────────────────────────────────────────────────────────

tab_scrape, tab_db, tab_logs = st.tabs(["🚀 Скрапинг", "📚 База данных", "📋 Журнал запусков"])


# ══ Вкладка 1 — Скрапинг ══════════════════════════════════════════════════════

with tab_scrape:
    st.header("Запуск скрапера")

    col_left, col_right = st.columns([1, 1], gap="large")
    with col_left:
        st.subheader("Источники")
        selected_sources = []
        for key, label in SOURCE_LABELS.items():
            if st.checkbox(label, value=True, key=f"src_{key}"):
                selected_sources.append(key)
        st.subheader("Лимит новых статей")
        max_articles = st.slider("Новых статей на источник",
                                   min_value=5, max_value=2000, value=50, step=5)
    with col_right:
        st.subheader("Поисковый запрос")
        custom_query = st.text_input(
            "Свой запрос (необязательно)",
            placeholder="например: pdf table extraction EHR",
            help="Если заполнено — используется вместо встроенных запросов скрапера. "
                 "Для PubMed можно использовать [tiab], AND/OR. "
                 "Для Habr — слова через пробел. "
                 "Для DOAJ — слова через +.",
        )
        st.subheader("Итог запроса")
        if selected_sources:
            mode = "**свой запрос**" if custom_query else "встроенные запросы"
            st.info(
                f"Источников: **{len(selected_sources)}**  \n"
                f"Лимит новых: **{max_articles}** на источник  \n"
                f"Режим поиска: {mode}"
            )
        else:
            st.warning("Выберите хотя бы один источник")

    st.divider()

    run_btn = st.button("▶ Запустить скрапинг", type="primary",
                         disabled=st.session_state.scraping_running or not selected_sources,
                         width="stretch")
    progress_bar         = st.progress(0)
    status_text          = st.empty()
    live_log_placeholder = st.empty()

    if run_btn and not st.session_state.scraping_running:
        st.session_state.scraping_running = True
        st.session_state.scraping_log     = []
        st.session_state.scraping_errors  = []
        st.session_state.new_article_ids  = []

        all_stats     = []
        total_sources = len(selected_sources)

        for src_idx, source_key in enumerate(selected_sources):
            cls, kwargs = SCRAPERS[source_key]
            label    = SOURCE_LABELS[source_key]
            scraper  = cls(**kwargs)
            log_id   = repo.start_log(source_key)
            counters = {"new": 0, "skipped": 0, "errors": 0, "fetched": 0}

            status_text.info(f"⏳ **{label}** ({src_idx + 1}/{total_sources})…")

            try:
                iter_kwargs: dict = {"max_articles": max_articles}
                if custom_query:
                    iter_kwargs["custom_query"] = custom_query.strip()
                for article in scraper.iter_articles(**iter_kwargs):
                    counters["fetched"] += 1
                    ok, reason = repo.save(article)
                    if ok:
                        counters["new"] += 1
                        st.session_state.scraping_log.append(f"✅ [{label}] {article.title[:80]}")
                        # Запоминаем ID новой статьи для разметки
                        saved = repo.get_articles(search=article.title[:50], limit=1)
                        if saved:
                            st.session_state.new_article_ids.append(saved[0].id)
                        if counters["new"] >= max_articles:
                            break
                    elif reason == "duplicate":
                        counters["skipped"] += 1
                    else:
                        counters["errors"] += 1
                        logging.getLogger("app").warning(
                            "[%s] ошибка сохранения (reason=%s): %s", label, reason, article.url)

                    inner = min(counters["new"] / max_articles, 1.0)
                    progress_bar.progress((src_idx + inner) / total_sources)
                    live_log_placeholder.code(
                        "\n".join(st.session_state.scraping_log[-50:]), language=None)

            except Exception as exc:
                st.session_state.scraping_log.append(f"❌ [{label}] Ошибка: {exc}")
                counters["errors"] += 1

            repo.finish_log(log_id, total_fetched=counters["fetched"],
                            total_new=counters["new"], total_skipped=counters["skipped"],
                            total_errors=counters["errors"])
            st.session_state.scraping_log.append(
                f"— [{label}] готово: новых={counters['new']} "
                f"пропущено={counters['skipped']} ошибок={counters['errors']}")
            all_stats.append({"source": label, **counters})

        progress_bar.progress(1.0)
        status_text.success("✅ Скрапинг завершён!")
        st.session_state.last_stats       = all_stats
        st.session_state.scraping_running = False
        st.rerun()

    # ── Лог ───────────────────────────────────────────────────────────────────
    if st.session_state.scraping_log:
        st.subheader("Лог последнего запуска")
        lf1, lf2, lf3 = st.columns(3)
        show_info    = lf1.checkbox("INFO",    value=True, key="log_info")
        show_warning = lf2.checkbox("WARNING", value=True, key="log_warn")
        show_errors  = lf3.checkbox("ERROR",   value=True, key="log_err")
        all_lines = st.session_state.scraping_log
        filtered  = [l for l in all_lines if (
            ("ERROR"   in l and show_errors) or
            ("WARNING" in l and show_warning) or
            (not any(x in l for x in ("ERROR","WARNING")) and show_info))]
        n_err  = sum(1 for l in all_lines if "ERROR"   in l)
        n_warn = sum(1 for l in all_lines if "WARNING" in l)
        lm1, lm2, lm3 = st.columns(3)
        lm1.metric("Всего строк", len(all_lines))
        lm2.metric("⚠ Warnings",  n_warn)
        lm3.metric("❌ Errors",    n_err)
        st.text_area("Лог", value="\n".join(filtered[-500:]), height=300,
                     key="log_textarea", label_visibility="collapsed")
        if st.session_state.scraping_errors:
            with st.expander(f"⚠ Ошибки ({len(st.session_state.scraping_errors)})", expanded=True):
                for err in st.session_state.scraping_errors:
                    st.error(err) if "ERROR" in err else st.warning(err)
        st.download_button("⬇ Скачать лог", data="\n".join(all_lines),
                            file_name=f"scraping_log_{datetime.now().strftime('%Y%m%d_%H%M')}.txt",
                            mime="text/plain")

    # ── Результаты ────────────────────────────────────────────────────────────
    if st.session_state.last_stats:
        st.subheader("Результаты последнего запуска")
        df = pd.DataFrame(st.session_state.last_stats)
        df.columns = ["Источник", "Новых", "Пропущено", "Ошибок", "Загружено"]
        st.dataframe(df[["Источник", "Загружено", "Новых", "Пропущено", "Ошибок"]],
                     width="stretch", hide_index=True)

    # ── Разметка новых статей ─────────────────────────────────────────────────
    new_ids = st.session_state.get("new_article_ids", [])
    if new_ids:
        st.divider()
        # Получаем актуальные данные из БД
        all_arts = repo.get_articles(limit=100000)
        id_to_art = {a.id: a for a in all_arts}
        new_arts  = [id_to_art[i] for i in new_ids if i in id_to_art]

        show_unlabeled = st.checkbox("Только неразмеченные", value=True, key="new_only_unlabeled")
        if show_unlabeled:
            display = [a for a in new_arts if a.is_relevant is None]
        else:
            display = new_arts

        st.subheader(f"Разметка новых статей — показано {len(display)} из {len(new_arts)}")

        if not display:
            st.success("Все новые статьи размечены!")
        else:
            for art in display:
                is_editing = st.session_state.edit_id == art.id
                with st.expander(
                    f"{RELEVANCE_ICON[art.is_relevant]}  "
                    f"[{SOURCE_LABELS.get(art.source_name, art.source_name)}]  "
                    f"{(art.title or art.url)[:90]}",
                    expanded=is_editing,
                ):
                    if not is_editing:
                        # Обычный вид: аннотация + кнопки
                        if art.abstract:
                            st.caption(art.abstract[:400])
                        if art.url:
                            st.markdown(f"[🔗 Открыть]({art.url})")
                        bc1, bc2, bc3, bc4 = st.columns([2, 2, 2, 1])
                        with bc1:
                            if st.button("✅ Релевантна", key=f"nr_{art.id}",
                                          width="stretch",
                                          disabled=art.is_relevant is True):
                                repo.update_article(art.id, is_relevant=True,
                                                    label_source="human", relevance_score=1.0)
                                st.rerun()
                        with bc2:
                            if st.button("❌ Нерелевантна", key=f"nn_{art.id}",
                                          width="stretch",
                                          disabled=art.is_relevant is False):
                                repo.update_article(art.id, is_relevant=False,
                                                    label_source="human", relevance_score=0.0)
                                st.rerun()
                        with bc3:
                            if st.button("✏️ Редактировать", key=f"ne_{art.id}",
                                          width="stretch"):
                                st.session_state.edit_id = art.id
                                st.rerun()
                        with bc4:
                            if st.button("🗑", key=f"nd_{art.id}", width="stretch"):
                                repo.delete_article(art.id)
                                st.session_state.new_article_ids = [
                                    i for i in st.session_state.new_article_ids if i != art.id]
                                st.rerun()
                    else:
                        # Режим редактирования — прямо внутри expander
                        result = render_edit_form(art, f"scrape_edit_{art.id}", show_text=True)
                        if result in ("saved", "deleted", "cancelled"):
                            st.session_state.edit_id = None
                            st.rerun()


# ══ Вкладка 2 — База данных ═══════════════════════════════════════════════════

with tab_db:
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
                columns=["Источник", "Статей"]).sort_values("Статей", ascending=False)
            st.bar_chart(src_df.set_index("Источник"))

    st.divider()

    with st.expander("🔍 Фильтры", expanded=True):
        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            f_source = st.selectbox("Источник", ["Все"] + list(SOURCE_LABELS.keys()),
                                     format_func=lambda x: "Все" if x == "Все" else SOURCE_LABELS[x])
        with fc2:
            f_lang = st.selectbox("Язык", ["Все", "en", "ru"],
                                   format_func=lambda x: {"Все":"Все","en":"English","ru":"Русский"}[x])
        with fc3:
            f_relevant = st.selectbox("Релевантность", ["Все", "Да", "Нет", "Не размечено"])
        fd1, fd2, fd3 = st.columns(3)
        with fd1:
            f_date_from = st.date_input("Загружено с",  value=None, key="db_df")
        with fd2:
            f_date_to   = st.date_input("Загружено по", value=None, key="db_dt")
        with fd3:
            f_search = st.text_input("Поиск в заголовке / аннотации", placeholder="pdf extraction…")

    articles = repo.get_articles(
        source_name = None if f_source == "Все" else f_source,
        language    = None if f_lang   == "Все" else f_lang,
        date_from   = datetime.combine(f_date_from, datetime.min.time()) if f_date_from else None,
        date_to     = datetime.combine(f_date_to,   datetime.max.time()) if f_date_to   else None,
        search      = f_search or None,
        limit       = 2000,
    )
    if f_relevant == "Да":
        articles = [a for a in articles if a.is_relevant is True]
    elif f_relevant == "Нет":
        articles = [a for a in articles if a.is_relevant is False]
    elif f_relevant == "Не размечено":
        articles = [a for a in articles if a.is_relevant is None]

    st.caption(f"Найдено: **{len(articles)}** статей")

    if not articles:
        st.info("Нет статей по выбранным фильтрам")
    else:
        rows = [{
            "ID":        a.id,
            "Источник":  SOURCE_LABELS.get(a.source_name, a.source_name),
            "Яз":        (a.language or "—").upper(),
            "Заголовок": a.title or "—",
            "Аннотация": (a.abstract or "")[:100] + ("…" if len(a.abstract or "") > 100 else ""),
            "Опубл.":    a.published_at.strftime("%Y-%m-%d") if a.published_at else "—",
            "Загружено": a.scraped_at.strftime("%Y-%m-%d %H:%M") if a.scraped_at else "—",
            "Релев.":    RELEVANCE_ICON[a.is_relevant],
            "Score":     f"{a.relevance_score:.2f}" if a.relevance_score is not None else "—",
            "URL":       a.url,
        } for a in articles]

        sel_event = st.dataframe(
            pd.DataFrame(rows), width="stretch", hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
            column_config={
                "URL":       st.column_config.LinkColumn("URL", display_text="🔗"),
                "Заголовок": st.column_config.TextColumn("Заголовок", width="large"),
                "Аннотация": st.column_config.TextColumn("Аннотация", width="large"),
            },
        )
        st.caption("💡 Кликните по строке в таблице чтобы открыть статью")

        # Определяем выбранную статью — из клика по таблице
        selected_rows = sel_event.selection.get("rows", []) if sel_event else []
        sel_art = articles[selected_rows[0]] if selected_rows else None

        if sel_art:

            with st.expander("📄 Содержимое", expanded=True):
                st.markdown(f"**{sel_art.title}**")
                st.markdown(
                    f"Источник: `{SOURCE_LABELS.get(sel_art.source_name, sel_art.source_name)}`  "
                    f"| Язык: `{sel_art.language}`  "
                    f"| Релевантность: {RELEVANCE_ICON[sel_art.is_relevant]}")
                if sel_art.url:
                    st.markdown(f"[🔗 Открыть статью]({sel_art.url})")
                if sel_art.abstract:
                    st.markdown("**Аннотация:**")
                    st.write(sel_art.abstract)
                if sel_art.text:
                    st.markdown("**Текст (первые 3000 символов):**")
                    st.text(sel_art.text[:3000] + ("…" if len(sel_art.text) > 3000 else ""))

            qc1, qc2 = st.columns(2)
            with qc1:
                if st.button("✅ Релевантна", key="db_rel", width="stretch",
                              disabled=sel_art.is_relevant is True):
                    repo.update_article(sel_art.id, is_relevant=True,
                                        label_source="human", relevance_score=1.0)
                    st.success("Отмечена как релевантная")
                    st.rerun()
            with qc2:
                if st.button("❌ Нерелевантна", key="db_norel", width="stretch",
                              disabled=sel_art.is_relevant is False):
                    repo.update_article(sel_art.id, is_relevant=False,
                                        label_source="human", relevance_score=0.0)
                    st.success("Отмечена как нерелевантная")
                    st.rerun()

            with st.expander("✏️ Редактировать поля статьи", expanded=False):
                result = render_edit_form(sel_art, f"db_edit_{sel_art.id}", show_text=True)
                if result in ("saved", "deleted", "cancelled"):
                    st.rerun()

        st.divider()
        export_df = pd.DataFrame([{
            "id": a.id, "source": a.source_name, "language": a.language,
            "title": a.title, "abstract": a.abstract, "url": a.url,
            "published_at": str(a.published_at or ""), "scraped_at": str(a.scraped_at or ""),
            "is_relevant": a.is_relevant, "relevance_score": a.relevance_score,
        } for a in articles])
        st.download_button("⬇ Скачать CSV",
                            data=export_df.to_csv(index=False, encoding="utf-8-sig"),
                            file_name=f"articles_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                            mime="text/csv")


# ══ Вкладка 3 — Журнал ════════════════════════════════════════════════════════

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
                dur  = f"{secs // 60}м {secs % 60}с"
            rows.append({
                "Источник":     SOURCE_LABELS.get(log.source_name, log.source_name),
                "Начало":       log.started_at.strftime("%Y-%m-%d %H:%M:%S") if log.started_at else "—",
                "Длительность": dur or "—",
                "Загружено":    log.total_fetched or 0,
                "Новых":        log.total_new     or 0,
                "Пропущено":    log.total_skipped or 0,
                "Ошибок":       log.total_errors  or 0,
                "Статус":       "✅" if not log.error_details else "❌",
            })
        st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
        error_logs = [l for l in logs if l.error_details]
        if error_logs:
            with st.expander(f"⚠ Детали ошибок ({len(error_logs)})"):
                for l in error_logs:
                    st.error(f"[{l.source_name}] "
                             f"{l.started_at.strftime('%Y-%m-%d %H:%M') if l.started_at else ''}:\n"
                             f"{l.error_details}")
    else:
        st.info("Журнал пуст — запустите скрапинг на первой вкладке")