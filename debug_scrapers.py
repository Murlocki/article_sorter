"""
debug_scrapers.py — изучаем структуру HTML страниц TDS
  python -m debug_scrapers
"""
import requests, feedparser, time
from bs4 import BeautifulSoup

s = requests.Session()
s.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0 Safari/537.36"
SEP = "─" * 60

# Берём несколько статей из TDS RSS
r   = s.get("https://towardsdatascience.com/feed", timeout=10)
feed = feedparser.parse(r.text)
urls = [e.get("link","") for e in feed.entries[:3]]

print(SEP)
print("TDS — структура HTML страницы статьи")
print(SEP)

for url in urls[:2]:
    time.sleep(1)
    r    = s.get(url, timeout=15)
    soup = BeautifulSoup(r.text, "lxml")
    print(f"\nURL: {url[:80]}")
    print(f"status={r.status_code}, size={len(r.text)}")

    # Смотрим <main> подробно
    main = soup.find("main")
    if main:
        # Все теги верхнего уровня в main
        print("Теги в <main>:")
        for child in main.children:
            if hasattr(child, "name") and child.name:
                cls = " ".join(child.get("class", []))[:40]
                print(f"  <{child.name} class='{cls}'> {len(child.get_text()):6d} симв")

        # Параграфы статьи
        paras = main.find_all("p")
        print(f"\nАбзацев <p>: {len(paras)}")
        for p in paras[:5]:
            t = p.get_text(strip=True)
            if len(t) > 30:
                print(f"  {t[:100]}")

        # Заголовок статьи
        h1 = main.find("h1")
        print(f"\nЗаголовок h1: {h1.get_text(strip=True)[:80] if h1 else 'НЕТ'}")

        # Мета-описание
        meta_desc = soup.find("meta", {"name": "description"})
        print(f"meta description: {meta_desc.get('content','')[:100] if meta_desc else 'НЕТ'}")

        # Полный текст main
        full = main.get_text(separator="\n", strip=True)
        print(f"\nПолный текст: {len(full)} символов")
        print(f"Первые 400 символов:\n{full[:400]}")

# ══ TDS поиск ════════════════════════════════════════════════════
print()
print(SEP)
print("TDS — есть ли страница поиска?")
print(SEP)

for url in [
    "https://towardsdatascience.com/search?q=pdf+extraction",
    "https://towardsdatascience.com/?s=pdf+extraction",
    "https://towardsdatascience.com/tag/pdf",
    "https://towardsdatascience.com/tag/ocr",
    "https://towardsdatascience.com/tag/nlp",
]:
    time.sleep(0.8)
    r    = s.get(url, timeout=10)
    soup = BeautifulSoup(r.text, "lxml")
    links = [a["href"] for a in soup.find_all("a", href=True)
             if "towardsdatascience.com" in a.get("href","")
             and len(a.get_text(strip=True)) > 20]
    print(f"  [{r.status_code}] {len(links)} ссылок — {url.split('towardsdatascience.com')[1]}")
    for l in links[:3]:
        print(f"    {l[:70]}")