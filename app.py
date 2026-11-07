#!/usr/bin/env python3
"""
Daily Briefing - a cross-checked news feed.

Pulls RSS feeds from established news organisations, groups articles that
describe the same event, and only shows a story when at least two
*independent* organisations have reported it.

Run:  python3 app.py      then open http://localhost:8000
No third-party packages needed (Python 3.8+).
"""

import concurrent.futures as cf
import email.utils
import html
import json
import math
import re
import threading
import time
import urllib.request
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import ai_check

PORT = 8000
CACHE_SECONDS = 15 * 60          # refetch feeds at most every 15 minutes
MAX_AGE_HOURS = 36               # "daily" window
MATCH_THRESHOLD = 0.30           # similarity needed to treat two articles as the same story
MERGE_THRESHOLD = 0.28           # similarity needed to merge two groups of articles
HERE = Path(__file__).resolve().parent

# ---------------------------------------------------------------------------
# Sources. "outlet" is what counts for independence: two BBC feeds reporting
# the same thing still count as ONE source.
# section: world | uk | region:<name>
# ---------------------------------------------------------------------------
FEEDS = [
    # World
    ("BBC News", "world", "https://feeds.bbci.co.uk/news/world/rss.xml"),
    ("The Guardian", "world", "https://www.theguardian.com/world/rss"),
    ("Sky News", "world", "https://feeds.skynews.com/feeds/rss/world.xml"),
    ("Financial Times", "world", "https://www.ft.com/world?format=rss"),
    ("The New York Times", "world", "https://rss.nytimes.com/services/xml/rss/nyt/World.xml"),
    ("The Washington Post", "world", "https://feeds.washingtonpost.com/rss/world"),
    ("NPR", "world", "https://feeds.npr.org/1004/rss.xml"),
    ("CBS News", "world", "https://www.cbsnews.com/latest/rss/world"),
    ("CBC News", "world", "https://www.cbc.ca/webfeed/rss/rss-world"),
    ("Al Jazeera", "world", "https://www.aljazeera.com/xml/rss/all.xml"),
    ("Deutsche Welle", "world", "https://rss.dw.com/xml/rss-en-world"),
    ("France 24", "world", "https://www.france24.com/en/rss"),
    ("Euronews", "world", "https://www.euronews.com/rss"),
    ("ABC News (Australia)", "world", "https://www.abc.net.au/news/feed/2942460/rss.xml"),
    # UK national
    ("BBC News", "uk", "https://feeds.bbci.co.uk/news/uk/rss.xml"),
    ("BBC News", "uk", "https://feeds.bbci.co.uk/news/politics/rss.xml"),
    ("The Guardian", "uk", "https://www.theguardian.com/uk-news/rss"),
    ("The Guardian", "uk", "https://www.theguardian.com/politics/rss"),
    ("Sky News", "uk", "https://feeds.skynews.com/feeds/rss/uk.xml"),
    ("Sky News", "uk", "https://feeds.skynews.com/feeds/rss/politics.xml"),
    ("Channel 4 News", "uk", "https://www.channel4.com/news/feed"),
    # UK regions
    ("BBC News", "region:London", "https://feeds.bbci.co.uk/news/england/london/rss.xml"),
    ("The Guardian", "region:London", "https://www.theguardian.com/uk/london/rss"),
    ("Evening Standard", "region:London", "https://www.standard.co.uk/news/london/rss"),
    ("BBC News", "region:Manchester", "https://feeds.bbci.co.uk/news/england/manchester/rss.xml"),
    ("Manchester Evening News", "region:Manchester", "https://www.manchestereveningnews.co.uk/news/?service=rss"),
    ("BBC News", "region:Birmingham", "https://feeds.bbci.co.uk/news/england/birmingham_and_black_country/rss.xml"),
    ("Birmingham Mail", "region:Birmingham", "https://www.birminghammail.co.uk/news/?service=rss"),
    ("BBC News", "region:Liverpool", "https://feeds.bbci.co.uk/news/england/merseyside/rss.xml"),
    ("Liverpool Echo", "region:Liverpool", "https://www.liverpoolecho.co.uk/news/?service=rss"),
    ("BBC News", "region:Yorkshire", "https://feeds.bbci.co.uk/news/england/leeds_and_west_yorkshire/rss.xml"),
    ("Yorkshire Post", "region:Yorkshire", "https://www.yorkshirepost.co.uk/news/rss"),
    ("BBC News", "region:Bristol", "https://feeds.bbci.co.uk/news/england/bristol/rss.xml"),
    ("Bristol Post", "region:Bristol", "https://www.bristolpost.co.uk/news/?service=rss"),
    ("BBC News", "region:Scotland", "https://feeds.bbci.co.uk/news/scotland/rss.xml"),
    ("The Guardian", "region:Scotland", "https://www.theguardian.com/uk/scotland/rss"),
    ("The Herald", "region:Scotland", "https://www.heraldscotland.com/news/rss/"),
    ("BBC News", "region:Wales", "https://feeds.bbci.co.uk/news/wales/rss.xml"),
    ("The Guardian", "region:Wales", "https://www.theguardian.com/uk/wales/rss"),
    ("WalesOnline", "region:Wales", "https://www.walesonline.co.uk/news/?service=rss"),
    ("BBC News", "region:Northern Ireland", "https://feeds.bbci.co.uk/news/northern_ireland/rss.xml"),
    ("Belfast Live", "region:Northern Ireland", "https://www.belfastlive.co.uk/news/?service=rss"),
    ("RTÉ News", "region:Northern Ireland", "https://www.rte.ie/feeds/rss/?index=/news/"),
]

# Titles owned by the same company often share copy, so they count as ONE
# independent source between them.
OWNER = {
    "Manchester Evening News": "Reach plc", "Birmingham Mail": "Reach plc",
    "Liverpool Echo": "Reach plc", "Bristol Post": "Reach plc",
    "WalesOnline": "Reach plc", "Belfast Live": "Reach plc",
}


def owner(outlet):
    return OWNER.get(outlet, outlet)


# Bulletin / round-up items that mention many unrelated stories
ROUNDUP = re.compile(r"^\d{1,2}/\d{1,2}:|evening news$|morning briefing|news quiz|^first thing:|^the latest:|^live:? ", re.I)

UA = "Mozilla/5.0 (DailyBriefing personal news reader)"
NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "media": "http://search.yahoo.com/mrss/",
    "dc": "http://purl.org/dc/elements/1.1/",
    "content": "http://purl.org/rss/1.0/modules/content/",
}

STOP = set("""
a about above after again against all also am an and any are as at be because been before being below
between both but by can could did do does doing down during each few for from further had has have having
he her here hers herself him himself his how i if in into is it its itself just me more most my myself no
nor not now of off on once only or other our ours out over own same she should so some such than that the
their theirs them themselves then there these they this those through to too under until up very was we
were what when where which while who whom why will with would you your yours new says say said told after
amid over us uk news live latest update updates first last year years day days week weeks month months
people man woman men women one two three four five six seven eight nine ten could may might must make
makes made get gets got take takes took call calls called back way want wants see sees seen go goes going
come comes watch video report reports reported week's today yesterday tomorrow time times still
""".split())


def stem(w):
    for suf in ("ings", "ing", "ied", "ies", "ers", "er", "ed", "es", "s"):
        if len(w) > len(suf) + 3 and w.endswith(suf):
            return w[: -len(suf)]
    return w


def tokens(text):
    words = re.findall(r"[a-zA-Z][a-zA-Z'\-]+|\d{2,}", text.lower())
    out = []
    for w in words:
        w = w.strip("'-").replace("'s", "")
        if len(w) < 3 or w in STOP:
            continue
        out.append(stem(w))
    return out


def clean(text):
    text = html.unescape(re.sub(r"<[^>]+>", " ", text or ""))
    return re.sub(r"\s+", " ", text).strip()


def parse_date(s):
    if not s:
        return None
    s = s.strip()
    try:
        d = email.utils.parsedate_to_datetime(s)
    except (TypeError, ValueError):
        d = None
    if d is None:
        try:
            d = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.timestamp()


def find_image(el):
    for tag in ("media:thumbnail", "media:content"):
        for m in el.findall(tag, NS):
            url = m.get("url")
            if url and (m.get("medium") in (None, "image") ):
                return url
    enc = el.find("enclosure")
    if enc is not None and (enc.get("type") or "").startswith("image"):
        return enc.get("url")
    return None


def fetch_feed(outlet, section, url):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/rss+xml, application/xml, text/xml, */*"})
    with urllib.request.urlopen(req, timeout=15) as r:
        data = r.read()
    root = ET.fromstring(data)
    items = []
    entries = root.iter("item")
    rows = list(entries) or list(root.iter("{http://www.w3.org/2005/Atom}entry"))
    for el in rows:
        def t(*names):
            for n in names:
                v = el.find(n, NS)
                if v is not None and (v.text or "").strip():
                    return v.text
            return ""
        title = clean(t("title", "atom:title"))
        if not title or ROUNDUP.search(title):
            continue
        link = t("link").strip()
        if not link:
            a = el.find("atom:link", NS)
            link = a.get("href") if a is not None else ""
        summary = clean(t("description", "atom:summary", "content:encoded"))
        if len(summary) > 400:
            summary = summary[:400].rsplit(" ", 1)[0] + "…"
        ts = parse_date(t("pubDate", "dc:date", "atom:published", "atom:updated"))
        items.append({
            "outlet": outlet, "section": section, "title": title, "link": link,
            "summary": summary, "ts": ts, "image": find_image(el),
        })
    return items


# ---------------------------------------------------------------------------
# Matching: TF-IDF cosine similarity, greedy centroid clustering
# ---------------------------------------------------------------------------
def vectorise(items):
    docs = []
    for it in items:
        # title counted twice: headlines carry the key facts
        docs.append(Counter(tokens(it["title"]) * 2 + tokens(it["summary"])[:40]))
    df = Counter()
    for d in docs:
        df.update(set(d))
    n = len(docs)
    vecs = []
    for d in docs:
        v = {w: c * math.log((n + 1) / (df[w] + 0.5)) for w, c in d.items() if df[w] < n * 0.15}
        norm = math.sqrt(sum(x * x for x in v.values())) or 1.0
        vecs.append({w: x / norm for w, x in v.items()})
    return vecs


def cosine(a, b):
    if len(a) > len(b):
        a, b = b, a
    return sum(x * b.get(w, 0.0) for w, x in a.items())


def cluster(items):
    vecs = vectorise(items)
    order = sorted(range(len(items)), key=lambda i: -(items[i]["ts"] or 0))
    clusters = []  # each: {"members": [...], "centroid": {...}}
    for i in order:
        v = vecs[i]
        if not v:
            continue
        best, best_s = None, 0.0
        for c in clusters:
            s = cosine(v, c["centroid"])
            if s > best_s:
                best, best_s = c, s
        if best is not None and best_s >= MATCH_THRESHOLD and set(tokens(items[i]["title"])) & best["title_words"]:
            best["title_words"] |= set(tokens(items[i]["title"]))
            best["members"].append(i)
            k = len(best["members"])
            cen = defaultdict(float)
            for w, x in best["centroid"].items():
                cen[w] += x * (k - 1) / k
            for w, x in v.items():
                cen[w] += x / k
            best["centroid"] = dict(cen)
        else:
            clusters.append({"members": [i], "centroid": dict(v), "title_words": set(tokens(items[i]["title"]))})
    # second pass: join clusters that turned out to be the same story
    merged = True
    while merged:
        merged = False
        for a in range(len(clusters)):
            for b in range(a + 1, len(clusters)):
                ca, cb = clusters[a], clusters[b]
                if cosine(ca["centroid"], cb["centroid"]) >= MERGE_THRESHOLD and ca["title_words"] & cb["title_words"]:
                    na, nb = len(ca["members"]), len(cb["members"])
                    cen = defaultdict(float)
                    for w, x in ca["centroid"].items():
                        cen[w] += x * na / (na + nb)
                    for w, x in cb["centroid"].items():
                        cen[w] += x * nb / (na + nb)
                    ca["centroid"] = dict(cen)
                    ca["members"] += cb["members"]
                    ca["title_words"] |= cb["title_words"]
                    del clusters[b]
                    merged = True
                    break
            if merged:
                break
    return clusters, vecs


NUMWORDS = {w: i for i, w in enumerate("zero one two three four five six seven eight nine ten eleven twelve".split())}
TOLL = re.compile(r"(?:at least |more than |over )?(\d+|" + "|".join(NUMWORDS) + r")\s+(?:people\s+|are\s+|were\s+)?(?:dead|killed|die|died|dies)"
                  r"|kill(?:s|ing|ed)\s+(?:at least\s+)?(\d+|" + "|".join(NUMWORDS) + r")", re.I)


def death_tolls(text):
    out = set()
    for a, b in TOLL.findall(text):
        v = (a or b).lower()
        out.add(int(v) if v.isdigit() else NUMWORDS[v])
    return out


# How established each outlet's newsroom and corrections record is.
# 1.0 = public-service broadcaster / paper of record, lower = regional or
# more commercially driven titles. Used as one part of the authenticity score.
STANDING = {
    "BBC News": 1.0, "RTÉ News": 0.97, "Financial Times": 0.97, "The Guardian": 0.95,
    "The New York Times": 0.95, "The Washington Post": 0.95, "NPR": 0.95, "CBC News": 0.95,
    "ABC News (Australia)": 0.95, "Deutsche Welle": 0.93, "Channel 4 News": 0.93, "Sky News": 0.92,
    "France 24": 0.9, "CBS News": 0.9, "Al Jazeera": 0.88, "Euronews": 0.87,
    "The Herald": 0.87, "Yorkshire Post": 0.86, "Evening Standard": 0.85,
}
COUNTRY = {
    "BBC News": "UK", "The Guardian": "UK", "Sky News": "UK", "Financial Times": "UK", "Channel 4 News": "UK",
    "The New York Times": "US", "The Washington Post": "US", "NPR": "US", "CBS News": "US", "CBC News": "CA",
    "Al Jazeera": "QA", "Deutsche Welle": "DE", "France 24": "FR", "Euronews": "EU", "ABC News (Australia)": "AU",
    "RTÉ News": "IE",
}


def authenticity(sources, owners, member_vecs, centroid, has_discrepancy, possible_ai=0):
    """0-98 score with a breakdown the page can show. Never 100: nothing here is certain."""
    n = len(owners)
    corroboration = [0, 0, 45, 55, 63, 69][n] if n < 6 else 72
    # standing: average of the best article per independent owner
    best = {}
    for s in sources:
        o = owner(s["outlet"])
        best[o] = max(best.get(o, 0), STANDING.get(s["outlet"], 0.8))
    avg = sum(best.values()) / len(best)
    standing = round(max(0.0, min(1.0, (avg - 0.8) / 0.2)) * 15)
    # agreement: how closely the reports match each other
    sims = [cosine(member_vecs[id(s)], centroid) for s in sources if id(s) in member_vecs]
    agree = sum(sims) / len(sims) if sims else 0
    agreement = round(max(0.0, min(1.0, (agree - 0.3) / 0.4)) * 10)
    countries = {COUNTRY[s["outlet"]] for s in sources if s["outlet"] in COUNTRY}
    spread = 5 if len(countries) >= 3 else 2 if len(countries) == 2 else 0
    penalty = 10 if has_discrepancy else 0
    ai_penalty = 10 * min(possible_ai, 2)
    score = max(5, min(98, corroboration + standing + agreement + spread - penalty - ai_penalty))
    rag = "green" if score >= 75 else "amber" if score >= 60 else "red"
    return {
        "score": score, "rag": rag,
        "parts": [
            ["Independent outlets confirming (%d)" % n, corroboration, 72],
            ["Standing of those outlets", standing, 15],
            ["How closely the reports agree", agreement, 10],
            ["Reported from %d countries" % len(countries) if countries else "International spread", spread, 5],
        ] + ([["Conflicting figures between reports", -penalty, 0]] if penalty else [])
          + ([["Report%s with possible signs of AI writing" % ("s" if possible_ai > 1 else ""), -ai_penalty, 0]] if ai_penalty else []),
    }


def candidate_articles(items):
    """Articles that belong to a story at least two independent owners reported."""
    clusters, _ = cluster(items)
    groups = []
    for c in clusters:
        members = [items[i] for i in c["members"]]
        if len({owner(m["outlet"]) for m in members}) >= 2:
            groups.append(members)
    return groups


def build_stories(items):
    clusters, vecs = cluster(items)
    stories = []
    for c in clusters:
        members = [items[i] for i in c["members"]]
        member_vecs = {id(items[i]): vecs[i] for i in c["members"]}
        outlets = sorted({m["outlet"] for m in members})
        owners = {owner(m["outlet"]) for m in members}
        if len(owners) < 2:
            continue
        # one article per outlet (earliest), for the source list
        per_outlet = {}
        for m in sorted(members, key=lambda m: m["ts"] or 9e18):
            per_outlet.setdefault(m["outlet"], m)
        sources = sorted(per_outlet.values(), key=lambda m: m["ts"] or 0, reverse=True)
        # headline: among the articles most typical of the group, the most recent one
        cen = c["centroid"]
        score = {id(m): sum(cen.get(w, 0) for w in set(tokens(m["title"]))) for m in members}
        top = max(score.values())
        lead = max((m for m in members if score[id(m)] >= 0.7 * top), key=lambda m: (bool(m["summary"]), m["ts"] or 0))
        world_w = sum(1 for m in members if m["section"] == "world")
        uk_w = sum(1 if m["section"] == "uk" else 0.5 for m in members if m["section"] != "world")
        main = "uk" if uk_w > world_w else "world"
        regions = sorted({m["section"].split(":", 1)[1] for m in members if m["section"].startswith("region:")}) if main == "uk" else []
        times = [m["ts"] for m in members if m["ts"]]
        image = next((m["image"] for m in sources if m["image"]), None)
        tolls = set()
        for m in sources:
            tolls |= death_tolls(m["title"])
        note = None
        if len(tolls) > 1:
            note = "Sources report different death tolls (" + ", ".join(str(x) for x in sorted(tolls)) + "). Figures may still be changing."
        possible_ai = sum(1 for s in sources if s.get("ai", {}).get("verdict") in ("possibly_ai", "likely_ai"))
        auth = authenticity(sources, owners, member_vecs, c["centroid"], bool(note), possible_ai)
        ai_checked = sum(1 for s in sources if s.get("ai", {}).get("verdict") not in (None, "unchecked"))
        stories.append({
            "authenticity": auth,
            "headline": lead["title"],
            "summary": lead["summary"] or next((m["summary"] for m in sources if m["summary"]), ""),
            "image": image,
            "section": main,
            "regions": regions,
            "outlet_count": len(outlets),
            "independent_count": len(owners),
            "discrepancy": note,
            "ai_checked": ai_checked,
            "ai_possible": possible_ai,
            "excluded": [],
            "first_seen": min(times) if times else None,
            "latest": max(times) if times else None,
            "sources": [{"ai": s.get("ai"), "outlet": s["outlet"], "owner": owner(s["outlet"]), "title": s["title"], "link": s["link"], "ts": s["ts"]} for s in sources],
        })
    stories.sort(key=lambda s: (-min(s["independent_count"], 6), -(s["latest"] or 0)))
    return stories


# ---------------------------------------------------------------------------
# Cache + refresh
# ---------------------------------------------------------------------------
_cache = {"at": 0, "data": None}
_lock = threading.Lock()


def gather():
    started = time.time()
    status = []
    all_items = []
    with cf.ThreadPoolExecutor(max_workers=16) as ex:
        futs = {ex.submit(fetch_feed, *f): f for f in FEEDS}
        for fut in cf.as_completed(futs):
            outlet, section, url = futs[fut]
            try:
                got = fut.result()
                status.append({"outlet": outlet, "section": section, "url": url, "ok": True, "count": len(got)})
                all_items.extend(got)
            except Exception as e:  # noqa: BLE001
                status.append({"outlet": outlet, "section": section, "url": url, "ok": False, "error": str(e)[:120]})
    cutoff = time.time() - MAX_AGE_HOURS * 3600
    seen, items = set(), []
    for it in all_items:
        if it["ts"] and it["ts"] < cutoff:
            continue
        key = it["link"] or (it["outlet"], it["title"])
        if key in seen:
            continue
        seen.add(key)
        items.append(it)
    # AI-content check on every article that could appear in a story
    groups = candidate_articles(items)
    to_check = [m for g in groups for m in g]
    excluded, ai_status = ai_check.scan(to_check)
    bad = {a["link"] for a in excluded}
    kept = [it for it in items if it["link"] not in bad]
    stories = build_stories(kept)
    # tell each story which of its reports were removed
    by_link = {src["link"]: s for s in stories for src in s["sources"]}
    excluded_out = []
    for g in groups:
        gone = [m for m in g if m["link"] in bad]
        if not gone:
            continue
        home = Counter(by_link[m["link"]]["headline"] for m in g if m["link"] in by_link)
        story = next((s for s in stories if home and s["headline"] == home.most_common(1)[0][0]), None)
        for m in gone:
            rec = {"outlet": m["outlet"], "title": m["title"], "link": m["link"], "ts": m["ts"], "ai": m["ai"],
                   "story": story["headline"] if story else None,
                   "story_dropped": story is None}
            excluded_out.append(rec)
            if story:
                story["excluded"].append(rec)
    new = ai_check.log_exclusions(excluded)
    ai_check.notify_mac(new)
    if new:
        print(f"[ai-check] Excluded {len(new)} new article(s) as likely AI-generated:")
        for a in new:
            print(f"   - {a['outlet']}: {a['title']}  ({a['ai'].get('evidence')})")
    ai_status["excluded"] = excluded_out
    ai_status["new_exclusions"] = [a["link"] for a in new]
    regions = sorted({f[1].split(":", 1)[1] for f in FEEDS if f[1].startswith("region:")})
    return {
        "generated": time.time(),
        "took": round(time.time() - started, 1),
        "articles_scanned": len(items),
        "stories": stories,
        "regions": regions,
        "feeds": sorted(status, key=lambda s: (s["section"], s["outlet"])),
        "window_hours": MAX_AGE_HOURS,
        "ai_check": ai_status,
    }


def get_data(force=False):
    with _lock:
        if force or not _cache["data"] or time.time() - _cache["at"] > CACHE_SECONDS:
            _cache["data"] = gather()
            _cache["at"] = time.time()
        return _cache["data"]


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body, ctype):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?")[0]
        if path in ("/", "/index.html"):
            self._send(200, (HERE / "index.html").read_bytes(), "text/html; charset=utf-8")
        elif path in ("/api/news", "/news.json"):
            data = get_data(force="refresh=1" in self.path)
            self._send(200, json.dumps(data).encode(), "application/json")
        else:
            self._send(404, b"Not found", "text/plain")

    def log_message(self, *a):
        pass


def build_site(out_dir):
    """Write a static copy (index.html + news.json) for hosting, e.g. on GitHub Pages."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    data = gather()
    (out / "news.json").write_text(json.dumps(data))
    (out / "index.html").write_bytes((HERE / "index.html").read_bytes())
    (out / ".nojekyll").write_text("")
    print(f"Built {out}: {len(data['stories'])} stories from {data['articles_scanned']} articles, "
          f"{len(data['ai_check'].get('excluded', []))} excluded as likely AI-generated")


if __name__ == "__main__":
    import sys
    if len(sys.argv) == 3 and sys.argv[1] == "--build":
        build_site(sys.argv[2])
        sys.exit(0)
    print(f"Daily Briefing running at http://localhost:{PORT}  (Ctrl+C to stop)")
    threading.Thread(target=get_data, daemon=True).start()  # warm the cache
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
