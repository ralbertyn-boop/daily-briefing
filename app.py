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
# section: world | uk | region:<name> | sport:<sport> | celebrity
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
    # Nation-wide feeds: they corroborate local stories but don't place them in an area
    ("BBC News", "uk", "https://feeds.bbci.co.uk/news/scotland/rss.xml"),
    ("BBC News", "uk", "https://feeds.bbci.co.uk/news/wales/rss.xml"),
    ("The Guardian", "uk", "https://www.theguardian.com/uk/scotland/rss"),
    ("The Guardian", "uk", "https://www.theguardian.com/uk/wales/rss"),
    ("The Herald", "uk", "https://www.heraldscotland.com/news/rss/"),
    ("The National", "uk", "https://www.thenational.scot/news/rss/"),
    ("The Scotsman", "uk", "https://www.scotsman.com/news/rss"),
    ("Nation.Cymru", "uk", "https://nation.cymru/feed/"),
    ("RTÉ News", "world", "https://www.rte.ie/feeds/rss/?index=/news/"),
]

# ---------------------------------------------------------------------------
# Local areas. Each has a rough centre (lat, lon) so the page can pick the
# nearest areas to a town, postcode or the viewer's location.
# ---------------------------------------------------------------------------
BBC = "https://feeds.bbci.co.uk/news/%s/rss.xml"
REACH = "https://www.%s/news/?service=rss"
NQ = "https://www.%s/news/rss/"
AREAS = [
    ("London", 51.507, -0.128, [("BBC News", BBC % "england/london"), ("The Guardian", "https://www.theguardian.com/uk/london/rss"),
                                ("Evening Standard", "https://www.standard.co.uk/news/london/rss"), ("MyLondon", REACH % "mylondon.news")]),
    ("Beds, Bucks and Herts", 51.90, -0.55, [("BBC News", BBC % "england/beds_bucks_and_herts")]),
    ("Berkshire", 51.44, -1.00, [("BBC News", BBC % "england/berkshire")]),
    ("Birmingham and Black Country", 52.49, -1.95, [("BBC News", BBC % "england/birmingham_and_black_country"),
                                ("Birmingham Mail", REACH % "birminghammail.co.uk"), ("Express & Star", "https://www.expressandstar.com/news/rss/")]),
    ("Bristol", 51.455, -2.59, [("BBC News", BBC % "england/bristol"), ("Bristol Post", REACH % "bristolpost.co.uk")]),
    ("Cambridgeshire", 52.35, -0.05, [("BBC News", BBC % "england/cambridgeshire"), ("Cambridge News", REACH % "cambridge-news.co.uk"),
                                ("Peterborough Telegraph", "https://www.peterboroughtoday.co.uk/news/rss")]),
    ("Cheshire", 53.20, -2.55, [("Cheshire Live", REACH % "cheshire-live.co.uk")]),
    ("Cornwall", 50.35, -4.90, [("BBC News", BBC % "england/cornwall"), ("Cornwall Live", REACH % "cornwalllive.com")]),
    ("Coventry and Warwickshire", 52.35, -1.55, [("BBC News", BBC % "england/coventry_and_warwickshire"), ("Coventry Telegraph", REACH % "coventrytelegraph.net")]),
    ("Cumbria", 54.55, -2.90, [("BBC News", BBC % "england/cumbria"), ("News & Star", NQ % "newsandstar.co.uk"),
                                ("Westmorland Gazette", NQ % "thewestmorlandgazette.co.uk"), ("Times & Star", NQ % "timesandstar.co.uk")]),
    ("Derbyshire", 53.00, -1.50, [("BBC News", BBC % "england/derbyshire"), ("Derby Telegraph", REACH % "derbytelegraph.co.uk")]),
    ("Devon", 50.72, -3.75, [("BBC News", BBC % "england/devon"), ("Devon Live", REACH % "devonlive.com"), ("Plymouth Herald", REACH % "plymouthherald.co.uk")]),
    ("Dorset", 50.75, -2.20, [("BBC News", BBC % "england/dorset"), ("Bournemouth Echo", NQ % "bournemouthecho.co.uk"), ("Dorset Echo", NQ % "dorsetecho.co.uk")]),
    ("Essex", 51.77, 0.55, [("BBC News", BBC % "england/essex"), ("Essex Live", REACH % "essexlive.news")]),
    ("Gloucestershire", 51.85, -2.25, [("BBC News", BBC % "england/gloucestershire"), ("Gloucestershire Live", REACH % "gloucestershirelive.co.uk")]),
    ("Hampshire", 51.00, -1.25, [("BBC News", BBC % "england/hampshire"), ("Southern Daily Echo", NQ % "dailyecho.co.uk"),
                                ("The News, Portsmouth", "https://www.portsmouth.co.uk/news/rss")]),
    ("Hereford and Worcester", 52.15, -2.45, [("BBC News", BBC % "england/hereford_and_worcester"), ("Worcester News", NQ % "worcesternews.co.uk")]),
    ("Humberside", 53.70, -0.35, [("BBC News", BBC % "england/humberside"), ("Hull Daily Mail", REACH % "hulldailymail.co.uk"),
                                ("Grimsby Telegraph", REACH % "grimsbytelegraph.co.uk")]),
    ("Kent", 51.25, 0.75, [("BBC News", BBC % "england/kent")]),
    ("Lancashire", 53.80, -2.60, [("BBC News", BBC % "england/lancashire"), ("Lancashire Telegraph", NQ % "lancashiretelegraph.co.uk"),
                                ("Lancashire Post", "https://www.lep.co.uk/news/rss"), ("Blackpool Gazette", "https://www.blackpoolgazette.co.uk/news/rss"),
                                ("Lancs Live", REACH % "lancs.live")]),
    ("Leeds and West Yorkshire", 53.75, -1.65, [("BBC News", BBC % "england/leeds_and_west_yorkshire"), ("Yorkshire Post", "https://www.yorkshirepost.co.uk/news/rss"),
                                ("Telegraph & Argus", NQ % "thetelegraphandargus.co.uk"), ("Examiner Live", REACH % "examinerlive.co.uk"),
                                ("Leeds Live", REACH % "leeds-live.co.uk")]),
    ("Leicester", 52.63, -1.13, [("BBC News", BBC % "england/leicester"), ("Leicester Mercury", REACH % "leicestermercury.co.uk")]),
    ("Lincolnshire", 53.10, -0.30, [("BBC News", BBC % "england/lincolnshire"), ("Lincolnshire Live", REACH % "lincolnshirelive.co.uk")]),
    ("Manchester", 53.48, -2.24, [("BBC News", BBC % "england/manchester"), ("Manchester Evening News", REACH % "manchestereveningnews.co.uk"),
                                ("The Bolton News", NQ % "theboltonnews.co.uk")]),
    ("Merseyside", 53.41, -2.98, [("BBC News", BBC % "england/merseyside"), ("Liverpool Echo", REACH % "liverpoolecho.co.uk")]),
    ("Norfolk", 52.65, 1.00, [("BBC News", BBC % "england/norfolk"), ("Eastern Daily Press", "https://www.edp24.co.uk/news/rss")]),
    ("Northamptonshire", 52.30, -0.90, [("BBC News", BBC % "england/northamptonshire")]),
    ("Nottingham", 53.00, -1.10, [("BBC News", BBC % "england/nottingham"), ("Nottingham Post", REACH % "nottinghampost.com")]),
    ("Oxford", 51.75, -1.30, [("BBC News", BBC % "england/oxford"), ("Oxford Mail", NQ % "oxfordmail.co.uk")]),
    ("Shropshire", 52.70, -2.75, [("BBC News", BBC % "england/shropshire"), ("Shropshire Star", "https://www.shropshirestar.com/news/rss/")]),
    ("Somerset", 51.10, -3.00, [("BBC News", BBC % "england/somerset"), ("Somerset Live", REACH % "somersetlive.co.uk")]),
    ("South Yorkshire", 53.45, -1.35, [("BBC News", BBC % "england/south_yorkshire"), ("The Star, Sheffield", "https://www.thestar.co.uk/news/rss")]),
    ("Stoke and Staffordshire", 52.85, -2.05, [("BBC News", BBC % "england/stoke_and_staffordshire"), ("Stoke Sentinel", REACH % "stokesentinel.co.uk")]),
    ("Suffolk", 52.20, 1.00, [("BBC News", BBC % "england/suffolk"), ("East Anglian Daily Times", "https://www.eadt.co.uk/news/rss")]),
    ("Surrey", 51.25, -0.45, [("BBC News", BBC % "england/surrey"), ("Surrey Live", REACH % "getsurrey.co.uk")]),
    ("Sussex", 50.92, -0.20, [("BBC News", BBC % "england/sussex"), ("The Argus", NQ % "theargus.co.uk")]),
    ("Tees", 54.57, -1.23, [("BBC News", BBC % "england/tees"), ("Teesside Live", REACH % "gazettelive.co.uk"), ("The Northern Echo", NQ % "thenorthernecho.co.uk")]),
    ("Tyne and Wear", 54.95, -1.55, [("BBC News", BBC % "england/tyne_and_wear"), ("Chronicle Live", REACH % "chroniclelive.co.uk"),
                                ("Shields Gazette", "https://www.shieldsgazette.com/news/rss"), ("Sunderland Echo", "https://www.sunderlandecho.com/news/rss")]),
    ("Wiltshire", 51.35, -1.95, [("BBC News", BBC % "england/wiltshire"), ("Swindon Advertiser", NQ % "swindonadvertiser.co.uk"),
                                ("Wiltshire Times", NQ % "wiltshiretimes.co.uk"), ("Salisbury Journal", NQ % "salisburyjournal.co.uk")]),
    ("York and North Yorkshire", 54.10, -1.30, [("BBC News", BBC % "england/york_and_north_yorkshire"), ("The Press, York", NQ % "yorkpress.co.uk")]),
    ("Glasgow and West Scotland", 55.86, -4.25, [("BBC News", BBC % "scotland/glasgow_and_west"), ("Glasgow Times", NQ % "eveningtimes.co.uk"),
                                ("Daily Record", REACH % "dailyrecord.co.uk")]),
    ("Edinburgh, Fife and East Scotland", 55.95, -3.19, [("BBC News", BBC % "scotland/edinburgh_east_and_fife"),
                                ("Edinburgh Evening News", "https://www.edinburghnews.scotsman.com/news/rss")]),
    ("Highlands and Islands", 57.48, -4.80, [("BBC News", BBC % "scotland/highlands_and_islands"), ("Press and Journal", "https://www.pressandjournal.co.uk/feed/")]),
    ("North East Scotland, Orkney and Shetland", 57.15, -2.30, [("BBC News", BBC % "scotland/north_east_orkney_and_shetland"),
                                ("Press and Journal", "https://www.pressandjournal.co.uk/feed/")]),
    ("South Scotland", 55.30, -3.50, [("BBC News", BBC % "scotland/south_scotland")]),
    ("Tayside and Central Scotland", 56.40, -3.60, [("BBC News", BBC % "scotland/tayside_and_central"), ("The Courier", "https://www.thecourier.co.uk/feed/")]),
    ("North West Wales", 53.05, -4.10, [("BBC News", BBC % "wales/north_west_wales"), ("Daily Post", REACH % "dailypost.co.uk")]),
    ("North East Wales", 53.05, -3.20, [("BBC News", BBC % "wales/north_east_wales"), ("Daily Post", REACH % "dailypost.co.uk")]),
    ("Mid Wales", 52.40, -3.60, [("BBC News", BBC % "wales/mid_wales")]),
    ("South West Wales", 51.75, -4.20, [("BBC News", BBC % "wales/south_west_wales"), ("WalesOnline", REACH % "walesonline.co.uk")]),
    ("South East Wales", 51.55, -3.10, [("BBC News", BBC % "wales/south_east_wales"), ("WalesOnline", REACH % "walesonline.co.uk"),
                                ("South Wales Argus", NQ % "southwalesargus.co.uk")]),
    ("Northern Ireland", 54.60, -6.50, [("BBC News", BBC % "northern_ireland"), ("Belfast Live", REACH % "belfastlive.co.uk"),
                                ("The Irish News", "https://www.irishnews.com/arc/outboundfeeds/rss/"),
                                ("News Letter", "https://www.newsletter.co.uk/news/rss")]),
]
for _name, _lat, _lon, _feeds in AREAS:
    for _outlet, _url in _feeds:
        FEEDS.append((_outlet, "region:" + _name, _url))

# ---------------------------------------------------------------------------
# Sport. Shown in its own section; a sport story also needs 2+ independent outlets.
# ---------------------------------------------------------------------------
BBCS = "https://feeds.bbci.co.uk/sport/%s/rss.xml"
GU = "https://www.theguardian.com/%s/rss"
SKY = "https://www.skysports.com/rss/%s"
SPORTS = {
    "Football": [("BBC Sport", BBCS % "football"), ("The Guardian", GU % "football"), ("Sky Sports", SKY % "11095"),
                 ("ESPN", "https://www.espn.com/espn/rss/soccer/news"), ("CBS Sports", "https://www.cbssports.com/rss/headlines/soccer/")],
    "Rugby": [("BBC Sport", BBCS % "rugby-union"), ("BBC Sport", BBCS % "rugby-league"), ("The Guardian", GU % "sport/rugby-union"),
              ("The Guardian", GU % "sport/rugbyleague"), ("Sky Sports", SKY % "12321"), ("Sky Sports", SKY % "12196")],
    "Cricket": [("BBC Sport", BBCS % "cricket"), ("The Guardian", GU % "sport/cricket"), ("Sky Sports", SKY % "12341"),
                ("ESPNcricinfo", "https://www.espncricinfo.com/rss/content/story/feeds/0.xml"), ("Wisden", "https://www.wisden.com/feed")],
    "Motor racing": [("BBC Sport", BBCS % "formula1"), ("BBC Sport", BBCS % "motorsport"), ("The Guardian", GU % "sport/formulaone"),
                     ("The Guardian", GU % "sport/motorsports"), ("Sky Sports", SKY % "12433"), ("ESPN", "https://www.espn.com/espn/rss/f1/news"),
                     ("ESPN", "https://www.espn.com/espn/rss/rpm/news"), ("Autosport", "https://www.autosport.com/rss/feed/all"),
                     ("The Race", "https://www.the-race.com/feed/"), ("Crash.net", "https://www.crash.net/rss/f1")],
    "Golf": [("BBC Sport", BBCS % "golf"), ("The Guardian", GU % "sport/golf"), ("Sky Sports", SKY % "12176"),
             ("ESPN", "https://www.espn.com/espn/rss/golf/news"), ("CBS Sports", "https://www.cbssports.com/rss/headlines/golf/"),
             ("Golf Monthly", "https://www.golfmonthly.com/feeds/all"), ("National Club Golfer", "https://www.nationalclubgolfer.com/feed/")],
    "NFL": [("BBC Sport", BBCS % "american-football"), ("The Guardian", GU % "sport/nfl"), ("Sky Sports", SKY % "12118"),
            ("ESPN", "https://www.espn.com/espn/rss/nfl/news"), ("CBS Sports", "https://www.cbssports.com/rss/headlines/nfl/"),
            ("Pro Football Talk", "https://profootballtalk.nbcsports.com/feed/")],
    "Grand Tour cycling": [("BBC Sport", BBCS % "cycling"), ("The Guardian", GU % "sport/cycling"),
                           ("Cyclingnews", "https://www.cyclingnews.com/rss/"), ("Cycling Weekly", "https://www.cyclingweekly.com/feeds/all"),
                           ("Velo", "https://velo.outsideonline.com/feed/")],
}
for _sport, _feeds in SPORTS.items():
    for _outlet, _url in _feeds:
        FEEDS.append((_outlet, "sport:" + _sport, _url))

# Celebrity and entertainment news, from broadcasters, papers and the main
# trade titles (no gossip sites). Shown under its own Celebrity button.
CELEBRITY = [
    ("BBC News", "https://feeds.bbci.co.uk/news/entertainment_and_arts/rss.xml"),
    ("The Guardian", GU % "culture"), ("The Guardian", GU % "film"), ("The Guardian", GU % "music"),
    ("The Guardian", GU % "tv-and-radio"),
    ("Sky News", "https://feeds.skynews.com/feeds/rss/entertainment.xml"),
    ("Evening Standard", "https://www.standard.co.uk/showbiz/rss"),
    ("Radio Times", "https://www.radiotimes.com/feed/"),
    ("Digital Spy", "https://www.digitalspy.com/rss/all.xml/"),
    ("NME", "https://www.nme.com/feed"),
    ("RTÉ News", "https://www.rte.ie/feeds/rss/?index=/entertainment/"),
    ("The Irish Times", "https://www.irishtimes.com/arc/outboundfeeds/feed-culture/?outputType=xml"),
    ("CBS News", "https://www.cbsnews.com/latest/rss/entertainment"),
    ("NBC News", "https://feeds.nbcnews.com/nbcnews/public/pop-culture"),
    ("ABC News (US)", "https://abcnews.go.com/abcnews/entertainmentheadlines"),
    ("E! News", "https://www.eonline.com/syndication/feeds/rssfeeds/topstories.xml"),
    ("Variety", "https://variety.com/feed/"),
    ("The Hollywood Reporter", "https://www.hollywoodreporter.com/feed/"),
    ("Deadline", "https://deadline.com/feed/"),
    ("Rolling Stone", "https://www.rollingstone.com/feed/"),
    ("Billboard", "https://www.billboard.com/feed/"),
    # International: Canada, Europe, Asia-Pacific (incl. Bollywood and K-pop)
    ("CBC News", "https://www.cbc.ca/webfeed/rss/rss-arts"),
    ("France 24", "https://www.france24.com/en/culture/rss"),
    ("Le Monde", "https://www.lemonde.fr/en/culture/rss_full.xml"),
    ("Euronews", "https://www.euronews.com/rss?level=vertical&name=culture"),
    ("The Times of India", "https://timesofindia.indiatimes.com/rssfeeds/1081479906.cms"),
    ("Hindustan Times", "https://www.hindustantimes.com/feeds/rss/entertainment/rssfeed.xml"),
    ("The Hindu", "https://www.thehindu.com/entertainment/feeder/default.rss"),
    ("The Indian Express", "https://indianexpress.com/section/entertainment/feed/"),
    ("NDTV", "https://feeds.feedburner.com/ndtvmovies-latest"),
    ("Yonhap", "https://en.yna.co.kr/RSS/culture.xml"),
    ("South China Morning Post", "https://www.scmp.com/rss/322296/feed"),
    ("The Straits Times", "https://www.straitstimes.com/news/life/rss.xml"),
    ("The Sydney Morning Herald", "https://www.smh.com.au/rss/culture.xml"),
    ("NZ Herald", "https://www.nzherald.co.nz/arc/outboundfeeds/rss/section/entertainment/?outputType=xml"),
]
for _outlet, _url in CELEBRITY:
    FEEDS.append((_outlet, "celebrity", _url))

# Titles owned by the same company often share copy, so they count as ONE
# independent source between them.
OWNER = {}
for _group, _titles in {
    "Reach plc": ["Manchester Evening News", "Birmingham Mail", "Liverpool Echo", "Bristol Post", "WalesOnline", "Belfast Live",
                  "MyLondon", "Cambridge News", "Cheshire Live", "Cornwall Live", "Coventry Telegraph", "Derby Telegraph", "Devon Live",
                  "Plymouth Herald", "Essex Live", "Gloucestershire Live", "Hull Daily Mail", "Grimsby Telegraph", "Lancs Live",
                  "Examiner Live", "Leeds Live", "Leicester Mercury", "Lincolnshire Live", "Nottingham Post", "Somerset Live",
                  "Stoke Sentinel", "Surrey Live", "Teesside Live", "Chronicle Live", "Daily Record", "Daily Post"],
    "Newsquest": ["The Herald", "The National", "Glasgow Times", "News & Star", "Westmorland Gazette", "Times & Star",
                  "Bournemouth Echo", "Dorset Echo", "Southern Daily Echo", "Worcester News", "Lancashire Telegraph",
                  "Telegraph & Argus", "The Bolton News", "Eastern Daily Press", "East Anglian Daily Times", "Oxford Mail",
                  "The Argus", "The Northern Echo", "Swindon Advertiser", "Wiltshire Times", "Salisbury Journal",
                  "The Press, York", "South Wales Argus"],
    "National World": ["Yorkshire Post", "The Star, Sheffield", "Lancashire Post", "Blackpool Gazette", "The News, Portsmouth",
                       "Shields Gazette", "Sunderland Echo", "Peterborough Telegraph", "The Scotsman", "Edinburgh Evening News",
                       "News Letter"],
    "DC Thomson": ["Press and Journal", "The Courier"],
    "MNA Media": ["Express & Star", "Shropshire Star"],
    # sport desks belong to the same companies as their news sites
    "BBC News": ["BBC Sport"], "Sky News": ["Sky Sports"], "CBS News": ["CBS Sports"],
    "Disney": ["ESPN", "ESPNcricinfo", "ABC News (US)"], "Motorsport Network": ["Autosport"],
    "NBCUniversal": ["NBC News", "E! News", "Pro Football Talk"],
    "Penske Media": ["Variety", "The Hollywood Reporter", "Deadline", "Rolling Stone", "Billboard"],
    "Future plc": ["Cyclingnews", "Cycling Weekly", "Golf Monthly"],
}.items():
    for _t in _titles:
        OWNER[_t] = _group


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
    "BBC Sport": 1.0, "Sky Sports": 0.9, "ESPN": 0.9, "ESPNcricinfo": 0.93, "Wisden": 0.88, "CBS Sports": 0.88,
    "Autosport": 0.9, "The Race": 0.88, "Crash.net": 0.82, "Pro Football Talk": 0.85, "Cyclingnews": 0.9,
    "Cycling Weekly": 0.88, "Velo": 0.88, "Golf Monthly": 0.85, "National Club Golfer": 0.83,
    "The Irish Times": 0.93, "NBC News": 0.9, "ABC News (US)": 0.9, "Variety": 0.9, "The Hollywood Reporter": 0.9,
    "Deadline": 0.88, "Billboard": 0.87, "Rolling Stone": 0.86, "Radio Times": 0.86, "NME": 0.84,
    "Digital Spy": 0.8, "E! News": 0.78,
    "Le Monde": 0.95, "The Hindu": 0.9, "The Indian Express": 0.88, "Hindustan Times": 0.84, "The Times of India": 0.82,
    "NDTV": 0.82, "Yonhap": 0.9, "South China Morning Post": 0.85, "The Straits Times": 0.88,
    "The Sydney Morning Herald": 0.9, "NZ Herald": 0.86,
}
COUNTRY = {
    "BBC News": "UK", "The Guardian": "UK", "Sky News": "UK", "Financial Times": "UK", "Channel 4 News": "UK",
    "The New York Times": "US", "The Washington Post": "US", "NPR": "US", "CBS News": "US", "CBC News": "CA",
    "Al Jazeera": "QA", "Deutsche Welle": "DE", "France 24": "FR", "Euronews": "EU", "ABC News (Australia)": "AU",
    "RTÉ News": "IE", "BBC Sport": "UK", "Sky Sports": "UK", "ESPN": "US", "CBS Sports": "US", "Pro Football Talk": "US",
    "ESPNcricinfo": "IN", "Velo": "US", "The Irish Times": "IE", "NBC News": "US", "ABC News (US)": "US",
    "E! News": "US", "Variety": "US", "The Hollywood Reporter": "US", "Deadline": "US", "Rolling Stone": "US",
    "Billboard": "US", "Le Monde": "FR", "The Hindu": "IN", "The Indian Express": "IN", "Hindustan Times": "IN",
    "The Times of India": "IN", "NDTV": "IN", "Yonhap": "KR", "South China Morning Post": "HK", "The Straits Times": "SG",
    "The Sydney Morning Herald": "AU", "NZ Herald": "NZ", "Radio Times": "UK", "NME": "UK", "Digital Spy": "UK", "Evening Standard": "UK",
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
        sport_w = sum(1 for m in members if m["section"].startswith("sport:"))
        uk_w = sum(1 if m["section"] == "uk" else 0.5 for m in members if m["section"].startswith(("uk", "region:")))
        main = "uk" if uk_w > world_w else "world"
        if sport_w and sport_w >= max(world_w, uk_w):
            main = "sport"
        celeb_w = sum(1 for m in members if m["section"] == "celebrity")
        if celeb_w and celeb_w >= max(world_w, uk_w, sport_w):
            main = "celebrity"
        sports = sorted({m["section"].split(":", 1)[1] for m in members if m["section"].startswith("sport:")}) if main == "sport" else []
        # A story is local only when local outlets carry most of it. Regional papers also run
        # national and world news, which otherwise leaks into every area's local view.
        local_n = sum(1 for m in members if m["section"].startswith("region:"))
        national_n = sum(1 for m in members if m["section"] in ("world", "uk"))
        local_areas = {m["section"].split(":", 1)[1] for m in members if m["section"].startswith("region:")}
        is_local = main == "uk" and local_n > national_n and len(local_areas) <= 4
        regions = sorted(local_areas) if is_local else []
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
            "link": lead["link"],
            "lead_outlet": lead["outlet"],
            "summary": lead["summary"] or next((m["summary"] for m in sources if m["summary"]), ""),
            "image": image,
            "section": main,
            "regions": regions,
            "sports": sports,
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
        # the same article can sit in two neighbouring areas' feeds: keep it once per area
        key = (it["link"] or it["title"], it["section"])
        if key in seen:
            continue
        seen.add(key)
        items.append(it)
    # AI-content check on every article that could appear in a story
    groups = candidate_articles(items)
    to_check = list({m["link"]: m for g in groups for m in g}.values())
    excluded, ai_status = ai_check.scan(to_check)
    for g in groups:  # copy results onto duplicate copies of the same article
        for m in g:
            if "ai" not in m:
                twin = next((t for t in to_check if t["link"] == m["link"]), None)
                if twin is not None:
                    m["ai"] = twin.get("ai")
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
    regions = [a[0] for a in AREAS]
    ok_urls = {f["url"] for f in status if f["ok"]}
    areas = [{"name": n, "lat": la, "lon": lo,
              "outlets": sorted({o for o, u in fs if u in ok_urls})} for n, la, lo, fs in AREAS]
    return {
        "generated": time.time(),
        "took": round(time.time() - started, 1),
        "articles_scanned": len(items),
        "stories": stories,
        "regions": regions,
        "areas": areas,
        "sports": list(SPORTS),
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
