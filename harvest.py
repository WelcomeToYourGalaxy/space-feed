#!/usr/bin/env python3
"""
harvest_space.py — the space-front wire: industry, launches, orbit, policy and
the accountability stories that trade press under-reports.

Self-contained: fetching, feed parsing, word-edge matching and deduplication are
all in this file. Reads sources_space.json, writes wire_space.json. Standard
library only — no dependencies, no API keys, no model calls.

    python3 harvest_space.py
    python3 harvest_space.py --dry-run
    python3 harvest_space.py --fixtures DIR
"""

import argparse
import json
import os
import re
import sys
import time
import gzip
import html
import io
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

HERE = os.path.dirname(os.path.abspath(__file__))
SOURCES_PATH = os.path.join(HERE, "sources_space.json")
OUT_PATH = os.path.join(HERE, "wire_space.json")

RETAIN_DAYS = 30
MAX_ITEMS = 1200
WORKERS = 10         # a few hundred wires now

# --------------------------------------------------------------------------
# Shared plumbing: fetching, feed parsing, word-edge matching, fingerprints.
# Identical to the astrobiology harvester's, inlined so this repository
# stands on its own.
# --------------------------------------------------------------------------
USER_AGENT = ("Mozilla/5.0 (compatible; space-life-news/1.0; "
              "+https://github.com/WelcomeToYourGalaxy/space-life-news)")

TIMEOUT = 25

SNIPPET_CHARS = 240

TAG_RE = re.compile(r"<[^>]+>")

WS_RE = re.compile(r"\s+")

PUNCT_RE = re.compile(r"[^\w\s]", re.UNICODE)

def build_gnews_url(loc):
    q = loc["query"] + " when:30d"
    return ("https://news.google.com/rss/search?q=" + urllib.parse.quote(q) +
            "&hl=" + loc["hl"] + "&gl=" + loc["gl"] + "&ceid=" + loc["ceid"])

def fetch(url, tries=3):
    last = None
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/rss+xml, application/atom+xml, application/xml;q=0.9, */*;q=0.8",
                "Accept-Encoding": "gzip",
                "Accept-Language": "*",
            })
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                raw = resp.read()
                if resp.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
                return raw
        except Exception as exc:                       # noqa: BLE001 — report, don't crash the run
            last = exc
            time.sleep(1.5 * (attempt + 1))
    print("  ! unreachable: %s (%s)" % (url[:90], last), file=sys.stderr)
    return None

def strip_ns(tag):
    return tag.split("}", 1)[1] if "}" in tag else tag

def text_of(el):
    return WS_RE.sub(" ", html.unescape(TAG_RE.sub(" ", el.text or ""))).strip() if el is not None else ""

def child(node, *names):
    for kid in node:
        if strip_ns(kid.tag) in names:
            return kid
    return None

def parse_date(raw):
    if not raw:
        return None
    raw = raw.strip()
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:  # noqa: BLE001
        pass
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:  # noqa: BLE001
        return None

def parse_feed(raw, src):
    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        # Some publishers serve a stray byte before the declaration.
        try:
            root = ET.fromstring(raw[raw.index(b"<"):])
        except Exception:  # noqa: BLE001
            return []

    nodes = [n for n in root.iter() if strip_ns(n.tag) == "item"]
    atom = False
    if not nodes:
        nodes = [n for n in root.iter() if strip_ns(n.tag) == "entry"]
        atom = True

    out = []
    for n in nodes:
        title = text_of(child(n, "title"))
        if atom:
            link = ""
            for kid in n:
                if strip_ns(kid.tag) == "link" and kid.get("rel", "alternate") == "alternate":
                    link = kid.get("href", "")
                    break
        else:
            link_el = child(n, "link")
            link = (link_el.text or "").strip() if link_el is not None else ""
            if not link:
                link = text_of(child(n, "guid"))
        if not title or not link:
            continue

        outlet_el = child(n, "source")
        outlet = text_of(outlet_el) if outlet_el is not None else ""
        if outlet and title.endswith(" - " + outlet):
            title = title[: -(len(outlet) + 3)].strip()
        elif not outlet and src["name"].startswith("Google News") and " - " in title:
            # Google News appends the outlet to the headline when it omits <source>.
            head, _, tail = title.rpartition(" - ")
            if head and 2 <= len(tail) <= 45:
                title, outlet = head.strip(), tail.strip()

        stamp = parse_date(text_of(child(n, "pubDate", "published", "updated", "date")))
        snippet = text_of(child(n, "description", "summary", "content"))[:SNIPPET_CHARS]

        out.append({
            "t": title,
            "u": link,
            "o": outlet or src["name"].replace("Google News · ", ""),
            "g": src["lang"],
            "r": src["region"],
            "k": src.get("kind", "news"),
            "d": stamp,
            "s": snippet,
            "w": src["name"],
        })
    return out

def _compile(term):
    if any(ord(ch) > 0x24F for ch in term):        # non-Latin script
        # substring matching is already prefix-like in scripts without word
        # breaks, so a trailing * is a no-op — strip it rather than search for
        # a literal asterisk, which is what used to happen.
        return term[:-1] if term.endswith("*") else term
    if term.endswith("*"):
        return re.compile(r"(?<![a-z0-9])" + re.escape(term[:-1]) + r"[a-z0-9\-]*", re.I)
    return re.compile(r"(?<![a-z0-9])" + re.escape(term) + r"(?![a-z0-9])", re.I)

def _compile_all(terms):
    return [_compile(t) for t in terms]

def hit(text, compiled):
    """True when any compiled term matches."""
    for c in compiled:
        if isinstance(c, str):
            if c in text:
                return True
        elif c.search(text):
            return True
    return False

def fingerprint(title):
    norm = PUNCT_RE.sub(" ", title.lower())
    return " ".join(WS_RE.sub(" ", norm).strip().split()[:9])

def canon_url(url):
    try:
        parts = urllib.parse.urlsplit(url)
        query = urllib.parse.parse_qsl(parts.query)
        query = [(k, v) for k, v in query if not k.lower().startswith("utm_")]
        return urllib.parse.urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/"),
                                        urllib.parse.urlencode(query), ""))
    except Exception:  # noqa: BLE001
        return url


# --------------------------------------------------------------------------
# Subjects.  Each story is tagged with every subject it matches, so a launch
# that is also a military payload appears under both.  Guards work as they do
# in harvest.py: a term only counts when a supporting word is present, because
# "launch" is what products do and "constellation" is what stars are.
# --------------------------------------------------------------------------
TOPICS = [
    ("launch", "Launches & pads", [
        ("rocket launch", None), ("launch vehicle", None), ("liftoff", None), ("lift-off", None),
        ("spaceport", None), ("cosmodrome", None), ("launch pad", None), ("launch site", None),
        ("static fire", None), ("scrub*", ["launch", "rocket", "countdown"]),
        ("falcon 9", None), ("starship", ["spacex", "launch", "flight", "booster", "orbital"]),
        ("new glenn", None), ("electron rocket", None), ("neutron rocket", None),
        ("ariane", None), ("vega-c", None), ("soyuz", None), ("proton-m", None), ("angara", None),
        ("long march", None), ("pslv", None), ("gslv", None), ("h3 rocket", None), ("h-iia", None),
        ("baikonur", None), ("vandenberg", None), ("cape canaveral", None), ("kennedy space center", None),
        ("kourou", None), ("wenchang", None), ("jiuquan", None), ("tanegashima", None),
        ("sriharikota", None), ("saxavord", None), ("esrange", None), ("andøya", None),
        ("alcântara", None), ("alcantara", ["base", "lançamento", "foguete", "espacial"]),
        ("boca chica", None), ("starbase", None), ("mahia", None), ("wallops", None),
        ("ракет", ["запуск", "космодром", "старт", "пуск"]),
        ("ракет", ["запуск", "космодром"]),
        ("космодром", None), ("발사", ["로켓", "위성", "우주"]), ("打ち上げ", ["ロケット", "衛星", "宇宙"]),
        ("发射", ["火箭", "卫星", "航天"]), ("發射", ["火箭", "衛星", "太空"]),
        ("إطلاق", ["صاروخ", "قمر صناعي", "فضاء"]), ("प्रक्षेपण", ["रॉकेट", "उपग्रह", "अंतरिक्ष"]),
        ("lancement de fusée", None), ("raketenstart", None), ("lanzamiento de cohete", None),
        ("lançamento de foguete", None), ("запуск ракеты", None), ("запуск ракети", None),
        ("火箭发射", None), ("火箭發射", None), ("ロケット打ち上げ", None), ("로켓 발사", None),
        ("إطلاق صاروخ", None), ("रॉकेट प्रक्षेपण", None), ("peluncuran roket", None),
        ("roket fırlatma", None), ("start rakiety", None), ("ปล่อยจรวด", None),
    ]),
    ("orbit", "Satellites & orbit", [
        ("satellite constellation", None), ("megaconstellation", None), ("mega-constellation", None),
        ("starlink", None), ("kuiper", ["amazon", "satellite", "constellation", "broadband"]),
        ("oneweb", None), ("guowang", None), ("qianfan", None), ("iris²", None), ("iris2", None),
        ("orbital debris", None), ("space debris", None), ("space junk", None), ("kessler", None),
        ("collision avoidance", ["satellite", "orbit", "spacecraft"]),
        ("deorbit*", None), ("reentry", ["satellite", "debris", "spacecraft", "capsule", "stage"]),
        ("geostationary", None), ("low earth orbit", None), ("leo satellite", None),
        ("ground station", ["satellite", "space", "antenna", "downlink"]),
        ("earth observation", None), ("remote sensing", None), ("gps jamming", None), ("gnss", None),
        ("спутник", ["орбит", "запуск", "связ", "группиров"]),
        ("卫星", ["星座", "发射", "轨道", "互联网"]), ("衛星", ["星座", "發射", "軌道"]),
        ("위성", ["발사", "궤도", "군집"]), ("衛星", ["打ち上げ", "軌道", "コンステレーション"]),
        ("weltraumschrott", None), ("débris spatiaux", None), ("basura espacial", None),
        ("lixo espacial", None), ("космический мусор", None), ("космічне сміття", None),
        ("空间碎片", None), ("太空碎片", None), ("スペースデブリ", None), ("우주 쓰레기", None),
        ("śmieci kosmiczne", None), ("uzay çöpü", None), ("ruimteafval", None),
    ]),
    ("military", "Military & security", [
        ("space force", None), ("space command", None), ("spacecom", None),
        ("anti-satellite", None), ("asat", None), ("kinetic strike", ["satellite", "orbit"]),
        ("space weapon*", None), ("militari*", ["space", "orbit", "satellite", "spatial", "espacial"]),
        ("golden dome", None), ("missile defense", ["space", "satellite", "orbit"]),
        ("missile defence", ["space", "satellite", "orbit"]),
        ("reconnaissance satellite", None), ("spy satellite", None), ("military satellite", None),
        ("national reconnaissance office", None), ("space fence", None), ("dual-use", ["space", "satellite", "launch"]),
        ("военный спутник", None), ("军事卫星", None), ("軍事衛星", None), ("군사 위성", None),
        ("satellite militaire", None), ("militärsatellit", None), ("satélite militar", None),
    ]),
    ("industry", "Industry & money", [
        ("spacex", None), ("blue origin", None), ("rocket lab", None), ("firefly aerospace", None),
        ("relativity space", None), ("stoke space", None), ("sierra space", None), ("axiom space", None),
        ("arianespace", None), ("avio", ["vega", "launch", "rocket"]), ("isar aerospace", None),
        ("rfa one", None), ("pld space", None), ("skyrora", None), ("orbex", None),
        ("landspace", None), ("ispace", None), ("galactic energy", None), ("i-space", None),
        ("agnikul", None), ("skyroot", None), ("dhruva space", None), ("interstellar technologies", None),
        ("northrop grumman", ["space", "launch", "satellite", "rocket"]),
        ("lockheed martin", ["space", "launch", "satellite"]), ("boeing", ["space", "starliner", "sls", "satellite"]),
        ("united launch alliance", None), ("maxar", None), ("planet labs", None), ("iceye", None),
        ("space industry", None), ("launch contract", None), ("space economy", None),
        ("valuation", ["space", "satellite", "launch", "rocket"]),
        ("ipo", ["space", "satellite", "launch", "rocket"]),
        ("funding round", ["space", "satellite", "launch", "rocket"]),
        ("industria espacial", None), ("indústria espacial", None), ("industrie spatiale", None),
        ("raumfahrtindustrie", None), ("industria spaziale", None), ("商业航天", None),
        ("宇宙産業", None), ("우주산업", None), ("太空產業", None), ("космическая отрасль", None),
        ("صناعة الفضاء", None), ("अंतरिक्ष उद्योग", None),
    ]),
    ("policy", "Policy & law", [
        ("outer space treaty", None), ("artemis accords", None), ("copuos", None),
        ("space law", None), ("launch licen*", None), ("faa licen*", ["launch", "rocket", "spaceport"]),
        ("fcc", ["satellite", "constellation", "spectrum", "orbit"]),
        ("itu", ["spectrum", "satellite", "orbit"]), ("spectrum allocation", None),
        ("space policy", None), ("space agency budget", None), ("space regulation", None),
        ("liability convention", None), ("registration convention", None),
        ("sanction*", ["space", "satellite", "launch", "roscosmos"]),
        ("export control*", ["space", "satellite", "launch", "itar"]), ("itar", None),
        ("politique spatiale", None), ("política espacial", None), ("weltraumpolitik", None),
        ("космическая политика", None), ("宇宙政策", None), ("太空政策", None), ("우주정책", None),
    ]),
    ("human", "Human spaceflight", [
        ("astronaut*", None), ("cosmonaut*", None), ("taikonaut*", None),
        ("international space station", None), ("iss deorbit", None), ("tiangong", None),
        ("crew dragon", None), ("starliner", None), ("soyuz ms", None), ("shenzhou", None),
        ("gaganyaan", None), ("artemis ii", None), ("artemis iii", None), ("lunar gateway", None),
        ("space tourism", None), ("suborbital flight", None), ("spacewalk", None),
        ("commercial space station", None), ("orbital reef", None), ("vast haven", None),
        ("astronaute", None), ("astronauta", None), ("宇宙飛行士", None), ("航天员", None),
        ("우주비행사", None), ("космонавт", None), ("رائد فضاء", None),
    ]),
    ("exploration", "Exploration & science", [
        ("lunar lander", None), ("moon mission", None), ("chang'e", None), ("chandrayaan", None),
        ("luna-2*", None), ("clps", None), ("intuitive machines", None), ("firefly blue ghost", None),
        ("mars rover", None), ("perseverance", None), ("curiosity rover", None), ("tianwen", None),
        ("exomars", None), ("europa clipper", None), ("juice mission", None), ("dragonfly", ["titan", "nasa"]),
        ("psyche mission", None), ("osiris-apex", None), ("hera mission", None), ("dart mission", None),
        ("planetary defense", None), ("asteroid deflection", None), ("sample return", None),
        ("space telescope", None), ("jwst", None), ("webb telescope", None), ("hubble", None),
        ("deep space network", None), ("interstellar probe", None),
        ("mission lunaire", None), ("misión lunar", None), ("missão lunar", None),
        ("月球任务", None), ("探月", None), ("月探査", None), ("달 탐사", None),
    ]),
    ("resources", "Resources & mining", [
        ("asteroid mining", None), ("space mining", None), ("lunar mining", None),
        ("helium-3", None), ("in-situ resource", None), ("isru", None), ("regolith", None),
        ("lunar water", None), ("space resources", None), ("orbital manufacturing", None),
        ("space solar power", None), ("minería espacial", None), ("mineração espacial", None),
        ("exploitation minière spatiale", None), ("weltraumbergbau", None), ("太空采矿", None),
        ("宇宙資源", None),
    ]),
    ("environment", "Environment & land", [
        ("launch emissions", None), ("rocket emissions", None), ("black carbon", ["rocket", "launch", "stratosphere"]),
        ("ozone", ["rocket", "launch", "reentry", "satellite"]),
        ("environmental impact", ["launch", "spaceport", "rocket", "satellite", "space"]),
        ("environmental review", ["launch", "spaceport", "rocket"]),
        ("wildlife refuge", ["launch", "spaceport", "rocket"]),
        ("indigenous land", ["launch", "spaceport", "telescope", "satellite", "space"]),
        ("sacred site", ["telescope", "launch", "spaceport", "observatory"]),
        ("mauna kea", None), ("quilombola", ["alcântara", "base", "lançamento", "espacial"]),
        ("light pollution", ["satellite", "constellation", "starlink", "astronomer"]),
        ("radio quiet", None), ("dark and quiet skies", None),
        ("impacto ambiental", ["foguete", "cohete", "lanzamiento", "lançamento", "espacial"]),
        ("impact environnemental", ["fusée", "lancement", "spatial"]),
        ("umweltauswirkungen", ["rakete", "start", "weltraum"]),
    ]),
    ("accountability", "Protest & accountability", [
        ("protest*", ["launch", "spaceport", "rocket", "satellite", "space", "telescope"]),
        ("lawsuit", ["launch", "spaceport", "rocket", "satellite", "space", "faa"]),
        ("court ruling", ["launch", "spaceport", "space", "satellite"]),
        ("subsid*", ["space", "launch", "rocket", "satellite"]),
        ("tax break", ["space", "launch", "rocket", "satellite"]),
        ("audit", ["nasa", "space force", "launch", "satellite", "esa"]),
        ("inspector general", ["nasa", "space"]), ("gao report", ["nasa", "space", "launch"]),
        ("cost overrun", ["nasa", "space", "launch", "sls", "satellite"]),
        ("safety violation*", ["launch", "space", "rocket"]),
        ("workers", ["spacex", "launch site", "spaceport", "rocket factory", "space company"]),
        ("union", ["spacex", "space company", "launch site", "aerospace workers"]),
        ("displac*", ["spaceport", "launch site", "space base"]),
        ("manifestación", ["espacial", "cohete", "lanzamiento"]),
        ("protesto", ["espacial", "foguete", "lançamento", "base"]),
    ]),
]

# --------------------------------------------------------------------------
# The gate.  ANCHOR terms are unambiguously about spaceflight and keep a story
# on their own.  AMBIGUOUS terms — launch, mission, constellation, orbit,
# payload — keep only alongside an anchor, because otherwise a product launch,
# a charity mission and a Netflix constellation all arrive.
# --------------------------------------------------------------------------
ANCHOR = [
    "spaceflight", "spacecraft", "spaceport", "cosmodrome", "launch vehicle", "launch pad",
    "launch site", "rocket launch", "orbital launch", "suborbital", "rocket engine", "booster stage",
    "satellite", "megaconstellation", "mega-constellation", "orbital debris", "space debris",
    "space junk", "geostationary", "low earth orbit", "space station", "spacewalk", "astronaut*",
    "cosmonaut*", "taikonaut*", "space agency", "space force", "space command", "space industry",
    "space economy", "space policy", "space law", "outer space treaty", "artemis accords",
    "nasa", "esa ", "jaxa", "isro", "roscosmos", "cnsa", "spacex", "blue origin", "rocket lab",
    "arianespace", "ariane", "starlink", "starship", "falcon 9", "new glenn", "soyuz", "long march",
    "vega-c", "pslv", "gslv", "electron rocket", "artemis", "tiangong", "shenzhou", "chang'e",
    "chandrayaan", "gaganyaan", "starliner", "crew dragon", "baikonur", "vandenberg",
    "cape canaveral", "kourou", "wenchang", "jiuquan", "tanegashima", "sriharikota", "saxavord",
    "esrange", "andøya", "wallops", "boca chica", "starbase", "alcântara", "alcantara",
    "kodiak launch", "sutherland spaceport", "mahia peninsula", "launch base", "launch complex",
    "space base", "asteroid mining", "space mining", "lunar mining", "space resources",
    "in-situ resource", "orbital manufacturing", "space solar power", "mauna kea",
    "fusée", "spatial*", "espacial", "espacio", "espaço", "raumfahrt", "weltraum", "rakete",
    "satellit*", "satélite", "satelita", "kosmodrom", "космодром", "космонавт", "ракета",
    "спутник", "космическ*", "航天", "太空", "火箭", "卫星", "衛星", "宇宙開発", "ロケット",
    "人工衛星", "우주", "로켓", "위성", "فضاء", "صاروخ", "قمر صناعي", "अंतरिक्ष", "उपग्रह",
    "रॉकेट", "মহাকাশ", "antariksa", "roket", "uzay", "uydu", "vũ trụ", "vệ tinh", "อวกาศ",
    "จรวด", "rymd", "ruimtevaart", "kosmiczn*", "διαστημ*", "חלל", "לוויין", "anga za juu",
]

AMBIGUOUS = [
    "launch*", "mission*", "orbit*", "constellation*", "payload*", "propulsion", "reentry",
    "telemetry", "ground station", "downlink", "spectrum", "regolith", "lander", "probe",
    "observatory", "telescope", "debris", "docking", "capsule", "booster", "pad abort",
]

BLOCK = [
    # the word "space" in its earthly senses, and the entertainment industry
    "office space", "parking space", "storage space", "coworking", "retail space",
    "space heater", "space bar", "headspace", "safe space", "green space", "crawl space",
    "starfield game", "no man's sky", "kerbal", "star wars", "star trek", "space marine",
    "warhammer", "box office", "streaming series", "season finale", "horoscope", "astrolog*",
    "zodiac", "космический гороскоп",
    # sport and misc namesakes
    "houston rockets", "rockets guard", "rockets coach", "nba", "premier league",
    "champions league", "europa league", "space jam",
]

ANCHOR_C = _compile_all(ANCHOR)
AMBIGUOUS_C = _compile_all(AMBIGUOUS)
BLOCK_C = _compile_all(BLOCK)
TOPICS_C = [(tid, label, [(_compile(t), _compile_all(g) if g else None) for t, g in terms])
            for tid, label, terms in TOPICS]


def relevant(text):
    """An anchor keeps a story on its own.  An ambiguous term — launch, mission,
    orbit, constellation — never keeps one by itself; it needs an anchor beside
    it, which is what separates a rocket launch from a product launch."""
    if hit(text, BLOCK_C):
        return False
    return hit(text, ANCHOR_C)


def topics_for(text):
    hits = []
    for tid, _label, terms in TOPICS_C:
        for term, guards in terms:
            if not hit(text, [term]):
                continue
            if guards and not hit(text, guards):
                continue
            hits.append(tid)
            break
    return hits


def load_sources():
    with open(SOURCES_PATH, encoding="utf-8") as fh:
        cfg = json.load(fh)
    srcs = []
    for s in cfg.get("direct", []):
        srcs.append({"name": s["name"], "lang": s["lang"], "region": s["region"],
                     "kind": s.get("kind", "news"), "url": s["url"]})
    for block, kind in (("gnews", "news"), ("watchdog", "watchdog")):
        for loc in cfg.get(block, []):
            srcs.append({
                "name": ("Google News · " if block == "gnews" else "Watchdog · ") + loc["label"],
                "lang": loc["lang"], "region": loc["region"], "kind": kind,
                "url": build_gnews_url(loc),
            })
    return srcs, cfg


def run(dry_run=False, fixtures=None):
    sources, cfg = load_sources()
    print("Reading %d wires…" % len(sources))

    def read(src):
        if fixtures:
            path = os.path.join(fixtures, re.sub(r"[^\w.-]", "_", src["name"]) + ".xml")
            if not os.path.exists(path):
                return src, None
            with open(path, "rb") as fh:
                return src, fh.read()
        return src, fetch(src["url"])

    results = []
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for src, raw in pool.map(read, sources):
            results.append((src, raw))

    previous = []
    if os.path.exists(OUT_PATH):
        try:
            with open(OUT_PATH, encoding="utf-8") as fh:
                previous = json.load(fh).get("items", [])
        except Exception:  # noqa: BLE001
            previous = []

    seen_fp, seen_url, items = set(), set(), []

    def absorb(row):
        fp = fingerprint(row["t"])
        cu = canon_url(row["u"])
        if fp in seen_fp or cu in seen_url:
            return False
        seen_fp.add(fp)
        seen_url.add(cu)
        items.append(row)
        return True

    stats, ok_count = [], 0
    for src, raw in results:
        stat = {"name": src["name"], "lang": src["lang"], "region": src["region"], "kept": 0, "ok": False}
        if raw:
            stat["ok"] = True
            ok_count += 1
            for row in parse_feed(raw, src):
                text = (row["t"] + " " + row["s"]).lower()
                if not relevant(text):
                    continue
                row["x"] = topics_for(text) or ["industry"]
                if src.get("kind") == "watchdog" and "accountability" not in row["x"]:
                    row["x"].append("accountability")
                if absorb(row):
                    stat["kept"] += 1
        stats.append(stat)
        print("  %-36s %s" % (src["name"][:36], "unreachable" if not raw else "%d kept" % stat["kept"]))

    fresh_urls = {canon_url(i["u"]) for i in items}
    for row in previous:
        if "x" in row:
            absorb(row)

    cutoff = int(time.time() * 1000) - RETAIN_DAYS * 86400000
    items = [i for i in items if (i.get("d") or cutoff + 1) >= cutoff]
    items.sort(key=lambda i: i.get("d") or 0, reverse=True)
    items = items[:MAX_ITEMS]
    fresh = sum(1 for i in items if canon_url(i["u"]) in fresh_urls)

    languages = {}
    for loc in cfg.get("gnews", []):
        languages.setdefault(loc["lang"], re.sub(r"\s*·.*$|\s*\(.*$|\s+\d+$", "", loc["label"]).strip())
    languages.setdefault("en", "English")

    regions = []
    for s in stats:
        if s["region"] not in regions:
            regions.append(s["region"])

    payload = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "counts": {"stories": len(items), "new_this_run": fresh,
                   "languages": len({i["g"] for i in items}),
                   "wires_ok": ok_count, "wires_total": len(sources)},
        "languages": languages,
        "regions": regions,
        "topics": [{"id": tid, "label": label} for tid, label, _ in TOPICS],
        "sources": stats,
        "items": items,
    }

    print("\n%d stories (%d new this run) · %d languages · %d/%d wires answered"
          % (len(items), fresh, payload["counts"]["languages"], ok_count, len(sources)))
    zero = [s["name"] for s in stats if s["ok"] and s["kept"] == 0]
    if zero:
        print("Answered but returned nothing on topic: " + ", ".join(zero))

    if dry_run:
        print("\n--dry-run: wire_space.json not written")
        return payload

    with open(OUT_PATH, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, separators=(",", ":"))
    print("Wrote %s (%.0f KB)" % (OUT_PATH, os.path.getsize(OUT_PATH) / 1024))
    return payload


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--fixtures")
    args = ap.parse_args()
    run(dry_run=args.dry_run, fixtures=args.fixtures)
