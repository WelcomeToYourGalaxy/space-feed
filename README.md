# space-feed

The space front, monitored: launches and pads, satellites and orbital debris, military use of
orbit, industry money, policy and law, human spaceflight, exploration, off-world resources,
environment and land, and protest and accountability.

`harvest_space.py` runs every two hours in GitHub Actions, reads 60 wires in 25 languages,
keeps what is genuinely about spaceflight, tags each story by subject, and writes
`wire_space.json`. `index.html` loads that file and renders it.

Nothing here rewrites a headline. Titles and snippets are the publishers' own, truncated but
never reworded, and every row keeps its original link. No model in the pipeline, no API key,
no paid service, no dependencies beyond the Python standard library.

## Files

| File | What it does |
|---|---|
| `harvest_space.py` | Reads every wire, filters, tags, deduplicates, writes `wire_space.json`. Self-contained. |
| `sources_space.json` | The wire list, including the eight watchdog searches. Edit this to add, drop or retune a feed. |
| `wire_space.json` | The output the page reads. Rewritten by the Action; do not hand-edit. |
| `index.html` | The monitor page. Self-contained, reads `wire_space.json` over HTTPS. |
| `.github/workflows/harvest.yml` | The schedule, plus a manual run button in the Actions tab. |

## Setup

1. Push these files to the repository root.
2. Settings → Actions → General → Workflow permissions → **Read and write permissions**, save.
   Without this the Action cannot commit `wire_space.json`.
3. Actions tab → **Harvest the space front** → *Run workflow*. The first run takes two to three
   minutes.
4. Settings → Pages → Source: **Deploy from a branch**, branch `main`, folder `/ (root)`. The
   site appears at `https://welcometoyourgalaxy.github.io/space-feed/` within a minute or two.
   Pages serves `index.html` at the repository root — a file named anything else returns 404 at
   that address.
5. Confirm `https://raw.githubusercontent.com/WelcomeToYourGalaxy/space-feed/main/wire_space.json`
   loads in a browser. That URL is what the page fetches; if it 404s, the harvester has not
   committed yet.

## Embedding

`index.html` is one file with no dependencies beyond two webfonts. Paste its contents into a
Weebly *Embed Code* element, or use the frame-wrapped version if you have it, or iframe the
Pages URL.

If you fork or rename this repository, change `REPO` near the top of the feed script in
`index.html` — that string is what points the page at its wire.

## Sources

**Trade press** — SpaceNews, NASASpaceflight, Spaceflight Now, European Spaceflight, Orbital
Today, Via Satellite, The Space Review.

**Policy** — Space Policy Online.

**Science desks** — Ars Technica Space, Space.com, SpaceDaily, Phys.org, Universe Today.

**Institutional** — NASA, NASA JPL, ESA space transportation, ESA Earth observation, DLR,
Planetary Society.

**Regional press** — Google News editions in English (US, UK, India, Australia, South Africa,
Nigeria, Kenya), Spanish (Spain, Mexico), Portuguese, French, German, Italian, Dutch, Swedish,
Greek, Polish, Russian, Ukrainian, Turkish, Arabic, Hebrew, Persian, Hindi, Bengali,
Indonesian, Vietnamese, Thai, Japanese, Chinese (simplified and traditional), Korean, Swahili.
Each query is written in that language, not translated at read time.

**Watchdog** — eight searches for what the trade press covers thinly: spaceport land and
communities, launch environmental impact, militarisation of orbit, debris and spectrum and
astronomy, mining and the commons, subsidies and contract protests, Alcântara, and Latin
American territory. Everything they return is tagged *Protest & accountability* and marked in
the list.

## What gets kept

A story must name something unambiguously about spaceflight: a rocket, a satellite, a
spaceport, an agency, a company, a treaty, a pad. Words that only sometimes mean space —
launch, mission, orbit, constellation, payload, debris — never qualify a story on their own.
That is what keeps product launches, charity missions and football fixtures out.

Matching respects word edges in Latin script, so "titanium" does not satisfy Titan. A trailing
`*` matches a word family. Scripts without word breaks — Chinese, Japanese, Thai — use
substring matching with guards, so 发射 counts only beside 火箭, 卫星 or 航天.

Blocked outright: office, parking, retail and storage space, space heaters, the film and game
franchises, and the Houston Rockets.

Each story carries every subject it matches, so a military payload on a commercial rocket
appears under both Military & security and Industry & money.

Deduplication runs on a nine-word title fingerprint and on the URL with tracking parameters
stripped. Stories carry forward between runs — the feed keeps 30 days and up to 1,200 rows.

## Coverage is uneven, and the file says so

`wire_space.json` records what each wire returned, or that it could not be reached. The page
prints all of it under *Sources & coverage*, zeros included. Expect Swahili, Bengali and
Persian to read near zero most days. That is a fact about where this industry gets covered,
not a bug to hide.

## Running it locally

```bash
python3 harvest_space.py              # full run
python3 harvest_space.py --dry-run    # harvest and report, write nothing
python3 harvest_space.py --fixtures tests/
```

Python 3.9 or later.
