"""
Fetch complete Congress member list + photos.
Bronnen:
- GitHub unitedstates/congress-legislators (538 leden, BioGuideID, partij, staat)
- InsiderFinance CDN voor foto's (slug-based: /congress/images/{slug}.jpg)

Slaat op als data/members.json
"""
from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("members")

LEGISLATORS_URL = "https://raw.githubusercontent.com/unitedstates/congress-legislators/gh-pages/legislators-current.json"
PHOTO_BASE = "https://www.insiderfinance.io/congress/images"
OUTPUT = Path(__file__).parent / "data" / "members.json"


def to_slug(name: str) -> str:
    s = name.lower().strip()
    s = re.sub(r"[^a-z0-9\s-]", "", s)
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"-+", "-", s)
    return s.strip("-")


def fetch_legislators() -> list[dict]:
    log.info("Fetching legislators from GitHub...")
    r = requests.get(LEGISLATORS_URL, timeout=20)
    r.raise_for_status()
    raw = r.json()
    log.info("Got %d legislators", len(raw))

    members = []
    for leg in raw:
        name_info = leg.get("name", {})
        ids = leg.get("id", {})
        terms = leg.get("terms", [])
        if not terms:
            continue
        current = terms[-1]

        first = name_info.get("first", "")
        last = name_info.get("last", "")
        official = name_info.get("official_full", f"{first} {last}")
        bio_id = ids.get("bioguide", "")

        chamber_raw = current.get("type", "")
        chamber = "Senate" if chamber_raw == "sen" else "House" if chamber_raw == "rep" else chamber_raw

        slug = to_slug(f"{first} {last}")

        members.append({
            "name": official,
            "first_name": first,
            "last_name": last,
            "bio_id": bio_id,
            "slug": slug,
            "party": current.get("party", ""),
            "state": current.get("state", ""),
            "district": current.get("district", ""),
            "chamber": chamber,
            "start_date": current.get("start", ""),
            "photo_url": "",  # filled in next step
            "photo_slug": slug,
        })

    return members


def check_photos(members: list[dict], max_check: int = 0) -> list[dict]:
    """Haal foto's op via Wikipedia API (betrouwbaar, altijd beschikbaar)."""
    import urllib.parse

    log.info("Fetching photos from Wikipedia for %d members...", len(members))
    found = 0
    to_check = members[:max_check] if max_check > 0 else members

    for i, m in enumerate(to_check):
        name = f"{m['first_name']} {m['last_name']}"
        wiki_name = urllib.parse.quote(name.replace(" ", "_"))
        url = f"https://en.wikipedia.org/w/api.php?action=query&titles={wiki_name}&prop=pageimages&format=json&pithumbsize=200"
        try:
            r = requests.get(url, headers={"User-Agent": "CongressTracker/1.0"}, timeout=8)
            if r.status_code == 200:
                pages = r.json().get("query", {}).get("pages", {})
                for pid, page in pages.items():
                    thumb = page.get("thumbnail", {}).get("source", "")
                    if thumb:
                        m["photo_url"] = thumb
                        found += 1
                        break
        except Exception:
            pass

        if (i + 1) % 50 == 0:
            log.info("  Checked %d/%d — %d photos found", i + 1, len(to_check), found)

        time.sleep(0.15)

    log.info("Photos found: %d/%d", found, len(to_check))
    return members


def save(members: list[dict]) -> None:
    OUTPUT.parent.mkdir(exist_ok=True)
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(members, f, indent=2, ensure_ascii=False)
    log.info("Saved %d members to %s", len(members), OUTPUT)


def main():
    members = fetch_legislators()
    members = check_photos(members)
    save(members)

    with_photo = sum(1 for m in members if m["photo_url"])
    parties = {}
    for m in members:
        p = m["party"]
        parties[p] = parties.get(p, 0) + 1

    print(f"\nSamenvatting:")
    print(f"  Totaal leden:  {len(members)}")
    print(f"  Met foto:      {with_photo}")
    print(f"  Partijen:      {parties}")
    print(f"  Senate:        {sum(1 for m in members if m['chamber'] == 'Senate')}")
    print(f"  House:         {sum(1 for m in members if m['chamber'] == 'House')}")


if __name__ == "__main__":
    main()
