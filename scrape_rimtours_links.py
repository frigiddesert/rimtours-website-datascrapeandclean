import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

BASE_URL = "https://rimtours.com/"

# Hub pages to scan beyond the homepage
EXTRA_PAGES = [
    "day-tour-list/",
    "multi-day-tours-list/",
    "multi-day-tour-calendar/",
    "day-tour-pricing-structure/",
    "multi-day-tour-pricing-structure/",
    "product-category/bikes-ready-to-ride/available-now/",
    "product-category/bikes-ready-to-ride/ebikes/",
    "category/biking/",
]

HEADERS = {"User-Agent": "LinkMapper/1.0"}


def fetch(url: str) -> BeautifulSoup:
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def clean_text(s: str) -> str:
    return " ".join(s.split()).strip()


def extract_links_from_nav(soup: BeautifulSoup, section: str):
    links = []
    # Grab all <a> inside <nav> elements
    for nav in soup.find_all("nav"):
        for a in nav.find_all("a", href=True):
            text = clean_text(a.get_text())
            href = urljoin(BASE_URL, a["href"])
            if text:
                links.append(
                    {
                        "section": section,
                        "context": "nav",
                        "text": text,
                        "url": href,
                    }
                )
    return links


def extract_links_from_footer(soup: BeautifulSoup, section: str):
    links = []
    for footer in soup.find_all("footer"):
        for a in footer.find_all("a", href=True):
            text = clean_text(a.get_text())
            href = urljoin(BASE_URL, a["href"])
            if text:
                links.append(
                    {
                        "section": section,
                        "context": "footer",
                        "text": text,
                        "url": href,
                    }
                )
    return links


def extract_all_links(soup: BeautifulSoup, section: str):
    links = []
    for a in soup.find_all("a", href=True):
        text = clean_text(a.get_text())
        href = urljoin(BASE_URL, a["href"])
        # Only keep on-site links
        if text and urlparse(href).netloc.endswith("rimtours.com"):
            links.append(
                {
                    "section": section,
                    "context": "body",
                    "text": text,
                    "url": href,
                }
            )
    return links


def main():
    import csv

    seen = set()
    rows = []

    # Homepage
    home_soup = fetch(BASE_URL)
    rows.extend(extract_links_from_nav(home_soup, "homepage"))
    rows.extend(extract_links_from_footer(home_soup, "homepage"))

    # Extra hub pages
    for path in EXTRA_PAGES:
        url = urljoin(BASE_URL, path)
        try:
            soup = fetch(url)
        except Exception as e:
            print(f"Failed to fetch {url}: {e}")
            continue
        rows.extend(extract_links_from_nav(soup, path))
        rows.extend(extract_links_from_footer(soup, path))
        rows.extend(extract_all_links(soup, path))

    # De-duplicate by (text, url)
    deduped = []
    for r in rows:
        key = (r["text"], r["url"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)

    with open("rimtours_links.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["section", "context", "text", "url"]
        )
        writer.writeheader()
        writer.writerows(deduped)

    print(f"Wrote {len(deduped)} unique links to rimtours_links.csv")


if __name__ == "__main__":
    main()
