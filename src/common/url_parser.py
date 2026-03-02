"""
url_parser
----------
Pure stateless functions for parsing search engine referrer URLs.
No class needed — same input always produces same output.

Designed to be registered directly as Spark UDFs without modification:

    spark.udf.register("parse_domain", parse_domain)
    spark.udf.register("parse_keyword", parse_keyword)
"""

import logging
from typing import Optional
from urllib.parse import parse_qs, urlparse

logger = logging.getLogger(__name__)


def is_external_search_referrer(
    referrer: str,
    domains: list[str],
    site_domain: str,
) -> bool:
    """
    Return True if the referrer is from a known external search engine.

    Filters out:
      - Empty referrers
      - Internal referrers (hostname contains site_domain)
      - Referrers whose hostname does not match any known search engine domain
    """
    if not referrer:
        return False
    hostname = _hostname(referrer)
    if not hostname:
        return False
    if site_domain and site_domain in hostname:
        return False
    return any(domain in hostname for domain in domains)


def parse_domain(referrer: str, domains: list[str]) -> Optional[str]:
    """
    Extract a normalized search engine domain from a referrer URL.

    Strips leading subdomains, keeping from the matched engine name onward.

    Examples:
        "http://www.google.com/search?q=ipod"     -> "google.com"
        "http://search.yahoo.com/search?p=zune"   -> "yahoo.com"
        "http://www.bing.com/search?q=zune"       -> "bing.com"
        "http://www.esshopzilla.com"               -> None
    """
    hostname = _hostname(referrer)
    if not hostname:
        return None

    matched = next((d for d in domains if d in hostname), None)
    if matched is None:
        return None

    parts = hostname.split(".")
    try:
        idx = next(i for i, p in enumerate(parts) if p == matched)
        return ".".join(parts[idx:])
    except StopIteration:
        return hostname


def parse_keyword(referrer: str, keyword_params: list[str]) -> Optional[str]:
    """
    Extract a search keyword from a referrer URL query string.

    Tries each param in keyword_params order — first match wins.
    Normalizes the result: lowercase and whitespace-collapsed.

    Examples:
        "http://www.google.com/search?q=Ipod"          -> "ipod"
        "http://search.yahoo.com/search?p=cd+player"   -> "cd player"
        "http://www.bing.com/search?q=Zune"            -> "zune"
    """
    try:
        query = parse_qs(urlparse(referrer).query)
    except Exception:
        logger.warning("Could not parse referrer URL: %s", referrer)
        return None

    for param in keyword_params:
        values = query.get(param, [])
        if values:
            keyword = " ".join(values[0].split()).lower()
            return keyword if keyword else None

    return None


# Internal helper function to safely extract hostname, returning None on failure

def _hostname(url: str) -> Optional[str]:
    try:
        return urlparse(url).hostname or None
    except Exception:
        return None
