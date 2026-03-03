"""Tests for src/common/url_parser.py"""

from common.url_parser import is_external_search_referrer, parse_domain, parse_keyword, _hostname

DOMAINS = ["google", "bing", "yahoo", "duckduckgo", "msn", "ask", "aol"]
KEYWORD_PARAMS = ["q", "p", "query", "search"]
SITE_DOMAIN = "esshopzilla.com"


# ---------- is_external_search_referrer ----------

class TestIsExternalSearchReferrer:
    def test_google_referrer(self):
        url = "http://www.google.com/search?q=ipod"
        assert is_external_search_referrer(url, DOMAINS, SITE_DOMAIN) is True

    def test_bing_referrer(self):
        url = "http://www.bing.com/search?q=zune"
        assert is_external_search_referrer(url, DOMAINS, SITE_DOMAIN) is True

    def test_internal_referrer(self):
        url = "http://www.esshopzilla.com/checkout/"
        assert is_external_search_referrer(url, DOMAINS, SITE_DOMAIN) is False

    def test_empty_referrer(self):
        assert is_external_search_referrer("", DOMAINS, SITE_DOMAIN) is False

    def test_non_search_engine(self):
        url = "http://www.facebook.com/page"
        assert is_external_search_referrer(url, DOMAINS, SITE_DOMAIN) is False

    def test_yahoo_subdomain(self):
        url = "http://search.yahoo.com/search?p=test"
        assert is_external_search_referrer(url, DOMAINS, SITE_DOMAIN) is True


# ---------- parse_domain ----------

class TestParseDomain:
    def test_google(self):
        assert parse_domain("http://www.google.com/search?q=ipod", DOMAINS) == "google.com"

    def test_yahoo_subdomain(self):
        assert parse_domain("http://search.yahoo.com/search?p=cd+player", DOMAINS) == "yahoo.com"

    def test_bing(self):
        assert parse_domain("http://www.bing.com/search?q=zune", DOMAINS) == "bing.com"

    def test_internal_site(self):
        assert parse_domain("http://www.esshopzilla.com", DOMAINS) is None

    def test_empty_string(self):
        assert parse_domain("", DOMAINS) is None

    def test_non_search_engine(self):
        assert parse_domain("http://www.facebook.com", DOMAINS) is None

    def test_duckduckgo(self):
        assert parse_domain("http://duckduckgo.com/?q=test", DOMAINS) == "duckduckgo.com"


# ---------- parse_keyword ----------

class TestParseKeyword:
    def test_google_q_param(self):
        url = "http://www.google.com/search?hl=en&q=Ipod&oq="
        assert parse_keyword(url, KEYWORD_PARAMS) == "ipod"

    def test_yahoo_p_param(self):
        url = "http://search.yahoo.com/search?p=cd+player&toggle=1"
        assert parse_keyword(url, KEYWORD_PARAMS) == "cd player"

    def test_no_keyword_param(self):
        url = "http://www.google.com/search?hl=en"
        assert parse_keyword(url, KEYWORD_PARAMS) is None

    def test_empty_query_string(self):
        url = "http://www.google.com/search"
        assert parse_keyword(url, KEYWORD_PARAMS) is None

    def test_empty_keyword_value(self):
        url = "http://www.google.com/search?q="
        assert parse_keyword(url, KEYWORD_PARAMS) is None

    def test_whitespace_normalization(self):
        url = "http://www.google.com/search?q=ipod++nano"
        assert parse_keyword(url, KEYWORD_PARAMS) == "ipod nano"

    def test_case_normalization(self):
        url = "http://www.bing.com/search?q=ZUNE+HD"
        assert parse_keyword(url, KEYWORD_PARAMS) == "zune hd"


# ---------- _hostname ----------

class TestHostname:
    def test_valid_url(self):
        assert _hostname("http://www.google.com/search") == "www.google.com"

    def test_empty_string(self):
        assert _hostname("") is None

    def test_https(self):
        assert _hostname("https://www.esshopzilla.com/checkout/") == "www.esshopzilla.com"
