"""
Smoke test: run SessionAwareAnalyzer against the sample data file and
assert the expected output matches the session-aware first-touch attribution.

Expected results (derived manually from data[98].sql):
    - google.com / ipod  / $480.00  (IP 67.98.123.1 $290 + IP 44.12.96.2 $190)
    - bing.com   / zune  / $250.00  (IP 23.8.61.21  $250)
    - yahoo session (IP 112.33.98.231) never purchased — excluded.

Run from project root:
    python tests/verify_sample.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from common.config import load_config
from common.analyzer import SessionAwareAnalyzer
from common.config import load_config

config = load_config()

SAMPLE_DATA = Path(__file__).parent.parent / config["Input_folder"] / "data[98].sql"

EXPECTED = [
    {"Search Engine Domain": "google.com", "Search Keyword": "ipod",  "Revenue": 480.00},
    {"Search Engine Domain": "bing.com",   "Search Keyword": "zune",  "Revenue": 250.00},
]


def main():
    config = load_config()
    analyzer = SessionAwareAnalyzer(config)

    with open(SAMPLE_DATA) as f:
        content = f.read()

    results = analyzer.process(content)

    print("=== Results ===")
    for r in results:
        print(f"  {r['Search Engine Domain']:<15} {r['Search Keyword']:<10} ${r['Revenue']:.2f}")

    print("\n=== Assertions ===")
    assert len(results) == len(EXPECTED), (
        f"Expected {len(EXPECTED)} rows, got {len(results)}"
    )
    for actual, expected in zip(results, EXPECTED):
        assert actual["Search Engine Domain"] == expected["Search Engine Domain"], (
            f"Domain mismatch: {actual['Search Engine Domain']} != {expected['Search Engine Domain']}"
        )
        assert actual["Search Keyword"] == expected["Search Keyword"], (
            f"Keyword mismatch: {actual['Search Keyword']} != {expected['Search Keyword']}"
        )
        assert abs(actual["Revenue"] - expected["Revenue"]) < 0.01, (
            f"Revenue mismatch: {actual['Revenue']} != {expected['Revenue']}"
        )

    filename, tab = analyzer.to_tab_delimited(results)
    print(f"\n=== Output file: {filename} ===")
    print(tab)

    print("All assertions passed.")


if __name__ == "__main__":
    main()
