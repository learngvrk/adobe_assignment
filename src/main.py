"""
CLI entry point for search keyword attribution.

Usage:
    python src/main.py <input_tsv_file>

Example:
    python src/main.py requirements/data[98].sql
"""

import sys
from pathlib import Path

# Allow running from project root: python src/main.py
sys.path.insert(0, str(Path(__file__).parent))

from common.config import load_config
from common.analyzer import SessionAwareAnalyzer


def main():
    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} <input_tsv_file>", file=sys.stderr)
        sys.exit(1)

    input_path = Path(sys.argv[1])
    if not input_path.exists():
        print(f"Error: file not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    config = load_config()
    analyzer = SessionAwareAnalyzer(config)

    tsv_content = input_path.read_text()
    results = analyzer.process(tsv_content)

    filename, tab_content = analyzer.to_tab_delimited(results)
    output_path = Path(filename)
    output_path.write_text(tab_content)

    print(f"Processed {len(results)} keyword group(s)")
    print(f"Output: {output_path}")


if __name__ == "__main__":
    main()
