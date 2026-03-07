"""Tests for src/main.py — CLI entry point."""

import subprocess
import sys
from pathlib import Path
from common.config import load_config
import pytest

config = load_config()
PROJECT_ROOT = Path(__file__).parent.parent
MAIN_SCRIPT = PROJECT_ROOT / "src" / "main.py"
SAMPLE_DATA = PROJECT_ROOT / config["Input_folder"] / "data[98].sql"


def run_cli(*args):
    """Run main.py as a subprocess and return the result."""
    return subprocess.run(
        [sys.executable, str(MAIN_SCRIPT), *args],
        capture_output=True,
        text=True,
    )


class TestCLI:
    def test_sample_data_output(self, tmp_path):
        """CLI produces correct output file with expected content."""
        result = subprocess.run(
            [sys.executable, str(MAIN_SCRIPT), str(SAMPLE_DATA)],
            capture_output=True,
            text=True,
            cwd=str(tmp_path),
        )
        assert result.returncode == 0
        assert "2 keyword group(s)" in result.stdout

        # Find the generated .tab file
        tab_files = list(tmp_path.glob("*_SearchKeywordPerformance.tab"))
        assert len(tab_files) == 1

        content = tab_files[0].read_text()
        lines = content.strip().split("\n")
        assert lines[0] == "Search Engine Domain\tSearch Keyword\tRevenue"
        assert "google.com\tipod\t480.00" in content
        assert "bing.com\tzune\t250.00" in content

    def test_no_arguments(self):
        """CLI exits with error when no file argument is provided."""
        result = run_cli()
        assert result.returncode != 0
        assert "Usage:" in result.stderr

    def test_file_not_found(self):
        """CLI exits with error for nonexistent file."""
        result = run_cli("/nonexistent/file.tsv")
        assert result.returncode != 0
        assert "file not found" in result.stderr

    def test_output_filename_format(self, tmp_path):
        """Output filename follows YYYY-mm-dd_SearchKeywordPerformance.tab."""
        subprocess.run(
            [sys.executable, str(MAIN_SCRIPT), str(SAMPLE_DATA)],
            capture_output=True,
            cwd=str(tmp_path),
        )
        tab_files = list(tmp_path.glob("*_SearchKeywordPerformance.tab"))
        assert len(tab_files) == 1
        # Filename starts with date pattern
        name = tab_files[0].name
        assert name.endswith("_SearchKeywordPerformance.tab")
        assert len(name.split("_")[0].split("-")) == 3  # YYYY-MM-DD
