"""Tests for src/common/config.py"""

from pathlib import Path

import pytest

from common.config import load_config


class TestLoadConfig:
    def test_default_path_loads(self):
        config = load_config()
        assert "site_domain" in config
        assert "search_engine_domains" in config
        assert "keyword_params" in config

    def test_default_has_expected_values(self):
        config = load_config()
        assert config["site_domain"] == "esshopzilla.com"
        assert "google" in config["search_engine_domains"]
        assert "q" in config["keyword_params"]

    def test_custom_toml_path(self, tmp_path):
        custom = tmp_path / "custom.toml"
        custom.write_text(
            'site_domain = "test.com"\n'
            'search_engine_domains = ["google"]\n'
            'keyword_params = ["q"]\n'
        )
        config = load_config(custom)
        assert config["site_domain"] == "test.com"
        assert config["search_engine_domains"] == ["google"]

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            load_config(Path("/nonexistent/config.toml"))
