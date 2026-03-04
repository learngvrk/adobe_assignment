"""Configuration loader for search engine attribution settings."""

import tomllib
from pathlib import Path

_DEFAULT_CONFIG_PATH = Path(__file__).parent / "search_engines.toml"
_REQUIRED_KEYS = ("search_engine_domains", "keyword_params")


def load_config(path: Path = _DEFAULT_CONFIG_PATH) -> dict:
    """
    Load search engine configuration from a TOML file.

    Args:
        path: Path to the TOML config file. Defaults to search_engines.toml
              co-located with this module.

    Returns:
        Dict with keys: site_domain, search_engine_domains, keyword_params.

    Raises:
        FileNotFoundError: If the config file does not exist.
        ValueError: If the TOML is invalid or required keys are missing.
    """
    try:
        with open(path, "rb") as f:
            config = tomllib.load(f)
    except tomllib.TOMLDecodeError as exc:
        raise ValueError(f"Invalid TOML in {path}: {exc}") from exc

    missing = [k for k in _REQUIRED_KEYS if k not in config]
    if missing:
        raise ValueError(
            f"Config {path} is missing required keys: {', '.join(missing)}"
        )

    return config
