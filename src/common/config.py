import tomllib
from pathlib import Path

_DEFAULT_CONFIG_PATH = Path(__file__).parent / "search_engines.toml"


def load_config(path: Path = _DEFAULT_CONFIG_PATH) -> dict:
    """
    Load search engine configuration from a TOML file.

    Args:
        path: Path to the TOML config file. Defaults to search_engines.toml
              co-located with this module.

    Returns:
        Dict with keys: site_domain, search_engine_domains, keyword_params.
    """
    with open(path, "rb") as f:
        return tomllib.load(f)
