"""Compatibility shim for legacy imports.

Canonical settings now live in src/config/settings.py.
"""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


_SRC_SETTINGS = Path(__file__).resolve().parents[1] / "src" / "config" / "settings.py"
_SPEC = spec_from_file_location("_clm_src_config_settings", _SRC_SETTINGS)
if _SPEC is None or _SPEC.loader is None:
    raise RuntimeError(f"Unable to load canonical config module from {_SRC_SETTINGS}")

_MODULE = module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)

Config = _MODULE.Config

__all__ = ["Config"]
