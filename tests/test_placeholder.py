import importlib

import pytest

MODULES = [
    "kwb",
    "kwb.schemas",
    "kwb.store",
    "kwb.pricing",
    "kwb.decide",
    "kwb.collectors",
    "kwb.parse",
    "kwb.forecast",
    "kwb.forecast.base",
    "kwb.forecast.baselines",
    "kwb.forecast.neural",
    "kwb.eval",
    "kwb.eval.scoring",
    "kwb.eval.backtest",
    "kwb.ops",
]


@pytest.mark.parametrize("name", MODULES)
def test_module_imports(name: str) -> None:
    importlib.import_module(name)
