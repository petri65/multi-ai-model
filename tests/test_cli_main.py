import importlib
import sys

import pytest


def _reload_cli_main(monkeypatch):
    """Reload cli_main to ensure patched availability flags are reset per test."""
    module = importlib.import_module("multiai.cli_main")
    return importlib.reload(module)


@pytest.mark.parametrize(
    "argv,patch_target,expected",
    [
        (
            [
                "multiai",
                "daily-merge",
                "--off-dir",
                "/tmp/off",
                "--on-dir",
                "/tmp/on",
                "--out",
                "/tmp/out.parquet",
                "--verbose",
            ],
            "multiai.pipeline.daily_merge.run_daily_merge",
            {
                "off_dir": "/tmp/off",
                "on_dir": "/tmp/on",
                "out": "/tmp/out.parquet",
                "verbose": True,
            },
        ),
        (
            [
                "multiai",
                "build-features",
                "--merged",
                "/tmp/merged.parquet",
                "--out",
                "/tmp/features.parquet",
                "--price-col",
                "mid_price",
                "--verbose",
            ],
            "multiai.pipeline.build_features.run_build_features",
            {
                "merged": "/tmp/merged.parquet",
                "out": "/tmp/features.parquet",
                "price_col": "mid_price",
                "verbose": True,
            },
        ),
    ],
)
def test_cli_subcommands_invoke_pipeline(monkeypatch, argv, patch_target, expected):
    cli_main = _reload_cli_main(monkeypatch)
    monkeypatch.setattr(cli_main, "dm_available", True)
    monkeypatch.setattr(cli_main, "bf_available", True)

    captured = {}

    def _fake_run(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs

    monkeypatch.setattr(patch_target, _fake_run)
    monkeypatch.setattr(sys, "argv", argv)

    cli_main.cli()

    assert "args" in captured, "Pipeline entry point was not invoked"

    if "off_dir" in expected:
        assert captured["args"] == (
            expected["off_dir"],
            expected["on_dir"],
            expected["out"],
            expected["verbose"],
        )
    else:
        assert captured["args"] == (
            expected["merged"],
            expected["out"],
            expected["price_col"],
            expected["verbose"],
        )
