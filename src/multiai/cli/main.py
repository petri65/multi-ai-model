import click
from rich import print
from pathlib import Path
from multiai.collectors.offchain.binance_l2_l10 import run_binance_l2
from multiai.collectors.onchain.ltc_mempool import run_litecoin_mempool
from multiai.pipeline.daily_merge import build_daily
from multiai.dataops.quantize import quantize_to_1s
from multiai.dataops.split_object_columns import split_object_columns_if_present
from multiai.dataops.merge_on_off import merge_on_off
import asyncio

@click.group()
def main():
    pass

@main.group()
def run():
    pass

@run.command("collect-offchain")
@click.option("--symbol", default="LTCUSDT")
@click.option("--outdir", required=True)
@click.option("--rotate-minutes", type=int, default=90)
def collect_offchain(symbol, outdir, rotate_minutes):
    print(f"[bold]Collecting Binance {symbol} at 200ms → {outdir}[/]")
    asyncio.run(run_binance_l2(symbol, outdir, rotate_minutes))

@run.command("collect-onchain")
@click.option("--rpc-url", default="http://127.0.0.1:9332")
@click.option("--rpc-user", required=True)
@click.option("--rpc-pass", required=True)
@click.option("--outdir", required=True)
@click.option("--rotate-minutes", type=int, default=90)
def collect_onchain(rpc_url, rpc_user, rpc_pass, outdir, rotate_minutes):
    print(f"[bold]Collecting Litecoin mempool at 200ms → {outdir}[/]")
    run_litecoin_mempool(rpc_url, rpc_user, rpc_pass, outdir, rotate_minutes)

@run.command("daily-merge")
@click.option("--off-dir", required=True)
@click.option("--on-dir", required=True)
@click.option("--out", required=True)
def daily_merge(off_dir, on_dir, out):
    print("[bold]Quantize → split → strict merge[/]")
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    build_daily(off_dir, on_dir, out)

@run.command("dataops")
@click.option("--on", "on_path", required=True)
@click.option("--off", "off_path", required=True)
@click.option("--whales", "whales_path", default=None)
@click.option("--out", "out_path", required=True)
def dataops(on_path, off_path, whales_path, out_path):
    import pandas as pd
    print("[bold]Step 1/4:[/] Quantize to 1s (forward snap + dedupe)")
    q_on = quantize_to_1s(pd.read_parquet(on_path))
    q_off = quantize_to_1s(pd.read_parquet(off_path))
    q_whales = None
    if whales_path:
        print("[bold]Step 1b/4:[/] Quantize whale aggregates to 1s grid")
        q_whales = quantize_to_1s(pd.read_parquet(whales_path))
    print("[bold]Step 2/4:[/] Split list/object columns (if present)")
    s_on = split_object_columns_if_present(q_on)
    s_off = split_object_columns_if_present(q_off)
    s_whales = split_object_columns_if_present(q_whales) if q_whales is not None else None
    print("[bold]Step 3/4:[/] Merge on/off{} into single file with strict schema".format(" + whales" if s_whales is not None else ""))
    merge_on_off(s_on, s_off, s_whales, out_path=out_path)
    print(f"[green]OK[/] Merged file written to: {out_path}")

if __name__ == "__main__":
    main()
