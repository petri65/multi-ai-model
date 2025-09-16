import click, pandas as pd
from pathlib import Path
from multiai.dataops.quantize_1s import quantize_to_1s
from multiai.dataops.split_lists import explode_lists
from multiai.dataops.merge_on_offchain import merge_quantized

@click.group()
def run(): ...

@run.command("quantize-on")
@click.option("--in-path", required=True)
@click.option("--out-path", required=True)
def quantize_on(in_path, out_path):
    quantize_to_1s(in_path).to_parquet(out_path, index=False)

@run.command("quantize-off")
@click.option("--in-path", required=True)
@click.option("--out-path", required=True)
def quantize_off(in_path, out_path):
    quantize_to_1s(in_path).to_parquet(out_path, index=False)

@run.command("split-lists")
@click.option("--in-path", required=True)
@click.option("--out-path", required=True)
def split_lists(in_path, out_path):
    df = pd.read_parquet(in_path)
    explode_lists(df).to_parquet(out_path, index=False)

@run.command("merge")
@click.option("--on", "on_path", required=True)
@click.option("--off", "off_path", required=True)
@click.option("--out", "out_path", required=True)
def merge(on_path, off_path, out_path):
    df = merge_quantized(on_path, off_path)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)

if __name__ == "__main__":
    run()
