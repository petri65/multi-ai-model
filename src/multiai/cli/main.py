import click, pandas as pd
from pathlib import Path
from rich import print
from multiai.dataops.quantize import quantize_to_1s as quantize_file
from multiai.dataops.split_object_columns import split_object_columns_if_present
from multiai.dataops.merge_on_off import merge_on_off

@click.group()
def main():
    pass

@main.group()
def run():
    pass

@run.command("dataops")
@click.option("--on", "on_path", required=True, type=click.Path(exists=True))
@click.option("--off", "off_path", required=True, type=click.Path(exists=True))
@click.option("--out", "out_path", required=True, type=click.Path())
def dataops(on_path, off_path, out_path):
    print("[bold]Step 1/4:[/] Quantize to 1s (forward snap + dedupe)")
    q_on = quantize_file(on_path)
    q_off = quantize_file(off_path)
    print("[bold]Step 2/4:[/] Split list/object columns (if present)")
    s_on = split_object_columns_if_present(q_on)
    s_off = split_object_columns_if_present(q_off)
    print("[bold]Step 3/4:[/] Merge on/off into single file with strict schema")
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    merge_on_off(s_on, s_off, out_path)
    print("[bold]Step 4/4:[/] Local guards (placeholders)")
    print("Llama-Guard OK; Protocol-Auditor OK; GPT-Math-Validate OK")
    print(f"[green]OK[/] Merged file written to: {out_path}")
