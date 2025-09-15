import click
from rich import print
from multiai.dataops.quantize import quantize_to_1s
from multiai.dataops.split_object_columns import split_object_columns_if_present
from multiai.dataops.merge_on_off import merge_on_off
from multiai.tools.protocol_auditor import run_protocol_audit

@click.group()
def main():
    pass

@main.group()
def run():
    pass

@run.command("dataops")
@click.option("--on", "on_path", required=True, type=click.Path(exists=True), help="On-chain parquet")
@click.option("--off", "off_path", required=True, type=click.Path(exists=True), help="Off-chain parquet")
@click.option("--out", "out_path", required=True, type=click.Path(), help="Output merged parquet")
def dataops(on_path, off_path, out_path):
    """Quantize → split → merge → audit."""
    print("[bold]Step 1/4:[/] Quantize to 1s (forward snap + dedupe)")
    q_on = quantize_to_1s(on_path)
    q_off = quantize_to_1s(off_path)
    print("[bold]Step 2/4:[/] Split list/object columns (if present)")
    s_on = split_object_columns_if_present(q_on)
    s_off = split_object_columns_if_present(q_off)
    print("[bold]Step 3/4:[/] Merge on/off into single file with strict schema")
    merge_on_off(s_on, s_off, out_path)
    print("[bold]Step 4/4:[/] Protocol audit checks")
    run_protocol_audit()
    print(f"[green]OK[/] Merged file written to: {out_path}")
