from __future__ import annotations
import yaml, re, pathlib

def _read_yaml_block(md_path: pathlib.Path) -> dict:
    text = md_path.read_text(encoding='utf-8')
    m = re.search(r"```yaml\n(.*?)\n```", text, re.DOTALL)
    if not m:
        raise SystemExit("core_protocol.md missing YAML block")
    return yaml.safe_load(m.group(1))

def run_protocol_audit() -> None:
    md = pathlib.Path(__file__).resolve().parents[2] / 'core_protocol.md'
    cfg = _read_yaml_block(md)
    # Minimal static assertions
    assert cfg.get('system_heartbeat_seconds') == 1, "Heartbeat must be 1s"
    horizons = cfg.get('horizons_seconds', [])
    assert horizons == [10,30,60,90,120,240], "Horizon set must be [10,30,60,90,120,240]"
    assert cfg.get('object_columns_split') is True, "Object column split must be enabled"
    assert cfg.get('merge_requires_no_nans') is True, "Merge must not contain NaNs"
    assert cfg.get('timestamp_rounding') == 'forward_ceiling_to_second', "Rounding must be forward to 1s"
    assert cfg.get('timestamp_strictly_increasing') is True, "Timestamps must increase"
