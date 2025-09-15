# multi-ai-model

Clean, minimal scaffold that **runs today**:

- `multiai run dataops` → quantize → split object columns → merge on/off into a single, causal 1‑second file.
- Governance stubs: Protocol Auditor (real checks for timing & schema), Llama Guard + GPT Math Validate (stubs).
- CI workflow with required checks.

## Install (editable)

```bash
pip install -e .
```

## Usage

```bash
multiai run dataops --on on_chain.parquet --off off_chain.parquet --out merged_15092025.parquet
```

## Object columns

We expect (if present) these list-like columns to be split to 9 scalar columns and originals dropped:
`orderbook_bid`, `orderbook_ask`, `mid_prices`, `spreads`.
