# Core Protocol
This document encodes the critical project rules.

## Rules
- Use GPU (cuda/mps) when available.
- Always quantize timestamps to 1s UTC ms.
- Sliding window = 240 steps (2s cadence).
- Prediction horizons: 10,30,60,90,120,240 seconds.
- Bayesian LSTM with dropout uncertainty.
- Distributional Kelly criterion with costs/slippage.
- Never expose raw data or secrets to cloud AIs.

## Machine-readable section
policy_file: policies/rules.yml
