# Autonomy Phases

- **Phase-0**: Pipeline wiring, forward-ceil quantization, strict merge, no-NaN guarantee, minimal CI (policy gate).
- **Phase-1**: Unit tests, calibration/backtest gates, attestation emit/verify, GitHub App scaffolding.
- **Next**:
  - Replace train/predict stubs with Bayesian LSTM (MC dropout, multi-horizon).
  - Implement queue+lease orchestrator & PR labels.
  - Add Observability-Audit artifacts and Governance log appender.
