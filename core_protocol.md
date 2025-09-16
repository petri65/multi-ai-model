# Core Protocol (excerpts, v0)
Absolute requirements: modularity cap ≤ 600 LoC/module; real data only; UTC ms timestamps; only Bayesian LSTM (MC Dropout/variational); GPU for training/inference. 
System timing: heartbeat 1s; window 240; horizons [10,30,60,90,120,240]; raw 200ms aligned to 1s by round-up and keep-latest in bucket.
Merging: one row per second; list/object columns split before model input; schema validated for orderbook L1–L10, spreads, mid-prices, whales, mempool stats.
Predictions & Kelly: prediction_10s … prediction_240s on each row; distributional Kelly with ≥20 bps costs and available-capital validation.

Pre-Commit Audit Checklist (machine rules live in policies/rules.yml and must pass). 
