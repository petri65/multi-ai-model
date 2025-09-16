# Core Protocol

```yaml
system_heartbeat_seconds: 1
horizons_seconds: [10, 30, 60, 90, 120, 240]
rotation_minutes: 90
object_columns_split: true
merge_requires_no_nans: true
timestamp_rounding: forward_ceiling_to_second
timestamp_strictly_increasing: true
single_timestamp_column_name: timestamp
model_type_allowed: BayesianLSTM
kelly_min_costs_bps: 20
```

The system must snap timestamps forward to the next full second, keep the latest record per second, split list-like object columns into 9 scalar columns, and merge on/off-chain to a single strictly-causal, 1s cadence dataset without empty cells. Predictions must expose 6 horizons and Kelly sizing must be distributional with costs >= 20 bps and available-capital validation.
