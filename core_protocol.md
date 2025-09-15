# Core Protocol

This file encodes the non-negotiable rules the system must obey.

```yaml
system_heartbeat_seconds: 1
horizons_seconds: [10, 30, 60, 90, 120, 240]
rotation_minutes: 90
object_columns_split: true
merge_requires_no_nans: true
timestamp_rounding: forward_ceiling_to_second
timestamp_strictly_increasing: true
single_timestamp_column_name: timestamp
```

- Timestamps must be snapped **forward** to the next full second and deduped keeping the **latest** original row within each second.
- After split, no list-like object columns may remain.
- On/Off-chain must merge to a single file with **no empty cells** and a **single** `timestamp` column.
