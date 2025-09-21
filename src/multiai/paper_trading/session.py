from __future__ import annotations

import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

from multiai.tools.kelly import kelly_optimal_fraction_gaussian
from multiai.tools.combiner import combine_allocations


def _ensure_datetime(series: pd.Series) -> pd.Series:
    ts = pd.to_datetime(series, utc=True)
    return ts.dt.tz_convert("UTC") if getattr(ts.dtype, "tz", None) else ts.dt.tz_localize("UTC")


def _detect_horizons(columns: Iterable[str]) -> List[int]:
    horizons: List[int] = []
    for col in columns:
        if col.startswith("pred_mu_h"):
            try:
                horizons.append(int(col.split("pred_mu_h", 1)[1]))
            except ValueError:
                continue
    horizons.sort()
    return horizons


def _norm_probabilities(mu: float, sigma: float) -> Tuple[float, float]:
    if not math.isfinite(mu) or not math.isfinite(sigma) or sigma <= 1e-12:
        return 0.5, 0.5
    z = (0.0 - mu) / (sigma * math.sqrt(2.0))
    prob_down = 0.5 * (1.0 + math.erf(z))
    prob_down = min(max(prob_down, 0.0), 1.0)
    prob_up = 1.0 - prob_down
    return prob_up, prob_down


@dataclass
class SessionConfig:
    duration_seconds: Optional[int] = 3600
    initial_capital: float = 100_000.0
    exposure_cap: float = 0.2
    hysteresis: float = 0.01
    stop_loss: float = 0.02
    take_profit: float = 0.04
    cost_bps_per_leg: float = 20.0


@dataclass
class SessionResult:
    log_path: str
    equity_path: str
    alerts_path: str
    session_start: Optional[pd.Timestamp]
    session_end: Optional[pd.Timestamp]
    rows: int

    def asdict(self) -> Dict[str, object]:
        return {
            "log_path": self.log_path,
            "equity_path": self.equity_path,
            "alerts_path": self.alerts_path,
            "session_start": self.session_start.isoformat() if self.session_start is not None else None,
            "session_end": self.session_end.isoformat() if self.session_end is not None else None,
            "rows": self.rows,
        }


class PaperTradingSession:
    def __init__(self, config: SessionConfig):
        self.config = config
        self.reset()

    def reset(self) -> None:
        self.cash = float(self.config.initial_capital)
        self.position_units = 0.0
        self.position_fraction = 0.0
        self.entry_price: Optional[float] = None
        self.session_start: Optional[pd.Timestamp] = None
        self.session_end: Optional[pd.Timestamp] = None
        self._alerts: List[Dict[str, object]] = []

    def _kelly_metrics(self, mu: float, sigma: float) -> Dict[str, float]:
        if not math.isfinite(mu) or not math.isfinite(sigma) or sigma <= 0.0:
            prob_up, prob_down = 0.5, 0.5
            return {
                "fraction": 0.0,
                "integral": 0.0,
                "gaussian_fraction": 0.0,
                "prob_up": prob_up,
                "prob_down": prob_down,
            }
        f_star, g_star, f_gauss = kelly_optimal_fraction_gaussian(
            mu,
            sigma,
            cost_bps_per_leg=float(self.config.cost_bps_per_leg),
            sl=float(self.config.stop_loss),
            tp=float(self.config.take_profit),
            f_cap=float(self.config.exposure_cap),
        )
        f_star = float(np.clip(f_star, -abs(self.config.exposure_cap), abs(self.config.exposure_cap)))
        prob_up, prob_down = _norm_probabilities(mu, sigma)
        return {
            "fraction": f_star,
            "integral": float(g_star),
            "gaussian_fraction": float(f_gauss),
            "prob_up": prob_up,
            "prob_down": prob_down,
        }

    def run(
        self,
        predictions: pd.DataFrame,
        market: Optional[pd.DataFrame] = None,
        price_col: str = "trade_price",
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if "timestamp" not in predictions.columns:
            raise ValueError("predictions frame requires 'timestamp'")
        preds = predictions.copy()
        preds["timestamp"] = _ensure_datetime(preds["timestamp"])
        preds = preds.sort_values("timestamp").reset_index(drop=True)
        horizons = _detect_horizons(preds.columns)
        if not horizons:
            raise ValueError("predictions frame missing pred_mu_h* columns")

        if market is not None:
            if "timestamp" not in market.columns:
                raise ValueError("market frame requires 'timestamp'")
            market_df = market.copy()
            market_df["timestamp"] = _ensure_datetime(market_df["timestamp"])
            if price_col not in market_df.columns:
                raise ValueError(f"market frame missing price column '{price_col}'")
            market_df = market_df.sort_values("timestamp")
            merged = pd.merge_asof(
                preds,
                market_df[["timestamp", price_col]],
                on="timestamp",
                direction="nearest",
            )
            merged = merged.rename(columns={price_col: "price"})
        else:
            if price_col not in preds.columns:
                raise ValueError(f"predictions frame missing price column '{price_col}'")
            merged = preds.rename(columns={price_col: "price"})

        limit = len(merged)
        if self.config.duration_seconds is not None:
            limit = min(limit, int(self.config.duration_seconds))
        if limit <= 0:
            return pd.DataFrame(), pd.DataFrame(columns=["timestamp", "type", "return"])

        logs: List[Dict[str, object]] = []
        cap = float(abs(self.config.exposure_cap))
        for idx in range(limit):
            row = merged.iloc[idx]
            ts = row["timestamp"]
            if not isinstance(ts, pd.Timestamp):
                ts = pd.Timestamp(ts, tz="UTC")
            price = float(row.get("price", np.nan))
            if not math.isfinite(price) or price <= 0.0:
                price = 0.0

            metrics_by_h: Dict[int, Dict[str, float]] = {}
            fractions: List[float] = []
            sigmas: List[float] = []
            for h in horizons:
                mu = float(row.get(f"pred_mu_h{h}", 0.0))
                sigma = float(row.get(f"pred_sigma_h{h}", 0.0))
                metrics = self._kelly_metrics(mu, sigma)
                metrics_by_h[h] = metrics
                fractions.append(metrics["fraction"])
                sigmas.append(max(abs(sigma), 1e-12))

            weighted = 0.0
            if fractions:
                weighted = combine_allocations(
                    np.array(fractions, dtype=float),
                    np.array(sigmas, dtype=float),
                    cap=cap,
                )

            target_fraction = float(weighted)
            raw_fraction = target_fraction
            target_fraction = float(np.clip(target_fraction, -cap, cap))

            if abs(target_fraction - self.position_fraction) < float(self.config.hysteresis):
                target_fraction = self.position_fraction

            if self.position_units != 0.0 and self.entry_price:
                direction = math.copysign(1.0, self.position_units)
                entry_price = max(self.entry_price, 1e-12)
                pnl = direction * ((price - entry_price) / entry_price)
                if pnl <= -float(self.config.stop_loss):
                    target_fraction = 0.0
                    self._alerts.append({"timestamp": ts, "type": "stop_loss", "return": pnl})
                elif pnl >= float(self.config.take_profit):
                    target_fraction = 0.0
                    self._alerts.append({"timestamp": ts, "type": "take_profit", "return": pnl})

            target_fraction = float(np.clip(target_fraction, -cap, cap))

            equity = self.cash + self.position_units * price
            desired_units = 0.0
            if price > 0.0 and equity > 0.0:
                desired_units = target_fraction * equity / price
            else:
                target_fraction = 0.0

            trade_units = desired_units - self.position_units
            if abs(trade_units) > 1e-9:
                self.cash -= trade_units * price
                self.position_units = desired_units
                if abs(self.position_units) < 1e-9:
                    self.entry_price = None
                else:
                    self.entry_price = price

            equity = self.cash + self.position_units * price
            position_notional = self.position_units * price
            actual_fraction = 0.0
            if equity > 1e-9:
                actual_fraction = position_notional / equity
            self.position_fraction = float(actual_fraction)

            log_row: Dict[str, object] = {
                "timestamp": ts,
                "price": price,
                "kelly_weighted": float(weighted),
                "allocation_fraction": target_fraction,
                "position_fraction": float(self.position_fraction),
                "cash": float(self.cash),
                "equity": float(equity),
                "position_notional": float(position_notional),
                "raw_fraction": float(raw_fraction),
            }

            for h in horizons:
                metrics = metrics_by_h[h]
                log_row[f"kelly_weight_h{h}"] = metrics["fraction"]
                log_row[f"kelly_integral_h{h}"] = metrics["integral"]
                log_row[f"kelly_gaussian_h{h}"] = metrics["gaussian_fraction"]
                log_row[f"prob_up_h{h}"] = metrics["prob_up"]
                log_row[f"prob_down_h{h}"] = metrics["prob_down"]

            logs.append(log_row)

            if self.session_start is None:
                self.session_start = ts
            self.session_end = ts

        log_df = pd.DataFrame(logs)
        alerts_df = pd.DataFrame(self._alerts)
        if alerts_df.empty:
            alerts_df = pd.DataFrame(
                {
                    "timestamp": pd.Series([], dtype="datetime64[ns, UTC]"),
                    "type": pd.Series([], dtype="string"),
                    "return": pd.Series([], dtype="float64"),
                }
            )
        else:
            alerts_df["timestamp"] = pd.to_datetime(alerts_df["timestamp"], utc=True)
            alerts_df["type"] = alerts_df["type"].astype("string")

        return log_df, alerts_df


def run(
    predictions_path: str,
    market_path: Optional[str] = None,
    out_dir: str = "outputs/paper_trading",
    price_col: str = "trade_price",
    config: Optional[SessionConfig] = None,
) -> SessionResult:
    cfg = config or SessionConfig()
    preds = pd.read_parquet(predictions_path)
    market_df = pd.read_parquet(market_path) if market_path else None
    session = PaperTradingSession(cfg)
    logs_df, alerts_df = session.run(preds, market_df, price_col=price_col)

    os.makedirs(out_dir, exist_ok=True)
    if logs_df.empty:
        timestamp_tag = datetime.now(timezone.utc)
    else:
        timestamp_tag = logs_df["timestamp"].iloc[0]
        if not isinstance(timestamp_tag, pd.Timestamp):
            timestamp_tag = pd.Timestamp(timestamp_tag, tz="UTC")
        timestamp_tag = timestamp_tag.tz_convert("UTC") if timestamp_tag.tzinfo else timestamp_tag.tz_localize("UTC")

    tag = timestamp_tag.strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(out_dir, f"paper_trading_{tag}.parquet")
    equity_path = os.path.join(out_dir, f"equity_curve_{tag}.parquet")
    alerts_path = os.path.join(out_dir, f"alerts_{tag}.parquet")

    logs_df.to_parquet(log_path, index=False)
    equity_df = logs_df[["timestamp", "equity"]].copy()
    equity_df.to_parquet(equity_path, index=False)
    alerts_df.to_parquet(alerts_path, index=False)

    return SessionResult(
        log_path=log_path,
        equity_path=equity_path,
        alerts_path=alerts_path,
        session_start=session.session_start,
        session_end=session.session_end,
        rows=len(logs_df),
    )
