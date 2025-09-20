import numpy as np
from typing import Tuple

def _grid_expect_log_growth(mu: float, sigma: float, f: float, cost_bps_roundtrip: float,
                            sl: float, tp: float, n_grid: int = 201) -> float:
    sigma = max(float(sigma), 1e-8)
    a = -abs(sl); b = abs(tp)
    xs = np.linspace(a, b, n_grid)
    pdf = np.exp(-0.5 * ((xs - mu)/sigma)**2) / (sigma * np.sqrt(2*np.pi))
    w = pdf / np.trapz(pdf, xs)
    costs = cost_bps_roundtrip / 10000.0
    r_net = xs - costs
    val = 1.0 + f * r_net
    if np.any(val <= 1e-12):
        return -1e9
    return float(np.sum(w * np.log(val)))

def kelly_optimal_fraction_gaussian(mu: float, sigma: float,
                                    cost_bps_per_leg: float = 20.0,
                                    sl: float = 0.02, tp: float = 0.02,
                                    f_cap: float = 1.0) -> Tuple[float, float, float]:
    if sigma <= 1e-12:
        return 0.0, 0.0, 0.0
    cost_roundtrip = 2.0 * cost_bps_per_leg
    f_gauss = float(mu / (sigma * sigma))
    phi = (1 + 5**0.5) / 2
    lo, hi = -abs(f_cap), abs(f_cap)
    c = hi - (hi - lo) / phi
    d = lo + (hi - lo) / phi
    def G(f): return _grid_expect_log_growth(mu, sigma, f, cost_roundtrip, sl, tp, n_grid=201)
    fc, fd = G(c), G(d)
    for _ in range(80):
        if fc < fd:
            lo = c; c, fc = d, fd; d = lo + (hi - lo) / phi; fd = G(d)
        else:
            hi = d; d, fd = c, fc; c = hi - (hi - lo) / phi; fc = G(c)
        if abs(hi - lo) < 1e-4:
            break
    f_star = (lo + hi) / 2.0
    G_star = G(f_star)
    return float(f_star), float(G_star), float(f_gauss)
