import numpy as np
def combine_allocations(f_by_h, sigma_by_h, cap=0.2, eps=1e-12):
    sig2 = np.maximum(sigma_by_h**2, eps)
    w = 1.0 / sig2
    w = w / np.maximum(w.sum(), eps)
    f = (w * f_by_h).sum()
    return float(np.clip(f, -abs(cap), abs(cap)))
