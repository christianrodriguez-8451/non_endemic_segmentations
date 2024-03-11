from __future__ import annotations
from typing import List, Dict
from itertools import product
import numpy as np
import sklearn as skl

def grid_search(search_space: Dict[str, list]) -> List[dict]:
    """Get all combinations of hyperparameters."""
    combos = product(*search_space.values())
    return [{k: v for k, v in zip(search_space.keys(), vals)}
            for vals in combos]


def generate_unique_id(tag_name: str, params: dict) -> str:
    params_alpha = sorted(params.keys())
    return '_'.join([tag_name, *[str(params[param]) for 
                                 param in params_alpha]])


def get_approximate_trustworthiness(X: np.ndarray, 
                                    X_emb: np.ndarray,
                                    n_samples: int = 10000
                                    ) -> float:
    # Randomly sample
    samples = np.random.choice(X.shape[0], n_samples, replace=False)
    X_small = X[samples]
    X_emb_small = X_emb[samples]

    return float(skl.manifold.trustworthiness(X_small, X_emb_small))