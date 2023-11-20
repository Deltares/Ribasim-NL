# %%
"""Test if ruff ignores this file in pre-commit"""
import pandas as pd

df = pd.DataFrame([1, 2, 3])

values = df[0].values  # should be deprecated as to_numpy() is the norm now

# %%
