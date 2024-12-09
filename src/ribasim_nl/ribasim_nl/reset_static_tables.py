# %%
import numpy as np
import pandas as pd


def reset_static_tables(model):
    # basin / profile
    if model.basin.node.df is not None:
        df = pd.DataFrame(
            {
                "node_id": np.repeat(model.basin.node.df.index.to_numpy(), 2),
                "level": [0.0, 1.0] * len(model.basin.node.df),
                "area": [0.01, 1000.0] * len(model.basin.node.df),
            }
        )
        df.index.name = "fid"
        model.basin.profile.df = df

        # basin / state
        df = model.basin.profile.df.groupby("node_id")[["level"]].max().reset_index()
        df.index.name = "fid"
        model.basin.state.df = df

        # basin / static
        df = pd.DataFrame(
            {
                "node_id": model.basin.node.df.index.to_numpy(),
                "precipitation": [0] * model.basin.node.df.index.to_numpy(),
                "potential_evaporation": [0] * model.basin.node.df.index.to_numpy(),
                "drainage": [0] * model.basin.node.df.index.to_numpy(),
                "infiltration": [0] * model.basin.node.df.index.to_numpy(),
            }
        )
        df.index.name = "fid"
        model.basin.static.df = df

    # tabulated_rating_curves / static
    if model.tabulated_rating_curve.static.df is not None:
        df = pd.DataFrame(
            {
                "node_id": np.repeat(model.tabulated_rating_curve.node.df.index.to_numpy(), 2),
                "level": [0.0, 5] * len(model.tabulated_rating_curve.node.df),
                "flow_rate": [0, 0.1] * len(model.tabulated_rating_curve.node.df),
            }
        )
        df.index.name = "fid"
        model.tabulated_rating_curve.static.df = df

    # pump / static
    if model.pump.static.df is not None:
        df = pd.DataFrame(
            {
                "node_id": model.pump.node.df.index.to_numpy(),
                "flow_rate": [0.1] * len(model.pump.node.df),
            }
        )
        df.index.name = "fid"
        model.pump.static.df = df

    # outlet / static
    if model.outlet.static.df is not None:
        df = pd.DataFrame(
            {
                "node_id": model.outlet.node.df.index.to_numpy(),
                "flow_rate": [5.0] * len(model.outlet.node.df),
            }
        )
        df.index.name = "fid"
        model.outlet.static.df = df

    # flow_boundary / static
    if model.flow_boundary.static.df is not None:
        df = pd.DataFrame(
            {
                "node_id": model.flow_boundary.node.df.index.to_numpy(),
                "flow_rate": [0.1] * len(model.flow_boundary.node.df),
            }
        )
        df.index.name = "fid"
        model.flow_boundary.static.df = df

    # level_boundary / static
    if model.level_boundary.static.df is not None:
        df = pd.DataFrame(
            {
                "node_id": model.level_boundary.node.df.index.to_numpy(),
                "level": [0.1] * len(model.level_boundary.node.df),
            }
        )
        df.index.name = "fid"
        model.level_boundary.static.df = df

    # manning_resistance / static
    if model.manning_resistance.static.df is not None:
        df = pd.DataFrame(
            {
                "node_id": model.manning_resistance.node.df.index.to_numpy(),
                "length": [100] * len(model.manning_resistance.node.df),
                "manning_n": [0.04] * len(model.manning_resistance.node.df),
                "profile_width": [10] * len(model.manning_resistance.node.df),
                "profile_slope": [1] * len(model.manning_resistance.node.df),
            }
        )
        df.index.name = "fid"
        model.manning_resistance.static.df = df

    return model
