import pandas as pd

from ribasim_nl import Model


def add_from_to_nodes_and_levels(model: Model, node_types=["outlet", "pump"]):
    """Add from and to nodes and levels

    Args:
        model (Model): ribasim-nl Model
    """
    for node_type in node_types:
        ribasim_node = getattr(model, node_type)
        meta_properties_df = pd.DataFrame(
            [(i, model.upstream_node_id(i), model.downstream_node_id(i)) for i in ribasim_node.node.df.index],
            columns=["node_id", "meta_from_node_id", "meta_to_node_id"],
        )

        for relative_side in ["from", "to"]:
            meta_properties_df[f"meta_{relative_side}_level"] = [
                model.basin.area.df.set_index("node_id").at[i, "meta_streefpeil"]
                if i in model.basin.node.df.index
                else model.level_boundary.static.df.set_index("node_id").at[i, "level"]
                for i in meta_properties_df[f"meta_{relative_side}_node_id"]
            ]

        ribasim_node.static.df = pd.merge(ribasim_node.static.df, meta_properties_df, on="node_id", how="inner")
