"""Helpers to derive node functions and target levels for coupling tables."""


def classify_node_by_control_name(model, node_id: int) -> str | None:
    """Classify a node from the name of its connected control node.

    Parameters
    ----------
    model : ribasim.Model
        Ribasim model containing node and link tables.
    node_id : int
        Node identifier of the controlled node.

    Returns
    -------
    str or None
        Returns ``"inlaat"``, ``"uitlaat"``, or ``"doorlaat"`` when one of
        these labels is found in the connected control-node name. Returns
        ``None`` when no connected control-node is present or when the control
        name does not contain a recognized function.
    """
    control_links = model.link.df[(model.link.df["link_type"] == "control") & (model.link.df["to_node_id"] == node_id)]

    if control_links.empty:
        return None

    control_node_ids = control_links["from_node_id"].tolist()
    control_names = model.node.df.loc[control_node_ids, "name"].dropna().astype(str).str.lower()

    for name in control_names:
        if "inlaat" in name:
            return "inlaat"
        if "uitlaat" in name:
            return "uitlaat"
        if "doorlaat" in name:
            return "doorlaat"

    return None


def get_node_function(model, node_id: int) -> str | None:
    """Return the function label for a node.

    Parameters
    ----------
    model : ribasim.Model
        Ribasim model containing the node and link tables.
    node_id : int
        Node identifier to classify.

    Returns
    -------
    str or None
        Returns ``None`` when the node identifier is not present in the model.
        Returns ``"Basin"`` or ``"LevelBoundary"`` for those node types.
        For other nodes, returns the function inferred from a connected
        control-node name when available. If no control-based function is
        found, the node type itself is returned.
    """
    if node_id not in model.node.df.index:
        return None

    node_type = model.node.df.at[node_id, "node_type"]
    if node_type in ["Basin", "LevelBoundary"]:
        return node_type

    node_function = classify_node_by_control_name(model, node_id)
    if node_function is None:
        return node_type

    return node_function
