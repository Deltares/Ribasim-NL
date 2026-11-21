"""Keys of nodes: identifiers that follow from the source data, by which code refers to nodes instead of by node ID.

A key is unique in the model of a water authority, and stays the same when the node IDs change: with another library
version, or when nodes are added or removed elsewhere in the model. In the coupled model a node is identified by its
authority, `meta_waterbeheerder`, and its key. See docs/dev/node-keys.qmd.

The models that are built from source data set the keys of their nodes: `lumped_keys` for the gravity controlled
input models, `ribasim_nl.crossings.model` for the level controlled ones. A script that adds a node can give it a key,
`Node(..., meta_key="...")`. `fill_keys` gives all other nodes a key when the model is written, see `Model.write`.
"""

from typing import TYPE_CHECKING, Any, NamedTuple

import geopandas as gpd
import numpy as np
import pandas as pd

from ribasim_nl.case_conversions import pascal_to_snake_case

if TYPE_CHECKING:
    from ribasim_nl.model import Model

# The node column with the key of a node
KEY_COLUMN = "meta_key"
# The node column with the water authority of a node, which is part of its identity in the coupled model
AUTHORITY_COLUMN = "meta_waterbeheerder"
# The arguments of the edit methods of the model that refer to a node, see `Model.apply_edits`
NODE_ARGUMENTS = {"node_id", "to_node_id", "from_node_id", "basin_id", "from_basin_id", "to_basin_id"}

CONNECTORS = ["Outlet", "Pump", "TabulatedRatingCurve", "ManningResistance", "LinearResistance"]
BOUNDARIES = ["LevelBoundary", "FlowBoundary", "Terminal"]
# Node types that are named after the nodes they control
CONTROLLERS = ["DiscreteControl", "ContinuousControl", "PidControl", "FlowDemand", "LevelDemand"]
STORING = "bergend"
# Between the water authority and the key of a node, where equal keys are numbered, see `fill_keys`
_SCOPE_SEPARATOR = "\x1f"


class NodeKey(NamedTuple):
    """A node in a model of several water authorities: its authority and its key."""

    authority: str
    key: str


def resolve_keys(model: "Model", value: Any) -> Any:
    """Replace every `NodeKey` in a structure of dicts, lists, tuples and sets by the ID of its node in the model.

    The key of a water authority that is not in the model gives None, so a model of some of the authorities can use
    the same configuration: items of sets and entries of dicts whose key is None are left out. A key that no node of
    its authority has raises a KeyError.
    """
    if isinstance(value, NodeKey):
        if value.authority not in set(model.node.df[AUTHORITY_COLUMN].dropna().unique()):
            return None
        return model.node_id(value.key, authority=value.authority)
    if isinstance(value, dict):
        resolved = ((resolve_keys(model, k), resolve_keys(model, v)) for k, v in value.items())
        return {k: v for k, v in resolved if k is not None}
    if isinstance(value, (set, frozenset)):
        return type(value)(i for i in (resolve_keys(model, item) for item in value) if i is not None)
    if isinstance(value, (list, tuple)) and not isinstance(value, NodeKey):
        return type(value)(resolve_keys(model, item) for item in value)
    return value


def _authority_scope(node: pd.DataFrame) -> pd.Series:
    """The water authority of each node, within which its key is unique.

    Empty for all nodes of a model of one water authority, in which only some of the nodes have their authority set.
    """
    scope = pd.Series("", index=node.index, dtype=object)
    if AUTHORITY_COLUMN in node.columns and len(node[AUTHORITY_COLUMN].dropna().unique()) > 1:
        scope = node[AUTHORITY_COLUMN].astype(object).where(node[AUTHORITY_COLUMN].notna(), "")
    return scope.astype(str)


def nonempty(values: pd.Series) -> pd.Series:
    """Whether the values are not missing and not an empty string."""
    text = values.astype(object).astype(str).str.strip()
    return values.notna() & ~text.isin(["", "nan", "None", "<NA>"])


def _location_order(geometry: gpd.GeoSeries) -> pd.Series:
    """The rank of each point by x, then y: an order that follows from the data, not from the node IDs."""
    order = np.lexsort((geometry.y.to_numpy(), geometry.x.to_numpy()))
    rank = np.empty(len(order), dtype=int)
    rank[order] = np.arange(len(order))
    return pd.Series(rank, index=geometry.index)


def number_duplicates(keys: pd.Series, order: pd.Series, taken: pd.Series | None = None) -> pd.Series:
    """Make keys unique by adding `@<n>` to equal keys, numbered by `order`.

    :param keys: the new keys, missing where a node gets none
    :param order: the order in which equal keys are numbered
    :param taken: keys that other nodes already have. A new key that is taken is numbered after them.
    """
    keys = keys.copy()
    new = keys.dropna()
    if new.empty:
        return keys
    first_number = pd.Series(1, index=new.index)
    is_taken = pd.Series(False, index=new.index)
    if taken is not None and not taken.dropna().empty:
        taken = taken.dropna().astype(str)
        base = taken.str.replace(r"@\d+$", "", regex=True)
        number = pd.to_numeric(taken.str.extract(r"@(\d+)$")[0], errors="coerce").fillna(1).astype(int)
        highest = number.groupby(base.to_numpy()).max()
        is_taken = new.isin(highest.index)
        first_number = new.map(highest).fillna(0).astype(int) + 1
    numbered = new.duplicated(keep=False) | is_taken
    rank = order[new.index[numbered]].groupby(new[numbered].to_numpy()).rank(method="first").astype(int) - 1
    keys[rank.index] = new[rank.index] + "@" + (first_number[rank.index] + rank).astype(str)
    return keys


def lumped_keys(
    node: gpd.GeoDataFrame, link: pd.DataFrame, hydroobject: gpd.GeoDataFrame | None
) -> tuple[pd.Series, pd.Series]:
    """Return the key and the kind of key per node ID, for the input model of a gravity controlled water authority.

    - A connector node on a structure gets the code of the structure, the `name` of the input model.
    - A connector node on open water gets the code of the hydroobject it is on, `ow:<code>`, or else its location in
      metres, `loc:<x>,<y>`.
    - A boundary gets its ID in the boundaries layer, `bnd:<boundary_id>`.
    - A Basin is named after the connector node it drains through, `up:<key>`, the smallest key if it has several, or
      else after the connector node it is fed by, `down:<key>`.

    Equal keys get `@1`, `@2`: along their hydroobject for the nodes on open water, by location for the others.

    :param node: the node table, by node ID
    :param link: the link table
    :param hydroobject: the hydroobjects of the HyDAMO data, with their `code`, if the model is lumped from them
    """
    key = pd.Series(pd.NA, index=node.index, dtype=object)
    kind = pd.Series(pd.NA, index=node.index, dtype=object)

    connector = node["node_type"].isin(CONNECTORS)
    named = connector & nonempty(node["name"])
    key[named] = node.loc[named, "name"].astype(object).astype(str).str.strip()
    kind[named] = "structure"

    open_water = connector & ~named
    if hydroobject is not None and open_water.any():
        code_column = "code" if "code" in hydroobject.columns else "CODE"
        lines = hydroobject[[code_column, "geometry"]].rename(columns={code_column: "code"})
        lines = lines[nonempty(lines["code"])]
        joined = gpd.sjoin_nearest(node.loc[open_water, ["geometry"]], lines, max_distance=1.0, distance_col="distance")
        # the nearest hydroobject, the smallest code if two are equally near
        joined = joined.sort_values(["distance", "code"], kind="stable")
        joined = joined[~joined.index.duplicated(keep="first")].sort_index(kind="stable")
        line_geometry = lines.geometry.loc[joined["index_right"]].to_numpy()
        along = pd.Series(
            [line.project(point) for line, point in zip(line_geometry, node.geometry.loc[joined.index], strict=True)],
            index=joined.index,
        )
        on_hydroobject = number_duplicates("ow:" + joined["code"].astype(object).astype(str), along)
        key[on_hydroobject.index] = on_hydroobject
        kind[on_hydroobject.index] = "hydroobject"
    rest = open_water & key.isna()
    key[rest] = location_keys(node.geometry[rest])
    kind[rest] = "location"

    boundary = node["node_type"].isin(BOUNDARIES)
    key[boundary] = "bnd:" + node.loc[boundary, "meta_id"].astype(int).astype(str)
    kind[boundary] = "boundary"

    key = number_duplicates(key, _location_order(node.geometry))

    flow = link[link["link_type"] == "flow"]
    basin_ids = node.index[node["node_type"] == "Basin"]
    for prefix, basin_column, connector_column, label in [
        ("down:", "to_node_id", "from_node_id", "downstream of connector"),
        ("up:", "from_node_id", "to_node_id", "upstream of connector"),
    ]:
        pairs = flow[flow[basin_column].isin(basin_ids)]
        pairs = pairs.assign(key=pairs[connector_column].map(key)).dropna(subset="key")
        first = pairs.sort_values("key", kind="stable").drop_duplicates(basin_column).set_index(basin_column)["key"]
        key[first.index] = prefix + first
        kind[first.index] = label
    return key, kind


def named_keys(node: gpd.GeoDataFrame, link: pd.DataFrame) -> pd.Series:
    """Return the key per node ID for a model that is built from named objects, such as that of Rijkswaterstaat.

    - A connector node gets the code of its structure, `meta_code_waterbeheerder`, or else its name.
    - A Basin gets its name, the name of its water.
    - A boundary gets `bnd:` and the code of its measuring location, `meta_meetlocatie_code`, or else its name.
    - A connector node without code or name gets the keys of the nodes it connects, `<from>><to>`.

    Equal keys get `@1`, `@2` by location. Nodes without any of these get no key here, see `fill_keys`.
    """
    key = pd.Series(pd.NA, index=node.index, dtype=object)
    order = _location_order(node.geometry)

    def column(name: str) -> pd.Series:
        values = node[name] if name in node.columns else pd.Series(pd.NA, index=node.index, dtype=object)
        return values.astype(object).where(nonempty(values), pd.NA)

    name = column("name")
    connector = node["node_type"].isin(CONNECTORS)
    code = column("meta_code_waterbeheerder")
    key[connector] = code[connector].where(code[connector].notna(), name[connector])
    basin = node["node_type"] == "Basin"
    key[basin] = name[basin]
    boundary = node["node_type"].isin(BOUNDARIES)
    location = column("meta_meetlocatie_code")
    key[boundary] = "bnd:" + location[boundary].where(location[boundary].notna(), name[boundary]).astype(object)
    key = number_duplicates(key.map(lambda value: value.strip() if isinstance(value, str) else value), order)

    # the other connector nodes, after the nodes they connect
    flow = link[link["link_type"] == "flow"]
    unnamed = node.index[connector & key.isna()]
    upstream = flow[flow["to_node_id"].isin(unnamed)].drop_duplicates("to_node_id").set_index("to_node_id")
    downstream = flow[flow["from_node_id"].isin(unnamed)].drop_duplicates("from_node_id").set_index("from_node_id")
    between = upstream["from_node_id"].map(key) + ">" + downstream["to_node_id"].map(key).reindex(upstream.index)
    between = number_duplicates(between.dropna().astype(object), order, taken=key)
    key[between.index] = between
    return key


def location_keys(geometry: pd.Series) -> list[str]:
    """Keys of nodes that no source object names: their location in whole metres, `loc:<x>,<y>`."""
    return [f"loc:{round(point.x)},{round(point.y)}" for point in geometry]


def _junction_keys(node: gpd.GeoDataFrame, link: pd.DataFrame, key: pd.Series) -> pd.Series:
    """Keys of the Junctions, after the nodes that their trees connect.

    `junction:<from>><to>#<n>x<m>`: the smallest key of the n nodes that flow into the Junction, through other
    Junctions or not, and the smallest of the m nodes it flows to.
    """
    junctions = node.index[node["node_type"] == "Junction"]
    if junctions.empty:
        return pd.Series(dtype=object)
    is_junction = set(junctions)
    flow = link[link["link_type"] == "flow"]
    upstream: dict[int, list[int]] = {}
    downstream: dict[int, list[int]] = {}
    for from_node_id, to_node_id in zip(flow["from_node_id"].to_numpy(), flow["to_node_id"].to_numpy(), strict=True):
        downstream.setdefault(int(from_node_id), []).append(int(to_node_id))
        upstream.setdefault(int(to_node_id), []).append(int(from_node_id))

    def ends(junction: int, neighbours: dict[int, list[int]]) -> set[str]:
        """The keys of the nodes that are reached from a Junction through Junctions only."""
        found: set[str] = set()
        seen = {junction}
        stack = [junction]
        while stack:
            for other in neighbours.get(stack.pop(), []):
                if other in seen:
                    continue
                seen.add(other)
                if other in is_junction:
                    stack.append(other)
                elif pd.notna(key.get(other)):
                    found.add(str(key[other]))
        return found

    keys = {}
    for junction in junctions:
        sources, targets = ends(int(junction), upstream), ends(int(junction), downstream)
        keys[junction] = f"junction:{min(sources, default='')}>{min(targets, default='')}#{len(sources)}x{len(targets)}"
    return pd.Series(keys, dtype=object)


def fill_keys(model: "Model") -> None:
    """Give the nodes without a key one, from their own properties or from the nodes they belong to.

    Keys that nodes already have stay as they are, but for those of the Junctions, which always follow from the nodes
    around them, since their trees are rebuilt along the way.

    - A connector node with a `meta_code_waterbeheerder` gets that code.
    - An RWZI gets `rwzi:<meta_rwzi_code>`, an Observation `observation:<name>`, a UserDemand `user_demand:<name>`.
    - A storing (bergend) Basin gets `bergend:<key>` of the Basin it stores for, and the node between them
      `bergend_link:<key>`.
    - A control or demand node gets its node type and the smallest key of the nodes it controls,
      `discrete_control:<key>`.
    - A Junction gets `junction:<from>><to>#<n>x<m>`, see `_junction_keys`.
    - Any other node gets its location in whole metres, `loc:<x>,<y>`.

    Equal keys are numbered `@1`, `@2` by location, after the numbers that are taken.
    """
    node = model.node.df
    if KEY_COLUMN not in node.columns:
        return
    key = node[KEY_COLUMN].astype(object).where(nonempty(node[KEY_COLUMN]), pd.NA)
    node_type = node["node_type"]
    junction = node_type == "Junction"
    key[junction] = pd.NA
    order = _location_order(node.geometry)

    # keys are unique per water authority, so equal keys are numbered within the nodes of an authority
    scope = _authority_scope(node) + _SCOPE_SEPARATOR

    def assign(new: pd.Series) -> None:
        """Set the new keys of nodes that have none, numbering the equal ones."""
        new = new[key[new.index].isna() & new.notna()]
        if new.empty:
            return
        has_key = key.notna()
        taken = scope[has_key] + key[has_key].astype(str)
        numbered = number_duplicates((scope[new.index] + new.astype(str)).astype(object), order, taken=taken)
        key[numbered.index] = numbered.str.split(_SCOPE_SEPARATOR, n=1).str[1]

    def column(name: str) -> pd.Series:
        return node[name] if name in node.columns else pd.Series(pd.NA, index=node.index, dtype=object)

    link = model.link.df
    flow = link[link["link_type"] == "flow"]
    control = link[link["link_type"] == "control"]

    # storing Basins, the node that connects each to the Basin it stores for, and that Basin
    storing = (node_type == "Basin") & (column("meta_categorie") == STORING)
    storing_ids = set(node.index[storing])
    is_basin = node_type == "Basin"
    neighbour = pd.concat(
        [
            flow[["from_node_id", "to_node_id"]].set_axis(["node_id", "other"], axis=1),
            flow[["to_node_id", "from_node_id"]].set_axis(["node_id", "other"], axis=1),
        ]
    )
    storing_link = neighbour[neighbour["node_id"].isin(storing_ids)].drop_duplicates("node_id")
    storing_link = storing_link[~is_basin.reindex(storing_link["other"]).to_numpy()]
    beyond = neighbour[neighbour["node_id"].isin(storing_link["other"]) & ~neighbour["other"].isin(storing_ids)]
    beyond = beyond.drop_duplicates("node_id").set_index("node_id")["other"]
    # per storing Basin: its connector node and the Basin it stores for
    storing_link = storing_link.assign(parent=storing_link["other"].map(beyond)).dropna(subset="parent")
    storing_connectors = set(storing_link["other"])

    # nodes with an identifier of their own
    connector = node_type.isin(CONNECTORS) & ~node.index.isin(storing_connectors)
    code = column("meta_code_waterbeheerder")
    assign(code[connector & nonempty(code)].astype(object).astype(str).str.strip())
    rwzi = column("meta_rwzi_code")
    has_rwzi = nonempty(rwzi) & (node_type == "FlowBoundary")
    assign("rwzi:" + rwzi[has_rwzi].astype(object).astype(str).str.replace(r"\.0$", "", regex=True))
    for kind in ("Observation", "UserDemand"):
        named = (node_type == kind) & nonempty(node["name"])
        assign(f"{pascal_to_snake_case(kind)}:" + node.loc[named, "name"].astype(object).astype(str))

    # the other nodes that stand on their own, by location
    derived = node_type.isin(CONTROLLERS) | junction | storing | node.index.isin(storing_connectors)
    alone = key.isna() & ~derived
    assign(pd.Series(location_keys(node.geometry[alone]), index=node.index[alone], dtype=object))

    # storing Basins and their connector nodes, after the Basin they store for
    parent_key = storing_link["parent"].map(key)
    assign(pd.Series(("bergend:" + parent_key).to_numpy(), index=storing_link["node_id"].to_numpy(), dtype=object))
    assign(pd.Series(("bergend_link:" + parent_key).to_numpy(), index=storing_link["other"].to_numpy(), dtype=object))

    # control and demand nodes, after the nodes they control
    controller = node_type.isin(CONTROLLERS) & key.isna()
    controlled = control[control["from_node_id"].isin(node.index[controller])]
    controlled = controlled.assign(key=controlled["to_node_id"].map(key)).dropna(subset="key")
    first = controlled.sort_values("key", kind="stable").drop_duplicates("from_node_id").set_index("from_node_id")
    prefix = node_type.reindex(first.index).map(pascal_to_snake_case)
    assign((prefix + ":" + first["key"]).astype(object))

    # what is left, by location, and last the Junctions, which are named after all the others
    left = key.isna() & ~junction
    assign(pd.Series(location_keys(node.geometry[left]), index=node.index[left], dtype=object))
    assign(_junction_keys(node, link, key))

    model.node.df[KEY_COLUMN] = key.astype("str")


def duplicate_key_issues(model: "Model") -> list[str]:
    """The nodes of a water authority that have the same key, for `ribasim_nl.validation.validate_model`."""
    node = model.node.df
    if KEY_COLUMN not in node.columns:
        return []
    keys = node.loc[nonempty(node[KEY_COLUMN]), KEY_COLUMN]
    duplicated = pd.concat([_authority_scope(node)[keys.index], keys], axis=1).duplicated(keep=False)
    if not duplicated.any():
        return []
    examples = keys[duplicated].sort_values(kind="stable").head(20).to_dict()
    return [f"{int(duplicated.sum())} nodes share their key with another node: {examples}"]


def validate_keys(model: "Model") -> None:
    """Check that every node has a key, and that no two nodes of a water authority have the same key."""
    keys = model.node.df[KEY_COLUMN]
    missing = keys.index[~nonempty(keys)].tolist()
    if missing:
        raise ValueError(f"{len(missing)} nodes without a key: {missing[:20]}")
    issues = duplicate_key_issues(model)
    if issues:
        raise ValueError(issues[0])
