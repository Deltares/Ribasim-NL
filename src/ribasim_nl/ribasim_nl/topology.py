from collections.abc import Iterable
from typing import Any, cast

from ribasim import Node
from ribasim.nodes import level_boundary

from ribasim_nl.model import Model


def reverse_existing_links(model: Model, link_ids: Iterable[int]) -> None:
    """Reverse only links that are present in the model."""
    assert model.link.df is not None
    link_df = cast(Any, model.link.df)

    for link_id in link_ids:
        if link_id in link_df.index:
            model.reverse_link(link_id=link_id)


def fill_missing_level_boundary_static_levels(model: Model, default_level: float = 0.0) -> None:
    """Fill missing LevelBoundary static levels before validating/writing a model."""
    assert model.level_boundary.static.df is not None
    level_boundary_static_df = cast(Any, model.level_boundary.static.df)

    missing_level_mask = level_boundary_static_df["level"].isna()
    level_boundary_static_df.loc[missing_level_mask, "level"] = default_level


def duplicate_level_boundary_for_link(
    model: Model,
    source_node_id: int,
    link_id: int,
    default_level: float = 0.0,
    name_suffix: str = "extra boundary",
    strict: bool = False,
) -> int | None:
    """Give a link its own copy of an existing LevelBoundary node.

    If the link is already connected to another LevelBoundary, that node_id is
    returned. This makes repeated notebook runs idempotent.
    """
    assert model.link.df is not None
    assert cast(Any, model.level_boundary.node).df is not None
    assert model.level_boundary.static.df is not None
    assert model.node.df is not None
    link_df = cast(Any, model.link.df)
    level_boundary_node_df = cast(Any, model.level_boundary.node).df
    level_boundary_static_df = cast(Any, model.level_boundary.static.df)
    node_df = cast(Any, model.node.df)

    if link_id not in link_df.index:
        if strict:
            raise KeyError(f"Link {link_id} bestaat niet in dit model.")
        return None

    from_node_id = int(cast(Any, link_df.at[link_id, "from_node_id"]))
    to_node_id = int(cast(Any, link_df.at[link_id, "to_node_id"]))
    link_node_ids = {from_node_id, to_node_id}

    if source_node_id not in link_node_ids:
        existing_boundary_node_ids = [
            node_id for node_id in [from_node_id, to_node_id] if node_id in level_boundary_node_df.index
        ]
        if existing_boundary_node_ids and not strict:
            return existing_boundary_node_ids[0]
        raise ValueError(f"Link {link_id} is niet verbonden met level boundary {source_node_id}.")

    if source_node_id not in level_boundary_node_df.index:
        raise ValueError(f"Node {source_node_id} is geen level boundary in dit model.")

    source_node = model.level_boundary[source_node_id]
    source_name = node_df.at[source_node_id, "name"] if "name" in node_df.columns else ""
    if not isinstance(source_name, str):
        source_name = ""
    new_name = f"{source_name} {name_suffix}".strip()

    boundary_node = model.level_boundary.add(
        Node(geometry=source_node.geometry, name=new_name),
        tables=[level_boundary.Static(level=[default_level])],
    )
    new_node_id = boundary_node.node_id

    node_columns = [column for column in node_df.columns if column not in {"node_type", "name", "geometry"}]
    node_df.loc[new_node_id, node_columns] = node_df.loc[source_node_id, node_columns]

    source_static_mask = level_boundary_static_df.node_id == source_node_id
    if source_static_mask.any():
        new_static_mask = level_boundary_static_df.node_id == new_node_id
        static_columns = [column for column in level_boundary_static_df.columns if column != "node_id"]
        source_static = level_boundary_static_df.loc[source_static_mask, static_columns].iloc[0].dropna()
        level_boundary_static_df.loc[new_static_mask, source_static.index] = source_static

    if from_node_id == source_node_id:
        model.redirect_link(link_id=link_id, from_node_id=new_node_id)
    else:
        model.redirect_link(link_id=link_id, to_node_id=new_node_id)

    return new_node_id


def duplicate_level_boundaries_for_links(
    model: Model,
    node_link_pairs: Iterable[tuple[int, int]],
    default_level: float = 0.0,
    name_suffix: str = "extra boundary",
    strict: bool = False,
) -> list[int | None]:
    """Duplicate LevelBoundary nodes for multiple ``(source_node_id, link_id)`` pairs."""
    return [
        duplicate_level_boundary_for_link(
            model=model,
            source_node_id=source_node_id,
            link_id=link_id,
            default_level=default_level,
            name_suffix=name_suffix,
            strict=strict,
        )
        for source_node_id, link_id in node_link_pairs
    ]
