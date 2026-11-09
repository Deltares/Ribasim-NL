"""Network helpers for coupling-level checks."""

from ribasim_nl.flow_paths import Routes


def first_non_junction(routes: Routes, node_id: int, upstream: bool) -> tuple[int | None, int | None, str | None]:
    """The node upstream of `node_id`, or downstream, through plain Junctions, see `ribasim_nl.flow_paths.Routes`.

    :return: the node if there is exactly one, the link of `node_id` on that side if there is one, and else why not
    """
    links = routes.links_at(int(node_id), upstream)
    if len(links) == 0:
        return None, None, "geen link"
    if len(links) > 1:
        return None, None, f"meerdere links: {links}"
    ends = routes.ends(links[0], upstream)
    if len(ends) != 1:
        return None, links[0], f"{len(ends)} nodes via Junctions: {sorted(ends)}"
    return next(iter(ends)), links[0], None
