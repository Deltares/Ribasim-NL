"""Make the figures of the documentation from the model data, and optionally upload them to MinIO.

The pages link to the figures at `doc-image/figures` on MinIO, since they are not in Git and CI has no access to the
data. Catch-up runs of main publish them, see `docs/dev/docs.qmd`.

    pixi run docs-figures [FIGURE ...]     # make them in data/docs_figures, from the data that is there
    pixi run docs-figures --pull           # pull the data that they read first
    pixi run docs-figures --pull --publish # also upload them, which needs write access in `.env`
"""

import argparse
import shlex
import subprocess
import sys
import traceback
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import shapely
import shapely.ops
import xarray as xr
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.lines import Line2D
from matplotlib.patches import Patch, Rectangle
from matplotlib.patheffects import withStroke
from minio import Minio
from PIL import Image
from ribasim_nl.aquo import waterbeheercode
from ribasim_nl.lumping.hydamo import read_areas, read_boundaries, read_hydamo_network, read_split_nodes
from ribasim_nl.lumping.network import add_basins_to_nodes_and_edges, check_split_nodes_used, create_graph, split_graph
from ribasim_nl.lumping.utils import read_layer
from ribasim_nl.settings import settings
from ribasim_nl.webmap_publish import BUCKET_NAME, MINIO_SERVER

DATA = settings.ribasim_nl_data_dir
OUT = DATA / "docs_figures"
PREFIX = "doc-image/figures/"
# Figures are overwritten by each catch-up run, so browsers should not keep them for long
CACHE_CONTROL = "public, max-age=3600"
RWS = DATA / "Rijkswaterstaat"

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK2 = "#52514e"
MUTED = "#898781"
HAIR = "#c3c2b7"
GRID = "#e1e0d9"
PLANE = "#f0efec"
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
BLUE, ORANGE, AQUA, YELLOW = SERIES[:4]
ORANGE_TINT, AQUA_TINT = "#f5b89f", "#93d9bf"

GRAVITY = [
    "AaenMaas",
    "BrabantseDelta",
    "DeDommel",
    "DrentsOverijsselseDelta",
    "HunzeenAas",
    "Limburg",
    "Noorderzijlvest",
    "RijnenIJssel",
    "StichtseRijnlanden",
    "ValleienVeluwe",
    "Vechtstromen",
]
LEVEL = [
    "AmstelGooienVecht",
    "Delfland",
    "HollandsNoorderkwartier",
    "HollandseDelta",
    "Rijnland",
    "Rivierenland",
    "Scheldestromen",
    "SchielandendeKrimpenerwaard",
    "WetterskipFryslan",
    "Zuiderzeeland",
]
assert set(GRAVITY) | set(LEVEL) | {"Rijkswaterstaat"} == set(waterbeheercode)

plt.rcParams.update(
    {
        "font.family": "sans-serif",
        "font.sans-serif": ["IBM Plex Sans", "Source Sans 3", "Noto Sans", "DejaVu Sans"],
        "font.size": 9,
        "axes.edgecolor": HAIR,
        "axes.labelcolor": INK2,
        "axes.titlecolor": INK,
        "axes.titlesize": 10,
        "axes.titleweight": "bold",
        "axes.titlelocation": "left",
        "axes.spines.top": False,
        "axes.spines.right": False,
        "xtick.color": MUTED,
        "ytick.color": MUTED,
        "xtick.labelcolor": INK2,
        "ytick.labelcolor": INK2,
        "legend.frameon": False,
        "savefig.facecolor": SURFACE,
        "figure.facecolor": SURFACE,
        "axes.facecolor": SURFACE,
    }
)


@dataclass(frozen=True)
class FigureGroup:
    """A function that makes figures, the names of its figures, and the arguments of `dvc pull` for its data."""

    make: Callable[[], None]
    outputs: tuple[str, ...]
    pull: tuple[str, ...]


FIGURES: dict[str, FigureGroup] = {}


def figure(outputs: tuple[str, ...], pull: tuple[str, ...]) -> Callable[[Callable[[], None]], Callable[[], None]]:
    """Register a function that makes the figures `outputs` from the data that the `pull` arguments get."""

    def register(function: Callable[[], None]) -> Callable[[], None]:
        FIGURES[function.__name__] = FigureGroup(function, outputs, pull)
        return function

    return register


def save(fig: Figure, name: str) -> None:
    """Save a figure as a PNG with a reduced palette, which makes maps with flat colours much smaller."""
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / f"{name}.png"
    fig.savefig(path)
    plt.close(fig)
    image = (
        Image.open(path).convert("RGB").quantize(colors=128, method=Image.Quantize.MEDIANCUT, dither=Image.Dither.NONE)
    )
    image.save(path, optimize=True)
    print(f"{path.name}: {path.stat().st_size / 1024:.0f} kB")


def read_model(database: Path, *layers: str) -> dict[str, gpd.GeoDataFrame]:
    """Read layers of the database of a model, with `Node` and `Link` indexed by their ID."""
    assert database.is_file(), f"{database} does not exist, pull it first, see docs/dev/docs.qmd"
    return {layer: gpd.read_file(database, layer=layer, fid_as_index=layer in ("Node", "Link")) for layer in layers}


def authority_of(node_id: pd.Series | np.ndarray) -> pd.Series:
    """Return the name of the authority of the node IDs of the coupled model."""
    names = {code: name for name, code in waterbeheercode.items()}
    return pd.Series(np.asarray(node_id) // 100_000).map(names)


def outlined(ax: Axes, x: float, y: float, text: str, size: float = 8, weight: str = "bold", color: str = INK) -> None:
    """Write a label with an outline of the surface colour, so it stays legible on a map."""
    ax.text(
        x,
        y,
        text,
        ha="center",
        va="center",
        fontsize=size,
        fontweight=weight,
        color=color,
        zorder=30,
        path_effects=[withStroke(linewidth=2.5, foreground=SURFACE)],
    )


def scale_bar(ax: Axes, length: float, label: str) -> None:
    """Draw a scale bar in the lower left corner of a map."""
    xmin, xmax = ax.get_xlim()
    ymin, ymax = ax.get_ylim()
    x, y = xmin + 0.04 * (xmax - xmin), ymin + 0.035 * (ymax - ymin)
    ax.add_patch(Rectangle((x, y), length, 0.007 * (ymax - ymin), color=INK2, lw=0, zorder=25))
    outlined(ax, x + length / 2, y + 0.03 * (ymax - ymin), label, weight="normal", color=INK2)


def map_axes(bounds: Iterable[float], width: float = 6.4, dpi: int = 150) -> tuple[Figure, Axes]:
    """Make a figure with one map that fills it, of the bounds (xmin, ymin, xmax, ymax) in RD coordinates."""
    xmin, ymin, xmax, ymax = bounds
    fig, ax = plt.subplots(figsize=(width, width * (ymax - ymin) / (xmax - xmin)), dpi=dpi)
    ax.set_xlim(xmin, xmax)
    ax.set_ylim(ymin, ymax)
    ax.set_aspect("equal")
    ax.set_axis_off()
    fig.subplots_adjust(0, 0, 1, 1)
    return fig, ax


NL_BOUNDS = (10_000.0, 300_000.0, 285_000.0, 625_000.0)
LABELS = {
    "AaenMaas": "Aa en Maas",
    "AmstelGooienVecht": "Amstel, Gooi\nen Vecht",
    "BrabantseDelta": "Brabantse\nDelta",
    "DeDommel": "De Dommel",
    "Delfland": "Delfland",
    "DrentsOverijsselseDelta": "Drents Overijsselse\nDelta",
    "HollandsNoorderkwartier": "Hollands\nNoorderkwartier",
    "HollandseDelta": "Hollandse\nDelta",
    "HunzeenAas": "Hunze en Aas",
    "Limburg": "Limburg",
    "Noorderzijlvest": "Noorderzijlvest",
    "RijnenIJssel": "Rijn en IJssel",
    "Rijnland": "Rijnland",
    "Rivierenland": "Rivierenland",
    "Scheldestromen": "Scheldestromen",
    "SchielandendeKrimpenerwaard": "Schieland",
    "StichtseRijnlanden": "Stichtse\nRijnlanden",
    "ValleienVeluwe": "Vallei en\nVeluwe",
    "Vechtstromen": "Vechtstromen",
    "WetterskipFryslan": "Wetterskip\nFryslân",
    "Zuiderzeeland": "Zuiderzeeland",
}


# where the label of an authority does not fit on the representative point of its largest part
LABEL_POINTS = {"Delfland": (74_000.0, 450_000.0), "SchielandendeKrimpenerwaard": (107_000.0, 440_500.0)}


def coupled_areas() -> gpd.GeoDataFrame:
    """Return the Basin / areas of the coupled model, with their authority."""
    areas = read_model(RWS / "runs/lhm_coupled/input/database.gpkg", "Basin / area")["Basin / area"]
    areas = areas[["node_id", "geometry"]].copy()
    areas["authority"] = authority_of(areas["node_id"]).to_numpy()
    assert areas["authority"].notna().all()
    return areas


@figure(
    outputs=("national-models", "national-flow"),
    pull=("run_lhm_coupled",),
)
def national() -> None:
    """Map of the models the coupled model is made of, and of its links by their mean flow rate."""
    areas = coupled_areas()
    fig, ax = map_axes(NL_BOUNDS)
    for names, color in ((GRAVITY, ORANGE_TINT), (LEVEL, AQUA_TINT)):
        areas[areas["authority"].isin(names)].plot(ax=ax, color=color, lw=0, zorder=1)
    areas[areas["authority"] == "Rijkswaterstaat"].plot(ax=ax, color=BLUE, lw=0, zorder=2)
    borders = gpd.GeoSeries(
        [g.buffer(0) for g in areas[areas["authority"] != "Rijkswaterstaat"].dissolve("authority").geometry],
        crs=areas.crs,
    )
    borders.boundary.plot(ax=ax, color=SURFACE, lw=0.9, zorder=3)
    for name, group in areas[areas["authority"] != "Rijkswaterstaat"].groupby("authority"):
        point = LABEL_POINTS.get(str(name))
        if point is None:
            largest = max(shapely.get_parts(group.union_all()), key=lambda g: g.area)
            point = largest.representative_point().coords[0]
        outlined(ax, point[0], point[1], LABELS[str(name)], size=6.5, weight="normal")
    ax.legend(
        handles=[
            Patch(color=BLUE, label="Main water system (Rijkswaterstaat)"),
            Patch(color=ORANGE_TINT, label="Gravity controlled, 11 water authorities"),
            Patch(color=AQUA_TINT, label="Level controlled, 10 water authorities"),
        ],
        loc="lower left",
        bbox_to_anchor=(0.0, 0.87),
        fontsize=8,
    )
    scale_bar(ax, 50_000, "50 km")
    save(fig, "national-models")

    # the links with their mean flow rate
    model = read_model(RWS / "runs/lhm_coupled/input/database.gpkg", "Link")["Link"]
    flow = xr.open_dataset(RWS / "runs/lhm_coupled/results/flow.nc")
    mean_flow = flow["flow_rate"].mean("time").to_series().abs()
    links = model[model["link_type"] == "flow"].copy()
    links["flow"] = links.index.map(mean_flow).to_numpy()
    fig, ax = map_axes(NL_BOUNDS)
    links.plot(ax=ax, color=GRID, lw=0.15, zorder=1)
    for lower, width, color in (
        (0.1, 0.35, "#86b6ef"),
        (1.0, 0.7, "#3987e5"),
        (10.0, 1.2, "#256abf"),
        (100.0, 2.2, "#0d366b"),
    ):
        links[links["flow"] >= lower].plot(ax=ax, color=color, lw=width, zorder=2 + width)
    ax.legend(
        handles=[
            Line2D([], [], color=GRID, lw=1, label="< 0.1 m³/s"),
            Line2D([], [], color="#86b6ef", lw=0.8, label="0.1 to 1 m³/s"),
            Line2D([], [], color="#3987e5", lw=1.2, label="1 to 10 m³/s"),
            Line2D([], [], color="#256abf", lw=1.7, label="10 to 100 m³/s"),
            Line2D([], [], color="#0d366b", lw=2.4, label="> 100 m³/s"),
        ],
        title="Mean flow rate over the run",
        title_fontsize=8,
        alignment="left",
        loc="upper left",
        fontsize=8,
    )
    scale_bar(ax, 50_000, "50 km")
    save(fig, "national-flow")


def node_markers(ax: Axes, nodes: gpd.GeoDataFrame, basin_colors: dict[int, str] | None = None) -> None:
    """Draw nodes with the same markers on every map, Basins in their own colour if given."""
    styles = {
        "ManningResistance": ("D", 20, INK),
        "Outlet": ("s", 22, INK),
        "TabulatedRatingCurve": ("s", 22, INK),
        "Pump": ("p", 42, INK),
        "Junction": ("o", 5, INK2),
        "LevelBoundary": ("v", 60, SURFACE),
        "FlowBoundary": ("^", 60, SURFACE),
        "Terminal": ("v", 60, SURFACE),
    }
    for node_type, (marker, size, color) in styles.items():
        selected = nodes[nodes["node_type"] == node_type]
        edge = INK if color == SURFACE else SURFACE
        ax.scatter(
            selected.geometry.x,
            selected.geometry.y,
            s=size,
            marker=marker,
            color=color,
            edgecolor=edge,
            linewidth=1.0,
            zorder=13,
        )
    basins = nodes[nodes["node_type"] == "Basin"]
    colors = [basin_colors.get(int(i), BLUE) if basin_colors else BLUE for i in basins.index]
    ax.scatter(
        basins.geometry.x, basins.geometry.y, s=58, marker="o", c=colors, edgecolor=SURFACE, linewidth=1.4, zorder=14
    )


NODE_LEGEND = {
    "Basin": Line2D([], [], marker="o", color=BLUE, mec=SURFACE, ls="", ms=8, label="Basin"),
    "ManningResistance": Line2D([], [], marker="D", color=INK, mec=SURFACE, ls="", ms=5, label="ManningResistance"),
    "Outlet": Line2D([], [], marker="s", color=INK, mec=SURFACE, ls="", ms=5, label="Outlet or TabulatedRatingCurve"),
    "Pump": Line2D([], [], marker="p", color=INK, mec=SURFACE, ls="", ms=7, label="Pump"),
    "Junction": Line2D([], [], marker="o", color=INK2, ls="", ms=2.5, label="Junction"),
    "LevelBoundary": Line2D([], [], marker="v", color=SURFACE, mec=INK, ls="", ms=7, label="LevelBoundary"),
}


def draw_links(ax: Axes, links: gpd.GeoDataFrame, color: str = INK2, lw: float = 0.8) -> None:
    """Draw flow links, with an arrow in the direction of flow on the longer ones."""
    for geometry in links.geometry:
        coords = np.asarray(geometry.coords)
        ax.plot(coords[:, 0], coords[:, 1], color=color, lw=lw, zorder=8, solid_capstyle="round")
        if geometry.length > 80:
            middle = geometry.interpolate(0.55, normalized=True)
            before = geometry.interpolate(geometry.project(middle) - 1)
            ax.annotate(
                "",
                xy=(middle.x, middle.y),
                xytext=(before.x, before.y),
                zorder=9,
                arrowprops={"arrowstyle": "-|>", "color": color, "lw": lw, "mutation_scale": 8},
            )


def window(
    database: Path, bounds: tuple[float, float, float, float]
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Read the nodes, flow links and Basin / areas of a model that are in the bounds."""
    nodes = gpd.read_file(database, layer="Node", fid_as_index=True, bbox=bounds)
    links = gpd.read_file(database, layer="Link", fid_as_index=True, bbox=bounds)
    links = links[links["link_type"] == "flow"]
    areas = gpd.read_file(database, layer="Basin / area", bbox=bounds)
    return nodes, links, areas


@figure(
    outputs=("main-water-system",),
    pull=("hws_transient", "data/Basisgegevens/RWS_waterschaps_grenzen/waterschap.gpkg"),
)
def main_water_system() -> None:
    """Map of the model of the main water system, with its Basins and nodes."""
    database = RWS / "modellen/hws_transient/input/database.gpkg"
    model = read_model(database, "Node", "Link", "Basin / area")
    fig, ax = map_axes(NL_BOUNDS)
    regions = gpd.read_file(DATA / "Basisgegevens/RWS_waterschaps_grenzen/waterschap.gpkg")
    regions.plot(ax=ax, color=PLANE, edgecolor=SURFACE, lw=0.6, zorder=0)
    model["Basin / area"].plot(ax=ax, color="#86b6ef", edgecolor=SURFACE, lw=0.25, zorder=1)
    links = model["Link"]
    links[links["link_type"] == "flow"].plot(ax=ax, color="#184f95", lw=0.45, zorder=2)
    nodes = model["Node"]
    styles = {
        "Pump": ("p", 13, ORANGE),
        "Outlet": ("s", 6, INK),
        "TabulatedRatingCurve": ("s", 6, INK),
        "UserDemand": ("^", 14, SERIES[6]),
        "LevelBoundary": ("v", 18, SERIES[7]),
        "FlowBoundary": ("v", 18, SERIES[7]),
    }
    for node_type, (marker, size, color) in styles.items():
        selected = nodes[nodes["node_type"] == node_type]
        ax.scatter(
            selected.geometry.x,
            selected.geometry.y,
            s=size,
            marker=marker,
            color=color,
            lw=0.2,
            edgecolor=SURFACE,
            zorder=4,
        )
    ax.legend(
        handles=[
            Patch(color="#86b6ef", label="Basin / area"),
            Line2D([], [], color="#184f95", lw=1, label="Link"),
            Line2D([], [], marker="s", color=INK, ls="", ms=3, label="Outlet or rating curve"),
            Line2D([], [], marker="p", color=ORANGE, ls="", ms=5, label="Pump"),
            Line2D([], [], marker="^", color=SERIES[6], ls="", ms=5, label="UserDemand"),
            Line2D([], [], marker="v", color=SERIES[7], ls="", ms=5, label="Boundary"),
        ],
        loc="upper left",
        fontsize=8,
    )
    scale_bar(ax, 50_000, "50 km")
    save(fig, "main-water-system")


COUPLING_WINDOW = (115_800.0, 476_900.0, 119_200.0, 480_300.0)


@figure(
    outputs=("coupling-before", "coupling-after"),
    pull=("samenvoegen", "run_lhm_coupled"),
)
def coupling() -> None:
    """Map of the border between two water authorities, before and after coupling."""
    before = window(RWS / "modellen/lhm_parts/input/database.gpkg", COUPLING_WINDOW)
    after = window(RWS / "runs/lhm_coupled/input/database.gpkg", COUPLING_WINDOW)
    colors = {"Rijnland": AQUA_TINT, "AmstelGooienVecht": "#b7d3f6", "Rijkswaterstaat": BLUE}
    for name, (nodes, links, areas) in (("coupling-before", before), ("coupling-after", after)):
        fig, ax = map_axes(COUPLING_WINDOW, width=5.2)
        authority = authority_of(areas["node_id"]).to_numpy()
        for key, color in colors.items():
            areas[authority == key].plot(ax=ax, color=color, lw=0, zorder=1)
        areas.boundary.plot(ax=ax, color=SURFACE, lw=0.8, zorder=2)
        draw_links(ax, links)
        node_markers(
            ax,
            nodes[
                nodes["node_type"].isin(
                    [
                        "Basin",
                        "ManningResistance",
                        "Outlet",
                        "TabulatedRatingCurve",
                        "Pump",
                        "LevelBoundary",
                        "FlowBoundary",
                    ]
                )
            ],
        )
        scale_bar(ax, 500, "500 m")
        save(fig, name)


@figure(
    outputs=("seasonal-map", "seasonal-level"),
    pull=("run_lhm_coupled",),
)
def seasonal() -> None:
    """Map of the difference of the summer and winter levels, and the levels of one Basin."""
    run = RWS / "runs/lhm_coupled"
    areas = gpd.read_file(run / "input/database.gpkg", layer="Basin / area")
    difference = (areas["meta_streefpeil_zomer"] - areas["meta_streefpeil_winter"]).abs()
    fig, ax = map_axes(NL_BOUNDS)
    gpd.GeoSeries([areas.union_all()], crs=areas.crs).plot(ax=ax, color=PLANE, lw=0, zorder=0)
    classes = [
        (0.0, 0.1, "#cde2fb", "up to 0.1 m"),
        (0.1, 0.2, "#86b6ef", "0.1 to 0.2 m"),
        (0.2, 0.3, "#3987e5", "0.2 to 0.3 m"),
        (0.3, np.inf, "#104281", "above 0.3 m"),
    ]
    handles = [Patch(color=PLANE, label="Fixed level, or no level management")]
    for lower, upper, color, label in classes:
        selected = areas[(difference > lower) & (difference <= upper)]
        selected.plot(ax=ax, color=color, lw=0, zorder=2)
        handles.append(Patch(color=color, label=label))
    ax.legend(
        handles=handles,
        title="Summer minus winter level",
        title_fontsize=8,
        alignment="left",
        loc="upper left",
        fontsize=8,
    )
    scale_bar(ax, 50_000, "50 km")
    save(fig, "seasonal-map")

    # the level of a Basin with a clear difference, with a median fit to its levels
    results = xr.open_dataset(run / "results/basin.nc")
    candidates = areas[(difference >= 0.2) & (difference <= 0.4) & (areas["meta_area_m2"].fillna(0) >= 0)].sort_values(
        "node_id", kind="stable"
    )
    level = results["level"].sel(node_id=candidates["node_id"].to_numpy()).to_pandas()
    summer = (level.index.month >= 4) & (level.index.month < 10)
    zomer = candidates.set_index("node_id")["meta_streefpeil_zomer"]
    winter = candidates.set_index("node_id")["meta_streefpeil_winter"]
    error = (level[summer] - zomer).abs().median() + (level[~summer] - winter).abs().median()
    node_id = int(error.sort_values(kind="stable").index[len(error) // 2])
    row = candidates.set_index("node_id").loc[node_id]
    print(
        "seasonal basin",
        node_id,
        authority_of(np.array([node_id])).iloc[0],
        row["meta_peilgebied"],
        row["meta_soort_peilbeheer"],
    )
    fig, ax = plt.subplots(figsize=(7.2, 3.0), dpi=150)
    times = level.index
    months = np.where(summer, row["meta_streefpeil_zomer"], row["meta_streefpeil_winter"])
    lower = np.where(summer, row["meta_ondergrens_zomer"], row["meta_ondergrens_winter"])
    upper = np.where(summer, row["meta_bovengrens_zomer"], row["meta_bovengrens_winter"])
    ax.fill_between(times, lower, upper, step="post", color="#cde2fb", lw=0, label="Between the lower and upper bound")
    ax.step(times, months, where="post", color=INK2, lw=1, ls=(0, (4, 2)), label="Target level")
    ax.plot(times, level[node_id], color=BLUE, lw=1.3, label="Simulated level")
    ax.set_ylabel("m NAP")
    ax.grid(axis="y", color=GRID, lw=0.6)
    ax.set_xlim(times[0], times[-1])
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, 1.2), ncol=3, fontsize=8)
    fig.tight_layout()
    save(fig, "seasonal-level")


@figure(
    outputs=("forcing-basin",),
    pull=("run_lhm_coupled",),
)
def forcing() -> None:
    """The forcing of one Basin of Delfland over the first two years."""
    results = xr.open_dataset(RWS / "runs/lhm_coupled/results/basin.nc")
    areas = gpd.read_file(
        RWS / "runs/lhm_coupled/input/database.gpkg",
        layer="Basin / area",
        where="node_id > 1500000 AND node_id < 1600000",
    )
    areas = areas.assign(area=areas.area).sort_values("area", ascending=False, kind="stable")
    rain = results["precipitation"].sel(node_id=areas["node_id"].to_numpy()).mean("time").to_series()
    drainage = results["drainage"].sel(node_id=areas["node_id"].to_numpy()).mean("time").to_series()
    node_id = int(areas.loc[areas["node_id"].isin(rain.index[(rain > 0) & (drainage > 0)]), "node_id"].iloc[0])
    print("forcing basin", node_id)
    basin = results.sel(node_id=node_id).to_dataframe()
    panels = [
        ("precipitation", "Precipitation"),
        ("evaporation", "Evaporation"),
        ("drainage", "Drainage"),
        ("infiltration", "Infiltration"),
    ]
    fig, axes = plt.subplots(len(panels), 1, figsize=(7.2, 5.0), dpi=150, sharex=True)
    for ax, (column, title) in zip(axes, panels, strict=True):
        ax.plot(basin.index, basin[column], color=BLUE, lw=0.9)
        ax.set_title(title)
        ax.set_ylabel("m³/s")
        ax.grid(axis="y", color=GRID, lw=0.6)
        ax.set_xlim(basin.index[0], basin.index[-1])
    fig.tight_layout()
    save(fig, "forcing-basin")


LEVEL_WINDOW = (78_000.0, 445_000.0, 83_000.0, 450_000.0)
LEVEL_MODELS = DATA / "Delfland/modellen"


def level_inputs() -> dict[str, gpd.GeoDataFrame]:
    """Read the layers of the crossings of Delfland in the window of the level controlled figures."""
    crossings = DATA / "Delfland/verwerkt/crossings.gpkg"
    layers = ["peilgebied", "aggregation_area", "hydroobject", "stuw", "gemaal", "crossings_hydroobject_filtered"]
    data = {layer: gpd.read_file(crossings, layer=layer, bbox=LEVEL_WINDOW) for layer in layers}
    data["shortest_path"] = gpd.read_file(
        DATA / "Delfland/verwerkt/shortest_path.gpkg", layer="shortest_path", bbox=LEVEL_WINDOW
    )
    return data


def colors_by_neighbours(polygons: gpd.GeoDataFrame, key: str, palette: list[str]) -> dict[int | str, str]:
    """Give each group of polygons a colour that differs from those it touches, largest groups first."""
    groups = polygons.dissolve(key)
    groups = groups.assign(area=groups.area).sort_values("area", ascending=False, kind="stable")
    colors: dict[int | str, str] = {}
    for name, geometry in groups.geometry.items():
        taken = {colors[other] for other in colors if groups.geometry[other].buffer(30).intersects(geometry)}
        colors[name] = next((color for color in palette if color not in taken), palette[0])
    return colors


TINTS = ["#b7d3f6", "#f5b89f", "#93d9bf", "#f8d68a", "#f4bdd2", "#a8d5a8", "#c4bcf0", "#f1a4a3"]


@figure(
    outputs=("level-1-delivery", "level-2-crossings", "level-3-input-model", "level-4-forcing"),
    pull=(
        "postprocessed_delfland",
        "crossings@delfland",
        "shortest_paths@delfland",
        "boezemmodel@delfland",
        "forcing_delfland",
    ),
)
def level_controlled() -> None:
    """Maps of one window of Delfland, for each step from the deliveries to the model."""
    data = level_inputs()
    boezem = data["peilgebied"][data["peilgebied"]["peilgebied_cat"] == 1]

    # 1: what the water board delivered
    fig, ax = map_axes(LEVEL_WINDOW, width=5.2)
    boezem.plot(ax=ax, color="#b7d3f6", lw=0, zorder=1)
    data["peilgebied"].boundary.plot(ax=ax, color=HAIR, lw=0.7, zorder=2)
    data["hydroobject"].plot(ax=ax, color=INK2, lw=0.5, zorder=3)
    ax.scatter(
        data["stuw"].geometry.x,
        data["stuw"].geometry.y,
        s=14,
        marker="s",
        color=ORANGE,
        edgecolor=SURFACE,
        linewidth=0.5,
        zorder=5,
    )
    ax.scatter(
        data["gemaal"].geometry.x,
        data["gemaal"].geometry.y,
        s=26,
        marker="p",
        color=INK,
        edgecolor=SURFACE,
        linewidth=0.6,
        zorder=5,
    )
    scale_bar(ax, 1000, "1 km")
    save(fig, "level-1-delivery")

    # 2: the crossings with the boundaries of the peilgebieden, and the paths through the boezem
    crossings = data["crossings_hydroobject_filtered"]
    crossings = crossings[crossings["peilgebied_from"].notna() | crossings["peilgebied_to"].notna()]
    structure = crossings["stuw"].notna() | crossings["gemaal"].notna()
    paths = data["shortest_path"]
    paths = gpd.GeoSeries.from_wkt(paths["shortest_path"].dropna(), crs=paths.crs)
    fig, ax = map_axes(LEVEL_WINDOW, width=5.2)
    boezem.plot(ax=ax, color="#b7d3f6", lw=0, zorder=1)
    data["peilgebied"].boundary.plot(ax=ax, color=HAIR, lw=0.7, zorder=2)
    data["hydroobject"].plot(ax=ax, color=HAIR, lw=0.5, zorder=3)
    paths.plot(ax=ax, color=BLUE, lw=1.4, zorder=4)
    ax.scatter(crossings[~structure].geometry.x, crossings[~structure].geometry.y, s=7, color=MUTED, lw=0, zorder=5)
    ax.scatter(
        crossings[structure].geometry.x,
        crossings[structure].geometry.y,
        s=20,
        color=INK,
        edgecolor=SURFACE,
        linewidth=0.6,
        zorder=6,
    )
    scale_bar(ax, 1000, "1 km")
    save(fig, "level-2-crossings")

    # 3 and 4: the input model and the final model, with a Basin in the same colour in both
    models = {}
    for name, model in (
        ("level-3-input-model", "Delfland_boezemmodel_2024_12_0"),
        ("level-4-forcing", "Delfland_forcing"),
    ):
        nodes, links, areas = window(LEVEL_MODELS / model / "input/database.gpkg", LEVEL_WINDOW)
        bergend = nodes.index[nodes["meta_categorie"] == "bergend"] if "meta_categorie" in nodes else []
        models[name] = nodes, links, areas[~areas["node_id"].isin(bergend)]
    colors = colors_by_neighbours(pd.concat([model[2] for model in models.values()]), "node_id", TINTS)
    for name, (nodes, links, doorgaand) in models.items():
        fig, ax = map_axes(LEVEL_WINDOW, width=5.2)
        doorgaand.plot(ax=ax, color=[colors[int(i)] for i in doorgaand["node_id"]], lw=0, zorder=1)
        doorgaand.boundary.plot(ax=ax, color=SURFACE, lw=0.8, zorder=2)
        draw_links(ax, links, lw=0.7)
        shown = nodes[
            nodes["node_type"].isin(
                [
                    "Basin",
                    "ManningResistance",
                    "Outlet",
                    "TabulatedRatingCurve",
                    "Pump",
                    "LevelBoundary",
                    "FlowBoundary",
                    "Junction",
                ]
            )
        ]
        node_markers(ax, shown)
        scale_bar(ax, 1000, "1 km")
        save(fig, name)


LUMPING = "DrentsOverijsselseDelta"
LUMPING_WINDOW = (187_512.0, 504_592.0, 192_512.0, 509_592.0)


def lumping_steps() -> dict[str, gpd.GeoDataFrame]:
    """Run the lumping of the window step by step, as `pipeline/00b_lump_model.py`, for its intermediate results."""
    verwerkt = DATA / LUMPING / "verwerkt"
    areas_path, input_path = verwerkt / "3_input/areas.gpkg", verwerkt / "3_input/ribasim_input.gpkg"
    network = read_hydamo_network(verwerkt / "4_ribasim/hydamo.gpkg", dx=250.0)
    areas = read_areas(areas_path, "areas")
    drainage_areas = read_layer(areas_path, "drainage_areas", explode=False, code_column="CODE")
    split_nodes, network = read_split_nodes(input_path, "split_nodes", network, buffer_distance=1.0)
    boundaries = read_boundaries(input_path, "boundaries", network.nodes, buffer_distance=1.0)
    graph = create_graph(network.nodes, network.edges)
    graph, used = split_graph(graph, split_nodes.copy(), network.edges)
    nodes, edges = add_basins_to_nodes_and_edges(graph, network.nodes, network.edges, used)
    used = check_split_nodes_used(used, nodes, edges)
    crs = "EPSG:28992"
    box = shapely.box(*LUMPING_WINDOW).buffer(300)
    return {
        "edges": edges.to_crs(crs).loc[lambda df: df.intersects(box)],
        "areas": areas.to_crs(crs).loc[lambda df: df.intersects(box)],
        "drainage_areas": drainage_areas.to_crs(crs).loc[lambda df: df.intersects(box)],
        "split_nodes": used[~used["split_type"].isin(["no_split", "harde_knip"])]
        .to_crs(crs)
        .loc[lambda df: df.intersects(box)],
        "boundaries": boundaries.to_crs(crs).loc[lambda df: df.intersects(box)],
    }


@figure(
    outputs=("lumping-1-inputs", "lumping-2-basins", "lumping-3-areas", "lumping-4-input-model", "lumping-5-fix-model"),
    pull=("parameterized_drents_overijsselse_delta --with-deps",),
)
def lumping() -> None:
    """Maps of one window of Drents Overijsselse Delta, for each step of the lumping and the fix script."""
    steps = lumping_steps()
    edges, areas, split_nodes, boundaries = steps["edges"], steps["areas"], steps["split_nodes"], steps["boundaries"]
    models = DATA / LUMPING / "modellen"
    if not (models / f"{LUMPING}_fix_model/input/database.gpkg").is_file():
        # not an output of DVC
        subprocess.run([sys.executable, "pipeline/drents_overijsselse_delta/01_fix_model.py"], check=True)
    inp = window(models / f"{LUMPING}_2024_6_3/input/database.gpkg", LUMPING_WINDOW)
    fix = window(models / f"{LUMPING}_fix_model/input/database.gpkg", LUMPING_WINDOW)

    # the lumped basin of each edge is its node ID in the input model, by meta_id
    basins = gpd.read_file(
        models / f"{LUMPING}_2024_6_3/input/database.gpkg",
        layer="Node",
        fid_as_index=True,
        where="node_type = 'Basin'",
        ignore_geometry=True,
    )
    node_id = pd.Series(basins.index.to_numpy(), index=basins["meta_id"].astype(int))
    edges = edges.assign(node_id=edges["basin"].map(node_id))

    colors: dict[int, str] = {}
    shapes = {}
    for df in (inp[2], fix[2]):
        for nid, geometry in zip(df["node_id"], df.geometry, strict=True):
            shapes.setdefault(int(nid), []).append(geometry)
    for nid, group in edges.dropna(subset=["node_id"]).groupby("node_id"):
        shapes.setdefault(int(nid), []).append(group.union_all().buffer(50))
    union = {nid: shapely.union_all(parts).intersection(shapely.box(*LUMPING_WINDOW)) for nid, parts in shapes.items()}
    for nid in sorted(union, key=lambda n: -union[n].area):
        taken = {colors[other] for other in colors if union[other].buffer(60).intersects(union[nid])}
        colors[nid] = next((color for color in SERIES if color not in taken), SERIES[0])
    tints = dict(zip(SERIES, TINTS, strict=True))

    def base(width: float = 5.2) -> tuple[Figure, Axes]:
        return map_axes(LUMPING_WINDOW, width=width)

    def lines_by_basin(ax: Axes, lw: float = 1.3) -> None:
        kept = edges.dropna(subset=["node_id"])
        edges[edges["node_id"].isna()].plot(ax=ax, color=MUTED, lw=0.9, linestyle=(0, (2, 1.5)), zorder=5)
        for nid, group in kept.groupby("node_id"):
            group.plot(ax=ax, color=colors[int(nid)], lw=lw, zorder=6)

    def points(ax: Axes, size: float = 20) -> None:
        ax.scatter(
            split_nodes.geometry.x,
            split_nodes.geometry.y,
            s=size,
            color=INK,
            edgecolor=SURFACE,
            linewidth=1.0,
            zorder=12,
        )
        ax.scatter(
            boundaries.geometry.x,
            boundaries.geometry.y,
            s=60,
            marker="v",
            color=SURFACE,
            edgecolor=INK,
            linewidth=1.2,
            zorder=13,
        )

    def labels(ax: Axes) -> None:
        box = shapely.box(*LUMPING_WINDOW).buffer(-300)
        for nid, group in edges.dropna(subset=["node_id"]).groupby("node_id"):
            inside = group[group.intersects(box)]
            if not inside.empty:
                geometry = inside.union_all()
                point = shapely.ops.nearest_points(geometry, geometry.centroid)[0]
                outlined(ax, point.x, point.y, str(int(nid)))

    def fill(ax: Axes, df: gpd.GeoDataFrame, outline: float = 1.0) -> None:
        for nid, group in df.groupby("node_id"):
            color = colors[int(nid)]
            group.plot(ax=ax, color=tints[color], lw=0, zorder=1)
            group.boundary.plot(ax=ax, color=color, lw=outline, zorder=4)

    # 1: the inputs
    fig, ax = base()
    areas.boundary.plot(ax=ax, color=HAIR, lw=0.5, zorder=2)
    steps["drainage_areas"].boundary.plot(ax=ax, color=INK2, lw=1.3, linestyle=(0, (5, 3)), zorder=3)
    edges.plot(ax=ax, color=INK2, lw=0.9, zorder=5)
    points(ax)
    scale_bar(ax, 1000, "1 km")
    save(fig, "lumping-1-inputs")

    # 2: basins
    fig, ax = base()
    areas.boundary.plot(ax=ax, color=HAIR, lw=0.5, zorder=2)
    lines_by_basin(ax, lw=1.4)
    points(ax)
    labels(ax)
    scale_bar(ax, 1000, "1 km")
    save(fig, "lumping-2-basins")

    # 3: areas
    fig, ax = base()
    fill(ax, inp[2], outline=1.2)
    areas.boundary.plot(ax=ax, color=HAIR, lw=0.4, zorder=2)
    lines_by_basin(ax, lw=1.0)
    points(ax, size=12)
    labels(ax)
    scale_bar(ax, 1000, "1 km")
    save(fig, "lumping-3-areas")

    # 4 and 5: the input model and the fix model
    for name, (nodes, links, basin_areas) in (("lumping-4-input-model", inp), ("lumping-5-fix-model", fix)):
        fig, ax = base()
        fill(ax, basin_areas)
        draw_links(ax, links)
        node_markers(ax, nodes, basin_colors=colors)
        if name == "lumping-5-fix-model":
            removed = inp[0].loc[inp[0].index.difference(fix[0].index)]
            ax.scatter(removed.geometry.x, removed.geometry.y, s=50, marker="x", color=MUTED, linewidth=1.5, zorder=15)
        for nid, row in nodes[nodes["node_type"] == "Basin"].iterrows():
            if shapely.box(*LUMPING_WINDOW).buffer(-150).contains(row.geometry):
                outlined(ax, row.geometry.x, row.geometry.y + 170, str(nid))
        scale_bar(ax, 1000, "1 km")
        save(fig, name)


@figure(
    outputs=("profiles-map", "profiles-area"),
    pull=("profiles_delfland",),
)
def profiles() -> None:
    """Map of the doorgaand and bergend hydro-objects of Delfland, and the area of one pair of Basins by level."""
    network = gpd.read_file(DATA / "Delfland/verwerkt/profielen/network.gpkg", layer="hydro_objects", bbox=LEVEL_WINDOW)
    route = network["main-route"].astype(bool)
    database = LEVEL_MODELS / "Delfland_profiles/input/database.gpkg"
    nodes = gpd.read_file(database, layer="Node", fid_as_index=True)
    links = gpd.read_file(database, layer="Link", fid_as_index=True, ignore_geometry=True)
    table = gpd.read_file(database, layer="Basin / profile")
    areas = gpd.read_file(database, layer="Basin / area", bbox=LEVEL_WINDOW)
    bergend = nodes.index[nodes["meta_categorie"] == "bergend"]
    box = shapely.box(*LEVEL_WINDOW).buffer(-500)

    # a doorgaand Basin with a large area in the window, and the bergend Basin joined to it by a ManningResistance
    in_window = areas[~areas["node_id"].isin(bergend) & areas.intersects(box)]
    in_window = in_window.assign(area=in_window.intersection(box).area).sort_values(
        "area", ascending=False, kind="stable"
    )
    manning = nodes.index[nodes["node_type"] == "ManningResistance"]
    pair = None
    for basin in in_window["node_id"]:
        # the ManningResistance connects both Basins, in either direction
        connectors = pd.concat(
            [
                links.loc[links["from_node_id"].eq(basin) & links["to_node_id"].isin(manning), "to_node_id"],
                links.loc[links["to_node_id"].eq(basin) & links["from_node_id"].isin(manning), "from_node_id"],
            ]
        )
        ends = pd.concat(
            [
                links.loc[links["from_node_id"].isin(connectors) & links["to_node_id"].isin(bergend), "to_node_id"],
                links.loc[links["to_node_id"].isin(connectors) & links["from_node_id"].isin(bergend), "from_node_id"],
            ]
        )
        if len(ends) == 1:
            pair = int(basin), int(ends.iloc[0])
            break
    assert pair is not None, "no Basin with a bergend Basin in the window"
    print("profile basins", pair)

    x, y = areas[areas["node_id"] == pair[0]].intersection(box).union_all().representative_point().coords[0]
    close = (x - 1250, y - 1250, x + 1250, y + 1250)
    fig, ax = map_axes(close, width=4.6)
    areas[areas["node_id"] == pair[0]].plot(ax=ax, color=PLANE, edgecolor=INK2, lw=1.0, zorder=1)
    network[~route].plot(ax=ax, color=ORANGE, lw=0.6, zorder=2)
    network[route].plot(ax=ax, color=BLUE, lw=1.5, zorder=3)
    ax.legend(
        handles=[
            Line2D([], [], color=BLUE, lw=1.5, label="Doorgaand, the main route"),
            Line2D([], [], color=ORANGE, lw=1, label="Bergend"),
            Patch(facecolor=PLANE, edgecolor=INK2, label=f"Basin {pair[0]}"),
        ],
        loc="upper left",
        fontsize=8,
        facecolor=SURFACE,
        frameon=True,
        edgecolor="none",
    )
    scale_bar(ax, 500, "500 m")
    save(fig, "profiles-map")

    fig, ax = plt.subplots(figsize=(4.6, 4.6), dpi=150)
    for node_id, color, label in ((pair[0], BLUE, "Doorgaand"), (pair[1], ORANGE, "Bergend")):
        row = table[table["node_id"] == node_id].sort_values("level", kind="stable")
        ax.plot(row["area"] / 1e4, row["level"], color=color, lw=2, marker="o", ms=4, label=label)
    ax.set_xlabel("Area [ha]")
    ax.set_ylabel("Level [m NAP]")
    ax.grid(color=GRID, lw=0.6)
    ax.set_xlim(left=0)
    ax.legend(loc="center left", fontsize=8)
    fig.tight_layout()
    save(fig, "profiles-area")


@figure(
    outputs=("authorities",),
    pull=("run_lhm_coupled",),
)
def authorities() -> None:
    """Bar chart of the number of Basins and connector nodes per authority in the coupled model."""
    nodes = gpd.read_file(
        RWS / "runs/lhm_coupled/input/database.gpkg", layer="Node", fid_as_index=True, ignore_geometry=True
    )
    nodes["authority"] = authority_of(nodes.index.to_series()).to_numpy()
    connectors = ["Outlet", "Pump", "TabulatedRatingCurve", "ManningResistance"]
    counts = pd.DataFrame(
        {
            "Basin": nodes[nodes["node_type"] == "Basin"].groupby("authority").size(),
            "Connector": nodes[nodes["node_type"].isin(connectors)].groupby("authority").size(),
        }
    ).fillna(0)
    counts = counts.loc[(counts["Basin"] + counts["Connector"]).sort_values(kind="stable").index]
    names = {
        **{key: value.replace("\n", " ") for key, value in LABELS.items()},
        "Rijkswaterstaat": "Rijkswaterstaat",
        "SchielandendeKrimpenerwaard": "Schieland en de Krimpenerwaard",
    }
    fig, ax = plt.subplots(figsize=(7.2, 6.0), dpi=150)
    ticks = [names[name] for name in counts.index]
    ax.barh(ticks, counts["Basin"], color=BLUE, height=0.7, label="Basins")
    ax.barh(
        ticks,
        counts["Connector"],
        left=counts["Basin"] + 12,
        color=ORANGE,
        height=0.7,
        label="Pumps, Outlets, rating curves and resistances",
    )
    ax.set_xlabel("Number of nodes")
    ax.grid(axis="x", color=GRID, lw=0.6)
    ax.set_axisbelow(True)
    ax.legend(loc="lower right", fontsize=8)
    ax.tick_params(axis="y", length=0)
    fig.tight_layout()
    save(fig, "authorities")


def upload(names: Iterable[str]) -> None:
    """Upload figures to the prefix on MinIO that the pages link to, overwriting the previous ones."""
    if not settings.aws_access_key_id or not settings.aws_secret_access_key:
        raise OSError("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set in the environment or .env file.")
    client = Minio(MINIO_SERVER, access_key=settings.aws_access_key_id, secret_key=settings.aws_secret_access_key)
    for name in names:
        client.fput_object(
            BUCKET_NAME,
            f"{PREFIX}{name}.png",
            str(OUT / f"{name}.png"),
            content_type="image/png",
            metadata={"Cache-Control": CACHE_CONTROL},
        )
        print(f"Uploaded {PREFIX}{name}.png")


def main() -> int:
    """Make the figures that are selected on the command line, or all, and return the exit code."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("figures", nargs="*", help=f"the groups to make, all by default: {', '.join(FIGURES)}")
    parser.add_argument("--pull", action="store_true", help="pull the data of the figures first")
    parser.add_argument("--publish", action="store_true", help="upload the figures to MinIO")
    args = parser.parse_args()
    unknown = set(args.figures) - set(FIGURES)
    assert not unknown, f"Unknown figures {sorted(unknown)}, choose from {list(FIGURES)}"

    failed = []
    for name in args.figures or FIGURES:
        print(name)
        group = FIGURES[name]
        # A figure that fails does not keep the others from being updated
        try:
            if args.pull:
                for target in group.pull:
                    subprocess.run(["dvc", "pull", *shlex.split(target)], check=True)
            group.make()
            missing = [output for output in group.outputs if not (OUT / f"{output}.png").is_file()]
            assert not missing, f"{name} did not make {missing}"
            if args.publish:
                upload(group.outputs)
        except Exception:
            traceback.print_exc()
            failed.append(name)
    if failed:
        print(f"Failed: {', '.join(failed)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
