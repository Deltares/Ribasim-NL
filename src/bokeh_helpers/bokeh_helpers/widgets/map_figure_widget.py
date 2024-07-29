# %%
from dataclasses import dataclass, field
from functools import partial
from typing import Literal

import bokeh.models as bokeh_models
from bokeh.models import (
    ColumnDataSource,
    Range1d,
    TapTool,
)
from bokeh.models.widgets import RadioGroup
from bokeh.plotting import figure

BACKGROUND_LAYERS = {
    "luchtfoto": {
        "url": (
            "https://service.pdok.nl/hwh/luchtfotorgb/wms/v1_0?"
            "service=WMS&version=1.3.0&request=GetMap&layers=Actueel_orthoHR"
            "&width=265&height=265&styles=&crs=EPSG:28992&format=image/jpeg"
            "&bbox={XMIN},{YMIN},{XMAX},{YMAX}"
        ),
        "class": "BBoxTileSource",
        "args": {
            "attribution": "Topografie: Esri Nederland, Community Map Contributors / Luchtfoto: PDOK, Luchtfoto Beeldmateriaal"
        },
    },
    "topografie": {
        "url": (
            "https://services.arcgisonline.nl/arcgis/rest/services/Basiskaarten/Topo/"
            "MapServer/export?"
            "bbox={XMIN},{YMIN},{XMAX},{YMAX}"
            "&layers=show"
            "&size=385,385"
            "&bboxSR=28892"
            "&dpi=2500"
            "&transparent=true"
            "&format=png"
            "&f=image"
        ),
        "class": "BBoxTileSource",
        "args": {
            "attribution": "Topografie: Esri Nederland, Community Map Contributors / Luchtfoto: PDOK, Luchtfoto Beeldmateriaal"
        },
    },
}

BOKEH_LOCATIONS_SETTINGS = {
    "size": 10,
    "line_color": "line_color",
    "fill_color": "fill_color",
    "selection_color": "red",
    "selection_fill_alpha": 1,
    "nonselection_fill_alpha": 0.6,
    "nonselection_line_alpha": 0.5,
    "hover_color": "red",
    "hover_alpha": 0.6,
    "line_width": 1,
    "legend_field": "label",
}

FIGURE_SETTINGS = {
    "active_scroll": "wheel_zoom",
    "toolbar_location": "above",
}


def get_tilesource(layer, map_configs=BACKGROUND_LAYERS):
    url = map_configs[layer]["url"]
    if "args" in map_configs[layer]:
        args = map_configs[layer]["args"]
    else:
        args = {}
    return getattr(bokeh_models, map_configs[layer]["class"])(url=url, **args)


def update_background(map_figure_widget, background_control_widget, attrname, old, new):
    """Update map_figure when background is selected"""
    tile_source = get_tilesource(background_control_widget.labels[new])
    idx = next(idx for idx, i in enumerate(map_figure_widget.renderers) if i.name == "background")
    map_figure_widget.renderers[idx].tile_source = tile_source


@dataclass
class MapFigure:
    bounds: tuple[float, float, float, float]
    locations_source: ColumnDataSource | None = None
    lines_source: ColumnDataSource | None = None
    tile_sources: list = field(default_factory=list)
    background: Literal["luchtfoto", "topografie"] = "topografie"
    figure_settings: dict[str, str] = field(default_factory=lambda: FIGURE_SETTINGS)
    map_figure_widget: figure | None = None
    background_control_widget: RadioGroup | None = None

    def __post_init__(self):
        # setup ranges
        x_range = Range1d(start=self.bounds[0], end=self.bounds[2], min_interval=100)
        y_range = Range1d(start=self.bounds[1], end=self.bounds[3], min_interval=100)

        # setup tools
        map_tap = TapTool(mode="xor")
        tools = [
            map_tap,
            "wheel_zoom",
            "pan",
            "reset",
            "save",
        ]

        # init map_widget
        self.map_figure_widget = figure(tools=tools, x_range=x_range, y_range=y_range, **self.figure_settings)

        # set settings
        self.map_figure_widget.axis.visible = False
        self.map_figure_widget.toolbar.logo = None
        self.map_figure_widget.toolbar.autohide = True
        self.map_figure_widget.xgrid.grid_line_color = None
        self.map_figure_widget.ygrid.grid_line_color = None
        self.map_figure_widget.select(type=TapTool)

        # backgrounds
        tile_source = get_tilesource(layer=self.background)
        self.map_figure_widget.add_tile(tile_source, name="background")

        # init background control widget
        labels = list(BACKGROUND_LAYERS.keys())
        self.background_control_widget = RadioGroup(labels=labels, active=labels.index(self.background))

        # add background control callback
        self.background_control_widget.on_change(
            "active",
            self.update_background,
        )

    @property
    def update_background(self):
        return partial(
            update_background,
            map_figure_widget=self.map_figure_widget,
            background_control_widget=self.background_control_widget,
        )
