from typing import Literal

import bokeh.models as bokeh_models
from bokeh.layouts import column
from bokeh.models import (
    HoverTool,
    Range1d,
    TapTool,
)
from bokeh.models.widgets import CheckboxGroup, Div, RadioGroup
from bokeh.plotting import figure

BOKEH_BACKGROUNDS = {
    "luchtfoto": {
        "url": (
            "https://service.pdok.nl/hwh/luchtfotorgb/wms/v1_0?"
            "service=WMS&version=1.3.0&request=GetMap&layers=Actueel_orthoHR"
            "&width=265&height=265&styles=&crs=EPSG:28992&format=image/jpeg"
            "&bbox={XMIN},{YMIN},{XMAX},{YMAX}"
        ),
        "class": "BBoxTileSource",
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
    },
}

BOKEH_SETTINGS = {
    "background": "topografie",
    "save_tool": "save",
    "active_scroll": "wheel_zoom",
    "toolbar_location": "above",
}


def get_tilesource(layer, map_configs=BOKEH_BACKGROUNDS):
    url = map_configs[layer]["url"]
    if "args" in map_configs[layer]:
        args = map_configs[layer]["args"]
    else:
        args = {}
    return getattr(bokeh_models, map_configs[layer]["class"])(url=url, **args)


def make_map(
    bounds: list[float, float, float, float],
    background: Literal["luchtfoto", "topografie"] = "topografie",
) -> figure:
    # figure ranges
    x_range = Range1d(start=bounds[0], end=bounds[2], min_interval=100)
    y_range = Range1d(start=bounds[1], end=bounds[3], min_interval=100)

    # set tools
    map_hover = HoverTool(
        tooltips=[
            ("name", "@name"),
            ("node id", "@node_id"),
            ("diepte 1", "@value"),
            ("diepte 2", "@value"),
        ]
    )

    map_hover.visible = False

    tools = [
        "tap",
        "wheel_zoom",
        "pan",
        "reset",
        "box_select",
        map_hover,
        "save",
    ]

    # initialize figure
    map_fig = figure(
        tools=tools,
        active_scroll="wheel_zoom",
        x_range=x_range,
        y_range=y_range,
        toolbar_location="above",
    )

    # misc settings
    map_fig.axis.visible = False
    map_fig.toolbar.logo = None
    map_fig.toolbar.autohide = True
    map_fig.xgrid.grid_line_color = None
    map_fig.ygrid.grid_line_color = None
    map_fig.select(type=TapTool)

    # add background
    tile_source = get_tilesource(background)
    map_fig.add_tile(tile_source, name="background")

    return map_fig


def make_options(
    map_overlays: dict,
    overlays_change,
    background_title: str,
    background_change,
):
    # set overlay and handlers
    overlay_options = list(map_overlays.keys())
    active_overlays = [idx for idx, (_, v) in enumerate(map_overlays.items()) if v["visible"]]
    overlay_control = CheckboxGroup(labels=overlay_options, active=active_overlays)
    overlay_control.on_change("active", overlays_change)

    # set background and handlers
    background_options = list(BOKEH_BACKGROUNDS.keys())
    background_active = list(BOKEH_BACKGROUNDS.keys()).index(BOKEH_SETTINGS["background"])
    background_control = RadioGroup(labels=background_options, active=background_active)
    background_control.on_change("active", background_change)
    map_controls = column(
        overlay_control,
        Div(text=f"<h6>{background_title}</h6>"),
        background_control,
    )
    return map_controls
