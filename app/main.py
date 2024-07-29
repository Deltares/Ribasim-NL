# %%
import math
import sys
from itertools import cycle
from pathlib import Path

import pandas as pd
import pytz
import ribasim
from bokeh.io import curdoc
from bokeh.layouts import column, row
from bokeh.models import (
    CheckboxGroup,
    ColorBar,
    ColumnDataSource,
    Div,
    FixedTicker,
    Legend,
    LinearColorMapper,
    RadioGroup,
    RangeSlider,
    Span,
)
from bokeh.palettes import Category20_20
from bokeh.plotting import figure

try:
    from widgets.date_time_slider import DatetimeSlider, get_formatter
    from widgets.map_figure import make_map
except ImportError:
    sys.path.insert(0, Path(__file__).parent.as_posix())
    from widgets.date_time_slider import DatetimeSlider, get_formatter
    from widgets.map_figure import make_map

pd.options.mode.chained_assignment = None

INIT_VARIABLE = "outflow_rate"


def get_colors():
    return cycle(Category20_20)


def get_columns(active_variables):
    return basin_results_df.columns[active_variables].to_list()


def get_basin_variables(basin_results_df):
    return [i for i in basin_results_df.columns if i not in ["node_id", "time"]]


def add_lines(attr, old, new):
    # remove renderers and legend items
    time_fig.renderers = []
    legend.items = []

    # populate graph
    colors = get_colors()
    node_ids = locations_source.data["node_id"][locations_source.selected.indices]
    min_y = []
    max_y = []
    for node_id in node_ids:
        for active_variable in time_fig_variables.active:
            variable_name = basin_variable_columns[active_variable]
            df = basin_results_df[basin_results_df.node_id == node_id][
                ["time", variable_name]
            ]
            max_y += [df[variable_name].max()]
            min_y += [df[variable_name].min()]
            source = ColumnDataSource(df)
            time_fig.line(
                x="time",
                y=variable_name,
                source=source,
                color=next(colors),
                legend_label=f"{node_id}_{variable_name}",
            )

    if min_y and max_y:
        max_y = max(max_y)
        min_y = min(min_y)
        if (max_y - min_y) > 1:
            time_fig.y_range.start = min_y * 0.95
            time_fig.y_range.end = max_y * 1.05


def move_time_line(attr, old, new):
    now = date_time_slider.value_as_datetime
    time_line.location = now


def update_map_values(attr, old, new):
    # get selected variable
    variable = basin_variable_columns[map_fig_variable.active]

    # slice results
    df_results_select = basin_results_df[
        basin_results_df["time"]
        == date_time_slider.value_as_datetime.date().isoformat()
    ].set_index("node_id")[variable]

    # update locations source
    locations_source.data["value"] = df_select.node_id.apply(
        lambda x: abs(df_results_select.at[x])
    ).to_list()

    # update map_fig_variable_range
    map_fig_variable_range.start = 0
    map_fig_variable_range.end = 100
    map_fig_variable_range.title = f"{variable} range"

    # update color bar title
    color_bar.title = variable


def update_cm(attr, old, new):
    low, high = map_fig_variable_range.value
    cm.low = low
    cm.high = high
    nb_ticks = len(color_bar.ticker.ticks)
    interval = (high - low) / (nb_ticks - 1)
    color_bar.ticker.ticks = [low + (i * interval) for i in range(nb_ticks)]


# read model
toml_file = next((i for i in sys.argv if i.lower().endswith(".toml")), None)
if toml_file is None:
    toml_file = r"d:\projecten\D2306.LHM_RIBASIM\02.brongegevens\Rijkswaterstaat\modellen\hws_2024_4_1\hws.toml"
model = ribasim.Model.read(toml_file)
results_dir = model.filepath.parent / model.results_dir

# basin results
basin_results_df = pd.read_feather(results_dir / "basin.arrow")
basin_results_df.reset_index(inplace=True)
basin_variable_columns = [
    i for i in basin_results_df.columns if i not in ["time", "node_id"]
]
if INIT_VARIABLE in basin_variable_columns:
    actives = [basin_variable_columns.index(INIT_VARIABLE)]
else:
    actives = [0]

# prepare nodes
# nodes_df = model.network.node.df
# support older versions
if hasattr(model, "network"):
    nodes_df = model.network.node.df
else:
    nodes_df = model.node_table().df
nodes_df["x"] = nodes_df.geometry.x
nodes_df["y"] = nodes_df.geometry.y

# map fig
map_fig = make_map(bounds=nodes_df.total_bounds)
df_select = nodes_df[nodes_df["node_type"] == "Basin"][["x", "y", "name", "node_id"]]

locations_source = ColumnDataSource(df_select)

palette = ["green", "yellow", "orange", "red"]
cm = LinearColorMapper(palette=palette, low=0, high=100, nan_color="grey")

locations_source.selected.on_change("indices", add_lines)

map_fig.scatter(
    x="x",
    y="y",
    size=8,
    source=locations_source,
    line_color=None,
    fill_color={"field": "value", "transform": cm},
    selection_fill_alpha=1,
    selection_line_color="yellow",
    selection_line_width=2,
    nonselection_fill_alpha=0.6,
    nonselection_line_alpha=0.5,
)

color_bar = ColorBar(
    color_mapper=cm,
    location=(0, 0),
    ticker=FixedTicker(ticks=[0, 25, 50, 75, 100]),
    title=INIT_VARIABLE,
)

map_fig.add_layout(color_bar, "left")


# time_fig
time_fig = figure(
    toolbar_location="above",
    name="time figure",
    x_range=(model.starttime, model.endtime),
)
legend = Legend(items=[])
time_fig.add_layout(legend)
time_fig.toolbar.logo = None
time_fig.xaxis.formatter = get_formatter()
time_fig.xaxis.major_label_orientation = math.pi / 4

time_line = Span(
    location=model.starttime, dimension="height", line_color="red", line_width=3
)

time_fig.add_layout(time_line)


# Controls

# controls: dateslider
date_time_slider = DatetimeSlider(
    start=pytz.utc.localize(model.starttime),
    end=pytz.utc.localize(model.endtime),
    value=pytz.utc.localize(model.starttime),
    step=model.solver.saveat * 1000,
    # format="%Y-%m-%d %H:%M:%S %Z",
)
date_time_slider.widget.js_link("value", time_line, "location")
# date_time_slider.widget.on_change("value", move_time_line)
date_time_slider.widget.on_change("value_throttled", update_map_values)

# controls: map fig variables
map_fig_variable = RadioGroup(labels=basin_variable_columns, active=actives[0])
map_fig_variable.on_change("active", update_map_values)

# controls: time fig variables
time_fig_variables = CheckboxGroup(labels=basin_variable_columns, active=actives)
time_fig_variables.on_change("active", add_lines)

# controls: map values
map_fig_variable_range = RangeSlider(start=0, end=100, value=(cm.low, cm.high))
map_fig_variable_range.on_change("value", update_cm)

# layouts
variables_layout = column(
    Div(text="<b>Variabelen:</b>"),
    row(
        column(Div(text="<b>kaart:</b>"), map_fig_variable),
        column(Div(text="<b>grafiek:</b>"), time_fig_variables),
    ),
)
control_layout = column(
    variables_layout, map_fig_variable_range, date_time_slider.widget
)

layout = row(map_fig, time_fig, control_layout)

curdoc().add_root(layout)
curdoc().title = f"ribasim: {model.filepath.stem}"

# init app
update_map_values(None, None, None)
