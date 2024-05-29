# %%
import sys

import ribasim
from bokeh.io import curdoc
from bokeh_helpers.widgets.map_figure_widget import MapFigure

toml_file = next((i for i in sys.argv if i.lower().endswith(".toml")), None)
if toml_file is None:
    toml_file = r"d:\projecten\D2306.LHM_RIBASIM\02.brongegevens\Rijkswaterstaat\modellen\hws_2024_4_4\hws.toml"

model = ribasim.Model.read(toml_file)

nodes_df = model.node_table().df

map_figure = MapFigure(bounds=nodes_df.total_bounds)

curdoc().add_root(map_figure.map_figure_widget)
curdoc().add_root(map_figure.background_control_widget)
curdoc().title = "Ribasim"

# %%
