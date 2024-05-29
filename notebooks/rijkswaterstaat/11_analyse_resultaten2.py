# %%
from datetime import timedelta

import pandas as pd
import ribasim
from ribasim_nl import CloudStorage

cloud = CloudStorage()

# Inlezen ribasim model
ribasim_model_dir = cloud.joinpath("Rijkswaterstaat", "modellen", "hws_2024_4_4")
plots_dir = ribasim_model_dir / "plots"
ribasim_toml = ribasim_model_dir / "hws.toml"
model = ribasim.Model.read(ribasim_toml)

start_time = model.starttime + timedelta(days=40)
end_time = model.endtime

ribasim_model_dir

plots_dir.mkdir(exist_ok=True)

flow_df = pd.read_feather(ribasim_toml.parent / "results" / "flow.arrow").set_index(
    "time"
)
flow_df = flow_df[flow_df.index > start_time]
basin_df = pd.read_feather(ribasim_toml.parent / "results" / "basin.arrow").set_index(
    "time"
)
basin_df = basin_df[basin_df.index > start_time]

meting_df = pd.read_excel(
    cloud.joinpath(
        "Rijkswaterstaat", "aangeleverd", "debieten_Rijn_Maas_2023_2024.xlsx"
    ),
    header=[0, 1, 2, 3],
    index_col=[0],
)
meting_df = meting_df[(meting_df.index > start_time) & (meting_df.index < end_time)]
meting_df = meting_df.resample("D").mean()


# %%
name = "Venlo"

Q_venlo_meting = meting_df["Debiet"]["(m3/s)"]["Venlo"].rename(
    columns={"Venlo": "meting"}
)
Q_venlo_berekening = flow_df[flow_df["edge_id"] == 146][["flow_rate"]].rename(
    columns={"flow_rate": "berekend"}
)

plot = pd.concat([Q_venlo_meting, Q_venlo_berekening]).plot(title=name, ylabel="m3/s")
fig = plot.get_figure()
fig.savefig(plots_dir / f"{name}_m3_s.png")

# %%

name = "Heel boven"

H_heel_meting = meting_df["Waterstand"]["(m) "][name].rename(
    columns={"_".join(name.split()): "meting"}
)
H_heel_berekening = basin_df[basin_df["node_id"] == 200][["level"]].rename(
    columns={"level": "berekend"}
)
plot = pd.concat([H_heel_meting, H_heel_berekening]).plot(title=name, ylabel="m NAP")
fig = plot.get_figure()
fig.savefig(plots_dir / f"{name}_m.png")

# %%

name = "Roermond boven"

H_roermond_meting = meting_df["Waterstand"]["(m) "][name].rename(
    columns={"_".join(name.split()): "meting"}
)
H_roermond_berekening = basin_df[basin_df["node_id"] == 177][["level"]].rename(
    columns={"level": "berekend"}
)
plot = pd.concat([H_roermond_meting, H_roermond_berekening]).plot(
    title=name, ylabel="m NAP"
)
fig = plot.get_figure()
fig.savefig(plots_dir / f"{name}_m.png")

# %%

name = "Belfeld boven"

H_belfeld_meting = meting_df["Waterstand"]["(m) "][name].rename(
    columns={"_".join(name.split()): "meting"}
)
H_belfeld_berekening = basin_df[basin_df["node_id"] == 178][["level"]].rename(
    columns={"level": "berekend"}
)
plot = pd.concat([H_belfeld_meting, H_belfeld_berekening]).plot(
    title=name, ylabel="m NAP"
)
fig = plot.get_figure()
fig.savefig(plots_dir / f"{name}_m.png")

# %%

name = "Belfeld boven"

H_belfeld_meting = meting_df["Waterstand"]["(m) "][name].rename(
    columns={"_".join(name.split()): "meting"}
)
H_belfeld_berekening = basin_df[basin_df["node_id"] == 178][["level"]].rename(
    columns={"level": "berekend"}
)
plot = pd.concat([H_belfeld_meting, H_belfeld_berekening]).plot(
    title=name, ylabel="m NAP"
)
fig = plot.get_figure()
fig.savefig(plots_dir / f"{name}_m.png")

# %%

name = "Bunde (Julianakanaal)"

H_belfeld_meting = meting_df["Waterstand"]["(m) "][name].rename(
    columns={name.split()[0]: "meting"}
)
H_belfeld_berekening = basin_df[basin_df["node_id"] == 174][["level"]].rename(
    columns={"level": "berekend"}
)
plot = pd.concat([H_belfeld_meting, H_belfeld_berekening]).plot(
    title=name, ylabel="m NAP"
)
fig = plot.get_figure()
fig.savefig(plots_dir / f"{name}_m.png")

# %%

name = "Echt (Julianakanaal)"

H_belfeld_meting = meting_df["Waterstand"]["(m) "][name].rename(
    columns={name.split()[0]: "meting"}
)
H_belfeld_berekening = basin_df[basin_df["node_id"] == 174][["level"]].rename(
    columns={"level": "berekend"}
)
plot = pd.concat([H_belfeld_meting, H_belfeld_berekening]).plot(
    title=name, ylabel="m NAP"
)
fig = plot.get_figure()
fig.savefig(plots_dir / f"{name}_m.png")

# %%
