# %%
from datetime import timedelta

import pandas as pd
import ribasim

from ribasim_nl import CloudStorage

cloud = CloudStorage()
CONFIG = {
    "Venlo": {"link_id": 171},
    "Heel boven": {"node_id": 8865},
    "Roermond boven": {"node_id": 9126},
    "Belfeld boven": {"node_id": 9422},
    "Bunde (Julianakanaal)": {"node_id": 7928},
    "Echt (Julianakanaal)": {"node_id": 8504},
    "Eijsden-grens": {"link_id": 159},
}

# Inlezen ribasim model
ribasim_model_dir = cloud.joinpath("Rijkswaterstaat/modellen/hws_transient")
plots_dir = ribasim_model_dir / "plots"
ribasim_toml = ribasim_model_dir / "hws.toml"
model = ribasim.Model.read(ribasim_toml)

start_time = model.starttime + timedelta(days=40)
end_time = model.endtime

ribasim_model_dir

plots_dir.mkdir(exist_ok=True)

flow_df = pd.read_feather(ribasim_toml.parent / "results" / "flow.arrow").set_index("time")
flow_df = flow_df[flow_df.index > start_time]
basin_df = pd.read_feather(ribasim_toml.parent / "results" / "basin.arrow").set_index("time")
basin_df = basin_df[basin_df.index > start_time]

meting_df = pd.read_excel(
    cloud.joinpath("Rijkswaterstaat/aangeleverd/debieten_Rijn_Maas_2023_2024.xlsx"),
    header=[0, 1, 2, 3],
    index_col=[0],
)
meting_df = meting_df[(meting_df.index > start_time) & (meting_df.index < end_time)]
meting_df = meting_df.resample("D").mean()

for k, v in CONFIG.items():
    name = k
    if "link_id" in v.keys():
        Q_meting = meting_df["Debiet"]["(m3/s)"][name]
        Q_meting.columns = ["meting"]
        Q_berekening = flow_df[flow_df["link_id"] == v["link_id"]][["flow_rate"]].rename(
            columns={"flow_rate": "berekend"}
        )

        plot = pd.concat([Q_meting, Q_berekening]).plot(title=name, ylabel="m3/s")
        fig = plot.get_figure()
        fig.savefig(plots_dir / f"{name}_m3_s.png")

    if "node_id" in v.keys():
        H_meting = meting_df["Waterstand"]["(m) "][name]
        H_meting.columns = ["meting"]
        H_berekening = basin_df[basin_df["node_id"] == v["node_id"]][["level"]].rename(columns={"level": "berekend"})
        plot = pd.concat([H_meting, H_berekening]).plot(title=name, ylabel="m NAP")
        fig = plot.get_figure()
        fig.savefig(plots_dir / f"{name}_m.png")
