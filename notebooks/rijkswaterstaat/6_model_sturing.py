# %%
import warnings

import openpyxl
import pandas as pd
from ribasim.nodes import (
    continuous_control,
    outlet,
    pid_control,
    pump,
    tabulated_rating_curve,
)

from ribasim_nl import CloudStorage, Model

warnings.filterwarnings(
    action="ignore",
    module="geopandas",
    message="CRS not set for some of the concatenation inputs.",
)

warnings.filterwarnings(
    action="ignore",
    module="openpyxl",
    message="Data Validation extension is not supported and will be removed",
)


cloud = CloudStorage()

UPDATE_KWK_DATA = True


# %% functies
def read_rating_curve(kwk_df):
    qh_df = kwk_df.loc[kwk_df.Eigenschap.to_list().index("Q(h) relatie") + 2 :][["Eigenschap", "Waarde"]].rename(
        columns={"Eigenschap": "level", "Waarde": "flow_rate"}
    )
    qh_df.dropna(inplace=True)
    return tabulated_rating_curve.Static(**qh_df.to_dict(orient="list"))


def read_qq_curve(kwk_df):
    return kwk_df.loc[kwk_df.Eigenschap.to_list().index("QQ relatie") + 2 :][["Eigenschap", "Waarde"]].rename(
        columns={"Eigenschap": "condition_flow_rate", "Waarde": "flow_rate"}
    )


def read_kwk_properties(kwk_df):
    properties = kwk_df[0:12][["Eigenschap", "Waarde"]].dropna().set_index("Eigenschap")["Waarde"]

    if "Kunstwerkcode" in properties:
        properties["Kunstwerkcode"] = str(properties["Kunstwerkcode"])
    return properties


def read_flow_kwargs(kwk_properties, include_crest_level=True):
    mapper = {
        "Capaciteit (m3/s)": "flow_rate",
        "minimale capaciteit (m3/s)": "min_flow_rate",
        "Streefpeil (m +NAP)": "min_upstream_level",
        "Streefpeil benedenstrooms (m +NAP)": "max_downstream_level",
    }

    kwargs = kwk_properties.rename(mapper).to_dict()
    if "flow_rate" in kwargs:
        kwargs["max_flow_rate"] = kwargs["flow_rate"]
    kwargs = {
        k: [v]
        for k, v in kwargs.items()
        if k in ["flow_rate", "min_flow_rate", "max_flow_rate", "min_upstream_level", "max_downstream_level"]
    }
    # kwargs["flow_rate"] = [kwk_properties["Capaciteit (m3/s)"]]
    return kwargs


def read_outlet(kwk_df, name=None):
    if "QQ relatie" in kwk_df["Eigenschap"].to_numpy():
        qq_properties = read_qq_curve(kwk_df)
        return outlet.Static(
            flow_rate=[max(qq_properties["flow_rate"].to_list())],
        )
    else:
        return outlet.Static(**read_flow_kwargs(read_kwk_properties(kwk_df), include_crest_level=True))


def read_pump(kwk_properties):
    kwargs = read_flow_kwargs(kwk_properties)
    return pump.Static(**kwargs)


def read_pid(control_properties, control_basin_id):
    if control_properties["Controle benedenstrooms"]:
        p = 500000
        i = 1e-07
    else:
        p = -500000
        i = -1e-07

    return [
        pid_control.Static(
            listen_node_id=[control_basin_id],
            target=[control_properties["Streefpeil (m+NAP)"]],
            listen_node_type="Basin",
            proportional=[p],
            integral=[i],
            derivative=[0.0],
        )
    ]


# %% Paden
ribasim_toml = cloud.joinpath("Rijkswaterstaat/modellen/hws_netwerk/hws.toml")
kwk_dir = cloud.joinpath("Rijkswaterstaat/verwerkt/kunstwerken")
kwk_xlsx = kwk_dir.joinpath("kunstwerken.xlsx")

# %%Inlezen
all_kwk_df = pd.read_excel(kwk_xlsx, sheet_name="kunstwerken")
all_kwk_df.loc[:, "code"] = all_kwk_df["code"].astype(str)
model = Model.read(ribasim_toml)

# %% Itereren over kunstwerken

all_kwk_df = all_kwk_df[all_kwk_df.in_model]
for gebied, kwks_df in all_kwk_df.groupby(by="gebied"):
    # get sheet-names
    file_name = kwk_dir / f"{gebied}.xlsx"
    workbook = openpyxl.open(file_name)
    sheet_names = workbook.sheetnames
    workbook.close()

    # kwk = next(i for i in kwks_df.itertuples() if i.naam == "Pr. Bernhardsluis")
    for kwk in kwks_df.itertuples():
        print(f"updating {kwk.naam}")
        # check if naam column exists as sheet in xlsx and read sheet
        if kwk.naam not in sheet_names:
            raise ValueError(f"{kwk.naam} not a sheet in {file_name}")

        kwk_df = pd.read_excel(file_name, sheet_name=kwk.naam)

        # get properties
        kwk_properties = read_kwk_properties(kwk_df)

        # check if code-value is the same in both Excels
        if kwk_properties["Kunstwerkcode"] != kwk.code:
            raise ValueError(
                f"code for {kwk.naam} do not match in `{file_name.name}` and `{kwk_xlsx.name}`: {kwk_properties['Kunstwerkcode']} != {kwk.code}"
            )

        # find existing node_id in model
        node_id = model.find_node_id(meta_code_waterbeheerder=str(kwk_properties["Kunstwerkcode"]))

        if UPDATE_KWK_DATA:
            # prepare static-data for updating node
            node_type = kwk_properties["Ribasim type"]
            if node_type == "TabulatedRatingCurve":
                data = [read_rating_curve(kwk_df)]
            elif node_type == "Outlet":
                data = [read_outlet(kwk_df, name=kwk.naam)]
            elif node_type == "Pump":
                data = [read_pump(kwk_properties)]
            else:
                raise ValueError(f"node-type {node_type} not yet implemented")

            # update model
            model.update_node(
                node_id=node_id,
                node_type=node_type,
                data=data,
                node_properties={"name": kwk.naam},
            )

        # check if structure has pid control
        if "PidControl" in kwk_df["Eigenschap"].to_numpy():
            control_properties = kwk_df.loc[kwk_df.Eigenschap.to_list().index("PidControl") + 1 :][
                ["Eigenschap", "Waarde"]
            ].set_index("Eigenschap")["Waarde"]

            if control_properties["Controle benedenstrooms"]:
                # waterlichaam should be upstream
                control_basin_id = model.find_node_id(
                    us_node_id=node_id,
                    name=control_properties["Waterlichaam"],
                )
            else:  # waterlichaam should be donstream
                control_basin_id = model.find_node_id(
                    ds_node_id=node_id,
                    name=control_properties["Waterlichaam"],
                )

            # add control node to network
            model.add_control_node(
                to_node_id=node_id,
                data=read_pid(control_properties, control_basin_id),
                ctrl_type="PidControl",
                node_offset=20,
            )
            # check if network-direction is correct with control-parameters

        # check if structure has qq control
        if "QQ relatie" in kwk_df["Eigenschap"].to_numpy():
            qq_properties = read_qq_curve(kwk_df)

            listen_node_id = model.find_node_id(meta_meetlocatie_code=str(kwk_properties["Meetlocatiecode"]))

            data = [
                continuous_control.Function(
                    input=qq_properties["condition_flow_rate"].to_list(),
                    output=qq_properties["flow_rate"].to_list(),
                    controlled_variable="flow_rate",
                ),
                continuous_control.Variable(
                    listen_node_id=[listen_node_id],
                    variable=["flow_rate"],
                ),
            ]

            model.add_control_node(
                to_node_id=node_id,
                data=data,
                ctrl_type="ContinuousControl",
                node_offset=20,
            )


# %% write
ribasim_toml = cloud.joinpath("Rijkswaterstaat/modellen/hws_sturing/hws.toml")

model.write(ribasim_toml)
