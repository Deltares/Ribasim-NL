"""Parameterisation of water board: Schieland en de Krimpenerwaard."""

import datetime

import peilbeheerst_model.ribasim_parametrization as ribasim_param
import xarray as xr
from peilbeheerst_model.assign_authorities import AssignAuthorities
from peilbeheerst_model.assign_parametrization import AssignMetaData
from peilbeheerst_model.controle_output import Control
from peilbeheerst_model.network_snapping import snap_model
from peilbeheerst_model.outlet_pump_scaler import OutletPumpScalingConfig, scale_outlets_pumps
from peilbeheerst_model.ribasim_feedback_processor import RibasimFeedbackProcessor
from ribasim import run_ribasim
from ribasim_nl.assign_lhm_fractions import assign_lhm_fractions
from ribasim_nl.assign_offline_budgets import AssignOfflineBudgets
from ribasim_nl.control import (
    add_controllers_to_connector_nodes,
    add_function_to_peilbeheerst_node_table,
    get_node_table_with_from_to_node_ids,
    set_node_functions,
)

from peilbeheerst_model import supply
from ribasim_nl import CloudStorage, Model, SetDynamicForcing, junctionify, merge_rwzi_model

AANVOER_CONDITIONS: bool = True
MIXED_CONDITIONS: bool = True
DYNAMIC_CONDITIONS: bool = True
RESCALE_FLOW_CAPACITIES: bool = True
ADD_LHM_FRACTIONS: bool = True
ADD_RWZI: bool = True
ADD_JUNCTIONS: bool = True

if MIXED_CONDITIONS and not AANVOER_CONDITIONS:
    AANVOER_CONDITIONS = True

MIXED_CONDITIONS_DESIGN_P = 18
MIXED_CONDITIONS_DESIGN_E = 5

# model settings
waterschap = "SchielandendeKrimpenerwaard"
base_model_versie = "2024_12_1"

# connect with the GoodCloud
cloud = CloudStorage()

# collect data from the base model, feedback form, waterauthority & RWS border
ribasim_base_model_dir = cloud.joinpath(waterschap, "modellen", f"{waterschap}_boezemmodel_{base_model_versie}")
FeedbackFormulier_path = cloud.joinpath(
    waterschap, "verwerkt/Feedback Formulier", f"feedback_formulier_{waterschap}.xlsx"
)
FeedbackFormulier_LOG_path = cloud.joinpath(
    waterschap, "verwerkt/Feedback Formulier", f"feedback_formulier_{waterschap}_LOG.xlsx"
)
ws_grenzen_path = cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/waterschap.gpkg")
RWS_grenzen_path = cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/Rijkswaterstaat.gpkg")
qlr_name = "output_controle_cc.qlr" if MIXED_CONDITIONS else "output_controle_202502.qlr"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr", qlr_name)
aanvoer_path = cloud.joinpath(waterschap, "aangeleverd/Na_levering/Wateraanvoer/HyDamo_metWasverzachter_20230905.gpkg")
profiles_path = cloud.joinpath(waterschap, "verwerkt/profielen")

cloud.synchronize(
    filepaths=[
        ribasim_base_model_dir,
        FeedbackFormulier_path,
        ws_grenzen_path,
        RWS_grenzen_path,
        qlr_path,
        aanvoer_path,
    ]
)

# refresh only the feedback form from cloud (instead of all "verwerkt" files)
# cloud.download_file(cloud.file_url(FeedbackFormulier_path))

work_dir = cloud.joinpath(waterschap, "modellen", f"{waterschap}_profiles")
work_dir.mkdir(parents=True, exist_ok=True)

output_dir = cloud.joinpath(waterschap, "modellen", f"{waterschap}_forcing")

ribasim_work_dir_model_toml = work_dir.joinpath("ribasim.toml")
output_dir_model_toml = output_dir.joinpath("ribasim.toml")

# set path to base model toml
ribasim_base_model_toml = ribasim_base_model_dir.joinpath("ribasim.toml")

unknown_streefpeil = (
    0.00012345  # we need a streefpeil to create the profiles, Q(h)-relations, and af- and aanslag peil for pumps
)
starttime = datetime.datetime(2017, 1, 1)
endtime = datetime.datetime(2020, 1, 1)
saveat = 3600 * 24
timestep_size = "d"
timesteps = 2
delta_crest_level = 0.1  # delta waterlevel of boezem compared to streefpeil till no water can flow through an outlet
default_level = 0.75 if AANVOER_CONDITIONS else -0.75  # default LevelBoundary level, similar to surrounding Maas

# recreate the feedback form for set_aanvoer_flags
# TODO, see if we can move set_aanvoer_flags to the feedback stage so we don't need this object
name = "HKV"
processor = RibasimFeedbackProcessor(
    name,
    waterschap,
    base_model_versie,
    FeedbackFormulier_path,
    ribasim_base_model_toml,
    work_dir,
    FeedbackFormulier_LOG_path,
    use_validation=True,
)

ribasim_model = Model.read(ribasim_work_dir_model_toml)

# network snapping (junctions are added at the very end, just before writing, so they stay
# transparent to all parametrization, classification and validation steps)
if ADD_JUNCTIONS:
    ribasim_model = snap_model(ribasim_model, profiles_path)

# check if meta_categorie in the basin.node.df is completely filled
missing_meta_categorie_node_ids = ribasim_model.basin.node.df.loc[
    ribasim_model.basin.node.df["meta_categorie"].isna()
].index.tolist()
if missing_meta_categorie_node_ids:
    raise ValueError(
        "Not all basins have a meta_categorie assigned. "
        f"Missing meta_categorie for basin node IDs: {missing_meta_categorie_node_ids}"
    )

# set forcing
if DYNAMIC_CONDITIONS:
    # Add dynamic meteo and groundwater from LHM zarr
    lhm_budget_path = cloud.joinpath("Basisgegevens/LHM/4.3/results/LHM_433_budgets_update_makkink")
    cloud.synchronize(filepaths=[lhm_budget_path], overwrite=False)
    budgets = xr.open_zarr(str(lhm_budget_path)).sel(time=slice(starttime, endtime))
    offline_budgets = AssignOfflineBudgets(budgets)

    forcing = SetDynamicForcing(
        model=ribasim_model,
        budgets=budgets,
        startdate=starttime,
        enddate=endtime,
    )
    ribasim_model = forcing.add()
    offline_budgets.compute_budgets(ribasim_model)
    assign_validation_path = output_dir / "results" / "assign_validation.png"
    assign_validation_path.parent.mkdir(parents=True, exist_ok=True)
    offline_budgets.plot_assign_validation(ribasim_model, path=assign_validation_path)


elif MIXED_CONDITIONS:
    ribasim_param.set_hypothetical_dynamic_forcing(
        ribasim_model, starttime, endtime, MIXED_CONDITIONS_DESIGN_P, MIXED_CONDITIONS_DESIGN_E
    )
else:
    forcing_dict = {
        "precipitation": ribasim_param.convert_mm_day_to_m_sec(0 if AANVOER_CONDITIONS else MIXED_CONDITIONS_DESIGN_P),
        "potential_evaporation": ribasim_param.convert_mm_day_to_m_sec(
            MIXED_CONDITIONS_DESIGN_E if AANVOER_CONDITIONS else 0
        ),
        "drainage": ribasim_param.convert_mm_day_to_m_sec(0),
        "infiltration": ribasim_param.convert_mm_day_to_m_sec(0),
    }
    ribasim_param.set_static_forcing(timesteps, timestep_size, starttime, forcing_dict, ribasim_model)

# reset pump capacity for each pump
ribasim_model.pump.static.df["flow_rate"] = 10 / 60  # 10m3/min

# add the default levels
if MIXED_CONDITIONS:
    ribasim_param.set_hypothetical_dynamic_level_boundaries(
        ribasim_model, starttime, endtime, -0.75, 0.75, DYNAMIC_CONDITIONS
    )
else:
    ribasim_model.level_boundary.static.df["level"] = default_level

# prepare 'aanvoergebieden'
if AANVOER_CONDITIONS:
    aanvoergebieden = supply.special_load_geometry(
        f_geometry=str(aanvoer_path), method="extract", layer="peilbesluitgebied", key="statusobject", value="3"
    )
else:
    aanvoergebieden = None

# add control, based on the meta_categorie
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="outlet")
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="pump")

# filter processor basin IDs to only those present in the model
basin_ids = set(ribasim_model.basin.node.df.index)
processor._basin_aanvoer_on = tuple(n for n in (processor.basin_aanvoer_on or ()) if n in basin_ids)
processor._basin_aanvoer_off = tuple(n for n in (processor.basin_aanvoer_off or ()) if n in basin_ids)

ribasim_param.set_aanvoer_flags(
    ribasim_model,
    aanvoergebieden,
    processor,
    basin_aanvoer_off=104,
    aanvoer_enabled=AANVOER_CONDITIONS,
)
# Apply outlet meta_aanvoer labelling and overrule non-hoofdwater routes when direct hoofdwater supply exists.
supply.SupplyOutlet(ribasim_model).exec(overruling_enabled=True)

ribasim_param.identify_node_meta_categorie(ribasim_model, aanvoer_enabled=AANVOER_CONDITIONS)
# ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)
# ribasim_param.add_continuous_control(ribasim_model, dy=-50)


ribasim_model.basin.area.df["meta_streefpeil"] = ribasim_model.basin.area.df["meta_streefpeil"].astype(float)

from_to_node_table = get_node_table_with_from_to_node_ids(ribasim_model)
from_to_node_function_table = add_function_to_peilbeheerst_node_table(ribasim_model, from_to_node_table)
from_to_node_function_table["demand"] = None

# change outlet functions
to_supply = (
    182,
    183,
    196,
    201,
    356,
    468,
    476,
    479,
    480,
    489,
    494,
    504,
    516,
    524,
    527,
    531,
    534,
    554,
    669,
    691,
    702,
    704,
    705,
    709,
    711,
)
to_flow_control = (
    # 182,
    # 183,
    220,  # basin die boezem voedt
    341,  # basin die boezem voedt
    354,
    358,
    373,  # basin die boezem voedt
    561,
    703,
)
to_drain = (
    364,  # labelled as "aanvoergemaal" but directed from polder to boezem
    612,
    467,
)
from_to_node_function_table = set_node_functions(
    from_to_node_function_table, to_supply=to_supply, to_flow_control=to_flow_control, to_drain=to_drain
)

outlet_copy = ribasim_model.outlet.static.df[
    [
        "node_id",
        "meta_categorie",
        "meta_from_node_id",
        "meta_to_node_id",
        "meta_from_level",
        "meta_to_level",
        "meta_aanvoer",
    ]
].copy()
pump_copy = ribasim_model.pump.static.df[
    [
        "node_id",
        "meta_categorie",
        "meta_func_afvoer",
        "meta_func_aanvoer",
        "meta_func_circulatie",
        "meta_from_node_id",
        "meta_to_node_id",
        "meta_from_level",
        "meta_to_level",
    ]
].copy()

# TODO: Add flushing
# Add flushing data
# flush = Flushing(ribasim_model)
# _, df_demand = flush.add_flushing(df_function=from_to_node_function_table)
# from_to_node_function_table = flush.update_function_table(df_demand, from_to_node_function_table)

add_controllers_to_connector_nodes(
    model=ribasim_model,
    node_functions_df=from_to_node_function_table,
    target_level_column="meta_streefpeil",
    drain_capacity=20,
)

# # set discharge to 0 in supply state as suggested by D2Hydro
# flow_demand_nodes = ribasim_model.flow_demand.node.df.index.unique()
# pump_with_flow_demand = ribasim_model.link.df.loc[ribasim_model.link.df.from_node_id.isin(flow_demand_nodes), "to_node_id"].unique()
# ribasim_model.pump.static.df.loc[
#     (ribasim_model.pump.static.df.node_id.isin(pump_with_flow_demand))
#     & (ribasim_model.pump.static.df.control_state == "aanvoer"),
#     ["flow_rate", "max_flow_rate"],
# ] = 0

# replace the meta_data to the pump and outlet tables again, as the add_controllers_to_connector_nodes function might have changed/added node_id's
outlet_columns_to_add_back = [
    "meta_categorie",
    "meta_from_node_id",
    "meta_to_node_id",
    "meta_from_level",
    "meta_to_level",
    "meta_aanvoer",
]
pump_columns_to_add_back = [
    "meta_categorie",
    "meta_func_afvoer",
    "meta_func_aanvoer",
    "meta_func_circulatie",
    "meta_from_node_id",
    "meta_to_node_id",
    "meta_from_level",
    "meta_to_level",
]
ribasim_model.outlet.static.df = ribasim_model.outlet.static.df.drop(columns=outlet_columns_to_add_back).merge(
    outlet_copy, on="node_id", how="left"
)
ribasim_model.pump.static.df = ribasim_model.pump.static.df.drop(columns=pump_columns_to_add_back).merge(
    pump_copy, on="node_id", how="left"
)

# # there are some duplicates in the discrete control? Remove them
# control = ribasim_model.link.df[ribasim_model.link.df.link_type == "control"]
# dup_control = []
# all_nodes = ribasim_model.node.df[["node_type"]]
# for to_node_id, group in control.groupby("to_node_id"):
#     if len(group) == 1:
#         continue
#     elif len(group) == 2:
#         group = group.merge(all_nodes, left_on="from_node_id", right_index=True, how="inner")
#         if set(group.node_type.tolist()) == {"DiscreteControl", "FlowDemand"}:
#             continue
#         else:
#             dup_control.append(group.from_node_id.iat[0])
#     else:
#         raise ValueError(
#             f"found {len(group)} incoming control links for {to_node_id=} from {set(group.from_node_id.tolist())}"
#         )
#
# for duplicate in dup_control:
#     ribasim_model.remove_node(duplicate, True)
#     print(f"Removed duplicate control node {duplicate}")


# assign metadata for pumps and basins
assign_metadata = AssignMetaData(
    authority=waterschap,
    model_name=ribasim_model,
    param_name="HHSK.gpkg",
)
assign_metadata.add_meta_to_pumps(
    layer="gemaal",
    mapper={
        "meta_name": {"node": ["name"]},
        "meta_capaciteit": {"static": ["flow_rate", "max_flow_rate"]},
    },
    max_distance=10,
    factor_flowrate=1 / 60,  # m3/min -> m3/s
)
assign_metadata.add_meta_to_basins(
    layer="aggregation_area",
    mapper={"meta_name": {"node": ["name"]}},
    min_overlap=0.95,
)

# increase_flow_rate_pumps = [395]
# ribasim_model.pump.static.df.loc[
#     ribasim_model.pump.static.df["node_id"].isin(increase_flow_rate_pumps), "flow_rate"
# ] *= 60

# last formatting of the tables
# only retain node_id's which are present in the .node table
ribasim_param.clean_tables(ribasim_model, waterschap)

# set the max_flow_rate to the flow_rate
ribasim_model.pump.static.df["max_flow_rate"] = ribasim_model.pump.static.df["flow_rate"].copy()

# Manning resistance
# there is a MR without geometry and without links for some reason
ribasim_model.manning_resistance.node.df.dropna(subset="geometry", inplace=True)

# lower the difference in waterlevel for each manning node
ribasim_model.manning_resistance.static.df["length"] = 10.0
ribasim_model.manning_resistance.static.df["manning_n"] = 0.01

if MIXED_CONDITIONS:
    ribasim_model.basin.static.df = None
    ribasim_param.set_dynamic_min_upstream_max_downstream(ribasim_model)

# add the water authority column to couple the model with
assign = AssignAuthorities(
    ribasim_model=ribasim_model,
    waterschap=waterschap,
    ws_grenzen_path=ws_grenzen_path,
    RWS_grenzen_path=RWS_grenzen_path,
    RWS_buffer=400,  # polygons match relatively good, lower buffer
    custom_nodes=None,
    fill_na_authority="Rijkswaterstaat",
)
ribasim_model = assign.assign_authorities()

# merge RWZI model
if ADD_RWZI:
    ribasim_model = merge_rwzi_model(ribasim_model, cloud.joinpath("Rijkswaterstaat/modellen/rwzi/rwzi.toml"))

# add LHM fractions
if ADD_LHM_FRACTIONS:
    assign_lhm_fractions(ribasim_model)

# set the pumps and outlets with unknown flow capacities to have unknown flow capacities in the model, so they can be scaled in the next step.
ribasim_model.outlet.static.df["meta_known_flow_rate"] = False
ribasim_model.pump.static.df["meta_known_flow_rate"] = True
ribasim_model.pump.static.df.loc[
    (ribasim_model.pump.static.df.max_flow_rate.isna()) | (ribasim_model.pump.static.df.max_flow_rate == 0),
    "meta_known_flow_rate",
] = False

ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 363,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 5, True  # actually not known, but its otherwise too low

ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 166,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 10, True  # actually not known, but its otherwise too low

# rescaling of outlets (and pumps)
if RESCALE_FLOW_CAPACITIES:
    ribasim_model, from_to_node_function_table = scale_outlets_pumps(
        OutletPumpScalingConfig(
            ribasim_model_path=ribasim_work_dir_model_toml,
            ribasim_model=ribasim_model,
            from_to_node_function_table=from_to_node_function_table,
            waterschap=waterschap,
            cloud=cloud,
            rescale_flow_capacities=RESCALE_FLOW_CAPACITIES,
            design_precipitation_event=MIXED_CONDITIONS_DESIGN_P,
            design_potential_evaporation_event=MIXED_CONDITIONS_DESIGN_E,
        )
    )
else:
    print(f"No scaling of outlets/pumps: {RESCALE_FLOW_CAPACITIES=}")

ribasim_model.outlet.static.df.loc[ribasim_model.outlet.static.df.node_id == 342, "max_flow_rate"] = 1.0


# check if meta_categorie in the basin.node.df is completely filled
missing_meta_categorie_node_ids = ribasim_model.basin.node.df.loc[
    ribasim_model.basin.node.df["meta_categorie"].isna()
].index.tolist()
if missing_meta_categorie_node_ids:
    raise ValueError(
        "Not all basins have a meta_categorie assigned. "
        f"Missing meta_categorie for basin node IDs: {missing_meta_categorie_node_ids}"
    )

# set numerical settings
# write model output

# add junctions last: a layout-only transformation merging overlapping flow links into a
# Junction. Done after all parametrization so junctions never break adjacency/validation.
if ADD_JUNCTIONS:
    ribasim_model = junctionify(ribasim_model)

ribasim_model.use_validation = True
ribasim_model.starttime = starttime
ribasim_model.endtime = endtime
ribasim_model.solver.saveat = saveat
ribasim_model.write(output_dir_model_toml)

# run model
run_ribasim(output_dir_model_toml)
ribasim_model.update_state()
ribasim_model.basin.state.write()

# model performance
controle_output = Control(work_dir=output_dir, qlr_path=qlr_path)
indicators = controle_output.run_dynamic_forcing() if MIXED_CONDITIONS else controle_output.run_all()
