"""Parameterisation of water board: Fryslan."""

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
    remove_duplicate_controls,
    set_node_functions,
)
from shapely.geometry import Point

from peilbeheerst_model import supply
from ribasim_nl import CloudStorage, Model, SetDynamicForcing, junctionify, merge_rwzi_model

AANVOER_CONDITIONS: bool = True
MIXED_CONDITIONS: bool = True
DYNAMIC_CONDITIONS: bool = True
RESCALE_FLOW_CAPACITIES: bool = True
ADD_LHM_FRACTIONS: bool = True
ADD_RWZI: bool = True
ADD_JUNCTIONS: bool = False

if MIXED_CONDITIONS and not AANVOER_CONDITIONS:
    AANVOER_CONDITIONS = True

MIXED_CONDITIONS_DESIGN_P = 11
MIXED_CONDITIONS_DESIGN_E = 2.6  # 0.3 L/s/ha

# model settings
waterschap = "WetterskipFryslan"
base_model_versie = "2025_5_1"

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
aanvoer_path = cloud.joinpath(waterschap, "aangeleverd/Na_levering/Wateraanvoer/aanvoer.gpkg")
meteo_path = cloud.joinpath("Basisgegevens/WIWB")
profiles_path = cloud.joinpath(waterschap, "verwerkt/profielen")
gaarkeuken_path = cloud.joinpath(waterschap, "aangeleverd/Na_levering/gaarkeuken.gpkg")

splitted_basin_21_path = cloud.joinpath(waterschap, "verwerkt/Splitting_basins/Opgeknipte_basin_21.gpkg")

cloud.synchronize(
    filepaths=[
        ribasim_base_model_dir,
        FeedbackFormulier_path,
        ws_grenzen_path,
        RWS_grenzen_path,
        qlr_path,
        aanvoer_path,
        meteo_path,
        profiles_path,
        gaarkeuken_path,
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
default_level = 10 if AANVOER_CONDITIONS else -2.3456  # default LevelBoundary level

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

# add junctions and network snapping
if ADD_JUNCTIONS:
    ribasim_model = snap_model(ribasim_model, profiles_path)
    ribasim_model = junctionify(ribasim_model)

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

# add the default levels
if MIXED_CONDITIONS:
    ribasim_param.set_hypothetical_dynamic_level_boundaries(
        ribasim_model, starttime, endtime, -2.3456, 10, DYNAMIC_CONDITIONS
    )
else:
    ribasim_model.level_boundary.static.df["level"] = default_level

# prepare 'aanvoergebieden'
if AANVOER_CONDITIONS:
    aanvoergebieden = supply.special_load_geometry(
        f_geometry=str(aanvoer_path),
        method="extract",
        # layer="aanvoer",
        key="aanvoer",
        value=1,
    )
else:
    aanvoergebieden = None

# add control, based on the meta_categorie
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="outlet")
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="pump")
ribasim_param.set_aanvoer_flags(
    ribasim_model,
    aanvoergebieden,
    processor,
    outlet_aanvoer_on=tuple(
        ribasim_model.outlet.static.df.loc[ribasim_model.outlet.static.df["meta_func_aanvoer"] == 1, "node_id"]
    ),
    aanvoer_enabled=AANVOER_CONDITIONS,
)
ribasim_param.identify_node_meta_categorie(ribasim_model, aanvoer_enabled=AANVOER_CONDITIONS)

# ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)
# ribasim_param.add_continuous_control(ribasim_model, dy=-50)

# remove non-free flowing outlets (which flow against gravity) based on the difference in streefpeil and the threshold
ribasim_model = ribasim_param.remove_non_free_flowing_outlets(
    ribasim_model=ribasim_model,
    to_exclude=[2183, 1394, 3302, 2625, 2910, 3497, 2209, 2547, 2705, 1123, 2398, 1037, 2209],
    threshold=0.02,
    printing=True,
)

ribasim_model.basin.area.df["meta_streefpeil"] = ribasim_model.basin.area.df["meta_streefpeil"].astype(float)

# create a table with from and to node ids, and the function of the node (supply, flow_control, drain)
from_to_node_table = get_node_table_with_from_to_node_ids(ribasim_model)
from_to_node_function_table = add_function_to_peilbeheerst_node_table(ribasim_model, from_to_node_table)
from_to_node_function_table["demand"] = None

# manually change the function of some nodes based upon model inspection
to_supply = (
    1312,
    1347,
    1512,
    1596,
    1647,
    1747,
    2128,
    2398,
    2434,
    2445,
    2616,
    2624,
    2670,
    2773,
    2783,
    2812,
    2857,
    2982,
    3023,
    3150,
    3188,
    3228,
    3272,
    3376,
    3411,
    3452,
    3760,
    3880,
    3882,
    3884,
    3888,
)
to_flow_control = (2452, 3064, 3065, 3068)

# look up dynamically-added drain node IDs by geometry
_trc_node_df = ribasim_model.tabulated_rating_curve.node.df
_drain_points = [Point(206421, 592530), Point(206360, 592679)]
to_drain_node_ids = tuple(_trc_node_df.loc[_trc_node_df.geometry.distance(p) < 1].index[0] for p in _drain_points)

to_drain = (2147, 2751, 2944, 3041, 3494, 3568, 3709, *to_drain_node_ids)

from_to_node_function_table = set_node_functions(
    from_to_node_function_table, to_supply=to_supply, to_flow_control=to_flow_control, to_drain=to_drain
)

# retain meta data of the outlets and pumps
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

# ribasim_model.node._update_used_ids()

# # Add flushing data
# flush = Flushing(
#     ribasim_model,
#     lhm_flushing_path="WetterskipFryslan/aangeleverd/Na_levering/WetterskipFryslan_doorspoeling.gpkg",
#     flushing_layer="WetterskipFryslan_doorspoeling",
#     flushing_id="flushing_id",
#     flushing_col="doorsp_mmj",
# )
# _, df_demand = flush.add_flushing(df_function=from_to_node_function_table)
# from_to_node_function_table = flush.update_function_table(df_demand, from_to_node_function_table)


add_controllers_to_connector_nodes(
    model=ribasim_model,
    node_functions_df=from_to_node_function_table,
    target_level_column="meta_streefpeil",
    drain_capacity=20,
)
remove_duplicate_controls(ribasim_model)

# add the meta_data to the pump and outlet tables again
# Outlet
ribasim_model.outlet.static.df = (
    ribasim_model.outlet.static.df.set_index("node_id").combine_first(outlet_copy.set_index("node_id")).reset_index()
)

# Pump
ribasim_model.pump.static.df = (
    ribasim_model.pump.static.df.set_index("node_id").combine_first(pump_copy.set_index("node_id")).reset_index()
)

# assign metadata for pumps and basins
assign_metadata = AssignMetaData(
    authority=waterschap,
    model_name=ribasim_model,
    param_name=f"{waterschap}.gpkg",
)
assign_metadata.add_meta_to_pumps(
    layer="gemaal",
    mapper={
        "meta_name": {"node": ["name"]},
        "meta_capaciteit": {"static": ["flow_rate", "max_flow_rate"]},
    },
    max_distance=100,
    factor_flowrate=1 / 60,  # m3/min -> m3/s
)
assign_metadata.add_meta_to_basins(
    layer="aggregation_area",
    mapper={"meta_name": {"node": ["name"]}},
    min_overlap=0.95,
)

ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.node_id == 2700, "max_flow_rate"] = 172 / 60
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.node_id == 3891, "max_flow_rate"] = 7340 / 60

# Manning resistance
# there is a MR without geometry and without links for some reason
mr_null_geom = ribasim_model.manning_resistance.node.df[ribasim_model.manning_resistance.node.df.geometry.isna()].index
ribasim_model.node.df = ribasim_model.node.df.drop(mr_null_geom)

# increase aanslagpeil for Woudagemaal
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 2436, "min_upstream_level"
] = -0.48  # 5 cm higher than streefpeil
ribasim_model.discrete_control.condition.df.loc[
    ribasim_model.discrete_control.condition.df.node_id == 14970, "threshold_high"
] = -0.48
ribasim_model.discrete_control.condition.df.loc[
    ribasim_model.discrete_control.condition.df.node_id == 14970, "threshold_low"
] = -0.48

# last formatting of the tables
# only retain node_id's which are present in the .node table
ribasim_param.clean_tables(ribasim_model, waterschap)
if MIXED_CONDITIONS:
    ribasim_model.basin.static.df = None
    ribasim_param.set_dynamic_min_upstream_max_downstream(ribasim_model)

# add the water authority column to couple the model with
assign = AssignAuthorities(
    ribasim_model=ribasim_model,
    waterschap=waterschap,
    ws_grenzen_path=ws_grenzen_path,
    RWS_grenzen_path=RWS_grenzen_path,
    custom_nodes={
        3820: "Noordzee",
        5022: "Rijkswaterstaat",
        10958: "Rijkswaterstaat",
    },
    fill_na_authority="Noordzee",
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

ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.node_id == 3863, "meta_known_flow_rate"] = (
    False  # unknown capacity, set temp value
)
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.node_id == 3863, "flow_rate"] = 0.1

# rescaling of outlets (and pumps)
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
