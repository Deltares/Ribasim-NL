"""Shared steps of the feedback and forcing scripts of the peilbeheerst water boards.

The scripts `pipeline/<water board>/01_feedback.py` and `pipeline/<water board>/03_forcing.py` contain the
water board specific edits. The steps they have in common are defined here.
"""

import warnings
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import geopandas as gpd

import ribasim_nl.ribasim_parametrization as ribasim_param
from ribasim_nl import CloudStorage, Model, junctionify, supply
from ribasim_nl.assign_authorities import AssignAuthorities
from ribasim_nl.assign_parametrization import AssignMetaData
from ribasim_nl.bgt_water import BGT_WATER
from ribasim_nl.capacity import bound_supply_inflows
from ribasim_nl.control import (
    add_controllers_to_connector_nodes,
    add_function_to_peilbeheerst_node_table,
    get_node_table_with_from_to_node_ids,
    remove_duplicate_controls,
)
from ribasim_nl.network_snapping import snap_model
from ribasim_nl.regional_model import ENDTIME, STARTTIME, add_inflows, add_lhm_forcing, finish_regional_model
from ribasim_nl.ribasim_feedback_processor import RibasimFeedbackProcessor
from ribasim_nl.settings import settings

# streefpeil used where it is unknown; we need one to create the profiles, Q(h)-relations, and af- and aanslag peil
UNKNOWN_STREEFPEIL = 0.00012345

# meta columns of the Outlet and Pump static tables that add_controllers_to_connector_nodes doesn't preserve
OUTLET_META_COLUMNS = [
    "meta_categorie",
    "meta_from_node_id",
    "meta_to_node_id",
    "meta_from_level",
    "meta_to_level",
    "meta_aanvoer",
]
PUMP_META_COLUMNS = [
    "meta_categorie",
    "meta_func_afvoer",
    "meta_func_aanvoer",
    "meta_func_circulatie",
    "meta_from_node_id",
    "meta_to_node_id",
    "meta_from_level",
    "meta_to_level",
]


# GeoPackage per water board in verwerkt/Crossings, with the hydro-objects, culverts and crossings, as also used in
# pipeline/<water board>/02_profiles.py
CROSSINGS = {
    "AmstelGooienVecht": "agv_crossings_v05.gpkg",
    "Delfland": "delfland_crossings_v08.gpkg",
    "HollandsNoorderkwartier": "hhnk_crossings_v26.gpkg",
    "HollandseDelta": "hd_crossings_v06.gpkg",
    "Rijnland": "rijnland_crossings_v05.gpkg",
    "Rivierenland": "wsrl_crossings_v06.gpkg",
    "Scheldestromen": "scheldestromen_crossings_v02.gpkg",
    "SchielandendeKrimpenerwaard": "hhsk_crossings_voor_profielen_met_OG_hydroobjecten.gpkg",
    "WetterskipFryslan": "wetterskip_crossings_v06.gpkg",
    "Zuiderzeeland": "zzl_crossings_v05.gpkg",
}


@dataclass(frozen=True)
class WaterBoardPaths:
    """Paths of the input and output of one water board in the peilbeheerst pipeline."""

    cloud: CloudStorage
    waterschap: str
    base_model_versie: str

    @property
    def base_model_toml(self) -> Path:
        return self.cloud.joinpath(
            self.waterschap, "modellen", f"{self.waterschap}_boezemmodel_{self.base_model_versie}", "ribasim.toml"
        )

    @property
    def feedback_form(self) -> Path:
        return self.cloud.joinpath(
            self.waterschap, "verwerkt/Feedback Formulier", f"feedback_formulier_{self.waterschap}.xlsx"
        )

    @property
    def feedback_form_log(self) -> Path:
        return self.cloud.joinpath(
            self.waterschap, "verwerkt/Feedback Formulier", f"feedback_formulier_{self.waterschap}_LOG.xlsx"
        )

    @property
    def ws_grenzen(self) -> Path:
        return self.cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/waterschap.gpkg")

    @property
    def rws_grenzen(self) -> Path:
        return self.cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/Rijkswaterstaat.gpkg")

    @property
    def profielen_dir(self) -> Path:
        return self.cloud.joinpath(self.waterschap, "verwerkt/profielen")

    @property
    def crossings(self) -> Path:
        return settings.ribasim_nl_data_dir / self.waterschap / "verwerkt/Crossings" / CROSSINGS[self.waterschap]

    @property
    def bgt_water(self) -> Path:
        return BGT_WATER

    @property
    def top10nl(self) -> Path:
        return settings.ribasim_nl_data_dir / "Basisgegevens/Top10NL/top10nl_Compleet.gpkg"

    def model_dir(self, stage: Literal["feedback", "profiles", "forcing"]) -> Path:
        """Directory of the model written by a pipeline stage."""
        return self.cloud.model_dir(self.waterschap, stage)

    def feedback_processor(self, work_dir: Path, use_validation: bool = True) -> RibasimFeedbackProcessor:
        """Processor of the feedback form, writing its model to `work_dir`."""
        work_dir.mkdir(parents=True, exist_ok=True)
        return RibasimFeedbackProcessor(
            "HKV",
            self.waterschap,
            self.base_model_versie,
            self.feedback_form,
            self.base_model_toml,
            work_dir,
            self.feedback_form_log,
            use_validation=use_validation,
        )


def process_feedback_form(paths: WaterBoardPaths, use_validation: bool = True) -> Model:
    """Apply the feedback form to the base model, and read the resulting model.

    With `use_validation=False` the model is written without validation, for forms that leave the model
    temporarily invalid.
    """
    work_dir = paths.model_dir("feedback")
    paths.feedback_processor(work_dir, use_validation=use_validation).run()
    with warnings.catch_warnings():
        warnings.simplefilter(action="ignore", category=FutureWarning)
        model = Model.read(work_dir / "ribasim.toml")
        model.set_crs("EPSG:28992")
    return model


def check_basin_meta_categorie(model: Model) -> None:
    """Raise if not all basins have a meta_categorie."""
    basin_node_df = model.filter_nodes("Basin")
    missing_node_ids = basin_node_df.loc[basin_node_df["meta_categorie"].isna()].index.tolist()
    if missing_node_ids:
        raise ValueError(
            "Not all basins have a meta_categorie assigned. "
            f"Missing meta_categorie for basin node IDs: {missing_node_ids}"
        )


def keep_model_supply_basins(processor: RibasimFeedbackProcessor, model: Model) -> None:
    """Keep only the Basins of the model in the aanvoer settings of the feedback form, for `set_aanvoer_flags`."""
    assert model.basin.node is not None and model.basin.node.df is not None
    basin_ids = set(model.basin.node.df.index)
    processor._basin_aanvoer_on = tuple(n for n in (processor.basin_aanvoer_on or ()) if n in basin_ids)
    processor._basin_aanvoer_off = tuple(n for n in (processor.basin_aanvoer_off or ()) if n in basin_ids)


def assign_pump_and_basin_names(
    model: Model,
    paths: WaterBoardPaths,
    *,
    param_name: str | None = None,
    pump_max_distance: float = 100,
    pump_flow_rate_factor: float = 1 / 60,
) -> None:
    """Assign the names and capacities of the pumping stations, and the names of the aggregation areas.

    :param model: model
    :param paths: paths of the water board
    :param param_name: GeoPackage with the parameters of the water board, defaults to `<water board>.gpkg`
    :param pump_max_distance: maximum distance between a Pump and its pumping station [m]
    :param pump_flow_rate_factor: factor from the capacity of the pumping stations to m3/s
    """
    assign_metadata = AssignMetaData(
        authority=paths.waterschap,
        model_name=model,
        param_name=param_name if param_name is not None else f"{paths.waterschap}.gpkg",
        sync=False,
    )
    assign_metadata.add_meta_to_pumps(
        layer="gemaal",
        mapper={
            "meta_name": {"node": ["name"]},
            "meta_capaciteit": {"static": ["flow_rate", "max_flow_rate"]},
        },
        max_distance=pump_max_distance,
        factor_flowrate=pump_flow_rate_factor,
    )
    assign_metadata.add_meta_to_basins(
        layer="aggregation_area",
        mapper={"meta_name": {"node": ["name"]}},
        min_overlap=0.95,
    )


def set_manning_resistance_outside_bergend(model: Model, length: float, manning_n: float) -> None:
    """Set the length and Manning's n of the ManningResistance nodes that don't connect a storing ('bergend') basin.

    The ManningResistance nodes of storing basins are parametrized by their area in `set_basin_profiles`.
    """
    static_df = model.manning_resistance.static.df
    assert static_df is not None and model.link.df is not None
    basin_node_df = model.filter_nodes("Basin")
    bergend_basin_ids = basin_node_df.index[basin_node_df["meta_categorie"] == "bergend"]
    link_df = model.link.df
    bergend_manning_ids = set(link_df.loc[link_df["from_node_id"].isin(bergend_basin_ids), "to_node_id"]) | set(
        link_df.loc[link_df["to_node_id"].isin(bergend_basin_ids), "from_node_id"]
    )
    is_bergend = static_df["node_id"].isin(bergend_manning_ids)
    assert is_bergend.sum() == len(bergend_basin_ids), "Expected one ManningResistance node per storing basin"
    static_df.loc[~is_bergend, ["length", "manning_n"]] = [length, manning_n]


def set_boundary_conditions(
    model: Model, paths: WaterBoardPaths, *, level_boundary_levels: tuple[float, float]
) -> Model:
    """Set the forcing and level boundaries, and the target levels around the Outlets and Pumps.

    The Basins get meteo and groundwater from the LHM budgets, see `add_lhm_forcing`. The LevelBoundaries alternate
    between a hypothetical dry and wet level.

    :param model: model of the profiles stage
    :param paths: paths of the water board
    :param level_boundary_levels: level of the LevelBoundaries when draining and when supplying [m NAP]
    :return: the model with forcing
    """
    check_basin_meta_categorie(model)
    model = add_lhm_forcing(model, paths.model_dir("forcing"), sprinkling="secondary")

    drain_level, supply_level = level_boundary_levels
    ribasim_param.set_hypothetical_dynamic_level_boundaries(model, STARTTIME, ENDTIME, drain_level, supply_level)
    ribasim_param.find_upstream_downstream_target_levels(model, node="outlet")
    ribasim_param.find_upstream_downstream_target_levels(model, node="pump")
    return model


def categorize_nodes(model: Model, *, supply_outlets: bool = True) -> None:
    """Categorize the Outlets and Pumps after `set_aanvoer_flags`, for `node_function_table`.

    :param model: model with the aanvoer flags of `set_aanvoer_flags`
    :param supply_outlets: label supplying Outlets with `supply.SupplyOutlet`, and overrule routes that are not
        hoofdwater where hoofdwater supplies directly
    """
    if supply_outlets:
        supply.SupplyOutlet(model).exec(overruling_enabled=True)
    ribasim_param.identify_node_meta_categorie(model, aanvoer_enabled=True)


def node_function_table(model: Model) -> gpd.GeoDataFrame:
    """Get the table with the function of each Outlet and Pump, see `ribasim_nl.control`.

    :param model: model of which the nodes are categorized with `categorize_nodes`
    :return: the from and to node and the function of each Outlet and Pump, without demand
    """
    assert model.basin.area.df is not None
    model.basin.area.df["meta_streefpeil"] = model.basin.area.df["meta_streefpeil"].astype(float)
    node_functions = add_function_to_peilbeheerst_node_table(model, get_node_table_with_from_to_node_ids(model))
    node_functions["demand"] = None
    return node_functions


def add_controllers(
    model: Model,
    node_functions: gpd.GeoDataFrame,
    *,
    remove_duplicates: bool,
    method: Literal["merge", "combine_first"] = "merge",
) -> None:
    """Add the controllers of `node_functions` to the Outlets and Pumps, keeping their meta columns.

    :param model: model
    :param node_functions: table of `node_function_table`
    :param remove_duplicates: remove DiscreteControls that control the same node
    :param method: how to restore the meta columns, see `preserve_connector_meta`
    """
    with preserve_connector_meta(model, method=method):
        add_controllers_to_connector_nodes(
            model, node_functions, target_level_column="meta_streefpeil", drain_capacity=20
        )
        if remove_duplicates:
            remove_duplicate_controls(model)


def complete_parametrization(
    model: Model,
    paths: WaterBoardPaths,
    *,
    custom_nodes: dict[int, str | None] | None = None,
    rws_buffer: float = 1000,
    fill_na_authority: str = "Rijkswaterstaat",
    known_pump_flow_rates: bool | None = True,
) -> Model:
    """Clean the tables, set dynamic level limits, assign the neighbouring authorities, and add the inflows.

    :param model: model with controllers
    :param paths: paths of the water board
    :param custom_nodes: authority of boundary nodes that `AssignAuthorities` does not find
    :param rws_buffer: distance to Rijkswaterstaat waters within which boundaries are assigned to it [m]
    :param fill_na_authority: authority of the boundaries that are not assigned otherwise
    :param known_pump_flow_rates: whether the Pump capacities are known, or None to keep `meta_known_flow_rate`;
        they are unknown where they are not set or zero
    :return: the model, of which the unknown capacities can be set with `set_design_capacities`
    """
    ribasim_param.clean_tables(model, paths.waterschap)
    model.basin.static.df = None
    ribasim_param.set_dynamic_min_upstream_max_downstream(model)
    model = AssignAuthorities(
        ribasim_model=model,
        waterschap=paths.waterschap,
        ws_grenzen_path=paths.ws_grenzen,
        RWS_grenzen_path=paths.rws_grenzen,
        RWS_buffer=rws_buffer,
        custom_nodes=custom_nodes if custom_nodes is not None else {},
        fill_na_authority=fill_na_authority,
    ).assign_authorities()
    model = add_inflows(model, paths.model_dir("forcing"))

    assert model.outlet.static.df is not None
    assert model.pump.static.df is not None
    model.outlet.static.df["meta_known_flow_rate"] = False
    pump_static_df = model.pump.static.df
    if known_pump_flow_rates is not None:
        pump_static_df["meta_known_flow_rate"] = known_pump_flow_rates
    unknown = pump_static_df["max_flow_rate"].isna() | (pump_static_df["max_flow_rate"] == 0)
    pump_static_df.loc[unknown, "meta_known_flow_rate"] = False
    return model


@contextmanager
def preserve_connector_meta(model: Model, method: Literal["merge", "combine_first"] = "merge") -> Generator[None]:
    """Restore Outlet and Pump meta columns after adding controllers, which may change or add node_ids.

    With "merge", the meta columns are replaced by their values before the block. With "combine_first",
    only missing values are filled in.
    """
    assert model.outlet.static.df is not None
    assert model.pump.static.df is not None
    outlet_meta = model.outlet.static.df[["node_id", *OUTLET_META_COLUMNS]].copy()
    pump_meta = model.pump.static.df[["node_id", *PUMP_META_COLUMNS]].copy()
    yield
    assert model.outlet.static.df is not None
    assert model.pump.static.df is not None
    if method == "merge":
        model.outlet.static.df = model.outlet.static.df.drop(columns=OUTLET_META_COLUMNS).merge(
            outlet_meta, on="node_id", how="left"
        )
        model.pump.static.df = model.pump.static.df.drop(columns=PUMP_META_COLUMNS).merge(
            pump_meta, on="node_id", how="left"
        )
    else:
        model.outlet.static.df = (
            model.outlet.static.df.set_index("node_id").combine_first(outlet_meta.set_index("node_id")).reset_index()
        )
        model.pump.static.df = (
            model.pump.static.df.set_index("node_id").combine_first(pump_meta.set_index("node_id")).reset_index()
        )


def write_and_run_forcing_model(model: Model, paths: WaterBoardPaths) -> None:
    """Write and run the forcing model and its steady state regression variants, and check the results.

    See `finish_regional_model`, which snaps the model to the hydro-objects of the profile generation.
    """
    check_basin_meta_categorie(model)
    bound_supply_inflows(model)

    def snap(model: Model) -> Model:
        """Route links along the hydro-objects, move Basin nodes onto them, and merge overlapping links."""
        model = snap_model(
            model,
            paths.profielen_dir,
            crossings_path=paths.crossings,
            water_path=paths.bgt_water,
            top10nl_path=paths.top10nl,
        )
        return junctionify(model)

    finish_regional_model(
        model,
        paths.model_dir("forcing") / "ribasim.toml",
        paths.waterschap,
        snap=snap,
        steady_state_regression=True,
    )
