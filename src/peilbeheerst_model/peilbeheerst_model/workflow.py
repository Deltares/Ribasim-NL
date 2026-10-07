"""Shared steps of the feedback and forcing scripts of the peilbeheerst water boards.

The scripts in `src/peilbeheerst_model/feedback` and `src/peilbeheerst_model/forcing` contain the
water board specific edits. The steps they have in common are defined here.
"""

import datetime
import warnings
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import xarray as xr
from ribasim import run_ribasim
from ribasim_nl.assign_offline_budgets import AssignOfflineBudgets

import peilbeheerst_model.ribasim_parametrization as ribasim_param
from peilbeheerst_model.controle_output import DYNAMIC_FORCING_METRICS, STATIC_FORCING_METRICS, Control
from peilbeheerst_model.ribasim_feedback_processor import RibasimFeedbackProcessor
from ribasim_nl import CloudStorage, Model, SetDynamicForcing, junctionify

# streefpeil used where it is unknown; we need one to create the profiles, Q(h)-relations, and af- and aanslag peil
UNKNOWN_STREEFPEIL = 0.00012345

# simulation period and output interval of the forcing models
STARTTIME = datetime.datetime(2017, 1, 1)
ENDTIME = datetime.datetime(2020, 1, 1)
SAVEAT = 3600 * 24

LHM_BUDGETS = "Basisgegevens/LHM/4.3/results/LHM_433_budgets_update_makkink"

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

    def model_dir(self, stage: Literal["feedback", "profiles", "forcing"]) -> Path:
        """Directory of the model written by a pipeline stage."""
        return self.cloud.model_dir(self.waterschap, stage)

    def qlr(self, mixed_conditions: bool) -> Path:
        """QGIS layer definition to inspect the model output with."""
        qlr_name = "output_controle_cc.qlr" if mixed_conditions else "output_controle_202502.qlr"
        return self.cloud.joinpath("Basisgegevens/QGIS_qlr", qlr_name)

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


def set_forcing(
    model: Model,
    cloud: CloudStorage,
    *,
    dynamic_conditions: bool,
    mixed_conditions: bool,
    aanvoer_conditions: bool,
    design_precipitation: float,
    design_evaporation: float,
    assign_validation_png: Path | None = None,
) -> Model:
    """Set the Basin forcing.

    Dynamic conditions use meteo and groundwater from the LHM budgets, mixed conditions a hypothetical
    dry and wet period with the design precipitation and evaporation (mm/day), and otherwise a static
    forcing. With `assign_validation_png`, the assignment of the LHM budgets is plotted to that file.
    """
    if dynamic_conditions:
        budgets = xr.open_zarr(str(cloud.joinpath(LHM_BUDGETS))).sel(time=slice(STARTTIME, ENDTIME))
        offline_budgets = AssignOfflineBudgets(budgets)
        model = SetDynamicForcing(model=model, budgets=budgets, startdate=STARTTIME, enddate=ENDTIME).add()
        offline_budgets.compute_budgets(model)
        if assign_validation_png is not None:
            assign_validation_png.parent.mkdir(parents=True, exist_ok=True)
            offline_budgets.plot_assign_validation(model, path=assign_validation_png)
    elif mixed_conditions:
        ribasim_param.set_hypothetical_dynamic_forcing(
            model, STARTTIME, ENDTIME, design_precipitation, design_evaporation
        )
    else:
        forcing_dict = {
            "precipitation": ribasim_param.convert_mm_day_to_m_sec(0 if aanvoer_conditions else design_precipitation),
            "potential_evaporation": ribasim_param.convert_mm_day_to_m_sec(
                design_evaporation if aanvoer_conditions else 0
            ),
            "drainage": ribasim_param.convert_mm_day_to_m_sec(0),
            "infiltration": ribasim_param.convert_mm_day_to_m_sec(0),
        }
        ribasim_param.set_static_forcing(2, "d", STARTTIME, forcing_dict, model)
    return model


@contextmanager
def preserve_connector_meta(model: Model, method: Literal["merge", "combine_first"] = "merge") -> Iterator[None]:
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


def write_and_run_forcing_model(
    model: Model,
    paths: WaterBoardPaths,
    *,
    mixed_conditions: bool,
    add_junctions: bool,
) -> None:
    """Write and run the forcing model, its steady state regression variants, and check the results."""
    output_dir = paths.model_dir("forcing")
    model_toml = output_dir / "ribasim.toml"
    qlr_path = paths.qlr(mixed_conditions)

    # add junctions last: a layout-only transformation merging overlapping flow links into a
    # Junction. Done after all parametrization so junctions never break adjacency/validation.
    if add_junctions:
        model = junctionify(model)

    model.use_validation = True
    model.starttime = STARTTIME
    model.endtime = ENDTIME
    model.solver.saveat = SAVEAT
    model.write(model_toml)
    ribasim_param.write_steady_state_regression_models(
        model_toml,
        {
            scenario: output_dir.parent / f"{paths.waterschap}_steady_state_{scenario}" / "ribasim.toml"
            for scenario in ("dry", "wet")
        },
        authority=paths.waterschap,
        qlr_path=qlr_path,
    )
    model.validate_ribasim_nl()

    # run model and use the final state as initial state
    run_ribasim(model_toml)
    model.update_state()
    model.basin.state.write()

    # model performance
    controle_output = Control(work_dir=output_dir, qlr_path=qlr_path)
    controle_output.run(metrics=DYNAMIC_FORCING_METRICS if mixed_conditions else STATIC_FORCING_METRICS)
