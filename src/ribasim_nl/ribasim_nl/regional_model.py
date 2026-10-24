"""Steps shared by the regional models of all water authorities, vrij afwaterend and peilbeheerst alike.

`add_lhm_forcing` sets the forcing, `add_inflows` adds the foreign inflows and the RWZIs, and `finish_regional_model`
lays out, writes, runs and checks the model. The pipelines of both kinds of water authorities call them, see
`pipeline/07_add_dynamic_forcing.py` and `ribasim_nl.workflow`.
"""

import datetime
from collections.abc import Callable
from pathlib import Path
from typing import Literal

import xarray as xr

from ribasim_nl.assign_lhm_fractions import assign_lhm_fractions
from ribasim_nl.assign_offline_budgets import AssignOfflineBudgets
from ribasim_nl.controle_output import DYNAMIC_FORCING_METRICS, Control
from ribasim_nl.layout import tidy_layout
from ribasim_nl.model import Model
from ribasim_nl.observations import add_observations
from ribasim_nl.parametrization.manning_resistance_table import read_manning_water, update_manning_resistance_reach
from ribasim_nl.performance import write_performance
from ribasim_nl.ribasim_parametrization import write_steady_state_regression_models
from ribasim_nl.rwzi import attach_rwzi_junctions, merge_rwzi_model
from ribasim_nl.set_forcing import SetDynamicForcing
from ribasim_nl.settings import settings
from ribasim_nl.transboundary_inflow import add_transboundary_inflow, import_transboundary_inflow

# simulation period and output interval of the regional models
STARTTIME = datetime.datetime(2017, 1, 1)
ENDTIME = datetime.datetime(2020, 1, 1)
SAVEAT = 3600 * 24

DATA_DIR = settings.ribasim_nl_data_dir
LHM_BUDGETS = DATA_DIR / "Basisgegevens/LHM/4.3/results/LHM_433_budgets_update_makkink"
TRANSBOUNDARY_INFLOW = DATA_DIR / "Basisgegevens/BuitenlandseAanvoer/aangeleverd/BuitenlandseAanvoer_V5.xlsx"
RWZI_TOML = DATA_DIR / "Rijkswaterstaat/modellen/rwzi/rwzi.toml"
RWZI_COVERAGE = DATA_DIR / "Rijkswaterstaat/modellen/rwzi/meta/RWZI_coordinates_model_coverage.geojson"
QLR = DATA_DIR / "Basisgegevens/QGIS_qlr/output_controle_cc.qlr"
# LHM budget of sprinkling from surface water
SPRINKLING_BUDGET = "bdgpssw_m3d"


def add_lhm_forcing(
    model: Model,
    output_dir: Path,
    *,
    sprinkling: Literal["primary", "secondary"],
    assign_fractions: bool = False,
    fraction_prefix: str | None = None,
) -> Model:
    """Add the meteo of the LHM, and its groundwater budgets as drainage, infiltration and surface runoff.

    The budgets per Basin are written to `mfms_budgets.arrow` in `output_dir`, for later verification, and their
    assignment is plotted to `results/assign_validation.png`.

    :param model: model of which all Basins have a `meta_categorie`
    :param output_dir: directory of the model
    :param sprinkling: Basins from which sprinkling from surface water is drawn; the vrij afwaterend models draw it
        from the primary Basins, the peilbeheerst models from the secondary (bergend) Basins
    :param assign_fractions: compute the (sub-)fractions of the budgets, see `AssignOfflineBudgets.compute_budgets`
    :param fraction_prefix: prefix of the fractions, the `waterbeheercode` of the authority
    :return: the model with forcing
    """
    budgets = xr.open_zarr(str(LHM_BUDGETS)).sel(time=slice(STARTTIME, ENDTIME))
    offline_budgets = AssignOfflineBudgets(budgets)
    primary_budgets = offline_budgets.primary_budget_keys - {SPRINKLING_BUDGET}
    secondary_budgets = offline_budgets.secondary_budget_keys - {SPRINKLING_BUDGET}
    (primary_budgets if sprinkling == "primary" else secondary_budgets).add(SPRINKLING_BUDGET)
    model = SetDynamicForcing(model=model, budgets=budgets, startdate=STARTTIME, enddate=ENDTIME).add()
    _, budgets_df = offline_budgets.compute_budgets(
        model,
        primary_budgets=primary_budgets,
        secondary_budgets=secondary_budgets,
        assign_fractions=assign_fractions,
        fraction_prefix=fraction_prefix,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    budgets_df.to_feather(output_dir / "mfms_budgets.arrow")
    assign_validation_png = output_dir / "results" / "assign_validation.png"
    assign_validation_png.parent.mkdir(parents=True, exist_ok=True)
    offline_budgets.plot_assign_validation(model, path=assign_validation_png)
    return model


def add_inflows(model: Model, output_dir: Path) -> Model:
    """Add the measured foreign inflows to the FlowBoundaries named after them, the RWZIs, and the LHM fractions.

    :param model: model
    :param output_dir: directory of the model, of which `meta/RWZI_coordinates_lhm_coverage.geojson` gets the RWZIs
    :return: the model with inflows
    """
    if model.flow_boundary.node is not None and model.flow_boundary.node.df is not None:
        add_transboundary_inflow(model, import_transboundary_inflow(TRANSBOUNDARY_INFLOW, STARTTIME, ENDTIME, model))
    model = merge_rwzi_model(model, RWZI_TOML, RWZI_COVERAGE, output_dir / "meta/RWZI_coordinates_lhm_coverage.geojson")
    assign_lhm_fractions(model)
    return model


def finish_regional_model(
    model: Model,
    toml_file: Path,
    authority: str,
    *,
    snap: Callable[[Model], Model],
    steady_state_regression: bool = False,
) -> Model:
    """Lay out, write, validate and run the model, use its final state as initial state, and check its results.

    The layout only changes the geometry, after all parametrization: `snap` routes the links along the waterways
    known for the authority, after which the BGT profile of ManningResistances follows their new links (see
    `update_manning_resistance_reach`), the RWZIs discharge on the links into their Basins (see #860), and nodes and links are
    moved apart. The results get `performance.xlsx` and `output_controle.gpkg`.

    :param model: parametrized model
    :param toml_file: TOML file to write the model to
    :param authority: water authority, as named in the koppeltabel of the observations
    :param snap: function that snaps the links and Basins to the waterways
    :param steady_state_regression: also write, run and check the dry and wet steady state variants
    :return: the model with its final state as initial state
    """
    model = snap(model)
    update_manning_resistance_reach(model, read_manning_water(model))
    model = attach_rwzi_junctions(model)
    model = add_observations(model, authority)
    model = tidy_layout(model)

    model.use_validation = True
    model.starttime = STARTTIME
    model.endtime = ENDTIME
    model.solver.saveat = SAVEAT
    if model.basin.time.df is not None:
        model.basin.time.filepath = Path("basin_time.nc")
    model.write(toml_file)
    if steady_state_regression:
        write_steady_state_regression_models(
            toml_file,
            {
                scenario: toml_file.parents[1] / f"{authority}_steady_state_{scenario}" / toml_file.name
                for scenario in ("dry", "wet")
            },
            authority=authority,
            qlr_path=QLR,
        )
    model.validate_ribasim_nl()

    result = model.run()
    if result.exit_code != 0:
        raise RuntimeError(f"Ribasim run failed for {toml_file} with exit code {result.exit_code}")
    model.update_state()
    model.basin.state.write()

    write_performance(model)
    Control(ribasim_toml=toml_file, qlr_path=QLR).run(metrics=DYNAMIC_FORCING_METRICS)
    return model
