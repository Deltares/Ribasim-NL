from pathlib import Path

from pydantic import BaseModel

from ribasim_nl.model import Model
from ribasim_nl.parametrization.basin_tables import update_basin_profile, update_basin_state, update_basin_static
from ribasim_nl.parametrization.flow_boundary_table import update_flow_boundary_static
from ribasim_nl.parametrization.level_boundary_table import update_level_boundary_static
from ribasim_nl.parametrization.manning_resistance_table import update_manning_resistance_static
from ribasim_nl.parametrization.node_table import populate_function_column
from ribasim_nl.parametrization.pump_and_outlet_tables import update_pump_outlet_static


class Parameterize(BaseModel):
    model: Model
    static_data_xlsx: Path | None = None
    profiles_gpkg: Path | None = None
    precipitation_mm_per_day: int | None = None
    evaporation_mm_per_day: int | None = None
    boundary_flow_rate_m3_sec: float = 0.0

    def run(self, **kwargs):
        # update class properties
        for key, value in kwargs.items():
            if value is not None:
                setattr(self, key, value)

        # add meta_function as we will need that for further parametization of Outlet and Pump
        if "meta_function" not in self.model.node_table().df.columns:
            for node_type in ["Pump", "Outlet"]:
                populate_function_column(model=self.model, static_data_xlsx=self.static_data_xlsx, node_type=node_type)

        # parameterize Pump and Outlet nodes
        for node_type in ["Pump", "Outlet"]:
            update_pump_outlet_static(
                self.model,
                node_type=node_type,
                static_data_xlsx=self.static_data_xlsx,
                code_column="meta_code_waterbeheerder",
            )

        # ManningResistance
        update_manning_resistance_static(self.model, profiles_gpkg=self.profiles_gpkg)

        # Basin profile and state
        update_basin_profile(model=self.model)
        update_basin_state(model=self.model)
        update_basin_static(model=self.model, precipitation_mm_per_day=10)

        # LevelBoundaries
        update_level_boundary_static(
            model=self.model,
            static_data_xlsx=self.static_data_xlsx,
            code_column="meta_code_waterbeheerder",
        )

        # FlowBoundaries
        update_flow_boundary_static(
            model=self.model,
            code_column="meta_code_waterbeheerder",
            meta_values={"meta_categorie": "buitenlandse aanvoer"},
            default_values={"flow_rate": 0.0},
        )
