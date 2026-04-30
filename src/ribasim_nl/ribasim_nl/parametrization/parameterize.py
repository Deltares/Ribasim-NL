from pathlib import Path

from pydantic import BaseModel

from ribasim_nl.berging import update_primary_basin_profiles
from ribasim_nl.model import Model
from ribasim_nl.parametrization.basin_tables import update_basin_state, update_basin_static
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
    max_pump_flow_rate: float | None = None

    def run(self, **kwargs) -> None:
        print("Start Parameterize.run()")
        print("kwargs ontvangen:", kwargs)

        # update class properties
        for key, value in kwargs.items():
            if value is not None:
                setattr(self, key, value)
                print(f"  Set self.{key} = {value}")

        print(f"static_data_xlsx: {self.static_data_xlsx}")
        if self.static_data_xlsx and not Path(self.static_data_xlsx).exists():
            raise FileNotFoundError(f"Bestand bestaat niet: {self.static_data_xlsx}")
        else:
            print("  static_data_xlsx bestaat lokaal.")

        # add meta_function as we will need that for further parametrization
        if "meta_function" not in self.model.node.df.columns:
            print("meta_function kolom ontbreekt -> wordt toegevoegd via populate_function_column()")
            for node_type in ["Pump", "Outlet"]:
                print(f"  Populeer meta_function voor {node_type}...")
                populate_function_column(model=self.model, static_data_xlsx=self.static_data_xlsx, node_type=node_type)
            print("  meta_function kolom toegevoegd.")
        else:
            print("  meta_function kolom al aanwezig.")

        # parameterize Pump and Outlet nodes
        for node_type in ["Pump", "Outlet"]:
            print(f"Start parametrisatie voor {node_type}")
            try:
                update_pump_outlet_static(
                    self.model,
                    node_type=node_type,
                    static_data_xlsx=self.static_data_xlsx,
                    code_column="meta_code_waterbeheerder",
                )
                print(f"  {node_type} succesvol geparametriseerd.")
            except Exception as e:
                raise Exception(f"Fout bij {node_type}: {type(e).__name__}: {e}") from e

        if self.max_pump_flow_rate is not None:
            print(f"max_pump_flow_rate: {self.max_pump_flow_rate}")
            mask = self.model.pump.static.df.flow_rate > self.max_pump_flow_rate
            if mask.any():
                print(f"  {mask.sum()} pompen beperkt tot {self.max_pump_flow_rate}")
                self.model.pump.static.df.loc[mask, "flow_rate"] = self.max_pump_flow_rate
            else:
                print("  Geen pompen boven max_pump_flow_rate.")

        print("Update ManningResistance...")
        update_manning_resistance_static(self.model, profiles_gpkg=self.profiles_gpkg)
        print("  ManningResistance bijgewerkt.")

        print("Update Basin-profiel en -state...")
        update_primary_basin_profiles(model=self.model)
        update_basin_state(model=self.model)
        update_basin_static(
            model=self.model,
            precipitation_mm_per_day=self.precipitation_mm_per_day,
            evaporation_mm_per_day=self.evaporation_mm_per_day,
        )
        print("  Basin-parameters ingesteld.")

        print("Update LevelBoundaries...")
        update_level_boundary_static(
            model=self.model,
            static_data_xlsx=self.static_data_xlsx,
            code_column="meta_code_waterbeheerder",
        )
        print("  LevelBoundaries bijgewerkt.")

        print("Parameterize.run() voltooid zonder fouten.")
