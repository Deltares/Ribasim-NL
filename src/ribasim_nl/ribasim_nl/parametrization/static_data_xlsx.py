from collections.abc import Callable
from pathlib import Path
from typing import Literal

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Border
from pydantic import BaseModel, ConfigDict

from ribasim_nl.case_conversions import pascal_to_snake_case
from ribasim_nl.model import Model
from ribasim_nl.parametrization.damo_profiles import DAMOProfiles, profile_level
from ribasim_nl.parametrization.empty_table import empty_table_df

description = [
    {"sheet": "defaults", "beschrijving": "default settings bij geen-data"},
    {"sheet": "Outlet & Pump", "beschrijving": "gemodelleerde instellingen bij Ribasim Pump en Outlet nodes"},
    {"kolom": "flow_rate", "beschrijving": "gemodelleerde capaciteit (m3/s) van het kunstwerk"},
    {
        "kolom": "code",
        "beschrijving": "code waterbeheerder van het object",
    },
    {
        "kolom": "name",
        "beschrijving": "naam van het object",
    },
    {
        "kolom": "flow_rate_mm_per_day",
        "beschrijving": "gemodelleerde capaciteit (mm/dag) van het kunstwerk t.o.v. voor het bovenstrooms/benedenstroomse (uitlaat/inlaat) stroomgebied",
    },
    {
        "kolom": "categorie",
        "beschrijving": "categorie van het kunstwerk. Is de logische link tussen de Pump/Outlet en defaults table.",
    },
    {
        "kolom": "opmerking_waterbeheerder",
        "beschrijving": "een opmerking van de waterbeheerder ter duiding van de ingevulde waarde(n)",
    },
    {"kolom": "upstream_level_offset", "beschrijving": "afslagpunt (meter) boven bovenstrooms streefpeil"},
    {"kolom": "downstream_level_offset", "beschrijving": "afslagpunt (meter) onder benedenstrooms streefpeil"},
]

defaults: dict[str, dict[str, object]] = {
    "Afvoergemaal": {
        "upstream_level_offset": 0.0,
        "downstream_level_offset": pd.NA,
        "flow_rate": pd.NA,
        "flow_rate_mm_per_day": 15,
        "function": "outlet",
    },
    "Aanvoergemaal": {
        "upstream_level_offset": 0.2,
        "downstream_level_offset": 0.0,
        "flow_rate": pd.NA,
        "flow_rate_mm_per_day": 4,
        "function": "inlet",
    },
    "Uitlaat": {
        "upstream_level_offset": 0.0,
        "downstream_level_offset": pd.NA,
        "flow_rate": pd.NA,
        "flow_rate_mm_per_day": 50,
        "function": "outlet",
    },
    "Inlaat": {
        "upstream_level_offset": 0.2,
        "downstream_level_offset": 0.0,
        "flow_rate": pd.NA,
        "flow_rate_mm_per_day": 4,
        "function": "inlet",
    },
}


class StaticData(BaseModel):
    xlsx_path: Path
    model: Model
    default_dict: dict[str, dict[str, object]] = defaults
    description_list: list[dict[str, str]] = description
    outlet: pd.DataFrame | None = None
    pump: pd.DataFrame | None = None
    basin: pd.DataFrame | None = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @property
    def defaults(self) -> pd.DataFrame:
        df = pd.DataFrame.from_dict(self.default_dict, orient="index")
        df.index.name = "categorie"
        return df

    @property
    def description(self) -> pd.DataFrame:
        df = pd.DataFrame(self.description_list)
        df.sort_values(by=["sheet", "kolom"], inplace=True)
        return df[["sheet", "kolom", "beschrijving"]]

    def reset_data_frame(self, node_type):
        if node_type in ["Pump", "Outlet"]:
            columns = ["node_id", "name", "code", "flow_rate", "min_upstream_level", "max_downstream_level"]
            extra_columns = ["categorie", "opmerking_waterbeheerder"]
            df = empty_table_df(
                model=self.model,
                table_type="Static",
                node_type=node_type,
                meta_columns=["meta_code_waterbeheerder", "name"],
            )
            df.rename(columns={"meta_code_waterbeheerder": "code"}, inplace=True)
            if node_type == "Pump":
                df["categorie"] = "Afvoergemaal"
            if node_type == "Outlet":
                df["categorie"] = "Uitlaat"
            df["opmerking_waterbeheerder"] = ""
            df = df[columns + extra_columns]
        elif node_type == "Basin":
            df = empty_table_df(
                model=self.model,
                table_type="Static",
                node_type=node_type,
                meta_columns=["meta_categorie"],
            )
            df.set_index("node_id", inplace=True)

            # add meta_code_waterbeheerder
            assert self.model.basin.area.df is not None
            area_df = self.model.basin.area.df.set_index("node_id")
            if "meta_code_waterbeheerder" in area_df.columns:
                mask = area_df.meta_code_waterbeheerder.notna()
                df.loc[mask[mask].index, "code_peilgebied"] = area_df[mask]["meta_code_waterbeheerder"]
            else:
                df.loc[:, "code_peilgebied"] = pd.Series(dtype=str)

            # add streefpeil
            if "meta_code_waterbeheerder" in area_df.columns:
                mask = area_df.meta_streefpeil.notna()
                df.loc[mask[mask].index, "streefpeil"] = area_df[mask]["meta_streefpeil"]
            else:
                df.loc[:, "streefpeil"] = pd.Series(dtype=float)

            # add profielid
            df["profielid"] = pd.Series(dtype=str)

            # sanitize
            df.rename(columns={"meta_categorie": "categorie"}, inplace=True)
            df = df.reset_index(drop=False)[["node_id", "categorie", "code_peilgebied", "profielid", "streefpeil"]]
        setattr(self, pascal_to_snake_case(node_type), df)
        return getattr(self, pascal_to_snake_case(node_type))

    def add_series(self, node_type, series, fill_na: bool = False) -> None:
        col = series.index.name

        # get dataframe
        df = getattr(self, pascal_to_snake_case(node_type))
        if df is None:
            df = self.reset_data_frame(node_type=node_type)

        # sanitize series to existing codes in node_type
        series = series[series.index.isin(df[col])]

        # mask table where row in series and set data
        mask = df[col].isin(series.index)
        if fill_na:
            mask = mask & df[series.name].isna()
        df.loc[mask, series.name] = series[df[mask][col]].to_numpy()

    def fill_min_upstream_level_from_profiles(
        self, node_type: Literal["Outlet", "Pump"], profiles_df: pd.DataFrame, divisor: float = 2
    ) -> None:
        """Fill missing min_upstream_level from the profile on the link flowing into the node.

        Args:
            node_type: Outlet or Pump
            profiles_df: profiles indexed by profile id, with bottom_level and invert_level
            divisor: the level is at 1/divisor of the profile depth above the bottom level
        """
        df = getattr(self, pascal_to_snake_case(node_type))
        assert df is not None, f"reset the {node_type} data frame first"
        link_df = self.model.link.df
        assert link_df is not None
        node_ids = df[df.min_upstream_level.isna()].node_id.to_numpy()
        profile_ids = [
            link_df[link_df.to_node_id == node_id].iloc[0]["meta_profielid_waterbeheerder"] for node_id in node_ids
        ]
        levels = profile_level(profiles_df, profile_ids, divisor=divisor)
        min_upstream_level = pd.Series(levels, index=pd.Index(node_ids, name="node_id"), name="min_upstream_level")
        self.add_series(node_type=node_type, series=min_upstream_level, fill_na=True)

    def fill_streefpeil_from_profiles(
        self, damo_profiles: DAMOProfiles, profiles_df: pd.DataFrame, divisor: float = 2
    ) -> None:
        """Fill missing Basin streefpeil and profielid from the profile with the lowest level around the basin.

        Args:
            damo_profiles: profiles to select the profile id per basin from
            profiles_df: processed profiles indexed by profile id, with bottom_level and invert_level
            divisor: the level is at 1/divisor of the profile depth above the bottom level
        """
        assert self.basin is not None, "reset the Basin data frame first"
        node_ids = self.basin[self.basin.streefpeil.isna()].node_id.to_numpy()
        profile_ids = [damo_profiles.get_profile_id(node_id) for node_id in node_ids]
        levels = profile_level(profiles_df, profile_ids, divisor=divisor)
        index = pd.Index(node_ids, name="node_id")
        self.add_series(node_type="Basin", series=pd.Series(profile_ids, index=index, name="profielid"), fill_na=True)
        self.add_series(node_type="Basin", series=pd.Series(levels, index=index, name="streefpeil"), fill_na=True)

    def fill_streefpeil_from_downstream_structures(
        self, sets_level: Callable[[pd.Series], pd.Series], fill_na: bool = True
    ) -> None:
        """Fill missing Basin streefpeil from the lowest min_upstream_level of the Outlets and Pumps downstream.

        Structures that set the level, such as weirs, and Pumps take precedence over others, such as culverts. Outlets
        downstream of a Basin get the level that sets it, and Outlets and Pumps without a min_upstream_level get the
        streefpeil of the Basin upstream.

        Args:
            sets_level: whether the Outlets with these codes set the level of the Basin upstream
            fill_na: only fill missing Basin streefpeil, instead of overwriting it
        """
        assert self.basin is not None and self.outlet is not None and self.pump is not None, "reset the data frames"
        model = self.model
        assert model.link.df is not None
        node_ids = self.basin[self.basin.streefpeil.isna()].node_id.to_numpy()
        ds_node_ids = []
        for node_id in node_ids:
            try:
                ds = model.downstream_node_id(int(node_id))
                if isinstance(ds, pd.Series):
                    ds_node_ids.append(ds.to_list())
                else:
                    ds_node_ids.append([ds])
            except KeyError:
                ds_node_ids.append([])

        ds_node_ids = pd.Series(ds_node_ids, index=node_ids).explode()
        ds_node_ids = ds_node_ids[ds_node_ids.isin(self.outlet.node_id) | ds_node_ids.isin(self.pump.node_id)]
        combined = pd.concat([self.outlet, self.pump])
        combined = combined.reset_index(drop=True)
        pump_node_ids = self.pump.node_id.values
        combined["source"] = combined.apply(lambda row: "pump" if row["node_id"] in pump_node_ids else "outlet", axis=1)

        valid_ids = ds_node_ids[ds_node_ids.isin(combined["node_id"])]
        streefpeil = combined.set_index("node_id").loc[valid_ids.to_list(), ["min_upstream_level", "code", "source"]]
        streefpeil["basin_node_id"] = valid_ids.loc[valid_ids.isin(streefpeil.index)].index
        streefpeil["node_id"] = streefpeil.index  # outlet/pump node_id
        streefpeil = streefpeil.set_index("basin_node_id")
        streefpeil.dropna(inplace=True)

        # sort so that the lowest min_upstream_level of each Basin comes first
        streefpeil = streefpeil.sort_index().sort_values(by="min_upstream_level", inplace=False)
        level_setting = streefpeil[sets_level(streefpeil["code"]) | (streefpeil["source"] == "pump")]
        level_setting_first = level_setting.groupby(level=0).first()
        all_first = streefpeil.groupby(level=0).first()

        # the other Outlets downstream get the min_upstream_level of the structure that sets the level
        streefpeil_update = streefpeil.loc[streefpeil.index.isin(level_setting_first.index)].copy()
        streefpeil_update["min_upstream_level"] = streefpeil_update.index.map(
            level_setting_first["min_upstream_level"].to_dict()
        )
        min_upstream_level_outlets = streefpeil_update.set_index("node_id")["min_upstream_level"]
        min_upstream_level_outlets.index.name = "node_id"
        self.add_series(node_type="Outlet", series=min_upstream_level_outlets, fill_na=False)

        # update basin levels
        streefpeil = level_setting_first.combine_first(all_first)
        streefpeil = streefpeil["min_upstream_level"]
        streefpeil.index.name = "node_id"
        streefpeil.name = "streefpeil"
        self.add_series(node_type="Basin", series=streefpeil, fill_na=fill_na)

        # Outlets and Pumps without min_upstream_level get the streefpeil of the Basin upstream
        for node_type in ("Outlet", "Pump"):
            static_df = getattr(self, node_type.lower())
            missing_min_level = static_df[static_df.min_upstream_level.isna()]
            upstream_basin_ids = model.link.df.set_index("to_node_id").loc[missing_min_level.node_id].from_node_id
            upstream_basin_levels = self.basin.set_index("node_id").reindex(upstream_basin_ids)["streefpeil"]
            upstream_basin_levels.index = missing_min_level.node_id
            upstream_basin_levels.name = "min_upstream_level"
            self.add_series(node_type=node_type, series=upstream_basin_levels.dropna(), fill_na=False)

    def fill_streefpeil_from_downstream_levels(self, include_pumps: bool = False) -> None:
        """Fill missing Basin streefpeil from the lowest level of the nodes downstream.

        Args:
            include_pumps: also use the min_upstream_level of Pumps, besides the streefpeil of Basins and the
                min_upstream_level of Outlets
        """
        assert self.basin is not None and self.outlet is not None, "reset the data frames"
        model = self.model
        node_ids = self.basin[self.basin.streefpeil.isna()].node_id.to_numpy()
        ds_node_ids = (model.downstream_node_id(i) for i in node_ids)
        ds_node_ids = [i.to_list() if isinstance(i, pd.Series) else [i] for i in ds_node_ids]
        ds_node_ids = pd.Series(ds_node_ids, index=node_ids).explode()

        frames = [self.basin, self.outlet, self.pump] if include_pumps else [self.basin, self.outlet]
        ds_levels = (
            pd.concat(frames, ignore_index=True)
            .set_index("node_id")["min_upstream_level"]
            .dropna()
            .groupby(level=0)
            .min()
        )

        streefpeil = ds_node_ids.map(ds_levels).dropna().groupby(level=0).min()
        streefpeil.name = "streefpeil"
        streefpeil.index.name = "node_id"
        self.add_series(node_type="Basin", series=streefpeil, fill_na=True)

    def update_model_basin_levels(self, profile_ids: bool = True) -> None:
        """Set the meta_streefpeil, and optionally the meta_profiellijnid, of the Basin / area table of the model.

        Args:
            profile_ids: also set the meta_profiellijnid from the profielid
        """
        assert self.basin is not None, "reset the Basin data frame first"
        area_df = self.model.basin.area.df
        assert area_df is not None
        area_df.set_index("node_id", inplace=True)
        streefpeil = self.basin.set_index("node_id")["streefpeil"]
        area_df.loc[streefpeil.index, "meta_streefpeil"] = streefpeil
        if profile_ids:
            profiellijnid = self.basin.set_index("node_id")["profielid"]
            area_df.loc[streefpeil.index, "meta_profiellijnid"] = profiellijnid
        area_df.reset_index(drop=False, inplace=True)
        area_df.index += 1
        area_df.index.name = "fid"
        area_df["meta_streefpeil"] = pd.to_numeric(area_df["meta_streefpeil"], errors="coerce")

    def write(self) -> None:
        # remove exel if exists
        if self.xlsx_path.exists():
            self.xlsx_path.unlink()
        else:
            self.xlsx_path.parent.mkdir(exist_ok=True, parents=True)

        # write sheets
        self.description.to_excel(self.xlsx_path, sheet_name="beschrijving", index=False)
        with pd.ExcelWriter(self.xlsx_path, mode="a", if_sheet_exists="replace") as xlsx_writer:
            self.defaults.to_excel(xlsx_writer, sheet_name="defaults")
            for node_type in ["Pump", "Outlet", "Basin"]:
                df = getattr(self, pascal_to_snake_case(node_type))
                if df is None:
                    df = self.reset_data_frame(node_type=node_type)
                df.to_excel(xlsx_writer, sheet_name=node_type, index=False)

        # fix formatting
        wb = load_workbook(self.xlsx_path)
        for sheet_name in pd.ExcelFile(self.xlsx_path).sheet_names:
            ws = wb[sheet_name]

            # freeze panes
            ws.freeze_panes = "A2"

            # add filters
            ws.auto_filter.ref = ws.dimensions

            # Remove cell borders
            for row in ws.iter_rows():
                for cell in row:
                    cell.border = Border()  # Removes borders

            # Auto-adjust column width
            for col in ws.columns:
                max_length = 0
                col_letter = col[0].column_letter  # Get column letter (e.g., 'A', 'B', etc.)

                for cell in col:
                    if cell.value:
                        max_length = max(max_length, len(str(cell.value)))

                ws.column_dimensions[col_letter].width = max_length + 2  # Add some padding

        # Save the modified file
        wb.save(self.xlsx_path)
        wb.close()
