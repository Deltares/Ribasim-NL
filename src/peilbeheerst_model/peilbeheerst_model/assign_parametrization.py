from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from ribasim import Model

from ribasim_nl import CloudStorage
from ribasim_nl import Model as ModelNL


class AssignMetaData:
    def __init__(
        self,
        authority: str,
        model_name: ModelNL | Model | Path | str,
        param_name: str,
    ):
        # Initialize cloudstorage
        self.cloud = CloudStorage()

        # Model
        if isinstance(model_name, Model) or isinstance(model_name, ModelNL):
            self.model_dir = model_name.filepath.parent
            self.model = model_name
        else:
            self.model_dir = self.cloud.joinpath(authority, "modellen", model_name)
            self.model = self._get_model_from_cloud()

        # Param file
        self.param_file = self.cloud.joinpath(
            authority,
            "verwerkt",
            "Parametrisatie_data",
            param_name,
        )

    def _get_model_from_cloud(self) -> Model:
        self.cloud.synchronize(filepaths=[self.model_dir], check_on_remote=True)
        model = Model.read(self.model_dir / "ribasim.toml")

        return model

    def get_paramfile_from_cloud(
        self,
        layer: str,
    ) -> tuple[pd.DataFrame, float, float]:
        # Load the paramfile
        self.cloud.synchronize(filepaths=[self.param_file], check_on_remote=True)
        df_object = gpd.read_file(self.param_file, layer=layer)
        if not df_object.index.is_unique:
            raise IndexError(f"The index of {layer=} is not unique")

        return df_object

    def _na_or_empty(self, series: pd.Series) -> pd.Series:
        return pd.isna(series) | (series == "")

    def _series_assigned(self, series: pd.Series) -> bool:
        return not self._na_or_empty(series).all()

    def _add_unassigned_columns(
        self,
        ribasim_type: str,
        mapper: dict[str, dict[str, list[str]]],
    ) -> dict[tuple[str, str, str], pd.Series]:
        # Add columns which do not exist yet
        restore_mapper = {}
        rtype = getattr(self.model, ribasim_type)
        for colmap in mapper.values():
            for ribasim_attr, ribasim_cols in colmap.items():
                df_ribasim = getattr(rtype, ribasim_attr).df
                for col in ribasim_cols:
                    if col in df_ribasim.columns and self._series_assigned(df_ribasim[col]):
                        restore_mapper[(ribasim_type, ribasim_attr, col)] = df_ribasim[col].copy()
                        df_ribasim[col] = pd.NA
                    elif col not in df_ribasim.columns:
                        df_ribasim[col] = pd.NA

        return restore_mapper

    def _restore_org_vals(self, restore_mapper: dict[tuple[str, str, str], pd.Series]) -> None:
        # Restore original values for those that are still N
        for (ribasim_type, ribasim_attr, col), org_vals in restore_mapper.items():
            df_ribasim = getattr(getattr(self.model, ribasim_type), ribasim_attr).df
            mrows = self._na_or_empty(df_ribasim[col])
            df_ribasim.loc[mrows, col] = org_vals.loc[mrows]

    def _get_matching_rows(
        self,
        ribasim_type: str,
        ribasim_attr: str,
        node_id: list[int],
    ) -> tuple[pd.DataFrame, np.ndarray]:
        df_ribasim = getattr(getattr(self.model, ribasim_type), ribasim_attr).df
        if ribasim_attr == "node":
            matching_rows = df_ribasim.index.isin(node_id)
        else:
            matching_rows = df_ribasim.node_id.isin(node_id).to_numpy()

        return df_ribasim, matching_rows

    def _nodeid_is_assigned(
        self,
        ribasim_type: str,
        mapper: dict[str, dict[str, list[str]]],
        node_id: int,
    ) -> bool:
        # Get a reference to the ribasim dataframe for the first mapped column
        colmap = next(iter(mapper.values()))
        ribasim_attr, ribasim_cols = next(iter(colmap.items()))
        df_ribasim = getattr(getattr(self.model, ribasim_type), ribasim_attr).df

        # Get a subset for the matching node_id
        df_ribasim, mrows = self._get_matching_rows(ribasim_type, ribasim_attr, [node_id])
        dfsub = df_ribasim[mrows]

        # Check for all columns if all values have not been assigned
        assigned = False
        for col in ribasim_cols:
            if self._series_assigned(dfsub[col]):
                assigned = True

        return assigned

    def add_meta_to_pumps(
        self,
        layer: str,
        mapper: dict[str, dict[str, list[str]]],
        max_distance: float = 100,
        factor_flowrate: float = 1.0,
    ) -> None:
        # get gemaal information
        df_gemaal = self.get_paramfile_from_cloud(layer)

        # Add columns which do not exist yet
        restore_cols = self._add_unassigned_columns("pump", mapper)

        # Add matching unassigned pumps to the ribasim model
        visited = {}
        for row in self.model.pump.node.df.itertuples():
            # A pump can already be assigned because there is logic in place
            # that checks for multiple overlapping ribasim pumps.
            # Check if this pump has been assigned for the first mapper.
            if self._nodeid_is_assigned("pump", mapper, row.Index):
                continue

            # Check if there are multiple overlapping pumps (within 1cm)
            rows = self.model.pump.node.df.sindex.query(row.geometry.buffer(0.01), predicate="intersects")
            node_id = self.model.pump.node.df.index[rows].tolist()
            node_id_str = ", ".join(map(str, node_id))
            if row.Index not in node_id:
                raise ValueError("Unexpected error: intersects does not return the buffered node_id")
            if len(rows) > 1:
                print(f"  - Multiple overlapping ribasim pumps for node_id={node_id_str}")

            # Only use gemalen which have not been assigned yet
            dfa = df_gemaal[~df_gemaal.index.isin(list(visited.keys()))].copy()

            # Find nearest gemaal
            idx = dfa.sindex.nearest(row.geometry, max_distance=max_distance, return_all=True)
            dfa = df_gemaal.iloc[idx[1, :]].copy()

            if len(dfa) == 0:
                print(f"  - Warning: No matching pump found for node_id={node_id_str}")
                continue
            elif len(dfa) > 1:
                print(f"  - Warning: Multiple matching pumps found for node_id={node_id_str}, using the first")

            # Assign metadata
            matching_row = dfa.iloc[0]
            for param_col, colmap in mapper.items():
                for ribasim_attr, ribasim_cols in colmap.items():
                    df_ribasim, mrows = self._get_matching_rows("pump", ribasim_attr, node_id)
                    for ribasim_col in ribasim_cols:
                        param_val = matching_row[param_col]
                        if "flow_rate" in ribasim_col:
                            param_val = pd.to_numeric(param_val) * factor_flowrate
                        df_ribasim.loc[mrows, ribasim_col] = param_val

        # Restore original values for those that are still NA
        self._restore_org_vals(restore_cols)

        return None

    def add_meta_to_basins(
        self,
        layer: str,
        mapper: dict[str, dict[str, list[str]]],
        min_overlap: float = 0.95,
    ) -> None:
        # get area information
        df_area = self.get_paramfile_from_cloud(layer)

        # Add columns which do not exist yet
        restore_cols = self._add_unassigned_columns("basin", mapper)

        # Add the matching areas to the ribasim model
        for row in self.model.basin.area.df.itertuples():
            if hasattr(row, "meta_node_id") and row.node_id != row.meta_node_id:
                # Skip "bergend"
                continue

            # Find overlapping area(s)
            idxs = df_area.sindex.query(row.geometry, predicate="intersects")
            dfa = df_area.iloc[idxs].copy()

            # Filter out insufficient overlapping areas and order by
            # overlap area and total area.
            dfa["i_area"] = dfa.geometry.intersection(row.geometry).area
            dfa["t_area"] = dfa.geometry.area
            dfa["f_area"] = dfa["i_area"] / dfa["t_area"]
            dfa = dfa[dfa.f_area >= min_overlap]
            dfa = dfa.sort_values(["i_area", "t_area"], ascending=False)

            if len(dfa) == 0:
                print(f"  - Warning: Found no matching area for basin #{row.node_id}")
                continue
            elif len(dfa) > 1:
                print(f"  - Warning: Multiple overlapping areas for basin #{row.node_id}, using the largest overlap")

            # Assign metadata
            matching_row = dfa.iloc[0]
            for param_col, colmap in mapper.items():
                for ribasim_attr, ribasim_cols in colmap.items():
                    df_ribasim, mrows = self._get_matching_rows("basin", ribasim_attr, [row.node_id])
                    df_ribasim.loc[mrows, ribasim_cols] = matching_row[param_col]

        # Restore original values for those that are still NA
        self._restore_org_vals(restore_cols)

        return None
