# %%
import calendar
from datetime import date
from pathlib import Path

import geopandas as gpd
import imod
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr
from pandas.tseries.offsets import MonthBegin
from rasterio import features
from scipy import ndimage


def get_decade_length(year, month, start_day):
    if start_day not in (1, 11, 21):
        raise ValueError("start_day must be 1, 11, or 21")

    days_in_month = calendar.monthrange(year, month)[1]

    if start_day == 1:
        return 10  # Days 1-10
    elif start_day == 11:
        return 10  # Days 11-20
    else:  # start_day == 21
        return days_in_month - 20


def count_afvoerklassen(da_afvoerklassen, da_landbouwzones, da_natuurzones, da_lgn_reclassed):

    # Start with an output array full of NaN
    da_afvoerklasse_counts = xr.full_like(da_afvoerklassen, np.nan, dtype=float)

    # ---------- Agriculture (landuse = 1) ----------
    mask_agri = da_lgn_reclassed == 1

    ds_agri = xr.Dataset(
        {
            "afvoer": da_afvoerklassen.where(mask_agri),
            "zone": da_landbouwzones.where(mask_agri),
        }
    )

    # Count cells for each (discharge class, agricultural zone)
    counts_agri = ds_agri.to_dataframe().dropna().groupby(["afvoer", "zone"]).size()

    # Assign the counts back to the grid
    for (afvoer, zone), count in counts_agri.items():
        sel = mask_agri & (da_afvoerklassen == afvoer) & (da_landbouwzones == zone)
        da_afvoerklasse_counts = da_afvoerklasse_counts.where(~sel, count)

    # ---------- Nature (landuse = 2) ----------
    mask_nat = da_lgn_reclassed == 2

    ds_nat = xr.Dataset(
        {
            "afvoer": da_afvoerklassen.where(mask_nat),
            "zone": da_natuurzones.where(mask_nat),
        }
    )

    # Count cells for each (discharge class, nature zone)
    counts_nat = ds_nat.to_dataframe().dropna().groupby(["afvoer", "zone"]).size()

    # Assign the counts back to the grid
    for (afvoer, zone), count in counts_nat.items():
        sel = mask_nat & (da_afvoerklassen == afvoer) & (da_natuurzones == zone)
        da_afvoerklasse_counts = da_afvoerklasse_counts.where(~sel, count)

    return da_afvoerklasse_counts


def plot_2d(da, factor, colors, levels, provincies, title, unit, mask, outputdir):

    da = da * factor
    # da = xr.where(mask>=0, da, np.nan)
    overlays = [{"gdf": provincies, "color": "none", "edgecolor": "black", "linewidth": 1.0}]
    da.attrs["units"] = unit
    fig, ax, cbar = imod.visualize.spatial.plot_map(
        da, colors, levels, figsize=(5, 5), overlays=overlays, return_cbar=True
    )
    ax.set_title(title.replace("Verandering", r"$\Delta$"), fontsize=15, fontweight=2.0)
    cbar.set_label(label=da.attrs["units"], fontsize=15, weight="bold")
    ax.axis("off")
    fig.tight_layout()
    fig.savefig(rf"{outputdir}\{title}.png", dpi=150)
    plt.close()


def fill_nearest(da):
    data = da.values

    mask = np.isnan(data)

    if not mask.any():
        return da

    idx = ndimage.distance_transform_edt(
        mask,
        return_distances=False,
        return_indices=True,
    )

    filled = data[tuple(idx)]

    return xr.DataArray(
        filled,
        coords=da.coords,
        dims=da.dims,
        attrs=da.attrs,
    )


def build_zone_class_lookup(df, zone_col, class_col, value_cols):
    """Build a dense lookup matrix keyed by (zone_id, discharge_class) for a set of variables."""
    zone_ids = pd.Index(pd.to_numeric(df[zone_col], errors="coerce").dropna().unique())
    zone_ids = zone_ids.sort_values()
    zone_to_idx = {int(zone_id): idx for idx, zone_id in enumerate(zone_ids)}

    lookup = {col: np.full((len(zone_ids), 6), np.nan, dtype=np.float64) for col in value_cols}

    for row in df[[zone_col, class_col, *value_cols]].itertuples(index=False):
        zone_id = int(getattr(row, zone_col))
        afvoerklasse = int(getattr(row, class_col))
        if zone_id not in zone_to_idx or not (1 <= afvoerklasse <= 6):
            continue
        zone_idx = zone_to_idx[zone_id]
        for col in value_cols:
            val = getattr(row, col)
            if pd.notna(val):
                lookup[col][zone_idx, afvoerklasse - 1] = float(val)

    return zone_to_idx, lookup


def lookup_zone_class_to_grid(zone_values, class_values, zone_to_idx, lookup_matrix):
    """Map zone IDs and discharge classes to a grid-shaped array using a pre-built lookup matrix."""
    flat_zone = np.asarray(zone_values).ravel()
    flat_class = np.asarray(class_values).ravel()
    out = np.full(flat_zone.shape, np.nan, dtype=np.float64)

    valid = (~pd.isna(flat_zone)) & (~pd.isna(flat_class))
    if not valid.any():
        return out.reshape(zone_values.shape)

    zones = np.asarray([int(v) for v in flat_zone[valid]], dtype=np.int64)
    classes = np.asarray([int(v) for v in flat_class[valid]], dtype=np.int64) - 1
    zone_idx = np.fromiter((zone_to_idx[z] for z in zones), dtype=np.int64, count=zones.size)
    out[valid] = lookup_matrix[zone_idx, classes]
    return out.reshape(zone_values.shape)


def build_year_decade_lookup(df, zone_col, value_cols):
    """Prebuild lookup tables for all combination of (year, decade) once."""
    result = {}
    for (year, decade), sel in df.groupby(["jaar", "tijdvak"]):
        zone_to_idx, lookup = build_zone_class_lookup(sel, zone_col, "AfvoerKlasse", value_cols)
        result[(int(year), int(decade))] = {"zone_to_idx": zone_to_idx, "lookup": lookup}
    return result


### new directories:

ROOT = Path(__file__).resolve().parents[3]

ANIMO_DATA_DIR = ROOT / "Basisgegevens" / "Delwaq" / "ANIMO"

ANIMO_INPUT_DIR = ANIMO_DATA_DIR / "aangeleverd"  # vervangt "data\1-external" of /
ANIMO_INTERIM_DIR = ANIMO_DATA_DIR / "interim"  # vervangt "data\2-interim" of /
ANIMO_OUTPUT_DIR = ANIMO_DATA_DIR / "output"  # vervangt "data/5-results" of \
ANIMO_FIGURES_DIR = ANIMO_DATA_DIR / "figures"  # vervangt "data/6-visualization" of \

# ###

# ### current directories, written in new style

# ROOT = Path(r"P:\11212767-lwkm2\Koppeling_ANIMO_Delwaq")

# ANIMO_DATA_DIR = ROOT / "data"

# ANIMO_INPUT_DIR = ANIMO_DATA_DIR / "1-external"
# ANIMO_INTERIM_DIR = ANIMO_DATA_DIR / "2-interim"
# ANIMO_OUTPUT_DIR = ANIMO_DATA_DIR / "5-results"
# ANIMO_FIGURES_DIR = ANIMO_DATA_DIR / "6-visualization"

PLOT_CONCS_DIR = ANIMO_INTERIM_DIR / "plots_flux-averaged_concs_per_basin"
PLOT_LHM_DIR = ANIMO_INTERIM_DIR / "plots_lhm-afvoer_per_basin"
PLOT_HRU_DIR = ANIMO_INTERIM_DIR / "plots_hru-afvoer_per_basin"

for d in [
    ANIMO_INPUT_DIR,
    ANIMO_INTERIM_DIR,
    ANIMO_OUTPUT_DIR,
    ANIMO_FIGURES_DIR,
    PLOT_CONCS_DIR,
    PLOT_LHM_DIR,
    PLOT_HRU_DIR,
]:
    d.mkdir(parents=True, exist_ok=True)

ds_concs_timeseries = []
da_afvoer_hru_timeseries = []
da_afvoer_lhm_timeseries = []

species_list = ["cNorg", "cNH4N", "cNO3N", "cNtot", "cPorg", "cPort", "cPtot"]
write_intermediate_output = False
apply_correction_deep_seepage = False
gdf_basins = gpd.read_file(ANIMO_INPUT_DIR / "shapes" / "lhm_coupled_full" / "basin_polygons_lhm_coupled_full.shp")
da_lgn = imod.idf.open(ANIMO_INPUT_DIR / "LGN250.IDF")
da_lgn = da_lgn.rio.write_crs("EPSG:28992", inplace=False)
da_landbouwzones = (
    xr.open_dataset(
        ANIMO_INPUT_DIR
        / "Uitspoelconcentraties_4opties"
        / "Concs_per_6Afvoerklassen_1991_2020"
        / "Afvoerklassen_per_landbouw_natuur_zone__6Afvoerklassen1991_2020_23062026.tif"
    )
    .sel(band=1)["band_data"]
    .drop_vars("band")
)
da_natuurzones = (
    xr.open_dataset(
        ANIMO_INPUT_DIR
        / "Uitspoelconcentraties_4opties"
        / "Concs_per_6Afvoerklassen_1991_2020"
        / "Afvoerklassen_per_landbouw_natuur_zone__6Afvoerklassen1991_2020_23062026.tif"
    )
    .sel(band=2)["band_data"]
    .drop_vars("band")
)
da_afvoerklassen = (
    xr.open_dataset(
        ANIMO_INPUT_DIR
        / "Uitspoelconcentraties_4opties"
        / "Concs_per_6Afvoerklassen_1991_2020"
        / "Afvoerklassen_per_landbouw_natuur_zone__6Afvoerklassen1991_2020_23062026.tif"
    )
    .sel(band=3)["band_data"]
    .drop_vars("band")
)
df_concs_landbouw = pd.read_csv(
    ANIMO_INPUT_DIR
    / "Uitspoelconcentraties_4opties"
    / "Concs_per_6Afvoerklassen_1991_2020"
    / "uitpoelconcentraties_decade_landbouw__6Afvoerklassen1991_2020_23062026.csv"
)
df_concs_natuur = pd.read_csv(
    ANIMO_INPUT_DIR
    / "Uitspoelconcentraties_4opties"
    / "Concs_per_6Afvoerklassen_1991_2020"
    / "uitpoelconcentraties_decade_natuur__6Afvoerklassen1991_2020_23062026.csv"
)

# For plotting
provincies = gpd.read_file(ANIMO_INPUT_DIR / "shapes" / "B1_Provinciale_indeling_van_NederlandPolygon.shp")
path_leg = ANIMO_INPUT_DIR / "residuals.leg"
res_leg = imod.visualize.read_imod_legend(path_leg)
levels_vracht_Ntot = [1.0, 2.5, 5.0, 10.0, 20.0, 30.0, 40.0, 50.0, 75.0]
levels_vracht_Ptot = [0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1.00, 1.25, 1.5]
levels_conc_Ntot = [1.0, 2.5, 5.0, 10.0, 15.0, 20.0, 30.0, 40.0, 50.0]
levels_conc_Ptot = [0.1, 0.25, 0.50, 1.0, 1.5, 2.0, 3.0, 4.0, 5.0]
levels_diff_Ntot = [-25.0, -10.0, -5.0, -1.0, -0.1, 0.1, 1.0, 5.0, 10.0, 25.0]
levels_diff_Ptot = [-2.5, -1.0, -0.5, -0.1, -0.01, 0.01, 0.1, 0.5, 1.0, 2.5]
levels_diff_Ntot_perc = [-40.0, -25.0, -10.0, -5.0, -1, 1.0, 5.0, 10.0, 25.0, 40.0]
levels_diff_Ptot_perc = [-40.0, -25.0, -10.0, -5.0, -1, 1.0, 5.0, 10.0, 25.0, 40.0]
levels_diff_discharge = [-200, -100, -50, -25, -10, 10, 25, 50, 100, 200]
levels_discharge_mm_day = [0.0, 0.1, 0.25, 0.5, 0.75, 1.0, 2.0, 3.0, 4.0]
colors = [
    "#1E90FF",  # No Risk (Dodger Blue)
    "#00CED1",  # Low Risk (Dark Turquoise)
    "#32CD32",  # Moderate-Low Risk (Lime Green)
    "#ADFF2F",  # Moderate Risk (Green-Yellow)
    "#FFD700",  # Increased Risk (Gold)
    "#FFA500",  # High Risk (Orange)
    "#FF4500",  # Very High Risk (Orange-Red)
    "#DC143C",  # Severe (Crimson)
    "#B22222",  # Extreme (Firebrick Red)
    "#8B0000",  # Critical (Dark Red)
]

# PREPROCESSING
print("Start preprocessing")

# Rename discharge classes
mapping = {
    "AfvKl_I": 1,
    "AfvKl_II": 2,
    "AfvKl_III": 3,
    "AfvKl_IV": 4,
    "AfvKl_V": 5,
    "AfvKl_VI": 6,
}
df_concs_landbouw["AfvoerKlasse"] = df_concs_landbouw["AfvoerKlasse"].map(mapping)
df_concs_natuur["AfvoerKlasse"] = df_concs_natuur["AfvoerKlasse"].map(mapping)

landbouw_value_cols = ["Wafv", "cNorg", "cNH4N", "cNO3N", "cNtot", "cPorg", "cPort", "cPtot"]
natuur_value_cols = ["Wafv", "cNorg", "cNH4N", "cNO3N", "cNtot", "cPorg", "cPort", "cPtot"]
landbouw_lookup = build_year_decade_lookup(df_concs_landbouw, "ID_LB", landbouw_value_cols)
natuur_lookup = build_year_decade_lookup(df_concs_natuur, "ID_NT", natuur_value_cols)

# align with WENR grid convention
da_lgn, da_landbouwzones = xr.align(da_lgn, da_landbouwzones, join="right")

# reclass LGN
lgnmap = {
    1: 1,  # 1=landbouw
    2: 1,
    3: 1,
    4: 1,
    5: 1,
    6: 1,
    7: 1,
    8: 4,  # 4=glas
    9: 1,
    10: 1,
    11: 2,  # 2=natuur
    12: 2,
    13: 2,
    14: 2,
    15: 5,  # 5=kale grond
    16: 6,  # 6=open water
    17: 2,
    18: 3,  # 3=stedelijk grasland
    19: 2,
    20: 2,
    21: 1,
    22: 3,
}
da_lgn_reclassed = da_lgn
for i in lgnmap:
    da_lgn_reclassed = xr.where(da_lgn == i, lgnmap.get(i), da_lgn_reclassed)
imod.idf.write(
    ANIMO_INTERIM_DIR / "lgn_reclassed.idf",
    da_lgn_reclassed,
)  # Misschien uiteindelijk alle intermediaire uitvoer in 1 blok zetten, zodat die makkelijk uit te schakelen is?

# Count afvoerklassen per landuse and calculate discharge class surface areas
# da_count_afvoerklassen = count_afvoerklassen(da_afvoerklassen, da_landbouwzones, da_natuurzones, da_lgn_reclassed)
# da_count_afvoerklassen = da_count_afvoerklassen.load()
# imod.idf.write(ANIMO_INTERIM_DIR / "counts_afvoerklassen.idf", da_count_afvoerklassen)
# da_afvoerklasse_areas = da_count_afvoerklassen*250.*250.
# da_afvoerklasse_areas = da_afvoerklasse_areas.load()
imod.idf.write(ANIMO_INTERIM_DIR / "afvoerklassen.idf", da_afvoerklassen)
imod.idf.write(ANIMO_INTERIM_DIR / "landbouwzones.idf", da_landbouwzones)
imod.idf.write(ANIMO_INTERIM_DIR / "natuurzones.idf", da_natuurzones)


# %% Read LHM hydrology, align with WENR grid convention
da_afvoer_lhm_m3_day = imod.idf.open(
    str(ANIMO_INTERIM_DIR / "LHM_fluxen" / "LHM_351" / "total_flux_m3_per_day_*.idf")
).squeeze("layer")
da_afvoer_lhm_m3_day, da_landbouwzones = xr.align(da_afvoer_lhm_m3_day, da_landbouwzones, join="right")

da_deep_seepage_correction_lhm_m3_day = imod.idf.open(
    str(ANIMO_INTERIM_DIR / "LHM_fluxen" / "LHM_351" / "deep_seepage_correction_m3_per_day_*.idf")
).squeeze("layer")
da_deep_seepage_correction_lhm_m3_day, da_landbouwzones = xr.align(
    da_deep_seepage_correction_lhm_m3_day, da_landbouwzones, join="right"
)


# %% Rasterize the basins
gdf = gdf_basins.to_crs(da_lgn.rio.crs)
gdf["area"] = gdf.geometry.area
shapes = [(geom, fid) for geom, fid in zip(gdf.geometry, gdf["node_id"], strict=True)]
transform = da_lgn.rio.transform()
out_shape = da_lgn.rio.shape
rasterized = features.rasterize(
    shapes=shapes,
    out_shape=out_shape,
    transform=transform,
    fill=np.nan,  # background (no basin)
    dtype="float32",
)
da_basins = xr.DataArray(rasterized, coords=da_lgn.coords, dims=da_lgn.dims, name="basin_id")
da_basin = da_basins.rio.write_crs(da_lgn.rio.crs)
# da_basin = da_basin.astype("Int64")

# Also make xarray of basin areas
area_lut = dict(zip(gdf["node_id"], gdf["area"], strict=True))
basin_ids = da_basins.values
area_grid = np.full(basin_ids.shape, np.nan, dtype="float64")
valid = ~np.isnan(basin_ids)
area_grid[valid] = np.vectorize(area_lut.get)(basin_ids[valid])
da_basin_area = xr.DataArray(
    area_grid,
    coords=da_basins.coords,
    dims=da_basins.dims,
    name="basin_area",
)
da_basin_area = da_basin_area.rio.write_crs(da_lgn.rio.crs)

# align with WENR grid convention
da_basin, da_landbouwzones = xr.align(da_basin, da_landbouwzones, join="right")
da_basin_area, da_landbouwzones = xr.align(da_basin_area, da_landbouwzones, join="right")
da_basin = da_basin.load()
da_basin_area = da_basin_area.load()
imod.idf.write(ANIMO_INTERIM_DIR / "basins.idf", da_basin)
imod.idf.write(ANIMO_INTERIM_DIR / "basin_area.idf", da_basin_area)


print("Done preprocessing")

# %%
# Loop over de tijd
# Maak een grid per stof. Hierin zitten nodata. Voor lgn = stedelijk grasland,
# vul met gemiddelde natuur over basis. Voor glas en open water, zet op nul.
# Check later of er dan nog nodata over blijft.
timestep = -1
ds_cum_load_delwaq_kg = 0.0
ds_cum_load_hru_kg = 0.0
ds_dry_weight_loads_per_basin_kg_day = []
ds_dry_weight_cum_loads_delwaq_kg = []
ds_dry_weight_cum_loads_hru_kg = []
for year in range(2017, 2020):
    decade_number = 0

    for month in range(1, 13):  # 13): #1, 13
        for day in [1, 11, 21]:  # , 21]:  #(1, 11, 21)
            first_day_decade = date(year, month, day)
            print("Processing for " + str(first_day_decade))
            decade_number += 1
            decade_length = get_decade_length(year, month, day)
            timestep = timestep + 1
            print("Decade length = " + str(decade_length))

            ###AFVOEREN

            ##1. LHM AFVOEREN
            da_afvoer_lhm_m3_day_t = da_afvoer_lhm_m3_day.isel(time=timestep)
            if apply_correction_deep_seepage:
                # LHM discharge needs to be corrected for 'diepe slootkwel'.
                # Correction is equal to difference between da_bdgflf and da_bdgflf_corr.

                da_correction = da_deep_seepage_correction_lhm_m3_day.isel(time=timestep)
                da_afvoer_lhm_m3_day_t = da_afvoer_lhm_m3_day_t - da_correction
            da_afvoer_lhm_m3_day_t = da_afvoer_lhm_m3_day_t.where(da_lgn.notna())
            da_afvoer_lhm_m3_day_t = da_afvoer_lhm_m3_day_t.load()
            # For visualization later:
            da_afvoer_lhm_grid_mm_day = 1000.0 * (da_afvoer_lhm_m3_day_t / (250 * 250))

            # Calculate total discharge per basin (LHM)
            valid = da_afvoer_lhm_m3_day_t.notna()
            da_afvoer_lhm_per_basin_m3_day = da_afvoer_lhm_m3_day_t.where(valid).groupby(da_basin).sum()

            # Create a DataArray of basin areas
            da_basin_area_m2 = xr.DataArray(
                data=[area_lut[int(b)] for b in da_afvoer_lhm_per_basin_m3_day.basin_id.values],
                coords={"basin_id": da_afvoer_lhm_per_basin_m3_day.basin_id},
                dims=("basin_id",),
                name="area_m2",
            )
            # Convert m3/day to mm/day
            da_afvoer_lhm_per_basin_mm_day = da_afvoer_lhm_per_basin_m3_day / da_basin_area_m2 * 1000
            df = da_afvoer_lhm_per_basin_mm_day.to_dataframe(name="Discharge").reset_index()

            # For visualization, project onto grid
            valid = da_basin.notna()
            da_afvoer_lhm_per_basin_on_grid_mm_day = da_afvoer_lhm_per_basin_mm_day.sel(
                basin_id=da_basin.where(valid, other=da_afvoer_lhm_per_basin_m3_day.basin_id.values[0])
            ).where(valid)

            ##2. HRU AFVOEREN
            # Get scaled HRU results for the current decade using precomputed lookup tables.
            df_discharges_and_concs_sel_landbouw = df_concs_landbouw[
                (df_concs_landbouw["jaar"] == year) & (df_concs_landbouw["tijdvak"] == decade_number)
            ]
            df_discharges_and_concs_sel_natuur = df_concs_natuur[
                (df_concs_natuur["jaar"] == year) & (df_concs_natuur["tijdvak"] == decade_number)
            ]

            if df_discharges_and_concs_sel_landbouw.empty:
                raise ValueError(f"No landbouw data found for year {year}, decade {decade_number}")
            if df_discharges_and_concs_sel_natuur.empty:
                raise ValueError(f"No natuur data found for year {year}, decade {decade_number}")

            landbouw_lookup_t = landbouw_lookup.get((year, decade_number))
            natuur_lookup_t = natuur_lookup.get((year, decade_number))

            if landbouw_lookup_t is None:
                raise ValueError(f"Missing precomputed landbouw lookup for year {year}, decade {decade_number}")
            if natuur_lookup_t is None:
                raise ValueError(f"Missing precomputed natuur lookup for year {year}, decade {decade_number}")

            ds_afvoer_hru_landbouw_mm_decade = xr.Dataset(
                {
                    "Wafv": (
                        da_landbouwzones.dims,
                        lookup_zone_class_to_grid(
                            da_landbouwzones.values,
                            da_afvoerklassen.values,
                            landbouw_lookup_t["zone_to_idx"],
                            landbouw_lookup_t["lookup"]["Wafv"],
                        ),
                    )
                },
                coords=da_landbouwzones.coords,
            )
            ds_afvoer_hru_natuur_mm_decade = xr.Dataset(
                {
                    "Wafv": (
                        da_natuurzones.dims,
                        lookup_zone_class_to_grid(
                            da_natuurzones.values,
                            da_afvoerklassen.values,
                            natuur_lookup_t["zone_to_idx"],
                            natuur_lookup_t["lookup"]["Wafv"],
                        ),
                    )
                },
                coords=da_natuurzones.coords,
            )

            """
            Note: it was noticed that there are quite some grid cells with
            landuse nature and agriculture for which ds_natuur and ds_landbouw
            at this stage are nodata. Check with WENR if these should be cells
            without any discharge ever.
            """

            ds_afvoer_hru_mm_decade = xr.where(
                da_lgn_reclassed == 1,
                ds_afvoer_hru_landbouw_mm_decade,
                ds_afvoer_hru_natuur_mm_decade,
            )
            da_afvoer_hru_mm_decade = ds_afvoer_hru_mm_decade["Wafv"]
            da_afvoer_hru_mm_day = 0.1 * da_afvoer_hru_mm_decade
            da_afvoer_hru_m3_day = (
                0.1 * 0.001 * da_afvoer_hru_mm_decade * 250.0 * 250.0
            )  # mm/decade to m3/day #NOG NETTER MAKEN DOOR DAADWERKELIJKE DECADELENGTE TE GEBRUIKEN!!!

            # Now, we need to fill urban areas with the LHM discharges, as these
            # are not calculated by ANIMO but did get a concentration above
            da_afvoer_hru_m3_day = xr.where(da_lgn_reclassed == 3, da_afvoer_lhm_m3_day_t, da_afvoer_hru_m3_day)
            da_afvoer_hru_m3_day = da_afvoer_hru_m3_day.load()

            # Calculate total discharge per basin (HRU)
            valid = da_afvoer_hru_m3_day.notna()
            da_afvoer_hru_per_basin_m3_day = da_afvoer_hru_m3_day.where(valid).groupby(da_basin).sum()

            # Project onto grid
            valid = da_basin.notna()
            da_afvoer_hru_per_basin_on_grid_m3_day = da_afvoer_hru_per_basin_m3_day.sel(
                basin_id=da_basin.where(valid, other=da_afvoer_hru_per_basin_m3_day.basin_id.values[0])
            ).where(valid)
            # Convert from m3/day to mm/day by dividing by basin area
            da_afvoer_hru_per_basin_on_grid_mm_day = 1000.0 * (da_afvoer_hru_per_basin_on_grid_m3_day / da_basin_area)

            ### CONCENTRATIONS
            ds_landbouw = xr.Dataset(
                {
                    var: (
                        da_landbouwzones.dims,
                        lookup_zone_class_to_grid(
                            da_landbouwzones.values,
                            da_afvoerklassen.values,
                            landbouw_lookup_t["zone_to_idx"],
                            landbouw_lookup_t["lookup"][var],
                        ),
                    )
                    for var in ["cNorg", "cNH4N", "cNO3N", "cNtot", "cPorg", "cPort", "cPtot"]
                },
                coords=da_landbouwzones.coords,
            )

            ds_natuur = xr.Dataset(
                {
                    var: (
                        da_natuurzones.dims,
                        lookup_zone_class_to_grid(
                            da_natuurzones.values,
                            da_afvoerklassen.values,
                            natuur_lookup_t["zone_to_idx"],
                            natuur_lookup_t["lookup"][var],
                        ),
                    )
                    for var in ["cNorg", "cNH4N", "cNO3N", "cNtot", "cPorg", "cPort", "cPtot"]
                },
                coords=da_natuurzones.coords,
            )

            ### Average concentrations natuur over basins, to be able to apply
            ### those values to urban grassland (not calculated by ANIMO)
            all_basins = np.unique(da_basin.fillna(-9999))
            means = ds_natuur.groupby(da_basin.fillna(-9999)).mean()
            means = means.reindex(basin_id=all_basins)
            ds_basinmean = means.sel(basin_id=da_basin.fillna(-9999))
            # Note: not all basins have nature, this results in nodata areas.
            # We need to accomodate for filling urban areas in basins that have no nature.
            # Solution: we just take the nearest non-Nan value.
            ds_basinmean_filled = ds_basinmean.copy()
            for var in ds_basinmean.data_vars:
                print("filling for var " + var)
                if ds_basinmean[var].dims == ("y", "x"):
                    ds_basinmean_filled[var] = fill_nearest(ds_basinmean[var])

            ### Apply basin means for nature to landuse type 18 (urban grass)
            ds_natuur = xr.where(da_lgn_reclassed == 3, ds_basinmean_filled, ds_natuur)

            ### Assign concentrations based on LHM landuse map.
            ds_concs = xr.where(da_lgn_reclassed == 1, ds_landbouw, np.nan)  # Landbouw
            ds_concs = xr.where(da_lgn_reclassed == 2, ds_natuur, ds_concs)  # Natuur
            ds_concs = xr.where(da_lgn_reclassed == 3, 0.0, ds_concs)  # ds_natuur, ds_concs) #Stedelijk grasland
            ds_concs = xr.where(da_lgn_reclassed == 4, 0.0, ds_concs)  # Glas
            ds_concs = xr.where(da_lgn_reclassed == 5, ds_landbouw, ds_concs)  # Kale grond
            ds_concs = xr.where(da_lgn_reclassed == 6, 0.0, ds_concs)  # Open water

            # Calculate per basin the flux-averaged concentration
            # Weighted sum of concentrations
            # Perhaps change implementation because now it's rather slow
            species = ["cNorg", "cNH4N", "cNO3N", "cNtot", "cPorg", "cPort", "cPtot"]

            ds_concs_per_basin = xr.Dataset()
            ds_concs = ds_concs.load()
            for var in species:
                print("Calculating flux-averaged concentrations over the basins for " + var)
                valid = ds_concs[var].notna() & da_afvoer_hru_m3_day.notna()

                num = (ds_concs[var] * da_afvoer_hru_m3_day).where(valid).groupby(da_basin).sum()
                den = da_afvoer_hru_m3_day.where(valid).groupby(da_basin).sum()

                ds_concs_per_basin[var] = num / den

            # Project onto grid <DO NOT DELETE>
            valid = da_basin.notna()
            ds_concs_per_basin_on_grid = ds_concs_per_basin.sel(
                basin_id=da_basin.where(valid, other=ds_concs_per_basin.basin_id.values[0])
            ).where(valid)

            ###VRACHTEN
            ##1 Based on basin-averaged LHM fluxes and concentrations, which is what we need for Delwaq
            # Calculate the dry weight load per basin
            ds_load_per_basin_kg_day = (
                0.001 * ds_concs_per_basin
            ) * da_afvoer_lhm_per_basin_m3_day  # conversion of concs from mg/L to kg/m3
            ds_load_per_basin_kg_day = xr.where(ds_load_per_basin_kg_day > 0.0, ds_load_per_basin_kg_day, 0.0)

            # Keep track of cumulatives
            ds_cum_load_delwaq_kg = ds_cum_load_delwaq_kg + ds_load_per_basin_kg_day.sum("basin_id") * decade_length
            ds_cum_load_delwaq_kg_ = ds_cum_load_delwaq_kg.expand_dims(time=[first_day_decade])
            # Append to time series
            ds_dry_weight_loads_per_basin_kg_day.append(ds_load_per_basin_kg_day)
            ds_dry_weight_cum_loads_delwaq_kg.append(ds_cum_load_delwaq_kg_)

            ##2 All based on HRU-fluxes and upscaled concentrations, as a check
            ds_load_kg_day_hru = da_afvoer_hru_m3_day * 0.001 * ds_concs  # ds_concs converted from mg/L to kg/m3
            ds_load_kg_day_hru = xr.where(ds_load_kg_day_hru > 0.0, ds_load_kg_day_hru, 0.0)
            ds_cum_load_hru_kg = ds_cum_load_hru_kg + ds_load_kg_day_hru.sum() * decade_length
            ds_cum_load_hru_kg_ = ds_cum_load_hru_kg.expand_dims(time=[first_day_decade])

            # Append to time series
            ds_dry_weight_cum_loads_hru_kg.append(ds_cum_load_hru_kg_)

            if write_intermediate_output:
                # Intermediate output block
                imod.idf.write(ANIMO_INTERIM_DIR / "da_afvoer_hru_mmdag.idf", da_afvoer_hru_mm_day)
                if apply_correction_deep_seepage:
                    imod.idf.write(ANIMO_INTERIM_DIR / "correctie_lhm_fluxen.idf", da_correction)
                imod.idf.write(ANIMO_INTERIM_DIR / "afvoer_lhm_grid_m3_day.idf", da_afvoer_lhm_m3_day_t)
                for par in [
                    "cNorg",
                    "cNH4N",
                    "cNO3N",
                    "cNtot",
                    "cPorg",
                    "cPort",
                    "cPtot",
                ]:
                    imod.idf.write(ANIMO_INTERIM_DIR / f"conc_{par}.idf", ds_concs[par])
                plot_2d(
                    da_afvoer_lhm_grid_mm_day,
                    1.0,
                    colors,
                    levels_discharge_mm_day,
                    provincies,
                    r"LHM Afvoer per grid " + str(first_day_decade),
                    r"mm/dag",
                    1,
                    PLOT_LHM_DIR,
                )
                plot_2d(
                    da_afvoer_lhm_per_basin_on_grid_mm_day,
                    1.0,
                    colors,
                    levels_discharge_mm_day,
                    provincies,
                    r"LHM Afvoer per basin " + str(first_day_decade),
                    r"mm/dag",
                    1,
                    PLOT_LHM_DIR,
                )
                plot_2d(
                    da_afvoer_hru_per_basin_on_grid_mm_day,
                    1.0,
                    colors,
                    levels_discharge_mm_day,
                    provincies,
                    r"HRU Afvoer per basin " + str(first_day_decade),
                    r"mm/dag",
                    1,
                    PLOT_HRU_DIR,
                )
                plot_2d(
                    da_afvoer_hru_mm_day,
                    1.0,
                    colors,
                    levels_discharge_mm_day,
                    provincies,
                    r"HRU Afvoer per grid " + str(first_day_decade),
                    r"mm/dag",
                    1,
                    PLOT_HRU_DIR,
                )
                plot_2d(
                    ds_concs_per_basin_on_grid["cNorg"],
                    1.0,
                    colors,
                    levels_conc_Ntot,
                    provincies,
                    r"Conc. Norg per basin " + str(first_day_decade),
                    r"mg/L",
                    1,
                    PLOT_CONCS_DIR,
                )
                plot_2d(
                    ds_concs_per_basin_on_grid["cNH4N"],
                    1.0,
                    colors,
                    levels_conc_Ntot,
                    provincies,
                    r"Conc. NH4-N per basin " + str(first_day_decade),
                    r"mg/L",
                    1,
                    PLOT_CONCS_DIR,
                )
                plot_2d(
                    ds_concs_per_basin_on_grid["cNO3N"],
                    1.0,
                    colors,
                    levels_conc_Ntot,
                    provincies,
                    r"Conc. NO3-N per basin " + str(first_day_decade),
                    r"mg/L",
                    1,
                    PLOT_CONCS_DIR,
                )
                plot_2d(
                    ds_concs_per_basin_on_grid["cNtot"],
                    1.0,
                    colors,
                    levels_conc_Ntot,
                    provincies,
                    r"Conc. Ntot per basin " + str(first_day_decade),
                    r"mg/L",
                    1,
                    PLOT_CONCS_DIR,
                )
                plot_2d(
                    ds_concs_per_basin_on_grid["cPorg"],
                    1.0,
                    colors,
                    levels_conc_Ptot,
                    provincies,
                    r"Conc. Porg per basin " + str(first_day_decade),
                    r"mg/L",
                    1,
                    PLOT_CONCS_DIR,
                )
                plot_2d(
                    ds_concs_per_basin_on_grid["cPort"],
                    1.0,
                    colors,
                    levels_conc_Ptot,
                    provincies,
                    r"Conc. Port per basin " + str(first_day_decade),
                    r"mg/L",
                    1,
                    PLOT_CONCS_DIR,
                )
                plot_2d(
                    ds_concs_per_basin_on_grid["cPtot"],
                    1.0,
                    colors,
                    levels_conc_Ptot,
                    provincies,
                    r"Conc. Ptot per basin " + str(first_day_decade),
                    r"mg/L",
                    1,
                    PLOT_CONCS_DIR,
                )

# %%
# %%
# Convert to dataframes
ds_dry_weight_loads_per_basin_kg_day = xr.concat(ds_dry_weight_loads_per_basin_kg_day, dim="time")
df_dry_weight_loads_per_basin_kg_day = ds_dry_weight_loads_per_basin_kg_day.to_dataframe().reset_index()
df_dry_weight_loads_per_basin_kg_day[species_list] = df_dry_weight_loads_per_basin_kg_day[species_list].clip(lower=0)

ds_dry_weight_cum_loads_delwaq_kg = xr.concat(ds_dry_weight_cum_loads_delwaq_kg, dim="time")
df_dry_weight_cum_loads_delwaq_kg = ds_dry_weight_cum_loads_delwaq_kg.to_dataframe().reset_index()
df_dry_weight_cum_loads_delwaq_kg[species_list] = df_dry_weight_cum_loads_delwaq_kg[species_list].clip(lower=0)

ds_dry_weight_cum_loads_hru_kg = xr.concat(ds_dry_weight_cum_loads_hru_kg, dim="time")
df_dry_weight_cum_loads_hru_kg = ds_dry_weight_cum_loads_hru_kg.to_dataframe().reset_index()
df_dry_weight_cum_loads_hru_kg[species_list] = df_dry_weight_cum_loads_hru_kg[species_list].clip(lower=0)

# ------------------------------------------------------------------
# Write parquet files
# ------------------------------------------------------------------
output_parquet_dir = ANIMO_OUTPUT_DIR

df_dry_weight_loads_per_basin_kg_day.to_parquet(
    output_parquet_dir / "dry_weight_loads_per_basin_kg_day.parquet",
    index=False,
)

df_dry_weight_cum_loads_delwaq_kg.to_parquet(
    output_parquet_dir / "dry_weight_cumulative_loads_delwaq_kg.parquet",
    index=False,
)

df_dry_weight_cum_loads_hru_kg.to_parquet(
    output_parquet_dir / "dry_weight_cumulative_loads_hru_kg.parquet",
    index=False,
)

print("Parquet files written.")
# %%
# For checking purpose, create:

# 1. 2d maps of weight loads per basin, over the entire period. For each species separately
df = df_dry_weight_loads_per_basin_kg_day.copy()
df["time"] = pd.to_datetime(df["time"])

# Compute duration represented by each timestep
# 1st -> 11th = 10 days
# 11th -> 21st = 10 days
# 21st -> 1st of next month = remaining days in month

df["days"] = 10
mask = df["time"].dt.day == 21
next_month = df.loc[mask, "time"] + MonthBegin(1)
df.loc[mask, "days"] = (next_month - df.loc[mask, "time"]).dt.days

# Convert kg/day to kg transported during each timestep
species_list = ["cNorg", "cNH4N", "cNO3N", "cNtot", "cPorg", "cPort", "cPtot"]
for par in species_list:
    df[f"{par}_kg"] = df[par] * df["days"]

kg_cols = [f"{par}_kg" for par in species_list]

# Duration of current simulation
period_days = df[["time", "days"]].drop_duplicates()["days"].sum()

loads_kg_year = df.groupby("basin_id")[kg_cols].sum() * (365.25 / period_days)

loads_kg_year.columns = species_list
loads_kg_year = loads_kg_year.reset_index()

gdf_plot = gdf.merge(loads_kg_year, left_on="node_id", right_on="basin_id", how="left")

for par in species_list:
    fig, ax = plt.subplots(figsize=(10, 8))

    valid_ids = da_afvoer_lhm_per_basin_mm_day.basin_id.values
    gdf_plot = gdf_plot[gdf_plot["node_id"].isin(valid_ids)].copy()

    vals = gdf_plot[par].dropna()
    bins = np.percentile(vals, np.arange(0, 101, 10))

    gdf_plot.plot(
        column=par,
        cmap="viridis",
        scheme="UserDefined",
        classification_kwds={"bins": bins},
        linewidth=0.2,
        edgecolor="black",
        legend=True,
        legend_kwds={
            "title": "Load (kg year$^{-1}$)",
            "loc": "lower left",
            "fmt": "{:.2e}",
        },
        missing_kwds={"color": "lightgrey", "edgecolor": "black", "label": "No data"},
        ax=ax,
    )

    ax.set_title(f"Time-averaged droge stoffvracht {par}", fontsize=14)
    ax.set_axis_off()

    plt.tight_layout()
    plt.savefig(ANIMO_FIGURES_DIR / f"Droge_stoffvracht_{par}_kg_per_jaar.png")
    plt.show()

# %%
# 2. Comparison of the cumulative loads between Delwaq input and HRU output

# ensure datetime
df_delwaq = df_dry_weight_cum_loads_delwaq_kg.copy()
df_hru = df_dry_weight_cum_loads_hru_kg.copy()

df_delwaq["time"] = pd.to_datetime(df_delwaq["time"])
df_hru["time"] = pd.to_datetime(df_hru["time"])

for par in species_list:
    fig, ax = plt.subplots(figsize=(8, 5))

    ax.plot(df_delwaq["time"], df_delwaq[par], label="DELWAQ", linewidth=2)

    ax.plot(df_hru["time"], df_hru[par], label="HRU", linewidth=2, linestyle="--")

    ax.set_title(f"Cumulative load: {par}", fontsize=20)
    ax.set_xlabel("Time", fontsize=16)
    ax.set_ylabel("Cumulative load (kg)", fontsize=16)
    ax.legend(fontsize=14)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(ANIMO_FIGURES_DIR / f"Cumul_Droge_stoffvracht_{par}_kg_Delwaq_vs_HRU.png")
    plt.show()

# %%
