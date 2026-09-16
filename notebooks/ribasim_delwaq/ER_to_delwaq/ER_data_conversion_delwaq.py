"""Script to convert ER data to ribasim-delwaq input.

created: 01-2019 by: Wilfred Altena
modified: 03-2019 by: Annelotte van der Linden
modified: 02-2024 by: Steven Kelderman
last modified: 11-2025 by: Jesse van Leeuwen

Aanpassing van conversie van ER data naar KRW-V input
In 'Functions' en tussen code onder 'Overige emissies ER' en boven 'Export B6_loads'
is de code van 02-2024 onveranderd gebleven. Hieronder de toelichting van deze versie:

Script om emissieoorzaken vanuit de ER in de laden en om te zetten naar KRW-verkenner
invoerbestanden. Hierbij wordt onderscheid gemaakt in emissieoorzaak en de daarbij
behorende berekeningswijze. Hier zijn drieverschillende berekeningswijzen bepaald.

1. industrie: Deze emissies zijn per jaar per bedrijf bekend voor N en P. Soms
zelfs met meerdere lozingspunten. De bedrijven file wordt apart handmatig ingeladen
in dit script.

2. emissieoorzaken met gedetailleerde jaarlijkse totalen vanuit
Deltares. Hierbij hoeft alleen de ruimtelijke verdeling van GAF90eenheden te worden
geinterpoleerd.

3.emissieoorzaken zonder detail. Hierbij zijn alleen de ER steekjaren
bekend en wordt er tussen deze jaren geinterpoleerd.

24/6/2026 - grote aanpassingen:

- optie validate/prognose verwijderd (doel onduidelijk)
- plotting functies opgeschoond

- data pipeline moet nog worden herzien en gedocumenteerd!!!


"""

# %%
# import os
# import sys

# print(sys.executable)
# print(os.environ.get("GDAL_DATA"))
# %%
# -------------------------------Packages---------------------------------------

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from ER_GAF_fractions_func import compute_overlap_df

# ------------------------ Import local module ----------------------------------

logger = logging.getLogger(__name__)
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )

logger.info("Starting ER data conversion script")

current_dir = Path(__file__).resolve().parent
root_dir = current_dir.parents[2]

# -------------------------------Conversions------------------------------------

conv_yr2sec = 60 * 60 * 24 * 365.25
conv_kg2g = 1000
conv_ton2g = 10**6

# -------------------------------Directories------------------------------------
model_name = "lhm_coupled_full"
toml_name = "lhm_coupled.toml"
model_path = Path(root_dir, "data/Rijkswaterstaat/modellen", model_name)
toml_path = model_path / toml_name
basin_path = model_path / "input/database.gpkg"

delwaq_data_path = root_dir / "data/Basisgegevens/Delwaq"
er_path = delwaq_data_path / "aangeleverd/Emissieregistratie"
emissies_buiten_ER_path = er_path / "Emissies_per_jaar_buiten_ER.csv"
ER_export_path = er_path / "ER_DataExport-2026-09-16-103931.xlsx"
OE_bedrijven_path = er_path / "OverigeEmissies_bedrijven__2024_01_24.csv"
gaf_path = er_path / "gaf_90.shp"

output_dir = delwaq_data_path / "output"
output_dir.mkdir(parents=True, exist_ok=True)
# %%
# -------------------------------Settings---------------------------------------
frac_doorgaand = 0.5  # deel ER op doorvoerende basin node
frac_bergend = 1 - frac_doorgaand  # deel ER op bergende basin node
make_plots = False

# -------------------------------Functions--------------------------------------


def validate_df(
    df, required_columns=("Year", "Value", "EmissionTypeId", "VariableId")
):  # $ specifically made for plotting functions
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}")


def lineplot_N_P(df, N, P, kg=True, set_log=False, title=None):
    """Generate lineplots for the different EMK per Sub."""
    if not make_plots:
        return

    validate_df(df)

    df_n = df[df["VariableId"] == N]
    df_p = df[df["VariableId"] == P]

    _fig, axes = plt.subplots(1, 2, figsize=(15, 6), sharey=False)

    emk_order = sorted(df["EmissionTypeId"].unique())

    sns.lineplot(
        data=df_n,
        x="Year",
        y="Value",
        hue="EmissionTypeId",
        hue_order=emk_order,
        errorbar=None,
        ax=axes[0],
    )

    axes[0].set_title(f"Sum of Emissions of {title} per EMK for N - totaal")
    axes[0].set_xlabel("Year")
    axes[0].set_ylabel("Sum of Emissions (kg/year)" if kg else "Sum of Emissions (g/s)")
    axes[0].grid(True)
    axes[0].set_ylim(bottom=0)
    if set_log:
        axes[0].set_yscale("log")

    sns.lineplot(
        data=df_p,
        x="Year",
        y="Value",
        hue="EmissionTypeId",
        hue_order=emk_order,
        errorbar=None,
        ax=axes[1],
    )

    axes[1].set_title(f"Sum of Emissions of {title} per EMK for P - totaal")
    axes[1].set_xlabel("Year")
    axes[1].set_ylabel("Sum of Emissions (kg/year)" if kg else "Sum of Emissions (g/s)")
    axes[1].grid(True)
    axes[1].set_ylim(bottom=0)
    if set_log:
        axes[1].set_yscale("log")

    if axes[1].legend_ is not None:
        axes[1].legend_.remove()

    axes[0].legend(
        title="EmissionTypeId",
        bbox_to_anchor=(0.5, -0.15),
        loc="upper center",
        ncol=2,
    )

    plt.tight_layout()
    plt.show()


def barplot_N_P(df, N, P, y_lim_min_n, y_lim_max_n, y_lim_min_p, y_lim_max_p, kg=True, title=None):
    """Generate barplots per year for N and P."""
    if not make_plots:
        return

    validate_df(df, required_columns=("Year", "Value", "VariableId"))

    df_n = df[df["VariableId"] == N]
    df_p = df[df["VariableId"] == P]

    _fig, axes = plt.subplots(1, 2, figsize=(15, 6), sharey=False)

    sns.barplot(data=df_n, x="Year", y="Value", errorbar=None, ax=axes[0])
    axes[0].set_title(f"Sum of Emissions of {title} for N - totaal")
    axes[0].set_xlabel("Year")
    axes[0].set_ylabel("Sum of Emissions (kg/year)" if kg else "Sum of Emissions (g/s)")
    axes[0].set_ylim(y_lim_min_n, y_lim_max_n)
    axes[0].grid(True)

    sns.barplot(data=df_p, x="Year", y="Value", errorbar=None, ax=axes[1])
    axes[1].set_title(f"Sum of Emissions of {title} for P - totaal")
    axes[1].set_xlabel("Year")
    axes[1].set_ylabel("Sum of Emissions (kg/year)" if kg else "Sum of Emissions (g/s)")
    axes[1].set_ylim(y_lim_min_p, y_lim_max_p)
    axes[1].grid(True)

    plt.tight_layout()
    plt.show()


# %%
# -------------------------------Couple emissions to LHM------------------------------------
# coupling based on GAF and basin polygons
koppeling = compute_overlap_df(
    gaf_path, basin_path, hws=False
)  # the HWS model itself does not not have a meta_categorie
koppeling["GAF-eenheid"] = koppeling["GAF-eenheid"].astype(int)

print("coupling GAF-emissions to LHM basin nodes completed")
logger.info("Coupling GAF emissions to LHM basin nodes completed")

# process fractions based on basin type, only splitting up doorgaand and bergend
# there is also the category "hoofdwater" (within waterboards), but these do not have a duplicate node in the same location
# the HWS model itself does not not have a meta_categorie
koppeling["fractie"] = koppeling.apply(
    lambda row: (
        row["fractie"] * frac_doorgaand
        if row["meta_categorie"] == "doorgaand"
        else row["fractie"] * frac_bergend
        if row["meta_categorie"] == "bergend"
        else row["fractie"]
    ),
    axis=1,
)

sum_per_node = koppeling.groupby("GAF-eenheid")["fractie"].sum().reset_index().sort_values(by="fractie")
sum_exceeding_1 = len(sum_per_node[sum_per_node["fractie"] > 1.05]) / len(sum_per_node) * 100
print(f"Percentage of GAF-units with sum of fractions exceeding 1 by more than 5%: {sum_exceeding_1:.2f}%")

# $ hashed code below causes kernel crash, fix later if we want this plot as diagnositc. However, we already checked and it shows that not all emissions in GAF-polygons are assigned to basins due to lack of overlap. However this is only the case for a minority of GAF polygons, so no immediate cause of concern

# _fig, ax = plt.subplots()
# ax.scatter(sum_per_node.index.to_numpy(), sum_per_node["fractie"].to_numpy(), s=1)
# ax.set_title("Sum of fractions per GAF (should not exceed 1.0)")
# ax.set_xlabel("Index")
# ax.set_ylabel("Sum of fractions per GAF-unit")
# ax.grid(True)
# _fig.savefig("sum_per_node.png", dpi=150, bbox_inches="tight")
# plt.close(_fig)

# $ check: eventueel kunnen de fracties per GAF met de emissies per GAF worden vermenigvuldigd om te checken of het matched met wat er uit dit script komt rollen als totale emissies

# %%
# -------------------------------Import data------------------------------------

# Direct download from the ER website at GAF90 level
emissions_gaf_kg_year = pd.read_excel(
    ER_export_path,
    sheet_name="Emissies",
    usecols=["Stofcode", "Stof", "Code_gebied", "Sector", "Subsector", "Emissieoorzaak", "Jaar", "Emissie"],
)

# Manual file to fill in Deltares ER yearly loads #$ not using this rn
Emissies_per_jaar_buiten_ER = pd.read_csv(emissies_buiten_ER_path, delimiter=";", encoding="latin1")

# Manual file to import bedrijven without coastal waters #$ also not rn
OverigeEmissies_bedrijven__2024_01_24 = pd.read_csv(OE_bedrijven_path, delimiter=";", encoding="latin1")


# %%

# -------------------------------Data processing------------------------------------

# define emissieoorzaken that are excluded #$(because they are incorporated by another data source?)
REMOVE_EMK = [
    "SBI",
    "spoeling nutri",
    "Effluenten RWZI",
    "Depositie NCP",
]

EMISSION_TYPES = ["Depositie Nederland", "Glastuinbouw", "Erfafspoeling", "Regenwaterriolen", "Meemesten sloten"]

emissions_gaf_kg_year = emissions_gaf_kg_year.loc[
    ~emissions_gaf_kg_year["Emissieoorzaak"].apply(lambda x: any(pattern in str(x) for pattern in REMOVE_EMK))
].copy()

print("Catagories considered:", emissions_gaf_kg_year["Emissieoorzaak"].unique().tolist())

# Rename emission types to 'OverigeEmissies' that are not in the list with emission types
emissions_gaf_kg_year.loc[~emissions_gaf_kg_year["Emissieoorzaak"].isin(EMISSION_TYPES), "Emissieoorzaak"] = (
    "OverigeEmissies"
)

# Rename 'Depositie Nederland' to 'Atmosferische depositie'
emissions_gaf_kg_year["Emissieoorzaak"] = emissions_gaf_kg_year["Emissieoorzaak"].replace(
    {"Depositie Nederland": "Atmosferische depositie"}
)

# Sum towards EMK per GAF per Year per subcategory
emissions_gaf_kg_year_sum = (
    emissions_gaf_kg_year.groupby(["Code_gebied", "Emissieoorzaak", "Stof", "Jaar"])["Emissie"].sum().reset_index()
)

sum_per_gaf_long = emissions_gaf_kg_year_sum.copy()

sum_per_gaf_long["Jaar"] = sum_per_gaf_long["Jaar"].astype(int)

# yearly sum for diagnostic
sum_per_emk_long = sum_per_gaf_long.groupby(["Emissieoorzaak", "Stof", "Jaar"])["Emissie"].sum().reset_index()

for stof in sum_per_emk_long["Stof"].unique():
    df_stof = sum_per_emk_long[sum_per_emk_long["Stof"] == stof]

    plt.figure(figsize=(10, 6))

    sns.lineplot(
        data=df_stof,
        x="Jaar",
        y="Emissie",
        hue="Emissieoorzaak",
        marker="o",
    )

    plt.title(f"Emissies voor {stof} per jaar totaal (kg)")
    plt.xlabel("Jaar")
    plt.ylabel("Emissie")
    plt.grid(True)

    plt.tight_layout()
    plt.show()

# year totals (all emissieoorzaken together)
sum_per_year_base_long = sum_per_gaf_long.groupby(["Stof", "Jaar"])["Emissie"].sum().reset_index()

print(
    sum_per_year_base_long.pivot(
        index="Jaar",
        columns="Stof",
        values="Emissie",
    )
)

logger.info("Yearly totals per EMK (emissieoorzaak) and per year computed")

# %%
#################################################################################
# interpolate ER data and include yearly emissions for specific emissieoorzaken #
#################################################################################

# Define desired interpolation range for the complete dataset

EMK_DETAIL = ["Erfafspoeling", "Glastuinbouw"]

start_year = sum_per_gaf_long["Jaar"].min()
end_year = sum_per_gaf_long["Jaar"].max()

year_range = range(start_year, end_year + 1)

start_year_detailed = Emissies_per_jaar_buiten_ER["Jaar"].min()
end_year_detailed = Emissies_per_jaar_buiten_ER["Jaar"].max()

logger.info(f"Using yearly specified total emissions from {emissies_buiten_ER_path!s}")
logger.info(f"Yearly totals available between years: {start_year_detailed}-{end_year_detailed}")
logger.info("Interpolating 5-yearly emission values per GAF-unit outside of this range when not specified by ER export")

# Direct interpolation for all emission causes over the complete range

all_interpolated = (
    sum_per_gaf_long[["Code_gebied", "Emissieoorzaak", "Stof", "Jaar", "Emissie"]]
    .set_index("Jaar")
    .groupby(["Code_gebied", "Emissieoorzaak", "Stof"])["Emissie"]
    .apply(
        lambda x: x.reindex(year_range).interpolate(
            method="linear",
            limit_area="inside",
        )
    )
    .rename("Value")
    .reset_index()
)

all_interpolated["interpolation_method"] = "Direct interpolation"


# Detailed EMKs: calculate GAF allocation fractions

emissions_gaf_detail = sum_per_gaf_long[sum_per_gaf_long["Emissieoorzaak"].isin(EMK_DETAIL)].copy()

emissions_gaf_detail["fraction"] = (
    emissions_gaf_detail["Emissie"]
    / emissions_gaf_detail.groupby(["Emissieoorzaak", "Jaar", "Stof"])["Emissie"].transform("sum")
).sort_values()


# Interpolate GAF fractions over the complete range

fractions_interpolated = (
    emissions_gaf_detail[["Code_gebied", "Emissieoorzaak", "Stof", "Jaar", "fraction"]]
    .set_index("Jaar")
    .groupby(["Code_gebied", "Emissieoorzaak", "Stof"])["fraction"]
    .apply(
        lambda x: x.reindex(year_range).interpolate(
            method="linear",
            limit_area="inside",
        )
    )
    .rename("fraction")
    .reset_index()
)


# Apply GAF allocation only where buiten-ER totals exist

detailed_allocated = fractions_interpolated.merge(
    Emissies_per_jaar_buiten_ER[["Emissieoorzaak", "Stof", "Jaar", "Emissie"]].rename(
        columns={"Emissie": "Emissie_buiten_ER"}
    ),
    on=["Emissieoorzaak", "Stof", "Jaar"],
    how="inner",
    validate="many_to_one",
)

detailed_allocated["Value"] = detailed_allocated["fraction"] * detailed_allocated["Emissie_buiten_ER"]

detailed_allocated["interpolation_method"] = "GAF allocation"
# NaNs turn up where there are no 5-year values specified in the ER-export for that specific EMK/GAF/year/substance combination.
# In this case the NaN is justified because we have no other way to estimate it.

# Replace directly interpolated values with GAF allocations
# where those allocations are available

key_columns = [
    "Code_gebied",
    "Emissieoorzaak",
    "Stof",
    "Jaar",
]

all_interpolated = all_interpolated.set_index(key_columns)
detailed_allocated = detailed_allocated.set_index(key_columns)

all_interpolated.loc[
    detailed_allocated.index,
    ["Value", "interpolation_method"],
] = detailed_allocated[["Value", "interpolation_method"]]


# Final combined dataframe

emissions_gaf_interpolated = all_interpolated.reset_index().sort_values(
    ["Code_gebied", "Emissieoorzaak", "Stof", "Jaar"],
    ignore_index=True,
)

# # check
# sum_per_gaf_long[
#     (sum_per_gaf_long["Emissieoorzaak"] == "Erfafspoeling")
#     & (sum_per_gaf_long["Code_gebied"] == 3)
#     & (sum_per_gaf_long["Stof"] == "N - Totaal")
# ]
# emissions_gaf_interpolated[
#     (emissions_gaf_interpolated["Emissieoorzaak"] == "Erfafspoeling")
#     & (emissions_gaf_interpolated["Code_gebied"] == 3)
#     & (emissions_gaf_interpolated["Stof"] == "N - Totaal")
# ]


# %%
# ---------------------------Include overige emissies bedrijven------------------------

print("processing of other emissions from industry...")
logger.info("Processing of other emissions from industry...")

# rename to emissions without industry and change variable names to accomodate existing workflow
emissions_without_industry = emissions_gaf_interpolated.rename(
    columns={
        "Code_gebied": "GAF-eenheid",
        "Emissieoorzaak": "EmissionTypeId",
        "Stof": "VariableId",
        "Jaar": "Year",
    }
)

# Emissions per GAF per Year per Sub per EMK
OverigeEmissies_bedrijven_long = pd.melt(
    OverigeEmissies_bedrijven__2024_01_24,
    id_vars=["GAF-eenheid", "VariableId", "EmissionTypeId"],
    var_name="Year",
    value_name="Value",
)

OverigeEmissies_bedrijven_long["Year"] = OverigeEmissies_bedrijven_long["Year"].astype(int)

# add emissions from industry to main dataframe with all other emissions from ER (filtered & interpolated)
emissions_total = pd.concat([OverigeEmissies_bedrijven_long, emissions_without_industry], ignore_index=True)

lineplot_N_P(
    emissions_total.groupby(["EmissionTypeId", "VariableId", "Year"], as_index=False)["Value"].sum(),
    "N - Totaal",
    "P - Totaal",
    kg=True,
    set_log=False,
)

barplot_N_P(
    emissions_total.groupby(["VariableId", "Year"], as_index=False)["Value"].sum(),
    "N - Totaal",
    "P - Totaal",
    y_lim_min_n=19_000_000,
    y_lim_max_n=44_000_000,
    y_lim_min_p=700_000,
    y_lim_max_p=2_000_000,
    kg=True,
)

# Change name of Industry to Overige emissies
emissions_total.loc[emissions_total["EmissionTypeId"] == "Industrie", "EmissionTypeId"] = "OverigeEmissies"

# Sum the values by adding emissions of industry
emissions_total = emissions_total.groupby(["GAF-eenheid", "EmissionTypeId", "VariableId", "Year"], as_index=False)[
    "Value"
].sum()

lineplot_N_P(
    emissions_total.groupby(["EmissionTypeId", "VariableId", "Year"], as_index=False)["Value"].sum(),
    "N - Totaal",
    "P - Totaal",
    kg=True,
    set_log=False,
    title="GAF-units",
)

# Generate barplot
barplot_N_P(
    emissions_total.groupby(["VariableId", "Year"], as_index=False)["Value"].sum(),
    "N - Totaal",
    "P - Totaal",
    y_lim_min_n=14_000_000,
    y_lim_max_n=44_000_000,
    y_lim_min_p=700_000,
    y_lim_max_p=2_000_000,
    kg=True,
    title="GAF-units",
)

print("created figures")
logger.info("Created summary figures")

# ---------------------------------Output--------------------------------------- #$ actual output

# koppel GAF-eenheid aan nodes
emissions_total_basin_coupling = koppeling.merge(emissions_total, how="inner", on="GAF-eenheid")

# bepaal emissie per node
emissions_total_basin_coupling["Value_new"] = (
    emissions_total_basin_coupling["Value"] * emissions_total_basin_coupling["fractie"]
)

sum_per_node = emissions_total_basin_coupling.groupby(
    ["NodeId", "EmissionTypeId", "Year", "VariableId"], as_index=False
).agg({"Value_new": "sum"})

sum_per_node = sum_per_node.rename(columns={"Value_new": "Value"})

# Generate lineplot
lineplot_N_P(
    sum_per_node.groupby(["EmissionTypeId", "VariableId", "Year"], as_index=False)["Value"].sum(),
    "N - Totaal",
    "P - Totaal",
    kg=True,
    set_log=False,
    title="basin nodes",
)

# Plot

# Generate barplot
barplot_N_P(
    sum_per_node.groupby(["VariableId", "Year"], as_index=False)["Value"].sum(),
    "N - Totaal",
    "P - Totaal",
    y_lim_min_n=0,
    y_lim_max_n=44_000_000,
    y_lim_min_p=0,
    y_lim_max_p=2_000_000,
    kg=True,
    title="basin nodes",
)

# convert from kg/y to g/s
sum_per_node["Value"] = sum_per_node["Value"] * conv_kg2g / conv_yr2sec  # from kg/365.25dgn to g/s

# Generate lineplot
lineplot_N_P(
    sum_per_node.groupby(["EmissionTypeId", "VariableId", "Year"], as_index=False)["Value"].sum(),
    "N",
    "P",
    kg=False,
    set_log=False,
    title="Node",
)
# Generate barplot
barplot_N_P(
    sum_per_node.groupby(["VariableId", "Year"], as_index=False)["Value"].sum(),
    "N",
    "P",
    0,
    525,
    0,
    37.5,
    kg=False,
    title="Node",
)


# %%

# convert N and P total to actual fractions

ER_df = sum_per_node.copy()

ER_df["VariableId"] = ER_df["VariableId"].replace(["N - Totaal", "P - Totaal"], ["N", "P"])

ER_df_wide = (
    ER_df.pivot_table(index=["NodeId", "Year"], columns="VariableId", values="Value")
    .rename_axis(
        columns=None  # removes 'VariableId' as the column name
    )
    .reset_index()
)

ER_df_wide["NO3"] = ER_df_wide["N"] * 0.8
ER_df_wide["NH4"] = ER_df_wide["N"] * 0.1
ER_df_wide["OON"] = ER_df_wide["N"] * 0.1
ER_df_wide["PO4"] = ER_df_wide["P"] * 0.5
ER_df_wide["AAP"] = ER_df_wide["P"] * 0.4
ER_df_wide["OOP"] = ER_df_wide["P"] * 0.1

ER_df_wide["time"] = pd.to_datetime(ER_df_wide.Year, format="%Y")
ER_df_wide.rename(columns={"NodeId": "node_id"}, inplace=True)

loads_df = ER_df_wide.melt(
    id_vars=["node_id", "time"],
    value_vars=["NO3", "NH4", "OON", "PO4", "AAP", "OOP"],
    var_name="substance",
    value_name="load",
)

output_path = output_dir / "ER_loads_g_s_df.parquet"

try:
    loads_df.to_parquet(output_path, index=False)
    logger.info("Saved loads_df to %s", output_path)
except Exception:
    logger.exception("ER data conversion failed")
    raise

# %%
# -------------------------- Couple loads to LHM datastructure ----------------------------

# model = Model.read(toml_path)
# model.basin.mass_load = loads_df
# model.write(toml_path)

# #%%
# # re-read saved model and check mass_load

# model = Model.read(toml_path)
# model.basin.mass_load

# %%
