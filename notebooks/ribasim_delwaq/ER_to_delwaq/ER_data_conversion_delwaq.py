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
- plotting scripts opgeschoond










"""
# %%
# -------------------------------Packages---------------------------------------

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from ER_GAF_fractions_func import compute_overlap_df

# ------------------------ Import local module ----------------------------------
from ribasim_nl.model import Model

from ribasim_nl import CloudStorage

current_dir = Path(__file__).resolve().parent
print(f"Current directory: {current_dir}")
print("Check if working directory is the script directory.")

# auto-check
root_dir = ""
if current_dir.parts[-2:] == ("ribasim_delwaq", "ER_to_delwaq"):
    print("match")
    root_dir = "../../../"

# -------------------------------Conversions------------------------------------

conv_yr2sec = 60 * 60 * 24 * 365.25
conv_kg2g = 1000
conv_ton2g = 10**6

# -------------------------------Directories------------------------------------
cloud = CloudStorage()

model_name = "lhm_coupled_full"
toml_name = "lhm_coupled.toml"
model_path = Path(root_dir, "data/Rijkswaterstaat/modellen", model_name)
# model_path = cloud.joinpath("Rijkswaterstaat", "modellen", model_name)
toml_path = model_path / toml_name
# cloud.synchronize(filepaths=[model_path], overwrite=False)
basin_path = model_path / "input/database.gpkg"

er_path = cloud.joinpath("Basisgegevens/Delwaq/aangeleverd/Emissieregistratie")
emissies_buiten_ER_path = er_path / "Emissies_per_jaar_buiten_ER.csv"
ER_export_path = er_path / "ER_DataExport-2024-01-29-142759.xlsx"
OE_bedrijven_path = er_path / "OverigeEmissies_bedrijven__2024_01_24.csv"
gaf_path = er_path / "gaf_90.shp"

cloud.synchronize(filepaths=[er_path], overwrite=False)

# %%
# -------------------------------Settings---------------------------------------
frac_doorgaand = 0.5  # deel ER op doorvoerende basin node
frac_bergend = 1 - frac_doorgaand  # deel ER op bergende basin node

# -------------------------------Functions--------------------------------------


def validate_df(
    df, required_columns=("Year", "Value", "EmissionTypeId", "VariableId")
):  # $ specifically made for plotting functions
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}")


def lineplot_N_P(df, N, P, kg=True, set_log=False, title=None):
    """Generate lineplots for the different EMK per Sub."""
    validate_df(df)

    df_n = df[df["VariableId"] == N]
    df_p = df[df["VariableId"] == P]

    _fig, axes = plt.subplots(1, 2, figsize=(15, 6), sharey=False)

    sns.lineplot(
        data=df_n,
        x="Year",
        y="Value",
        hue="EmissionTypeId",
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
    validate_df(df)

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
koppeling = compute_overlap_df(gaf_path, basin_path, hws=False)
koppeling["GAF-eenheid"] = koppeling["GAF-eenheid"].astype(int)


print("coupling GAF-emissions to LHM basin nodes completed")

# process fractions based on basin type, only splitting up doorgaand and bergend
# there is also the category "hoofdwater" (within waterboards), but these do not have a duplicate node in the same location
# the actual HWS does not not have a meta_categorie
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

# Direct download from the ER website at GAF90 level #$ MAKE SEARCH FOR MOST RECENT DATE INSTEAD OF MANUALLY WRITING
ER_data_EMK_GAF90 = pd.read_excel(
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

# filter out unnecessary emissieoorzaken #$ done in previous version, but why?
EMISSIEOORZAKEN = ER_data_EMK_GAF90["Emissieoorzaak"].unique().tolist()
remove_EMK = ["SBI", "spoeling nutri", "Effluenten RWZI", "Depositie NCP"]
EMISSIEOORZAKEN_FILTER = [EMK for EMK in EMISSIEOORZAKEN if not any(rem in EMK for rem in remove_EMK)]
ER_data_EMK_GAF90_fltr = ER_data_EMK_GAF90.loc[ER_data_EMK_GAF90["Emissieoorzaak"].isin(EMISSIEOORZAKEN_FILTER)].copy()
print("Catagories considered:", ER_data_EMK_GAF90_fltr["Emissieoorzaak"].unique().tolist())


EMISSION_TYPES = ["Depositie Nederland", "Glastuinbouw", "Erfafspoeling", "Regenwaterriolen", "Meemesten sloten"]

# Rename emission types to 'OverigeEmissies' that are not in the list with emission types
ER_data_EMK_GAF90_fltr.loc[~ER_data_EMK_GAF90_fltr["Emissieoorzaak"].isin(EMISSION_TYPES), "Emissieoorzaak"] = (
    "OverigeEmissies"
)

# Rename 'Depositie Nederland' to 'Atmosferische depositie'
ER_data_EMK_GAF90_fltr.loc[ER_data_EMK_GAF90_fltr["Emissieoorzaak"].isin(["Depositie Nederland"]), "Emissieoorzaak"] = (
    "Atmosferische depositie"
)

# Sum towards EMK per GAF per Year per sub
ER_data_EMK_GAF90_fltr_sum = (
    ER_data_EMK_GAF90_fltr.groupby(["Code_gebied", "Emissieoorzaak", "Stof", "Jaar"])["Emissie"].sum().reset_index()
)

# Generate pivot tabel of the GAF area, emissieoorzaak and substance
ER_data_EMK_GAF90_fltr_sum_piv = pd.pivot_table(
    ER_data_EMK_GAF90_fltr_sum, index=["Code_gebied", "Emissieoorzaak", "Stof"], values="Emissie", columns="Jaar"
).reset_index()

# Transpose columns to long format
sum_per_GAF_base_long = pd.melt(
    ER_data_EMK_GAF90_fltr_sum_piv,
    id_vars=["Code_gebied", "Emissieoorzaak", "Stof"],
    var_name="Jaar",
    value_name="Emissie",
)

sum_per_GAF_base_long["Jaar"] = sum_per_GAF_base_long["Jaar"].astype(int)

# Sum results per EMK
sum_per_EMK_base_long = sum_per_GAF_base_long.groupby(["Emissieoorzaak", "Stof", "Jaar"])["Emissie"].sum().reset_index()

sum_per_EMK_base_short = pd.pivot(
    sum_per_EMK_base_long, index=["Emissieoorzaak", "Stof"], columns="Jaar"
).reset_index()  # Check

# Sum results per Year
sum_per_Year_base_long = sum_per_GAF_base_long.groupby(["Stof", "Jaar"])["Emissie"].sum().reset_index()

sum_per_Year_base_short = pd.pivot(sum_per_Year_base_long, index=["Stof"], columns="Jaar").reset_index()  # Check

print("yearly totals per EMK and per year computed")

###

# %%###### If all yearly totals are known from Deltares#######
# for these EMKs there is more detailed information available at Deltares emissieregistratie

# List all the EMKs that have detailed emission values
EMK_DETAIL = ["Erfafspoeling", "Glastuinbouw"]

# Create a filtered subset of EMKs
ER_data_EMK_GAF90_detail = ER_data_EMK_GAF90_fltr_sum_piv[
    ER_data_EMK_GAF90_fltr_sum_piv["Emissieoorzaak"].isin(EMK_DETAIL)
].copy()

# Merge pivot on known values from Deltares that are not on the ER website available
ER_data_EMK_GAF90_detail_merge = ER_data_EMK_GAF90_detail.merge(
    Emissies_per_jaar_buiten_ER, on=["Emissieoorzaak", "Stof"], how="left"
)

# Loop through available years to calculate fraction per GAF area
for year in [2010, 2015, 2020]:
    ER_data_EMK_GAF90_detail_merge[f"fac_{year}"] = ER_data_EMK_GAF90_detail_merge.groupby(
        ["Emissieoorzaak", "Stof", "Jaar"]
    )[year].transform(lambda x: x / x.sum())


# Interpolate all the fractions in the years between the known years
ER_data_EMK_GAF90_detail_merge["fac_2011"] = (
    0.8 * ER_data_EMK_GAF90_detail_merge["fac_2010"] + 0.2 * ER_data_EMK_GAF90_detail_merge["fac_2015"]
)
ER_data_EMK_GAF90_detail_merge["fac_2012"] = (
    0.6 * ER_data_EMK_GAF90_detail_merge["fac_2010"] + 0.4 * ER_data_EMK_GAF90_detail_merge["fac_2015"]
)
ER_data_EMK_GAF90_detail_merge["fac_2013"] = (
    0.4 * ER_data_EMK_GAF90_detail_merge["fac_2010"] + 0.6 * ER_data_EMK_GAF90_detail_merge["fac_2015"]
)
ER_data_EMK_GAF90_detail_merge["fac_2014"] = (
    0.2 * ER_data_EMK_GAF90_detail_merge["fac_2010"] + 0.8 * ER_data_EMK_GAF90_detail_merge["fac_2015"]
)

# Interpolate all the fractions in the years between the known years
ER_data_EMK_GAF90_detail_merge["fac_2016"] = (
    0.8 * ER_data_EMK_GAF90_detail_merge["fac_2015"] + 0.2 * ER_data_EMK_GAF90_detail_merge["fac_2020"]
)
ER_data_EMK_GAF90_detail_merge["fac_2017"] = (
    0.6 * ER_data_EMK_GAF90_detail_merge["fac_2015"] + 0.4 * ER_data_EMK_GAF90_detail_merge["fac_2020"]
)
ER_data_EMK_GAF90_detail_merge["fac_2018"] = (
    0.4 * ER_data_EMK_GAF90_detail_merge["fac_2015"] + 0.6 * ER_data_EMK_GAF90_detail_merge["fac_2020"]
)
ER_data_EMK_GAF90_detail_merge["fac_2019"] = (
    0.2 * ER_data_EMK_GAF90_detail_merge["fac_2015"] + 0.8 * ER_data_EMK_GAF90_detail_merge["fac_2020"]
)


# Generate new empty dataframe to be filled with unkown yearly values of new emissies scaled to proportion of GAF area

ER_data_EMK_GAF90_detail_inter = pd.DataFrame()
# Loop though the unknown years to add new column with new emissions
for i in [2011, 2012, 2013, 2014, 2016, 2017, 2018, 2019]:
    temp = ER_data_EMK_GAF90_detail_merge.copy()
    # Calculate the new column 'new_values' for the specific year
    temp["new_values"] = temp[f"fac_{i}"] * temp["Emissie"]
    # Add the new column 'new_values' to the 'new' DataFrame
    ER_data_EMK_GAF90_detail_inter = pd.concat([ER_data_EMK_GAF90_detail_inter, temp], ignore_index=True)

# Make a pivot of ['Code_gebied', 'Emissieoorzaak', 'Stof'] rows that have values
ER_data_EMK_GAF90_detail_inter_short = pd.pivot_table(
    ER_data_EMK_GAF90_detail_inter, index=["Code_gebied", "Emissieoorzaak", "Stof"], columns="Jaar", values="new_values"
).reset_index()

# Rename columns
ER_data_EMK_GAF90_detail_inter_short.rename(
    columns={
        "Code_gebied": "GAF-eenheid",
        "Emissieoorzaak": "EmissionTypeId",
        "new_values": "Value",
        "Stof": "VariableId",
    },
    inplace=True,
)

# Transpose columns to long format
ER_data_EMK_GAF90_detail_inter_long = pd.melt(
    ER_data_EMK_GAF90_detail_inter_short,
    id_vars=["GAF-eenheid", "EmissionTypeId", "VariableId"],
    var_name="Year",
    value_name="Value",
)

# %%%########### just interpolating#############

EMK_INTERPOLATION = ["Atmosferische depositie", "Meemesten sloten", "OverigeEmissies", "Regenwaterriolen"]

# Create a filtered subset using .isin
ER_data_EMK_GAF90_inter = ER_data_EMK_GAF90_fltr_sum_piv[
    ER_data_EMK_GAF90_fltr_sum_piv["Emissieoorzaak"].isin(EMK_INTERPOLATION)
].copy()


# Interpolate between the known year emissions
ER_data_EMK_GAF90_inter[2011] = 0.8 * ER_data_EMK_GAF90_inter[2010] + 0.2 * ER_data_EMK_GAF90_inter[2015]
ER_data_EMK_GAF90_inter[2012] = 0.6 * ER_data_EMK_GAF90_inter[2010] + 0.4 * ER_data_EMK_GAF90_inter[2015]
ER_data_EMK_GAF90_inter[2013] = 0.4 * ER_data_EMK_GAF90_inter[2010] + 0.6 * ER_data_EMK_GAF90_inter[2015]
ER_data_EMK_GAF90_inter[2014] = 0.2 * ER_data_EMK_GAF90_inter[2010] + 0.8 * ER_data_EMK_GAF90_inter[2015]

ER_data_EMK_GAF90_inter[2016] = 0.8 * ER_data_EMK_GAF90_inter[2015] + 0.2 * ER_data_EMK_GAF90_inter[2020]
ER_data_EMK_GAF90_inter[2017] = 0.6 * ER_data_EMK_GAF90_inter[2015] + 0.4 * ER_data_EMK_GAF90_inter[2020]
ER_data_EMK_GAF90_inter[2018] = 0.4 * ER_data_EMK_GAF90_inter[2015] + 0.6 * ER_data_EMK_GAF90_inter[2020]
ER_data_EMK_GAF90_inter[2019] = 0.2 * ER_data_EMK_GAF90_inter[2015] + 0.8 * ER_data_EMK_GAF90_inter[2020]

# Rename columns
ER_data_EMK_GAF90_inter.rename(
    columns={"Code_gebied": "GAF-eenheid", "Emissieoorzaak": "EmissionTypeId", "Stof": "VariableId"}, inplace=True
)

# Transpose to long format
ER_data_EMK_GAF90_inter_long = pd.melt(
    ER_data_EMK_GAF90_inter,
    id_vars=["GAF-eenheid", "EmissionTypeId", "VariableId"],
    var_name="Year",
    value_name="Value",
)

print("interpolation of unknown years completed")

# %%%######

emissies_overig_without_industry = pd.concat([ER_data_EMK_GAF90_detail_inter_long, ER_data_EMK_GAF90_inter_long])

# Sum per EMK
sum_per_EMK_wo_industry_long = (
    emissies_overig_without_industry.groupby(["EmissionTypeId", "VariableId", "Year"])["Value"].sum().reset_index()
)

sum_per_EMK_wo_industry_short = pd.pivot(
    sum_per_EMK_wo_industry_long, index=["EmissionTypeId", "VariableId"], columns="Year"
).reset_index()

lineplot_N_P(sum_per_EMK_wo_industry_long, "N - Totaal", "P - Totaal", kg=True, set_log=False, title="GAF")

# Sum results per Year
sum_per_Year_wo_industry_long = (
    sum_per_EMK_wo_industry_long.groupby(["VariableId", "Year"])["Value"].sum().reset_index()
)

sum_per_Year_wo_industry_short = pd.pivot(
    sum_per_Year_wo_industry_long, index=["VariableId"], columns="Year"
).reset_index()  # Check

barplot_N_P(
    sum_per_Year_wo_industry_long,
    "N - Totaal",
    "P - Totaal",
    y_lim_min_n=14_000_000,
    y_lim_max_n=24_000_000,
    y_lim_min_p=700_000,
    y_lim_max_p=1_200_000,
    kg=True,
    title="GAF",
)

print("processing of other emissions without industry completed")

###

# ---------------------------Overige emissies bedrijven-------------------------

# Emissions per GAF per Year per Sub per EMK
OverigeEmissies_bedrijven_long = pd.melt(
    OverigeEmissies_bedrijven__2024_01_24,
    id_vars=["GAF-eenheid", "VariableId", "EmissionTypeId"],
    var_name="Year",
    value_name="Value",
)

OverigeEmissies_bedrijven_long["Year"] = OverigeEmissies_bedrijven_long["Year"].astype(int)

print("processing of other emissions from industry completed")

# ---------------------------------Output---------------------------------------

# voeg beide dataframes samen in 1 dataframe per GAF
emissies_totaal = pd.concat([OverigeEmissies_bedrijven_long, emissies_overig_without_industry], ignore_index=True)

# Sum per EMK
sum_per_EMK_total_short = emissies_totaal.pivot_table(
    index=["EmissionTypeId", "VariableId"], columns="Year", values="Value", aggfunc="sum"
).reset_index()

# Make long format
sum_per_EMK_total_long = sum_per_EMK_total_short.melt(
    id_vars=["VariableId", "EmissionTypeId"], var_name="Year", value_name="Value"
)

# l ineplot_N_P(sum_per_EMK_total_long, 'N - Totaal', 'P - Totaal', kg=True, set_log=False)

# Sum per Year
sum_per_Year_total_short = emissies_totaal.pivot_table(
    index=["VariableId"], columns="Year", values="Value", aggfunc="sum"
).reset_index()

# Make long format
sum_per_Year_total_long = sum_per_Year_total_short.melt(id_vars=["VariableId"], var_name="Year", value_name="Value")

sum_per_Year_total_long["Year"] = sum_per_Year_total_long["Year"].astype(int)

# b arplot_N_P(sum_per_Year_total_long, 'N - Totaal', 'P - Totaal', y_lim_min_n=19_000_000, y_lim_max_n=24_000_000, y_lim_min_p=700_000, y_lim_max_p=1_200_000, kg=True)


#################################################

# Samenvoegen Industrie met Overige Emissies

emissies_totaal = emissies_totaal.rename(columns={"Stof": "VariableId"})

emissies_totaal_sum = emissies_totaal.groupby(["GAF-eenheid", "EmissionTypeId", "VariableId", "Year"], as_index=False)[
    "Value"
].sum()

# Change name of Industry to Overige emissies
emissies_totaal_sum.loc[emissies_totaal_sum["EmissionTypeId"] == "Industrie", "EmissionTypeId"] = "OverigeEmissies"

# Sum the values by adding emissions of industry
emissies_totaal_sum2 = emissies_totaal_sum.groupby(
    ["GAF-eenheid", "EmissionTypeId", "VariableId", "Year"], as_index=False
)["Value"].sum()

# Check

# Sum per EMK long
sum_per_EMK_final_long = emissies_totaal_sum2.groupby(["EmissionTypeId", "VariableId", "Year"], as_index=False)[
    "Value"
].sum()

# Sum per EMK short
sum_per_EMK_final_short = pd.pivot(
    sum_per_EMK_final_long, index=["EmissionTypeId", "VariableId"], columns="Year"
).reset_index()

# Generate barplot
lineplot_N_P(sum_per_EMK_final_long, "N - Totaal", "P - Totaal", kg=True, set_log=False, title="GAF")

# Sum per Year
sum_per_Year_final_long = emissies_totaal_sum2.groupby(["VariableId", "Year"], as_index=False)["Value"].sum()

# Sum per Year
sum_per_Year_final_short = pd.pivot(sum_per_Year_final_long, index=["VariableId"], columns="Year").reset_index()

sum_per_Year_final_long["Year"] = sum_per_Year_final_long["Year"].astype(int)

# Generate barplot
barplot_N_P(
    sum_per_Year_final_long,
    "N - Totaal",
    "P - Totaal",
    y_lim_min_n=14_000_000,
    y_lim_max_n=24_000_000,
    y_lim_min_p=700_000,
    y_lim_max_p=1_200_000,
    kg=True,
    title="GAF",
)

print("created figures")

# ---------------------------------Output--------------------------------------- #$ actual output

# koppel GAF-eenheid aan nodes
emissies_totaal_sum2_merged_with_nodes = koppeling.merge(emissies_totaal_sum2, how="inner", on="GAF-eenheid")

# bepaal emissie per node
emissies_totaal_sum2_merged_with_nodes["Value_new"] = (
    emissies_totaal_sum2_merged_with_nodes["Value"] * emissies_totaal_sum2_merged_with_nodes["fractie"]
)

# emissies_sum_per_node = emissies_per_node.groupby(
#     ['NodeId', 'EmissionTypeId', 'Year', 'VariableId'],
#     as_index=False).sum()

sum_per_node = emissies_totaal_sum2_merged_with_nodes.groupby(
    ["NodeId", "EmissionTypeId", "Year", "VariableId"], as_index=False
).agg({"Value_new": "sum"})

# Check

# Sum per EMK long
sum_per_EMK_per_node_long = sum_per_node.groupby(["EmissionTypeId", "VariableId", "Year"], as_index=False)[
    "Value_new"
].sum()

# Sum per EMK short
sum_per_EMK_per_node_short = pd.pivot(
    sum_per_EMK_per_node_long, index=["EmissionTypeId", "VariableId"], columns="Year"
).reset_index()

# Sum per Year
sum_per_Year_per_node_long = sum_per_node.groupby(["VariableId", "Year"], as_index=False)["Value_new"].sum()

# Sum per Year
sum_per_Year_per_node_short = pd.pivot(sum_per_Year_per_node_long, index=["VariableId"], columns="Year").reset_index()

sum_per_Year_per_node_long["Year"] = sum_per_Year_per_node_long["Year"].astype(int)


# Plot
temp = sum_per_EMK_per_node_long.rename(columns={"Value_new": "Value"})

# Generate lineplot
lineplot_N_P(temp, "N - Totaal", "P - Totaal", kg=True, set_log=False, title="Node")

# Plot
temp = sum_per_Year_per_node_long.rename(columns={"Value_new": "Value"})

# Generate barplot
barplot_N_P(
    temp,
    "N - Totaal",
    "P - Totaal",
    y_lim_min_n=0,
    y_lim_max_n=24_000_000,
    y_lim_min_p=0,
    y_lim_max_p=1_200_000,
    kg=True,
    title="Node",
)


#########################

# maak dataframe met de gewenste kolommmen
DifusseEmissions_OE = sum_per_node[["NodeId", "EmissionTypeId", "VariableId", "Value_new", "Year"]]

DifusseEmissions_OE = DifusseEmissions_OE.rename(columns={"Value_new": "Value"})

DifusseEmissions_OE["Value"] = DifusseEmissions_OE["Value"] * conv_kg2g / conv_yr2sec  # from kg/365.25dgn to g/s
# DifusseEmissions_OE.insert(1, 'EmissionTypeID', 'OverigeEmissies')

DifusseEmissions_OE.insert(4, "Percentage", 0)

DifusseEmissions_OE.insert(6, "Period", 0)

# DifusseEmissions_OE['VariableId'].replace(['N - Totaal', 'P - Totaal'], ['N', 'P'], inplace=True)
DifusseEmissions_OE["VariableId"] = DifusseEmissions_OE["VariableId"].replace(["N - Totaal", "P - Totaal"], ["N", "P"])

# Check

# Sum per EMK long
DifusseEmissions_OE_per_EMK_long = DifusseEmissions_OE.groupby(
    ["EmissionTypeId", "VariableId", "Year"], as_index=False
)["Value"].sum()

# Sum per EMK short
DifusseEmissions_OE_per_EMK_short = pd.pivot(
    DifusseEmissions_OE_per_EMK_long, index=["EmissionTypeId", "VariableId"], columns="Year"
).reset_index()

# Sum per Year
DifusseEmissions_OE_per_Year_long = DifusseEmissions_OE.groupby(["VariableId", "Year"], as_index=False)["Value"].sum()

# Sum per Year
DifusseEmissions_OE_per_Year_short = pd.pivot(
    DifusseEmissions_OE_per_Year_long, index=["VariableId"], columns="Year"
).reset_index()

DifusseEmissions_OE_per_Year_long["Year"] = DifusseEmissions_OE_per_Year_long["Year"].astype(int)

# Generate lineplot
lineplot_N_P(DifusseEmissions_OE_per_EMK_long, "N", "P", kg=False, set_log=False, title="Node")
# Generate barplot
barplot_N_P(DifusseEmissions_OE_per_Year_long, "N", "P", 0, 525, 0, 37.5, kg=False, title="Node")


# %%

# convert N and P total to actual fractions

ER_df = DifusseEmissions_OE.copy()

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

# %%
# -------------------------- Couple loads to LHM datastructure ----------------------------

model = Model.read(toml_path)
model.basin.mass_load = loads_df
model.write(toml_path)

# #%%
# # re-read saved model and check mass_load

# model = Model.read(toml_path)
# model.basin.mass_load

# %%
