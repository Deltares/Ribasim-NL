"""Script to convert ER data to ribasim-delwaq input.

created: 01-2019 by: Wilfred Altena
modified: 03-2019 by: Annelotte van der Linden
modified: 02-2024 by: Steven Kelderman
last modified: 08-2025 by: Jesse van Leeuwen

Aanpassing van conversie van ER data naar KRW-V input
In 'Functions' en tussen code onder 'Overige emissies ER' en boven 'Export B6_loads'
is de code van 02-2024 onveranderd gebleven. Hieronder de toelichting van deze versie:


Script om emissieoorzaken vanuit de ER in de laden en om te zetten naar KRW-verkenner
invoerbestanden. Hierbij wordt onderscheid gemaakt in emissieoorzaak en de daarbij
behorende berekeningswijze. Hier zijn drieverschillende berekeningswijzen bepaald.
1. industrie: Deze emissies zijn per jaar per bedrijf bekend voor N en P. Soms
zelfs met meerdere lozingspunten. De bedrijven file wordt apart handmatig ingeladen
in dit script. 2. emissieoorzaken met gedetailleerde jaarlijkse totalen vanuit
Deltares. Hierbij hoeft alleen de ruimtelijke verdeling van GAF90eenheden te worden
geinterpoleerd. 3.emissieoorzaken zonder detail. Hierbij zijn alleen de ER steekjaren
bekend en wordt er tussen deze jaren geinterpoleerd.




"""
# -------------------------------conversion-------------------------------------

import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from ER_GAF_fractions_func import compute_overlap_df

conv_yr2sec = 60 * 60 * 24 * 365.25
conv_kg2g = 1000
conv_ton2g = 10**6

# -------------------------------Packages---------------------------------------

# -------------------------------Functions--------------------------------------


def makedir(path):
    """Make directory if not existing."""
    if not os.path.exists(path):
        print("path doesn't exist. trying to make")
        os.makedirs(path)


def write_dataframe(filename, df):
    """Write a dataframe to csv."""
    # Write dataframe to csv-file
    df.to_csv(filename, header=True, index=False, sep=";")  # , header=False)


def lineplot_N_P(file, N, P, kg=True, set_log=False, title=None):
    """Generate lineplots for the different EMK per Sub."""
    # Filter the DataFrame for 'N - totaal' 'P - Totaal'
    filtered_df_n = file[file["VariableId"] == N]
    filtered_df_p = file[file["VariableId"] == P]

    # Set up the subplots
    fig, axes = plt.subplots(1, 2, figsize=(15, 6), sharey=False)

    # Define manual colormap
    category_colors = {
        "Atmosferische depositie": "blue",
        "Meemesten sloten": "green",
        "Glastuinbouw": "red",
        "Erfafspoeling": "yellow",
        "Regenwaterriolen": "aqua",
        "OverigeEmissies": "black",
        "Emissiesbedrijven": "purple",
        "Industrie": "purple",
    }

    # Plot for 'N - totaal'
    sns.lineplot(
        x="Year",
        y="Value",
        hue="EmissionTypeId",
        data=filtered_df_n,
        palette=category_colors,
        errorbar=None,
        ax=axes[0],
    )
    axes[0].set_title(f"Sum of Emissions of {title} per EMK for N - totaal")
    axes[0].set_xlabel("Year")
    if kg:
        axes[0].set_ylabel("Sum of Emissions (kg/year)")
    else:
        axes[0].set_ylabel("Sum of Emissions (g/s)")
    axes[0].legend().remove()
    axes[0].grid(True)
    axes[0].set_ylim(0)
    if set_log:
        axes[0].set_yscale("log")
    else:
        pass

    # Plot for 'P - totaal'
    sns.lineplot(
        x="Year",
        y="Value",
        hue="EmissionTypeId",
        data=filtered_df_p,
        palette=category_colors,
        errorbar=None,
        ax=axes[1],
    )
    axes[1].set_title(f"Sum of Emissions of {title} per EMK for P - totaal")
    axes[1].set_xlabel("Year")
    if kg:
        axes[1].set_ylabel("Sum of Emissions (kg/year)")
    else:
        axes[1].set_ylabel("Sum of Emissions (g/s)")
    axes[1].grid(True)
    axes[0].set_ylim(0)
    if set_log:
        axes[0].set_yscale("log")
    else:
        pass

    # Get handles and labels from the first plot
    handles, labels = axes[0].get_legend_handles_labels()
    # Create legend using handles and labels from axes[0] plot
    plt.legend(
        handles=handles,
        labels=labels,
        title="EmissionTypeId",
        bbox_to_anchor=(-0.1, -0.1),
        loc="upper center",
        ncol=len(category_colors) // 2,
    )

    plt.show()


def barplot_N_P(file, N, P, y_lim_min_n, y_lim_max_n, y_lim_min_p, y_lim_max_p, kg=True, title=None):
    """Generate Barplot per Year per sub."""
    # Filter the DataFrame for 'N - totaal' and 'P - Totaal'
    filtered_df_n = file[file["VariableId"] == N]
    filtered_df_p = file[file["VariableId"] == P]

    # Set up the subplots
    fig, axes = plt.subplots(1, 2, figsize=(15, 6), sharey=False)

    # Plot for 'N - totaal'
    sns.barplot(x="Year", y="Value", data=filtered_df_n, errorbar=None, ax=axes[0])
    axes[0].set_title(f"Sum of Emissions of {title} for N - totaal")
    axes[0].set_xlabel("Year")
    if kg:
        axes[0].set_ylabel("Sum of Emissions (kg/year)")
    else:
        axes[0].set_ylabel("Sum of Emissions (g/s)")
    axes[0].set_ylim(y_lim_min_n, y_lim_max_n)
    axes[0].grid(True)
    # axes[0].set_ylim(450, 525)

    # Plot for 'P - totaal'
    sns.barplot(x="Year", y="Value", data=filtered_df_p, errorbar=None, ax=axes[1])
    axes[1].set_title(f"Sum of Emissions of {title} for P - totaal")
    axes[1].set_xlabel("Year")
    if kg:
        axes[1].set_ylabel("Sum of Emissions (kg/year)")
    else:
        axes[1].set_ylabel("Sum of Emissions (g/s)")
    axes[1].set_ylim(y_lim_min_p, y_lim_max_p)
    # axes[1].set_ylim(27.5, 37.5)
    axes[1].grid(True)

    plt.show()


# -------------------------------Settings---------------------------------------
d = {}
d["run"] = "validatie"  # validatie, prognose

schematisatie = "Ribasim-NL"  # $ was 'LKM25', moet nieuwe bestandstructuur komen

# # -------------------------------Directories------------------------------------

inputdir = (
    "P:/krw-verkenner/01_landsdekkende_schematisatie/LKM25 schematisatie/OverigeEmissies/KRW_Tussenevaluatie_2024/"
)
model_path = Path(os.environ["RIBASIM_NL_DATA_DIR"]) / "DeDommel/modellen/DeDommel_2025_7_0"
basin_node_path = model_path / "database.gpkg"

# -------------------------------Import data------------------------------------

koppeling = compute_overlap_df(
    gaf_path="P:/11210327-lwkm2/01_data/Emissieregistratie/gaf_90.shp",
    basin_path=basin_node_path,
)

koppeling["GAF-eenheid"] = koppeling["GAF-eenheid"].astype(int)

# $ check voor nieuwe koppeling
sum_per_node = koppeling.groupby("NodeId")["fractie"].sum().reset_index()
sum_per_node = koppeling.groupby("GAF-eenheid")["fractie"].sum().reset_index().sort_values(by="fractie")

with pd.option_context("display.max_rows", None):
    print(sum_per_node)

# $ eventueel kunnen de fracties per GAF met de emissies per GAF worden vermenigvuldigd om te checken of het matched met wat er uit dit script komt rollen als totale emissies

# Manual file to fill in Deltares ER yearly loads #$ not using this rn
Emissies_per_jaar_buiten_ER = pd.read_csv(
    os.path.join(inputdir, "Emissies_per_jaar_buiten_ER.csv"), delimiter=";", encoding="latin1"
)

# Direct download from the ER website at GAF90 level #$ MAKE SEARCH FOR MOST RECENT DATE INSTEAD OF MANUALLY WRITING
ER_data_EMK_GAF90 = pd.read_excel(
    os.path.join(inputdir, "ER_DataExport-2024-01-29-142759.xlsx"),  # $ I have a recent import but need a match
    sheet_name="Emissies",
    usecols=["Stofcode", "Stof", "Code_gebied", "Sector", "Subsector", "Emissieoorzaak", "Jaar", "Emissie"],
)

# Manual file to import bedrijven without coastal waters #$ also not rn
OverigeEmissies_bedrijven__2024_01_24 = pd.read_csv(
    os.path.join(inputdir, "OverigeEmissies_bedrijven__2024_01_24.csv"), delimiter=";", encoding="latin1"
)

# -------------------------------Overige emissies ER----------------------------

# Make selection of available EMKs
EMISSIEOORZAKEN = ER_data_EMK_GAF90["Emissieoorzaak"].unique().tolist()

# List of EMKs not to use in this script or part
remove_EMK = ["SBI", "spoeling nutri", "Effluenten RWZI", "Depositie NCP"]

# Generate filter of right EMKs
EMISSIEOORZAKEN_FILTER = [EMK for EMK in EMISSIEOORZAKEN if not any(rem in EMK for rem in remove_EMK)]
print(EMISSIEOORZAKEN_FILTER)


# Filter for the right emissieoorzaken
ER_data_EMK_GAF90_fltr = ER_data_EMK_GAF90.loc[ER_data_EMK_GAF90["Emissieoorzaak"].isin(EMISSIEOORZAKEN_FILTER)].copy()
print(ER_data_EMK_GAF90_fltr["Emissieoorzaak"].unique().tolist())


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

#####################################################################################

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

# Generate output

if d["run"] == "validatie":
    DifusseEmissions_OE["EmissionTypeId"] = "OverigeEmissies"

    DifusseEmissions_OE = DifusseEmissions_OE.groupby(
        ["NodeId", "EmissionTypeId", "Year", "VariableId"], as_index=False
    ).sum()

elif d["run"] == "prognose":
    pass


# ---------------------------------Export B6_loads---------------------------------------

# vanaf hier bevat DifusseEmissions_OE de juiste data voor export naar B6_loads.inc bestand

output_path = model_path / "delwaq"

grouped = DifusseEmissions_OE.groupby(["NodeId", "VariableId"])

with open(output_path / "B6_loads.inc", "w") as f:
    for (node_id, variable_id), group in grouped:
        f.write(f"ITEM 'Basin_{node_id}' CONCENTRATIONS '{variable_id}' LINEAR TIME LINEAR DATA '{variable_id}'\n")

        for _, row in group.iterrows():
            year = row["Year"]
            value = row["Value"]
            f.write(f"'{year}/01/01-00:00:00' {value:.6f}\n")

        f.write("\n")
