"""
Created on Fri Oct  9 18:02:38 2026

@author: janssen_gs

Kwaliteitsclassificatie van KRW meetreeksen voor validatieperiode(s).
Beoordeling van de dekking per kwartaal en waarnemingsfrequentie voor Ntot en Ptot.
"""

# %%
from pathlib import Path

import numpy as np
import pandas as pd

# %%
# =============================================================================
# CONFIGURATIE (eenvoudig aan te passen voor toekomstige validatieperiodes)
# =============================================================================
PARQUET_FILE = Path(
    r"p:/11212767-lwkm2/RibasimNL/Validatie/Waterkwaliteit/KRWMeetwaarden_1990_2025_20260615_1432.parquet"
)

# Validatieperiode
START_YEAR = 2017
END_YEAR = 2019

# Doel-parameters
PARAMETERS = ["Ntot", "Ptot"]

# Kolom die de meetlocatie/reeks uniek identificeert
LOCATION_COL = "KRW_monitoringslocatie"

# Uitvoerbestand (optioneel, indien gewenst als parquet op te slaan)
OUTPUT_PARQUET = None  # bijv. PARQUET_FILE.with_name("KRWMeetwaarden_met_kwaliteitslabel.parquet")
OUTPUT_CSV_PER_LOCATION = Path(
    r"p:/11212767-lwkm2/RibasimNL/Validatie/Waterkwaliteit/KRWMeetwaarden_met_kwaliteitslabel_per_locatie.csv"
)


# =============================================================================
# FUNCTIES VOOR KWALITEITSCLASSIFICATIE
# =============================================================================
def evaluate_series_quality(
    df: pd.DataFrame,
    start_year: int,
    end_year: int,
    parameters: list[str],
    location_col: str = "KRW_monitoringslocatie",
) -> pd.DataFrame:
    """Berekenen dekkingsstatistieken voor KRW meetreeksen.

    Berekent per meetlocatie en per parameter de dekkingsstatistieken en kent
    een kwaliteitsklasse (A, B, C, D of E) toe voor de opgegeven periode [start_year, end_year].

    Kwaliteitsklassen:
      - Klasse A (Uitstekend):
          * Volledige dekking van alle kwartalen in de validatieperiode (100%)
          * Voldoende waarnemingsdichtheid (mediaan >= 2.5 metingen/kwartaal, i.e. ~maandelijks)
      - Klasse B (Goed):
          * Hoge kwartaaldekking (>= 80% van de kwartalen) OF alle kwartalen gedekt
            met lagere waarnemingsfrequentie (< 2.5 metingen/kwartaal).
      - Klasse C (Matig):
          * Matige kwartaaldekking (>= 50% van de kwartalen gedekt).
      - Klasse D (Zwak):
          * Lage dekking (< 50% van de kwartalen gedekt), maar valt niet onder klasse E.
      - Klasse E (Onvoldoende):
          * Maximaal 2 kwartalen gedekt (<= 2) én NIET alle van die kwartalen bevatten
            voor elke maand een meting (kwartalen_volledig_bemonsterd < kwartalen_gedekt of 0 kwartalen).
    """
    total_years = end_year - start_year + 1
    total_quarters = total_years * 4

    # Filter op de gewenste parameters en de validatieperiode
    sub = df[df["parameter"].isin(parameters) & df["jaar"].between(start_year, end_year)].copy()

    # Zorg voor correcte datetime en kwartaalaanduiding
    sub["datum_dt"] = pd.to_datetime(sub["datum"], format="%d-%m-%Y", errors="coerce")
    sub["quarter"] = sub["datum_dt"].dt.to_period("Q")
    sub["season"] = sub["datum_dt"].dt.quarter
    sub["meting_jaar"] = sub["datum_dt"].dt.year
    sub["meting_maand"] = sub["datum_dt"].dt.month

    records = []

    for param in parameters:
        p_df = sub[sub["parameter"] == param]
        # Alle unieke locaties in de volledige dataset voor deze parameter
        all_locs = df[df["parameter"] == param][location_col].unique()

        if len(p_df) > 0:
            # Aantal metingen per locatie en kwartaal
            q_counts = p_df.groupby([location_col, "quarter"]).size().unstack(fill_value=0)

            # Aantal unieke maanden bemonsterd per locatie en kwartaal (max. 3 per kwartaal)
            q_months = p_df.groupby([location_col, "quarter"])["meting_maand"].nunique().unstack(fill_value=0)
            # Aantal kwartalen waarin alle 3 de maanden minstens 1 meting hebben
            n_q_full_months = (q_months == 3).sum(axis=1)

            # Statistieken per locatie
            obs_total = q_counts.sum(axis=1)
            n_q_covered = (q_counts > 0).sum(axis=1)
            med_obs_covered_q = q_counts.replace(0, np.nan).median(axis=1).fillna(0)
            seasons_covered = p_df.groupby(location_col)["season"].nunique()
            years_covered = p_df.groupby(location_col)["meting_jaar"].nunique()

            stats_df = pd.DataFrame(
                {
                    location_col: q_counts.index,
                    "parameter": param,
                    "n_obs_validatie": obs_total.values,
                    "kwartalen_gedekt": n_q_covered.values,
                    "kwartaaldekking_pct": (n_q_covered.values / total_quarters) * 100.0,
                    "kwartalen_volledig_bemonsterd": n_q_full_months.reindex(q_counts.index, fill_value=0).values,
                    "mediaan_obs_per_kwartaal": med_obs_covered_q.values,
                    "jaren_gedekt": years_covered.reindex(q_counts.index).fillna(0).values,
                    "seizoenen_gedekt": seasons_covered.reindex(q_counts.index).fillna(0).values,
                }
            )
        else:
            stats_df = pd.DataFrame(
                columns=[
                    location_col,
                    "parameter",
                    "n_obs_validatie",
                    "kwartalen_gedekt",
                    "kwartaaldekking_pct",
                    "kwartalen_volledig_bemonsterd",
                    "mediaan_obs_per_kwartaal",
                    "jaren_gedekt",
                    "seizoenen_gedekt",
                ]
            )

        # Locaties die in de dataset voorkomen voor deze parameter maar 0 metingen hebben in validatieperiode
        missing_locs = set(all_locs) - set(stats_df[location_col])
        if missing_locs:
            missing_df = pd.DataFrame(
                {
                    location_col: list(missing_locs),
                    "parameter": param,
                    "n_obs_validatie": 0,
                    "kwartalen_gedekt": 0,
                    "kwartaaldekking_pct": 0.0,
                    "kwartalen_volledig_bemonsterd": 0,
                    "mediaan_obs_per_kwartaal": 0.0,
                    "jaren_gedekt": 0,
                    "seizoenen_gedekt": 0,
                }
            )
            stats_df = pd.concat([stats_df, missing_df], ignore_index=True)

        # Toekennen kwaliteitsklasse
        def assign_class(row):
            n_cov = row["kwartalen_gedekt"]
            n_full = row["kwartalen_volledig_bemonsterd"]

            # Klasse E: maximaal 2 kwartalen gedekt én niet al die kwartalen hebben voor elke maand een meting
            if n_cov <= 2 and (n_cov == 0 or n_full < n_cov):
                return "E"

            cov_ratio = n_cov / total_quarters
            med_obs = row["mediaan_obs_per_kwartaal"]

            # Klasse A: 100% van de kwartalen gedekt én voldoende waarnemingsdichtheid (>= 2.5 per kwartaal)
            if cov_ratio >= 1.0 and med_obs >= 2.5:
                return "A"
            # Klasse B: >= 80% van de kwartalen gedekt, of 100% kwartalen met lagere frequentie
            elif cov_ratio >= 0.80:
                return "B"
            # Klasse C: >= 50% van de kwartalen gedekt (ten minste de helft van de periode)
            elif cov_ratio >= 0.50:
                return "C"
            # Klasse D: overige reeksen (< 50% van de kwartalen gedekt, maar niet klasse E)
            else:
                return "D"

        stats_df["kwaliteitsklasse"] = stats_df.apply(assign_class, axis=1)
        records.append(stats_df)

    quality_summary = pd.concat(records, ignore_index=True)
    return quality_summary


def add_quality_labels_to_dataframe(
    df: pd.DataFrame,
    quality_summary: pd.DataFrame,
    location_col: str = "KRW_monitoringslocatie",
) -> pd.DataFrame:
    """Koppeling kwaliteitsklasse en dekkingsstatistiek

    Voegt de berekende kwaliteitsklasse en dekkingsstatistieken toe aan het oorspronkelijke dataframe
    op basis van [location_col, parameter].
    """
    merged = df.merge(
        quality_summary[[location_col, "parameter", "kwaliteitsklasse", "kwartalen_gedekt", "kwartaaldekking_pct"]],
        on=[location_col, "parameter"],
        how="left",
    )
    return merged


# =============================================================================
# HOOFDPROGRAMMA
# =============================================================================
if __name__ == "__main__":
    print(f"Inlezen parquet bestand: {PARQUET_FILE}")
    df = pd.read_parquet(PARQUET_FILE)
    print(f"Dataframe ingelezen: {len(df):,} rijen.")

    print(f"\nEvalueren kwaliteitslabels voor periode {START_YEAR}-{END_YEAR} voor parameters: {PARAMETERS}...")
    quality_summary = evaluate_series_quality(
        df=df,
        start_year=START_YEAR,
        end_year=END_YEAR,
        parameters=PARAMETERS,
        location_col=LOCATION_COL,
    )

    print("\n--- Overzicht kwaliteitsklassen per parameter ---")
    for param in PARAMETERS:
        p_stats = quality_summary[quality_summary["parameter"] == param]
        print(f"\nParameter: {param} (totaal {len(p_stats)} meetlocaties)")
        counts = p_stats["kwaliteitsklasse"].value_counts().reindex(["A", "B", "C", "D", "E"], fill_value=0)
        for label, count in counts.items():
            pct = (count / len(p_stats)) * 100.0
            print(f"  Klasse {label}: {count:4d} locaties ({pct:5.1f}%)")

    # Voeg kwaliteitslabels toe aan het dataframe
    print("\nToevoegen kwaliteitskolommen aan dataframe...")
    df_with_qc = add_quality_labels_to_dataframe(
        df=df,
        quality_summary=quality_summary,
        location_col=LOCATION_COL,
    )

    print("\nVoorbeeld van verrijkt dataframe (Ntot / Ptot):")
    sample_cols = [LOCATION_COL, "datum", "jaar", "parameter", "meetwaarde", "kwaliteitsklasse", "kwartalen_gedekt"]
    print(df_with_qc[df_with_qc["parameter"].isin(PARAMETERS)][sample_cols].head(10))

    # %%
    if OUTPUT_PARQUET:
        print(f"\nOpslaan verrijkt dataframe naar: {OUTPUT_PARQUET}")
        df_with_qc.to_parquet(OUTPUT_PARQUET, index=False)
        print("Opslaan voltooid.")

    # Opslaan van één record per meetlocatie als CSV
    if OUTPUT_CSV_PER_LOCATION:
        print(f"\nGenereren CSV met één regel per meetlocatie: {OUTPUT_CSV_PER_LOCATION}")

        # Pivot quality_summary zodat elke parameter eigen kolommen krijgt per locatie
        qc_pivot = quality_summary.pivot(
            index=LOCATION_COL,
            columns="parameter",
            values=[
                "kwaliteitsklasse",
                "kwartalen_gedekt",
                "kwartaaldekking_pct",
                "kwartalen_volledig_bemonsterd",
                "mediaan_obs_per_kwartaal",
            ],
        )
        # Maak vlakke kolomnamen: bijv. 'kwaliteitsklasse_Ntot', 'kwaliteitsklasse_Ptot'
        qc_pivot.columns = [f"{col}_{param}" for col, param in qc_pivot.columns]
        qc_pivot = qc_pivot.reset_index()

        # Behoud eventuele algemene locatie-metadata (bijv. coördinaten) indien aanwezig in df
        meta_cols = [
            c
            for c in df.columns
            if c in [LOCATION_COL, "X", "Y", "x", "y", "geometry", "waterlichaam"] and c in df.columns
        ]
        if meta_cols:
            loc_meta = df[meta_cols].drop_duplicates(subset=[LOCATION_COL])
            loc_summary = loc_meta.merge(qc_pivot, on=LOCATION_COL, how="right")
        else:
            loc_summary = qc_pivot

        loc_summary.to_csv(OUTPUT_CSV_PER_LOCATION, index=False, sep=";")
        print(f"CSV opgeslagen ({len(loc_summary):,} unieke locaties).")
