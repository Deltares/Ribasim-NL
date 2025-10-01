# %%
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

# === PADEN (pas aan indien nodig) ===
PAD_SELECTIES = Path(
    r"d:\projecten\D2306.LHM_RIBASIM\02.brongegevens\Noorderzijlvest\verwerkt\koppeling_meetreeksen.xlsx"
)
PAD_AFVOER = Path(
    r"d:\projecten\D2306.LHM_RIBASIM\02.brongegevens\Landelijk\resultaatvergelijking\meetreeksen\Metingen_afvoer_dag_totaal.csv"
)
PAD_AANVOER = Path(
    r"d:\projecten\D2306.LHM_RIBASIM\02.brongegevens\Landelijk\resultaatvergelijking\meetreeksen\Metingen_aanvoer_dag_totaal.csv"
)
PAD_FLOW = Path(
    r"d:\projecten\D2306.LHM_RIBASIM\02.brongegevens\Noorderzijlvest\modellen\Noorderzijlvest_dynamic_model\results\flow.arrow"
)
UITDIR = Path("grafieken_2017_m3s")
JAAR = 2017
SEC_PER_DAY = 1.0


# === Helpers ===
def read_selecties(path: Path) -> pd.DataFrame:
    """
    Leest selectie met kolommen:
      - Locatie
      - Link_id_1, Link_id_2, Link_id_3 (optioneel, hoofdletter-L)
    """
    df = pd.read_excel(path)
    df.columns = [str(c).strip() for c in df.columns]
    if "Locatie" not in df.columns:
        raise ValueError("Kolom 'Locatie' niet gevonden in selectie-Excel.")
    link_cols = [c for c in ["Link_id_1", "Link_id_2", "Link_id_3"] if c in df.columns]
    for c in link_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    df["Locatie"] = df["Locatie"].astype(str).str.strip()
    return df[["Locatie"] + link_cols].copy()


def _read_csv_smart(path: Path) -> pd.DataFrame:
    """Autodetectie van scheiding en decimaal (NL/EN) + fallback naar Excel."""
    for sep, dec in [(";", ","), (",", "."), (None, ".")]:
        try:
            if sep is None:
                return pd.read_csv(path, sep=None, engine="python")
            df = pd.read_csv(path, sep=sep, decimal=dec, engine="python")
            if df.shape[1] > 1:
                return df
        except Exception:
            pass
    return pd.read_excel(path)


def read_wide_measure_table(path: Path) -> pd.DataFrame:
    """
    Lees wide-meetbestand:
      - 1e kolom = datum (YYYY-MM-DD)
      - overige kolommen = locatienamen (dagtotalen)
    """
    df = _read_csv_smart(path) if path.suffix.lower() == ".csv" else pd.read_excel(path)
    df.columns = [str(c).strip() for c in df.columns]
    date_col = df.columns[0]
    df[date_col] = pd.to_datetime(df[date_col].astype(str).str.strip(), format="%Y-%m-%d", errors="coerce")
    df = df[df[date_col].notna()].copy()
    df.rename(columns={date_col: "Datum"}, inplace=True)
    for c in df.columns:
        if c != "Datum":
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def filter_year(df: pd.DataFrame, jaar: int) -> pd.DataFrame:
    start = pd.Timestamp(jaar, 1, 1)
    stop = pd.Timestamp(jaar + 1, 1, 1)
    return df[(df["Datum"] >= start) & (df["Datum"] < stop)].sort_values("Datum").copy()


def _detect_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def read_flow_arrow_sum_links(path: Path, link_ids: list[int]) -> pd.DataFrame:
    """
    Lees Ribasim results/flow.arrow.
    Herkent kolommen: time/datetime/date/timestep, link_id/edge_id/link/id, flow_rate/flow/discharge/q/value.
    Sommeert flow (m3/s) over opgegeven link_ids per tijdstap.
    Retourneert: ['Datum','flow_rate'] in m3/s (zonder middelen).
    """
    try:
        df = pd.read_feather(path)
    except Exception as e:
        raise RuntimeError(f"Kon flow.arrow niet lezen als Feather: {e}")

    df.columns = [str(c) for c in df.columns]
    time_col = _detect_col(df, ["time", "datetime", "date", "timestep"])
    id_col = _detect_col(df, ["link_id", "edge_id", "link", "id"])
    q_col = _detect_col(df, ["flow_rate", "flow", "discharge", "q", "value"])
    if not time_col or not id_col or not q_col:
        raise ValueError("Verplichte kolommen niet gevonden in flow.arrow. Gevonden: " + ", ".join(df.columns))

    df[time_col] = pd.to_datetime(df[time_col], errors="coerce", utc=True)
    df = df[df[time_col].notna()].copy()
    df[time_col] = df[time_col].dt.tz_convert(None)  # naive timestamps

    df[id_col] = pd.to_numeric(df[id_col], errors="coerce").astype("Int64")
    df[q_col] = pd.to_numeric(df[q_col], errors="coerce")

    ids = pd.to_numeric(pd.Series(link_ids), errors="coerce").dropna().astype(int).unique().tolist()
    if not ids:
        return pd.DataFrame(columns=["Datum", "flow_rate"])

    sdf = df[df[id_col].isin(ids)][[time_col, q_col]].copy()
    if sdf.empty:
        return pd.DataFrame(columns=["Datum", "flow_rate"])

    # som over link_ids per tijdstap (geen resample!)
    sdf = sdf.groupby(time_col, as_index=False)[q_col].sum()
    return sdf.rename(columns={time_col: "Datum", q_col: "flow_rate"}).sort_values("Datum")


def plot_locatie(loc: str, meas_df_m3s: pd.DataFrame | None, model_m3s: pd.DataFrame | None, outdir: Path, jaar: int):
    """
    Één y-as (rechts) in m³/s. Linker as verborgen.
    Zowel metingen (m³/s) als model (m³/s) op dezelfde as.
    """
    has_meas = meas_df_m3s is not None and not meas_df_m3s.empty
    has_model = model_m3s is not None and not model_m3s.empty
    if not (has_meas or has_model):
        return False

    fig, ax = plt.subplots(figsize=(10, 4))

    # lijnen tekenen
    lines, labels = [], []
    if has_meas:
        (l1,) = ax.plot(meas_df_m3s["Datum"], meas_df_m3s[loc], label="Metingen (m³/s)", color="tab:blue", linewidth=2)
        lines.append(l1)
        labels.append(l1.get_label())
    if has_model:
        (l2,) = ax.plot(
            model_m3s["Datum"],
            model_m3s["flow_rate"],
            label="Model (m³/s)",
            color="tab:orange",
            linewidth=2,
            linestyle="--",
        )
        lines.append(l2)
        labels.append(l2.get_label())

    # y-as rechts, links verbergen
    ax.yaxis.tick_right()
    ax.yaxis.set_label_position("right")
    ax.spines["left"].set_visible(False)
    ax.tick_params(axis="y", which="both", left=False)

    ax.set_ylabel("Debiet (m³/s)")
    ax.set_xlabel("Datum")
    ax.set_title(f"{loc} — {jaar}")
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend(lines, labels, loc="best")

    fig.tight_layout()
    outdir.mkdir(parents=True, exist_ok=True)
    fig.savefig(outdir / f"{loc}_{jaar}.png", dpi=150)
    plt.close(fig)
    return True


# === MAIN ===
def main():
    sel = read_selecties(PAD_SELECTIES)
    df_afvoer = read_wide_measure_table(PAD_AFVOER)
    df_aanvoer = read_wide_measure_table(PAD_AANVOER)

    # Filter metingen op 2017
    afv_2017 = filter_year(df_afvoer, JAAR)
    aanv_2017 = filter_year(df_aanvoer, JAAR)

    cols_afv = set(afv_2017.columns) - {"Datum"}
    cols_aanv = set(aanv_2017.columns) - {"Datum"}

    UITDIR.mkdir(parents=True, exist_ok=True)

    plotted, missing = [], []

    for _, r in sel.iterrows():
        loc = str(r["Locatie"]).strip()
        link_ids = [
            int(x)
            for x in [r.get("Link_id_1", pd.NA), r.get("Link_id_2", pd.NA), r.get("Link_id_3", pd.NA)]
            if pd.notna(x)
        ]

        # --- Metingen: dagtotaal → m³/s (delen door 86400), plus speciale case ---
        meas_df_m3s = None
        if loc in cols_afv:
            meas_df = afv_2017[["Datum", loc]].copy()
            meas_df_m3s = meas_df.copy()
            meas_df_m3s[loc] = meas_df_m3s[loc] / SEC_PER_DAY
        elif loc in cols_aanv:
            meas_df = aanv_2017[["Datum", loc]].copy()
            meas_df_m3s = meas_df.copy()
            meas_df_m3s[loc] = meas_df_m3s[loc] / SEC_PER_DAY

        # Specifiek: Gaarkeuken Noorderzijlvest -> metingen * -1
        if meas_df_m3s is not None and not meas_df_m3s.empty:
            if loc == "Gaarkeuken Noorderzijlvest":
                meas_df_m3s[loc] = -meas_df_m3s[loc]

        # --- Model: m³/s (ruw), filter op 2017; geen middelen ---
        model_m3s = None
        if link_ids:
            try:
                model_raw = read_flow_arrow_sum_links(PAD_FLOW, link_ids)  # m³/s
                model_m3s = filter_year(model_raw, JAAR)  # raw binnen 2017
            except Exception as e:
                print(f"[warn] {loc}: probleem met flow.arrow: {e}")

        ok = plot_locatie(loc, meas_df_m3s, model_m3s, UITDIR, JAAR)
        (plotted if ok else missing).append(loc)

    print(f"Klaar. Grafieken opgeslagen in: {UITDIR.resolve()}")
    if plotted:
        print(f"✓ Geplot: {len(plotted)} locaties")
    if missing:
        print("⚠ Overgeslagen/geen data: " + ", ".join(missing[:12]) + (" ..." if len(missing) > 12 else ""))


if __name__ == "__main__":
    main()
