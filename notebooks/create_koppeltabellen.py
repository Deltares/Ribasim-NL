# %%
import re
from pathlib import Path

import pandas as pd
from shapely import wkt
from shapely.geometry import Point

from ribasim_nl import CloudStorage, Model

REQUIRED_KOPPELTABEL_COLUMNS = {
    "Waterschap",
    "new_from_node_geometry",
    "new_to_node_geometry",
    "new_link_id",
}


def parse_geometry_value(value) -> list[Point | None]:
    if pd.isna(value):
        return []
    if isinstance(value, Point):
        return [value]
    if isinstance(value, list):
        return [geom if isinstance(geom, Point) else None for geom in value]

    text = str(value).strip()
    if not text or text in {"None", "nan", "[NONE]"}:
        return []

    if text.startswith("[") and text.endswith("]"):
        parts = text[1:-1].split(", ")
        geometries = []
        for part in parts:
            part = part.strip()
            if part in {"None", "NONE", "nan", "[NONE]"}:
                geometries.append(None)
            else:
                geometries.append(wkt.loads(part.strip("<>")))
        return geometries

    return [wkt.loads(text.strip("<>"))]


def get_link_endpoints(geometry) -> tuple[Point, Point] | None:
    if geometry is None or getattr(geometry, "is_empty", False):
        return None

    boundary = geometry.boundary
    if not hasattr(boundary, "geoms") or len(boundary.geoms) != 2:
        return None

    from_point, to_point = boundary.geoms
    if not isinstance(from_point, Point) or not isinstance(to_point, Point):
        return None

    return from_point, to_point


def quantize_point(point: Point, tolerance: float) -> tuple[int, int]:
    return (round(point.x / tolerance), round(point.y / tolerance))


def build_deelmodel_output_path(excel_path: Path, toml_path: Path) -> Path:
    source_stem = excel_path.stem
    if source_stem.startswith("Transformed_koppeltabel"):
        output_stem = f"{source_stem}_{toml_path.stem}"
    else:
        output_stem = f"Transformed_koppeltabel_{source_stem}_{toml_path.stem}"
    return toml_path.parent / f"{output_stem}.xlsx"


def build_deelmodel_specifics_output_path(excel_path: Path, toml_path: Path) -> Path:
    return toml_path.parent / f"{excel_path.stem}_{toml_path.stem}.xlsx"


def validate_koppeltabel_columns(koppeltabel: pd.DataFrame, source_excel_path: Path) -> None:
    missing_columns = sorted(REQUIRED_KOPPELTABEL_COLUMNS - set(koppeltabel.columns))
    if missing_columns:
        raise ValueError(
            f"{source_excel_path} lijkt geen getransformeerde koppeltabel te zijn. "
            f"Ontbrekende kolommen: {missing_columns}"
        )


def get_model_authorities(model: Model) -> set[str]:
    node_df = model.node.df
    if "meta_waterbeheerder" not in node_df.columns:
        raise ValueError("model.node.df bevat geen kolom 'meta_waterbeheerder'.")
    return {str(value) for value in node_df["meta_waterbeheerder"].dropna().unique()}


def format_id_list_for_excel(link_ids: list[int | None]) -> object:
    cleaned = [int(link_id) for link_id in link_ids if link_id is not None]
    if not cleaned:
        return None
    if len(cleaned) == 1:
        return str([cleaned[0]])
    return str(cleaned)


def count_link_ids(link_ids: object) -> int:
    if pd.isna(link_ids):
        return 0

    text = str(link_ids).strip()
    if not text:
        return 0

    if text.startswith("[") and text.endswith("]"):
        values = [value.strip() for value in text[1:-1].split(",") if value.strip()]
        return len(values)

    return 1


def trim_specific_operation(spec_op: object, available_link_count: int) -> object:
    if pd.isna(spec_op):
        return spec_op

    if available_link_count <= 0:
        return None

    spec_text = str(spec_op).strip()
    if spec_text in {"optellen", "negatief_maken", "optellen_en_negatief_maken"}:
        return spec_text

    matches = re.findall(r"[+-]?link\d+", spec_text)
    if not matches:
        return spec_text

    kept_terms: list[str] = []
    for term in matches:
        sign = ""
        token = term
        if term[0] in "+-":
            sign = term[0]
            token = term[1:]

        link_number = int(token.removeprefix("link"))
        if link_number > available_link_count:
            continue

        if not kept_terms:
            kept_terms.append(token if sign == "+" else f"{sign}{token}")
        else:
            kept_terms.append(f"{sign or '+'}{token}")

    if not kept_terms:
        return None

    return "".join(kept_terms)


def build_link_endpoint_lookup(
    links: pd.DataFrame,
    tolerance: float,
) -> tuple[pd.DataFrame, dict[tuple[tuple[int, int], tuple[int, int]], list[int]]]:
    valid_link_ids: list[int] = []
    lookup: dict[tuple[tuple[int, int], tuple[int, int]], list[int]] = {}

    for link_id, link_row in links.iterrows():
        endpoints = get_link_endpoints(link_row["geometry"])
        if endpoints is None:
            continue

        from_point, to_point = endpoints
        valid_link_ids.append(int(link_id))
        key = (quantize_point(from_point, tolerance), quantize_point(to_point, tolerance))
        lookup.setdefault(key, []).append(int(link_id))

    return links.loc[valid_link_ids], lookup


def _candidate_match_records(
    links: pd.DataFrame,
    from_point: Point | None,
    to_point: Point | None,
    tolerance: float,
    endpoint_lookup: dict[tuple[tuple[int, int], tuple[int, int]], list[int]] | None = None,
) -> list[dict[str, object]]:
    if from_point is None or to_point is None:
        return []

    candidate_link_ids: list[int] | None = None
    if endpoint_lookup is not None:
        direct_key = (quantize_point(from_point, tolerance), quantize_point(to_point, tolerance))
        reverse_key = (quantize_point(to_point, tolerance), quantize_point(from_point, tolerance))
        direct_candidates = endpoint_lookup.get(direct_key, [])
        reverse_candidates = endpoint_lookup.get(reverse_key, [])
        merged_candidates = list(dict.fromkeys([*direct_candidates, *reverse_candidates]))
        if merged_candidates:
            candidate_link_ids = merged_candidates

    candidates: list[dict[str, object]] = []
    links_to_check = links.loc[candidate_link_ids] if candidate_link_ids is not None else links

    for link_id, link_row in links_to_check.iterrows():
        endpoints = get_link_endpoints(link_row["geometry"])
        if endpoints is None:
            continue

        model_from_point, model_to_point = endpoints
        direct_from = from_point.distance(model_from_point)
        direct_to = to_point.distance(model_to_point)
        swapped_from = from_point.distance(model_to_point)
        swapped_to = to_point.distance(model_from_point)

        direct_ok = direct_from <= tolerance and direct_to <= tolerance
        swapped_ok = swapped_from <= tolerance and swapped_to <= tolerance
        if not direct_ok and not swapped_ok:
            continue

        if direct_ok and (not swapped_ok or direct_from + direct_to <= swapped_from + swapped_to):
            candidates.append(
                {
                    "link_id": int(link_id),
                    "orientation": "direct",
                    "distance_sum": direct_from + direct_to,
                }
            )
        else:
            candidates.append(
                {
                    "link_id": int(link_id),
                    "orientation": "swapped",
                    "distance_sum": swapped_from + swapped_to,
                }
            )

    return sorted(candidates, key=lambda item: (item["distance_sum"], item["link_id"]))


def match_link_ids_by_geometry(
    model: Model,
    from_points: list[Point | None],
    to_points: list[Point | None],
    tolerance: float = 0.1,
) -> list[int | None]:
    links = model.link.df.copy()
    if "geometry" not in links.columns:
        raise ValueError("model.link.df bevat geen geometry-kolom.")

    links, endpoint_lookup = build_link_endpoint_lookup(links, tolerance)
    matched_link_ids: list[int | None] = []

    for from_point, to_point in zip(from_points, to_points, strict=False):
        candidates = _candidate_match_records(links, from_point, to_point, tolerance, endpoint_lookup=endpoint_lookup)
        if not candidates:
            matched_link_ids.append(None)
            continue

        matched_link_ids.append(candidates[0]["link_id"])

    return matched_link_ids


def create_deelmodel_koppeltabel(
    source_excel_path: str | Path,
    toml_path: str | Path,
    tolerance: float = 0.1,
) -> Path:
    source_excel_path = Path(source_excel_path)
    toml_path = Path(toml_path)

    model = Model.read(toml_path)
    authorities = get_model_authorities(model)

    koppeltabel = pd.read_excel(source_excel_path)
    validate_koppeltabel_columns(koppeltabel, source_excel_path)
    deelmodel_koppeltabel = koppeltabel[koppeltabel["Waterschap"].isin(authorities)].copy()

    new_link_ids: list[object] = []

    for _, row in deelmodel_koppeltabel.iterrows():
        from_points = parse_geometry_value(row.get("new_from_node_geometry"))
        to_points = parse_geometry_value(row.get("new_to_node_geometry"))

        if len(from_points) != len(to_points):
            new_link_ids.append(None)
            continue

        matched_link_ids = match_link_ids_by_geometry(
            model=model,
            from_points=from_points,
            to_points=to_points,
            tolerance=tolerance,
        )
        new_link_ids.append(format_id_list_for_excel(matched_link_ids))

    deelmodel_koppeltabel["new_link_id"] = new_link_ids

    output_path = build_deelmodel_output_path(source_excel_path, toml_path)
    deelmodel_koppeltabel.to_excel(output_path, index=False)
    return output_path


def create_deelmodel_specifics(
    source_specifics_path: str | Path,
    deelmodel_koppeltabel_path: str | Path,
    toml_path: str | Path,
) -> Path:
    source_specifics_path = Path(source_specifics_path)
    deelmodel_koppeltabel_path = Path(deelmodel_koppeltabel_path)
    toml_path = Path(toml_path)

    specifics = pd.read_excel(source_specifics_path)
    deelmodel_koppeltabel = pd.read_excel(deelmodel_koppeltabel_path)

    merge_columns = ["Waterschap", "MeetreeksC", "Aan/Af"]
    specifics = specifics.merge(
        deelmodel_koppeltabel[[*merge_columns + "new_link_id"]],
        on=merge_columns,
        how="inner",
    )

    specifics["available_link_count"] = specifics["new_link_id"].apply(count_link_ids)
    specifics["Specifiek"] = specifics.apply(
        lambda row: trim_specific_operation(row["Specifiek"], row["available_link_count"]),
        axis=1,
    )
    specifics = specifics[specifics["available_link_count"] > 0].copy()
    specifics = specifics.drop(columns=["new_link_id", "available_link_count"])

    output_path = build_deelmodel_specifics_output_path(source_specifics_path, toml_path)
    specifics.to_excel(output_path, index=False)
    return output_path


cloud = CloudStorage()
source_excel_path = cloud.joinpath(
    r"Basisgegevens/resultaatvergelijking/koppeltabel_2026/Transformed_koppeltabel_versie_Samenwerkdag_26052026_Feedback_Verwerkt_HydroLogic.xlsx"
)
source_specifics_path = cloud.joinpath(
    r"Basisgegevens/resultaatvergelijking/koppeltabel_2026/Specifiek_bewerking_versieSamenwerkdag_26052026.xlsx"
)

toml_paths = [
    Path(
        r"d:/projecten/D2306.LHM_RIBASIM/12.qgis_projects/samenwerkdag/DrentsOverijsselseDelta-Vechtstromen_HunzeenAas-RWS_coupled/DOD-Vechtstromen_HunzeenAas-RWS_coupled.toml"
    ),
    Path(
        r"d:/projecten/D2306.LHM_RIBASIM/12.qgis_projects/samenwerkdag/Dommel-AAM-Limburg-RWS_coupled/Dommel-AAM-Limburg-RWS_coupled.toml"
    ),
    Path(r"d:/projecten\D2306.LHM_RIBASIM/12.qgis_projects/samenwerkdag/HDSR-RWS_coupled/HDSR-RWS_coupled.toml"),
    Path(
        r"d:/projecten/D2306.LHM_RIBASIM/12.qgis_projects/samenwerkdag/RijnenIJssel-RWS_coupled/RijnenIJssel-RWS_coupled.toml"
    ),
    Path(r"d:/projecten/D2306.LHM_RIBASIM/12.qgis_projects/samenwerkdag/VenV-RWS_coupled/VenV-RWS_coupled.toml"),
    Path(
        r"d:/projecten/D2306.LHM_RIBASIM/12.qgis_projects/samenwerkdag/WetterskipFryslan-Noorderzijlvest-HunzeenAas-RWS_coupled/WF-NZV-HunzeenAas-RWS_coupled.toml"
    ),
]

for toml_path in toml_paths:
    deelmodel_koppeltabel_path = build_deelmodel_output_path(source_excel_path, toml_path)
    deelmodel_specifics_path = build_deelmodel_specifics_output_path(source_specifics_path, toml_path)

    if not deelmodel_koppeltabel_path.exists():
        deelmodel_koppeltabel_path = create_deelmodel_koppeltabel(
            toml_path=toml_path,
            source_excel_path=source_excel_path,
        )
    if not deelmodel_specifics_path.exists():
        create_deelmodel_specifics(
            source_specifics_path=source_specifics_path,
            deelmodel_koppeltabel_path=deelmodel_koppeltabel_path,
            toml_path=toml_path,
        )
