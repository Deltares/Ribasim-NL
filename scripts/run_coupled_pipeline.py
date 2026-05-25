from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TMP_ROOT = ROOT / "scripts" / "_coupled_pipeline_tmp"
SITECUSTOMIZE_DIR = ROOT / "scripts" / "rdo_noord_sitecustomize"
RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")


@dataclass(frozen=True)
class PipelineConfig:
    key: str
    model_name: str
    submodel_authorities: list[str]
    dynamic_authorities: list[str]
    full_control_scripts: list[Path]


PIPELINES: dict[str, PipelineConfig] = {
    "hdsr-rws": PipelineConfig(
        key="hdsr-rws",
        model_name="HDSR-RWS",
        submodel_authorities=["Rijkswaterstaat", "StichtseRijnlanden"],
        dynamic_authorities=["StichtseRijnlanden"],
        full_control_scripts=[ROOT / "notebooks" / "stichtse_rijnlanden" / "04_add_full_control.py"],
    ),
    "venv-rws": PipelineConfig(
        key="venv-rws",
        model_name="VenV-RWS",
        submodel_authorities=["Rijkswaterstaat", "ValleienVeluwe"],
        dynamic_authorities=["ValleienVeluwe"],
        full_control_scripts=[ROOT / "notebooks" / "vallei_en_veluwe" / "04_add_full_control.py"],
    ),
    "dod-vecht-hunze-rws": PipelineConfig(
        key="dod-vecht-hunze-rws",
        model_name="DOD-Vechtstromen_HunzeenAas-RWS",
        submodel_authorities=[
            "Rijkswaterstaat",
            "DrentsOverijsselseDelta",
            "Vechtstromen",
            "HunzeenAas",
        ],
        dynamic_authorities=["DrentsOverijsselseDelta", "Vechtstromen", "HunzeenAas"],
        full_control_scripts=[
            ROOT / "notebooks" / "drents_overijsselse_delta" / "04_add_full_control.py",
            ROOT / "notebooks" / "vechtstromen" / "04_add_full_control.py",
            ROOT / "notebooks" / "hunze_en_aas" / "04_add_full_control.py",
        ],
    ),
    "wf-nzv-hunze-rws": PipelineConfig(
        key="wf-nzv-hunze-rws",
        model_name="WF-NZV-HunzeenAas-RWS",
        submodel_authorities=[
            "Rijkswaterstaat",
            "WetterskipFryslan",
            "Noorderzijlvest",
            "HunzeenAas",
        ],
        dynamic_authorities=["Noorderzijlvest", "HunzeenAas"],
        full_control_scripts=[
            ROOT / "notebooks" / "noorderzijlvest" / "04_add_full_control.py",
            ROOT / "notebooks" / "hunze_en_aas" / "04_add_full_control.py",
        ],
    ),
    "rij-rws": PipelineConfig(
        key="rij-rws",
        model_name="RijnenIJssel-RWS",
        submodel_authorities=["Rijkswaterstaat", "RijnenIJssel"],
        dynamic_authorities=["RijnenIJssel"],
        full_control_scripts=[ROOT / "notebooks" / "rijn_en_ijssel" / "04_add_full_control.py"],
    ),
    "aam-limburg-rws": PipelineConfig(
        key="aam-limburg-rws",
        model_name="AAM-Limburg-RWS",
        submodel_authorities=["Rijkswaterstaat", "AaenMaas", "Limburg"],
        dynamic_authorities=["AaenMaas", "Limburg"],
        full_control_scripts=[
            ROOT / "notebooks" / "aa_en_maas" / "04_add_full_control.py",
            ROOT / "notebooks" / "limburg" / "04_add_full_control.py",
        ],
    ),
    "dommel-aam-rws": PipelineConfig(
        key="dommel-aam-rws",
        model_name="Dommel-AAM-RWS",
        submodel_authorities=["Rijkswaterstaat", "DeDommel", "AaenMaas"],
        dynamic_authorities=["DeDommel", "AaenMaas"],
        full_control_scripts=[
            ROOT / "notebooks" / "de_dommel" / "04_add_full_control.py",
            ROOT / "notebooks" / "aa_en_maas" / "04_add_full_control.py",
        ],
    ),
    "dommel-aam-limburg-rws": PipelineConfig(
        key="dommel-aam-limburg-rws",
        model_name="Dommel-AAM-Limburg-RWS",
        submodel_authorities=["Rijkswaterstaat", "DeDommel", "AaenMaas", "Limburg"],
        dynamic_authorities=["DeDommel", "AaenMaas", "Limburg"],
        full_control_scripts=[
            ROOT / "notebooks" / "de_dommel" / "04_add_full_control.py",
            ROOT / "notebooks" / "aa_en_maas" / "04_add_full_control.py",
            ROOT / "notebooks" / "limburg" / "04_add_full_control.py",
        ],
    ),
    "brabantse-delta-rws": PipelineConfig(
        key="brabantse-delta-rws",
        model_name="BrabantseDelta-RWS",
        submodel_authorities=["Rijkswaterstaat", "BrabantseDelta"],
        dynamic_authorities=["BrabantseDelta"],
        full_control_scripts=[ROOT / "notebooks" / "brabantse_delta" / "04_add_full_control.py"],
    ),
}


def safe_label(label: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", label).strip("_").lower()


def run_step(label: str, command: list[str], env: dict[str, str], log_dir: Path, *, dry_run: bool) -> None:
    print(f"\n=== {label} ===", flush=True)
    print(" ".join(command), flush=True)
    if dry_run:
        return

    log_file = log_dir / RUN_ID / f"{safe_label(label)}.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("w", encoding="utf-8", errors="replace") as log:
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        assert process.stdout is not None
        for line in process.stdout:
            print(line, end="", flush=True)
            log.write(line)

        return_code = process.wait()

    if return_code != 0:
        print(f"\nStap faalde met exit code {return_code}. Log: {log_file}", flush=True)
        raise subprocess.CalledProcessError(return_code, command)


def write_samenvoegen_script(config: PipelineConfig, tmp_dir: Path) -> Path:
    src = ROOT / "notebooks" / "samenvoegen_modellen.py"
    dst = tmp_dir / f"samenvoegen_modellen_{config.key}.py"
    text = src.read_text(encoding="utf-8")
    replacement = (
        "sub_models: dict[str, list[str]] = {\n"
        f'    "{config.model_name}": {config.submodel_authorities!r},\n'
        "}\n\n\n# A spec consists"
    )
    text = re.sub(
        r"sub_models: dict\[str, list\[str\]\] = \{.*?\n\}\n\n\n# A spec consists",
        replacement,
        text,
        flags=re.DOTALL,
    )
    dst.write_text(text, encoding="utf-8")
    return dst


def write_koppelen_script(config: PipelineConfig, tmp_dir: Path) -> Path:
    src = ROOT / "notebooks" / "koppelen_modellen.py"
    dst = tmp_dir / f"koppelen_modellen_{config.key}.py"
    text = src.read_text(encoding="utf-8")
    replacement = f'sub_models: list[str] | bool = ["{config.model_name}"]\n\nremove_nodes ='
    text = re.sub(
        r"sub_models: list\[str\] \| bool = \[.*?\]\n\nremove_nodes =",
        replacement,
        text,
        flags=re.DOTALL,
    )
    dst.write_text(text, encoding="utf-8")
    return dst


def make_env(tmp_dir: Path) -> dict[str, str]:
    joblib_tmp_dir = tmp_dir / "joblib"
    mpl_config_dir = tmp_dir / "matplotlib"
    cache_dir = tmp_dir / "cache"
    numba_cache_dir = tmp_dir / "numba_cache"
    for path in [tmp_dir, joblib_tmp_dir, mpl_config_dir, cache_dir, numba_cache_dir]:
        path.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["TEMP"] = str(tmp_dir)
    env["TMP"] = str(tmp_dir)
    env["TMPDIR"] = str(tmp_dir)
    env["JOBLIB_TEMP_FOLDER"] = str(joblib_tmp_dir)
    env["MPLCONFIGDIR"] = str(mpl_config_dir)
    env["XDG_CACHE_HOME"] = str(cache_dir)
    env["NUMBA_CACHE_DIR"] = str(numba_cache_dir)
    env["RIBASIM_NL_TEMP_ROOT"] = str(tmp_dir)
    env["RIBASIM_NL_DISABLE_CONTEXTILY"] = "1"
    env["PYTHONFAULTHANDLER"] = "1"
    env["PYTHONPATH"] = (
        str(SITECUSTOMIZE_DIR) if not env.get("PYTHONPATH") else f"{SITECUSTOMIZE_DIR}{os.pathsep}{env['PYTHONPATH']}"
    )
    return env


STEP_ORDER = {
    "self-test": 0,
    "full-control": 1,
    "bergend": 2,
    "dynamic": 3,
    "samenvoegen": 4,
    "koppelen": 5,
    "report-coupling-levels": 6,
    "check-coupling-levels": 7,
}


def coupled_model_toml_path(config: PipelineConfig) -> Path:
    return (
        ROOT
        / "data"
        / "Rijkswaterstaat"
        / "modellen"
        / "lhm_sub_models"
        / f"{config.model_name}_coupled"
        / f"{config.model_name}_coupled.toml"
    )


def run_pipeline(
    config: PipelineConfig,
    *,
    dry_run: bool,
    start_at: str,
    apply_coupling_levels: bool,
    apply_rws_inlet_min_upstream: bool,
    apply_max_downstream_level: bool,
    manning_n: float | None,
) -> None:
    tmp_dir = TMP_ROOT / config.key
    log_dir = tmp_dir / "logs"
    env = make_env(tmp_dir)
    python_command = [sys.executable, "-X", "faulthandler"]
    start_order = STEP_ORDER[start_at]

    def should_run(step: str) -> bool:
        return STEP_ORDER[step] >= start_order

    if should_run("self-test"):
        run_step(
            f"Python import self-test: {config.model_name}",
            [
                *python_command,
                "-c",
                "import tempfile; print(tempfile.mkdtemp()); from ribasim_nl import Model; print('ribasim_nl ok')",
            ],
            env,
            log_dir,
            dry_run=dry_run,
        )

    if should_run("full-control"):
        for script in config.full_control_scripts:
            run_step(
                f"Full control: {script.parent.name}",
                [*python_command, str(script)],
                env,
                log_dir,
                dry_run=dry_run,
            )

    only_merge_authorities = sorted(set(config.submodel_authorities) - set(config.dynamic_authorities))
    print(
        "\n=== Alleen samenvoegen/koppelen ===\n"
        f"Deze authorities worden niet door full-control/bergend/dynamic gehaald: {', '.join(only_merge_authorities)}",
        flush=True,
    )

    if config.dynamic_authorities and should_run("bergend"):
        run_step(
            f"Bergend model: {config.model_name}",
            [*python_command, str(ROOT / "notebooks" / "05_add_bergend.py"), *config.dynamic_authorities],
            env,
            log_dir,
            dry_run=dry_run,
        )
    if config.dynamic_authorities and should_run("dynamic"):
        run_step(
            f"Dynamic forcing: {config.model_name}",
            [*python_command, str(ROOT / "notebooks" / "07_add_dynamic_forcing.py"), *config.dynamic_authorities],
            env,
            log_dir,
            dry_run=dry_run,
        )

    samenvoegen_script = write_samenvoegen_script(config, tmp_dir)
    koppelen_script = write_koppelen_script(config, tmp_dir)

    if should_run("samenvoegen"):
        run_step(
            f"Samenvoegen modellen: {config.model_name}",
            [*python_command, str(samenvoegen_script)],
            env,
            log_dir,
            dry_run=dry_run,
        )
    if should_run("koppelen"):
        run_step(
            f"Koppelen modellen: {config.model_name}",
            [*python_command, str(koppelen_script)],
            env,
            log_dir,
            dry_run=dry_run,
        )
    model_toml_path = coupled_model_toml_path(config)

    if should_run("report-coupling-levels"):
        command = [
            *python_command,
            str(ROOT / "notebooks" / "report_coupling_levels.py"),
            "--toml-file",
            str(model_toml_path),
            "--output-gpkg",
            "coupling_level_report_uitgekleed.gpkg",
        ]
        if apply_rws_inlet_min_upstream or apply_coupling_levels:
            command.append("--apply-rws-inlet-min-upstream")
        if apply_max_downstream_level:
            command.append("--apply-max-downstream-level")

        run_step(
            f"Report coupling levels: {config.model_name}",
            command,
            env,
            log_dir,
            dry_run=dry_run,
        )

    if should_run("check-coupling-levels"):
        if not apply_coupling_levels:
            print(
                "\n=== Check coupling levels overgeslagen ===\n"
                "Gebruik --apply-coupling-levels om de oude check_coupling_levels.py --apply stap te draaien.",
                flush=True,
            )
            return

        command = [
            *python_command,
            str(ROOT / "notebooks" / "check_coupling_levels.py"),
            "--toml-file",
            str(model_toml_path),
            "--apply",
        ]
        if manning_n is not None:
            command.extend(["--manning-n", str(manning_n)])

        run_step(
            f"Check coupling levels: {config.model_name}",
            command,
            env,
            log_dir,
            dry_run=dry_run,
        )

    print(f"\nKlaar: {config.model_name} pipeline afgerond.", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run een of meer coupled submodel pipelines.")
    parser.add_argument(
        "pipelines",
        nargs="*",
        choices=[*PIPELINES.keys(), "all"],
        help="Pipeline key. Gebruik 'all' om ze sequentieel te draaien.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print stappen zonder ze uit te voeren.")
    parser.add_argument("--list", action="store_true", help="Toon beschikbare pipeline keys.")
    parser.add_argument(
        "--start-at",
        choices=list(STEP_ORDER),
        default="self-test",
        help="Start de pipeline vanaf deze stap.",
    )
    parser.add_argument(
        "--apply-rws-inlet-min-upstream",
        action="store_true",
        help=(
            "Laat report_coupling_levels.py alleen toegestane RWS->model inlaat "
            "min_upstream_level-correcties toepassen op basis van profielhoogte, "
            "inclusief FlowDemand-gestuurde inlaten."
        ),
    )
    parser.add_argument(
        "--apply-max-downstream-level",
        action="store_true",
        help=(
            "Laat report_coupling_levels.py max_downstream_level corrigeren voor "
            "aanvoer-rijen van inlaten met direct downstream Outlet/Pump; geen Manning en geen doorlaten."
        ),
    )
    parser.add_argument(
        "--apply-coupling-levels",
        action="store_true",
        help=(
            "Draai de oude check_coupling_levels.py --apply stap na het rapport. "
            "De beperkte RWS->model inlaat min_upstream-correctie uit het rapport wordt dan ook toegepast."
        ),
    )
    parser.add_argument(
        "--manning-n",
        type=float,
        default=0.01,
        help=(
            "Waarde voor ManningResistance / static.manning_n in check_coupling_levels.py. "
            "Standaard 0.01; gebruik --manning-n uitdrukkelijk als je een andere waarde wilt."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.list:
        for key, config in PIPELINES.items():
            print(f"{key}: {config.model_name} = {', '.join(config.submodel_authorities)}")
        return 0

    selected_keys = args.pipelines or ["all"]
    if "all" in selected_keys:
        selected_keys = list(PIPELINES)

    for key in selected_keys:
        run_pipeline(
            PIPELINES[key],
            dry_run=args.dry_run,
            start_at=args.start_at,
            apply_coupling_levels=args.apply_coupling_levels,
            apply_rws_inlet_min_upstream=args.apply_rws_inlet_min_upstream,
            apply_max_downstream_level=args.apply_max_downstream_level,
            manning_n=args.manning_n,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
