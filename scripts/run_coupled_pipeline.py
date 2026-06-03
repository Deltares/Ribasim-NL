from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
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


@dataclass(frozen=True)
class StepSpec:
    label: str
    command: list[str]


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
    "dod-vecht-wf-nzv-hunze-rws": PipelineConfig(
        key="dod-vecht-wf-nzv-hunze-rws",
        model_name="DOD-Vechtstromen-WF-NZV-HunzeenAas-RWS",
        submodel_authorities=[
            "Rijkswaterstaat",
            "DrentsOverijsselseDelta",
            "Vechtstromen",
            "WetterskipFryslan",
            "Noorderzijlvest",
            "HunzeenAas",
        ],
        dynamic_authorities=[
            "DrentsOverijsselseDelta",
            "Vechtstromen",
            "Noorderzijlvest",
            "HunzeenAas",
        ],
        full_control_scripts=[
            ROOT / "notebooks" / "drents_overijsselse_delta" / "04_add_full_control.py",
            ROOT / "notebooks" / "vechtstromen" / "04_add_full_control.py",
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
    "alle-regionaal-rws": PipelineConfig(
        key="alle-regionaal-rws",
        model_name="AlleRegionaal-RWS",
        submodel_authorities=[
            "Rijkswaterstaat",
            "WetterskipFryslan",
            "DrentsOverijsselseDelta",
            "Noorderzijlvest",
            "Vechtstromen",
            "HunzeenAas",
            "Limburg",
            "DeDommel",
            "AaenMaas",
            "BrabantseDelta",
            "ValleienVeluwe",
            "StichtseRijnlanden",
            "RijnenIJssel",
        ],
        dynamic_authorities=[
            "DrentsOverijsselseDelta",
            "Noorderzijlvest",
            "Vechtstromen",
            "HunzeenAas",
            "Limburg",
            "DeDommel",
            "AaenMaas",
            "BrabantseDelta",
            "ValleienVeluwe",
            "StichtseRijnlanden",
            "RijnenIJssel",
        ],
        full_control_scripts=[
            ROOT / "notebooks" / "drents_overijsselse_delta" / "04_add_full_control.py",
            ROOT / "notebooks" / "noorderzijlvest" / "04_add_full_control.py",
            ROOT / "notebooks" / "vechtstromen" / "04_add_full_control.py",
            ROOT / "notebooks" / "hunze_en_aas" / "04_add_full_control.py",
            ROOT / "notebooks" / "limburg" / "04_add_full_control.py",
            ROOT / "notebooks" / "de_dommel" / "04_add_full_control.py",
            ROOT / "notebooks" / "aa_en_maas" / "04_add_full_control.py",
            ROOT / "notebooks" / "brabantse_delta" / "04_add_full_control.py",
            ROOT / "notebooks" / "vallei_en_veluwe" / "04_add_full_control.py",
            ROOT / "notebooks" / "stichtse_rijnlanden" / "04_add_full_control.py",
            ROOT / "notebooks" / "rijn_en_ijssel" / "04_add_full_control.py",
        ],
    ),
    "alle-regionaal-zonder-wf-rws": PipelineConfig(
        key="alle-regionaal-zonder-wf-rws",
        model_name="AlleRegionaal-ZonderWF-RWS",
        submodel_authorities=[
            "Rijkswaterstaat",
            "DrentsOverijsselseDelta",
            "Noorderzijlvest",
            "Vechtstromen",
            "HunzeenAas",
            "Limburg",
            "DeDommel",
            "AaenMaas",
            "BrabantseDelta",
            "ValleienVeluwe",
            "StichtseRijnlanden",
            "RijnenIJssel",
        ],
        dynamic_authorities=[
            "DrentsOverijsselseDelta",
            "Noorderzijlvest",
            "Vechtstromen",
            "HunzeenAas",
            "Limburg",
            "DeDommel",
            "AaenMaas",
            "BrabantseDelta",
            "ValleienVeluwe",
            "StichtseRijnlanden",
            "RijnenIJssel",
        ],
        full_control_scripts=[
            ROOT / "notebooks" / "drents_overijsselse_delta" / "04_add_full_control.py",
            ROOT / "notebooks" / "noorderzijlvest" / "04_add_full_control.py",
            ROOT / "notebooks" / "vechtstromen" / "04_add_full_control.py",
            ROOT / "notebooks" / "hunze_en_aas" / "04_add_full_control.py",
            ROOT / "notebooks" / "limburg" / "04_add_full_control.py",
            ROOT / "notebooks" / "de_dommel" / "04_add_full_control.py",
            ROOT / "notebooks" / "aa_en_maas" / "04_add_full_control.py",
            ROOT / "notebooks" / "brabantse_delta" / "04_add_full_control.py",
            ROOT / "notebooks" / "vallei_en_veluwe" / "04_add_full_control.py",
            ROOT / "notebooks" / "stichtse_rijnlanden" / "04_add_full_control.py",
            ROOT / "notebooks" / "rijn_en_ijssel" / "04_add_full_control.py",
        ],
    ),
}

DEFAULT_PIPELINE_KEYS = [
    "hdsr-rws",
    "venv-rws",
    "dod-vecht-hunze-rws",
    "wf-nzv-hunze-rws",
    "rij-rws",
    "dommel-aam-limburg-rws",
    "brabantse-delta-rws",
]


def safe_label(label: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", label).strip("_").lower()


def powershell_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


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


def run_step_in_new_window(step: StepSpec, env: dict[str, str], log_dir: Path, step_tmp_dir: Path) -> None:
    if os.name != "nt":
        run_step(step.label, step.command, env, log_dir, dry_run=False)
        return

    print(f"\n=== {step.label} ===", flush=True)
    print(" ".join(step.command), flush=True)

    log_file = log_dir / RUN_ID / f"{safe_label(step.label)}.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    runner = step_tmp_dir / f"{safe_label(step.label)}.ps1"
    command = " ".join(powershell_literal(arg) for arg in step.command)
    runner.write_text(
        "\n".join(
            [
                "$ErrorActionPreference = 'Continue'",
                f"Set-Location -LiteralPath {powershell_literal(str(ROOT))}",
                f"Write-Host {powershell_literal(f'=== {step.label} ===')}",
                f"Write-Host {powershell_literal(' '.join(step.command))}",
                f"& {command} 2>&1 | Tee-Object -FilePath {powershell_literal(str(log_file))}",
                "$exitCode = $LASTEXITCODE",
                "Write-Host ''",
                'Write-Host "Exit code: $exitCode"',
                "exit $exitCode",
            ]
        ),
        encoding="utf-8",
    )

    process = subprocess.Popen(
        [
            "powershell.exe",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(runner),
        ],
        cwd=ROOT,
        env=env,
        creationflags=subprocess.CREATE_NEW_CONSOLE,
    )
    return_code = process.wait()
    if return_code != 0:
        print(f"\nStap faalde met exit code {return_code}. Log: {log_file}", flush=True)
        raise subprocess.CalledProcessError(return_code, step.command)


def run_steps_parallel(
    group_label: str,
    steps: list[StepSpec],
    tmp_dir: Path,
    log_dir: Path,
    *,
    dry_run: bool,
    max_workers: int | None,
    new_windows: bool,
) -> None:
    if len(steps) == 0:
        return

    print(f"\n=== {group_label} parallel ({len(steps)} stappen) ===", flush=True)
    if dry_run:
        for step in steps:
            print(f"\n--- {step.label} ---", flush=True)
            if new_windows and os.name == "nt":
                print("(nieuw venster)", flush=True)
            print(" ".join(step.command), flush=True)
        return

    workers = max_workers or len(steps)
    workers = min(workers, len(steps))

    def run_one(step: StepSpec) -> None:
        step_tmp_dir = tmp_dir / "parallel" / safe_label(step.label)
        env = make_env(step_tmp_dir)
        if new_windows:
            run_step_in_new_window(step, env, log_dir, step_tmp_dir)
        else:
            run_step(step.label, step.command, env, log_dir, dry_run=False)

    failures: list[tuple[StepSpec, Exception]] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(run_one, step): step for step in steps}
        for future in as_completed(futures):
            step = futures[future]
            try:
                future.result()
            except Exception as exc:
                failures.append((step, exc))
                print(f"\nParallelle stap faalde: {step.label}", flush=True)

    if failures:
        first_step, first_exception = failures[0]
        print(
            f"\n{len(failures)} parallelle stap(pen) gefaald. Eerste fout: {first_step.label}",
            flush=True,
        )
        if isinstance(first_exception, subprocess.CalledProcessError):
            raise first_exception
        raise RuntimeError(f"Parallelle stap gefaald: {first_step.label}") from first_exception


def write_samenvoegen_script(config: PipelineConfig, tmp_dir: Path) -> Path:
    src = ROOT / "notebooks" / "samenvoegen_modellen.py"
    dst = tmp_dir / f"samenvoegen_modellen_{config.key}.py"
    text = src.read_text(encoding="utf-8")
    authorities = "\n".join(f'        "{authority}",' for authority in config.submodel_authorities)
    replacement = (
        "sub_models: dict[str, list[str]] = {\n"
        f'    "{config.model_name}": [\n'
        f"{authorities}\n"
        "    ],\n"
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
    "report-applied-changes": 7,
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
    parallel_until_samenvoegen: bool,
    parallel_workers: int | None,
    parallel_new_windows: bool,
    apply_rws_inlet_min_upstream: bool,
    apply_direct_min_upstream_level: bool,
    apply_max_downstream_level: bool,
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
        full_control_steps = [
            StepSpec(
                label=f"Full control: {script.parent.name}",
                command=[*python_command, str(script)],
            )
            for script in config.full_control_scripts
        ]
        if parallel_until_samenvoegen:
            run_steps_parallel(
                f"Full control: {config.model_name}",
                full_control_steps,
                tmp_dir,
                log_dir,
                dry_run=dry_run,
                max_workers=parallel_workers,
                new_windows=parallel_new_windows,
            )
        else:
            for step in full_control_steps:
                run_step(step.label, step.command, env, log_dir, dry_run=dry_run)

    only_merge_authorities = sorted(set(config.submodel_authorities) - set(config.dynamic_authorities))
    print(
        "\n=== Alleen samenvoegen/koppelen ===\n"
        f"Deze authorities worden niet door full-control/bergend/dynamic gehaald: {', '.join(only_merge_authorities)}",
        flush=True,
    )

    if config.dynamic_authorities and should_run("bergend"):
        if parallel_until_samenvoegen:
            run_steps_parallel(
                f"Bergend model: {config.model_name}",
                [
                    StepSpec(
                        label=f"Bergend model: {authority}",
                        command=[*python_command, str(ROOT / "notebooks" / "05_add_bergend.py"), authority],
                    )
                    for authority in config.dynamic_authorities
                ],
                tmp_dir,
                log_dir,
                dry_run=dry_run,
                max_workers=parallel_workers,
                new_windows=parallel_new_windows,
            )
        else:
            run_step(
                f"Bergend model: {config.model_name}",
                [*python_command, str(ROOT / "notebooks" / "05_add_bergend.py"), *config.dynamic_authorities],
                env,
                log_dir,
                dry_run=dry_run,
            )
    if config.dynamic_authorities and should_run("dynamic"):
        if parallel_until_samenvoegen:
            run_steps_parallel(
                f"Dynamic forcing: {config.model_name}",
                [
                    StepSpec(
                        label=f"Dynamic forcing: {authority}",
                        command=[*python_command, str(ROOT / "notebooks" / "07_add_dynamic_forcing.py"), authority],
                    )
                    for authority in config.dynamic_authorities
                ],
                tmp_dir,
                log_dir,
                dry_run=dry_run,
                max_workers=parallel_workers,
                new_windows=parallel_new_windows,
            )
        else:
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
            "--verdachte-output-gpkg",
            "verdachte_punten.gpkg",
        ]
        if apply_rws_inlet_min_upstream:
            command.append("--apply-rws-inlet-min-upstream")
        if apply_direct_min_upstream_level or apply_max_downstream_level:
            command.append("--apply-direct-min-upstream-level")
        if apply_max_downstream_level:
            command.append("--apply-max-downstream-level")

        run_step(
            f"Report coupling levels: {config.model_name}",
            command,
            env,
            log_dir,
            dry_run=dry_run,
        )

    if should_run("report-applied-changes"):
        command = [
            *python_command,
            str(ROOT / "notebooks" / "report_applied_model_changes.py"),
            "--toml-file",
            str(model_toml_path),
            "--output-gpkg",
            "toegepaste_model_wijzigingen.gpkg",
        ]
        run_step(
            f"Report applied changes: {config.model_name}",
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
        "--parallel-until-samenvoegen",
        action="store_true",
        help=(
            "Draai full-control per script en bergend/dynamic per authority parallel. "
            "Vanaf samenvoegen_modellen draait de pipeline weer sequentieel."
        ),
    )
    parser.add_argument(
        "--parallel-workers",
        type=int,
        default=None,
        help="Maximaal aantal parallelle processen per stapgroep. Standaard: alle processen tegelijk.",
    )
    parser.add_argument(
        "--parallel-new-windows",
        action="store_true",
        help="Open parallelle stappen in aparte Windows-vensters. De pipeline wacht nog steeds op alle stappen.",
    )
    parser.add_argument(
        "--no-apply-coupling-levels",
        action="store_true",
        help=(
            "Draai report_coupling_levels.py alleen rapporterend. Standaard past de pipeline "
            "de toegestane coupling-level correcties toe."
        ),
    )
    parser.add_argument(
        "--apply-rws-inlet-min-upstream",
        action="store_true",
        help=(
            "Pas RWS->model inlaat min_upstream_level-correcties toe. Dit staat standaard aan "
            "in de pipeline; gebruik deze vlag alleen nog voor een specifieke apply als "
            "--no-apply-coupling-levels is gezet."
        ),
    )
    parser.add_argument(
        "--apply-max-downstream-level",
        action="store_true",
        help=(
            "Pas max_downstream_level-correcties toe voor "
            "aanvoer-rijen van inlaten en doorlaten op basis van het directe downstream Basin. "
            "Dit staat standaard aan in de pipeline. FlowDemand-gestuurde nodes worden hierbij "
            "overgeslagen. De bijbehorende directe min_upstream-correcties worden dan ook toegepast."
        ),
    )
    parser.add_argument(
        "--apply-direct-min-upstream-level",
        action="store_true",
        help=(
            "Pas directe min_upstream_level-correcties toe voor afwijkende rijen met een direct "
            "upstream Basin, inclusief FlowDemand-gestuurde doorlaten. Dit staat standaard aan "
            "in de pipeline. Er wordt niet via ManningResistance/Junction doorgelopen."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.parallel_workers is not None and args.parallel_workers < 1:
        raise SystemExit("--parallel-workers moet minimaal 1 zijn.")

    if args.list:
        for key, config in PIPELINES.items():
            print(f"{key}: {config.model_name} = {', '.join(config.submodel_authorities)}")
        return 0

    selected_keys = args.pipelines or ["all"]
    if "all" in selected_keys:
        selected_keys = list(DEFAULT_PIPELINE_KEYS)

    default_apply_coupling_levels = not args.no_apply_coupling_levels
    apply_rws_inlet_min_upstream = default_apply_coupling_levels or args.apply_rws_inlet_min_upstream
    apply_direct_min_upstream_level = default_apply_coupling_levels or args.apply_direct_min_upstream_level
    apply_max_downstream_level = default_apply_coupling_levels or args.apply_max_downstream_level

    for key in selected_keys:
        run_pipeline(
            PIPELINES[key],
            dry_run=args.dry_run,
            start_at=args.start_at,
            parallel_until_samenvoegen=args.parallel_until_samenvoegen,
            parallel_workers=args.parallel_workers,
            parallel_new_windows=args.parallel_new_windows,
            apply_rws_inlet_min_upstream=apply_rws_inlet_min_upstream,
            apply_direct_min_upstream_level=apply_direct_min_upstream_level,
            apply_max_downstream_level=apply_max_downstream_level,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
