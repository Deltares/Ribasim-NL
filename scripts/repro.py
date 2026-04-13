"""Run DVC stages sequentially with per-stage logging and a summary."""

import logging
import subprocess
import sys
from datetime import datetime

logger = logging.getLogger(__name__)

VRIJ_AFWATEREND = [
    "aa_en_maas",
    "brabantse_delta",
    "de_dommel",
    "limburg",
    "rijn_en_ijssel",
    "vallei_en_veluwe",
    "vechtstromen",
    "drents_overijsselse_delta",
    "stichtse_rijnlanden",
    "hunze_en_aas",
    "noorderzijlvest",
]


def run_stages(stages: list[str], logfile: str | None = None) -> bool:
    """Run each DVC stage, log output, and print a summary.

    Returns True if all stages succeeded.
    """
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if logfile:
        handlers.append(logging.FileHandler(logfile, mode="w", encoding="utf-8"))

    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=handlers,
    )

    logger.info(f"Start run: {datetime.now()}")
    results: dict[str, int] = {}

    for stage in stages:
        logger.info(f"\n=== Running stage: {stage} ===")
        result = subprocess.run(
            ["uv", "run", "dvc", "repro", "--keep-going", "--force", stage],
            encoding="utf-8",
            errors="replace",
        )
        results[stage] = result.returncode
        status = "SUCCESS" if result.returncode == 0 else f"FAILED (exit code {result.returncode})"
        logger.info(f"{stage}: {status}")

    logger.info("\n=== SUMMARY ===")
    for stage, code in results.items():
        status = "SUCCESS" if code == 0 else f"FAILED (exit code {code})"
        logger.info(f"{stage}: {status}")

    logger.info(f"End run: {datetime.now()}")
    return all(code == 0 for code in results.values())


if __name__ == "__main__":
    stages = sys.argv[1:] if len(sys.argv) > 1 else VRIJ_AFWATEREND
    success = run_stages(stages, logfile="repro.log")
    raise SystemExit(0 if success else 1)
