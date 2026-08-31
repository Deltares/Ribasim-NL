#!/bin/bash

# Submit an isolated Ribasim simulation run on SLURM.
# Run directly on the login node.
#
# Usage:
#   ./run.sh <name> <model_dir> [--after=<jobid>] [key=value ...]
#
# Examples:
#   ./run.sh lhm_coupled_3yr data/Rijkswaterstaat/modellen/lhm_coupled \
#     --after=222574 endtime="2020-01-01 00:00:00"
#
#   ./run.sh lhm_parts data/Rijkswaterstaat/modellen/lhm_parts \
#     --after=$(grep samenvoegen repro_jobs.txt | cut -f2) solver.abstol=1e-6
#
# The model is copied to runs/<name>/ for isolation. The named Ribasim core
# under /p/ribasim-nl/bin is shared by all runs.
# TOML overrides are applied in-place before the run starts.

set -euo pipefail

PARTITION=4vcpu
TIME=7-00:00:00
RUNS_DIR=/p/ribasim-nl/runs

# Install the configured core if needed and archive it under its stable name.
pixi run archive-core
RIBASIM_NAME=$(pixi run python scripts/install_ribasim_core.py --print-name)
RIBASIM_BIN="/p/ribasim-nl/bin/${RIBASIM_NAME}/bin/ribasim"

if [[ ! -x "${RIBASIM_BIN}" ]]; then
  echo "Error: archive-core did not create ${RIBASIM_BIN}." >&2
  exit 1
fi

# Parse arguments
NAME=$1; shift
MODEL_DIR=$1; shift

AFTER=""
OVERRIDES=()
for arg in "$@"; do
  case $arg in
    --after=*) AFTER="${arg#--after=}" ;;
    *=*) OVERRIDES+=("$arg") ;;
    *) echo "Unknown argument: $arg" >&2; exit 1 ;;
  esac
done

# Build dependency flag
DEP_FLAG=""
if [[ -n "${AFTER}" ]]; then
  DEP_FLAG="--dependency=afterok:${AFTER}"
fi

RUN_DIR="${RUNS_DIR}/${NAME}"

# Quote overrides for embedding in heredoc
QUOTED_OVERRIDES=""
for o in "${OVERRIDES[@]+"${OVERRIDES[@]}"}"; do
  QUOTED_OVERRIDES+=" \"$o\""
done

# Create output directory so SLURM can write the log file
if [[ -e "${RUN_DIR}" ]]; then
  echo "Error: ${RUN_DIR} already exists. Remove it or choose a different name." >&2
  exit 1
fi
mkdir -p "${RUN_DIR}"

# Submit
JOB_ID=$(sbatch --parsable ${DEP_FLAG} \
  --job-name="${NAME}" --partition=${PARTITION} --time=${TIME} \
  --output="${RUN_DIR}/slurm-%j.out" \
  <<EOF
#!/bin/bash
set -euo pipefail
module load pixi

# Setup isolated run directory
cp -r "${MODEL_DIR}/." "${RUN_DIR}/"

# Find the TOML file in the run directory
TOML=\$(find "${RUN_DIR}" -maxdepth 1 -name "*.toml" | head -1)
if [[ -z "\${TOML}" ]]; then
  echo "Error: no .toml file found in ${RUN_DIR}" >&2
  exit 1
fi
echo "Using TOML: \${TOML}"

# Apply overrides
${QUOTED_OVERRIDES:+pixi run edit-toml "\${TOML}"${QUOTED_OVERRIDES}}

# Run
"${RIBASIM_BIN}" --version
srun "${RIBASIM_BIN}" "\${TOML}"
EOF
)

echo "Submitted job ${JOB_ID} (${NAME})"
echo "${NAME} ${JOB_ID} ${MODEL_DIR} ${OVERRIDES[*]:-}" >> ${RUNS_DIR}/jobs.txt
