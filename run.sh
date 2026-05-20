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
# The model and Ribasim core are copied to runs/<name>/ for isolation.
# TOML overrides are applied in-place before the run starts.

set -euo pipefail

PARTITION=4vcpu
TIME=7-00:00:00
RUNS_DIR=/p/11212758-ribasim-maas-2026/ribasim-nl-runs

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
if [[ -d "${RUN_DIR}/model" ]]; then
  echo "Error: ${RUN_DIR}/model already exists. Remove it or choose a different name." >&2
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
cd $PWD

# Setup isolated run directory
cp -r "${MODEL_DIR}" "${RUN_DIR}/model"
cp -r bin/ribasim "${RUN_DIR}/ribasim"

# Find the TOML file in the model directory
TOML=\$(find "${RUN_DIR}/model" -maxdepth 1 -name "*.toml" | head -1)
if [[ -z "\${TOML}" ]]; then
  echo "Error: no .toml file found in ${RUN_DIR}/model" >&2
  exit 1
fi
echo "Using TOML: \${TOML}"

# Apply overrides
${QUOTED_OVERRIDES:+pixi run edit-toml "\${TOML}"${QUOTED_OVERRIDES}}

# Run
srun "${RUN_DIR}/ribasim/bin/ribasim" "\${TOML}"
EOF
)

echo "Submitted job ${JOB_ID} (${NAME})"
echo "${NAME} ${JOB_ID} ${MODEL_DIR} ${OVERRIDES[*]:-}" >> ${RUNS_DIR}/jobs.txt
