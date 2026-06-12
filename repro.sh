#!/bin/bash

# Submit the full Ribasim-NL pipeline as individual SLURM jobs.
# Run directly on the login node: ./repro.sh
#
# Direct-command variant: each parallel job runs the stage's underlying
# `pixi run python ...` commands directly against the shared data/ directory,
# instead of `dvc repro`. This avoids concurrent DVC pipeline-lock contention
# (.dvc/tmp/rwlock) on the shared checkout, which was failing many jobs.
#
# DVC is still used for data, but only in two serial, uncontended jobs:
#   * "pull"   : fetches all DVC-tracked inputs once, before the pipeline runs
#   * "commit" : runs `dvc commit` + `dvc push` once at the end to register and
#                upload all produced outputs
#
# SLURM dependencies ensure ordering; outputs are handed off via the shared
# data/ directory (the same filesystem-based hand-off used before).

module load pixi

PARTITION=4vcpu
TIME=1-00:00:00
PIXI="module load pixi; cd $PWD"

# Stagger job start times to avoid hammering the GoodCloud WebDAV server with
# concurrent downloads from the cloud.synchronize calls that some stages still
# do (causes IncompleteRead/503). Each successive staggered job starts STAGGER
# seconds later via --begin. Set BEGIN before a submit to delay; defaults to "now".
STAGGER=30
STAGGER_COUNT=0

submit() {
  local name=$1 dep=$2 time=$3; shift 3
  sbatch --parsable --dependency="${dep}" --begin="${BEGIN:-now}" \
    --job-name="${name}" --partition=${PARTITION} --time="${time}" \
    --wrap="${PIXI}; srun bash -c $(printf '%q' "$*")"
}

# Join a list of commands into a fail-fast "&& " chain.
chain() {
  local out="" cmd
  for cmd in "$@"; do
    if [[ -z "${out}" ]]; then out="${cmd}"; else out="${out} && ${cmd}"; fi
  done
  echo "${out}"
}

py() { echo "pixi run python $*"; }

# Step 0: fetch all DVC-tracked inputs once (serial, no lock contention).
JOB_PULL=$(submit pull singleton ${TIME} "pixi run dvc pull --force")
DEP_PULL="afterok:${JOB_PULL}"

# Step 1: shared rwzi dependency (needs pulled inputs).
JOB_RWZI=$(submit rwzi "${DEP_PULL}" ${TIME} "$(py notebooks/create_rwzi_model.py)")
DEP="afterok:${JOB_RWZI}"

# Step 2: 22 independent stage jobs
JOBIDS=""

# 11 dynamic chains (parameterized -> bergend -> dynamic), key:item
for pair in \
  aa_en_maas:AaenMaas \
  brabantse_delta:BrabantseDelta \
  de_dommel:DeDommel \
  drents_overijsselse_delta:DrentsOverijsselseDelta \
  hunze_en_aas:HunzeenAas \
  limburg:Limburg \
  noorderzijlvest:Noorderzijlvest \
  rijn_en_ijssel:RijnenIJssel \
  stichtse_rijnlanden:StichtseRijnlanden \
  vallei_en_veluwe:ValleienVeluwe \
  vechtstromen:Vechtstromen
do
  key="${pair%%:*}"; item="${pair##*:}"
  cmds=$(chain \
    "$(py notebooks/${key}/_preprocess_profielen.py)" \
    "$(py notebooks/${key}/01_fix_model.py)" \
    "$(py notebooks/${key}/02_prepare_model.py)" \
    "$(py notebooks/${key}/03_parameterize_model.py)" \
    "$(py notebooks/${key}/04_add_full_control.py)" \
    "$(py notebooks/05_add_bergend.py ${item})" \
    "$(py notebooks/07_add_dynamic_forcing.py ${item})")
  BEGIN="now+$((STAGGER_COUNT * STAGGER))seconds"
  JID=$(submit "${key}" "${DEP}" ${TIME} "${cmds}")
  JOBIDS="${JOBIDS}:${JID}"
  STAGGER_COUNT=$((STAGGER_COUNT + 1))
done

# 10 peilbeheerst chains (feedback -> profiles -> forcing), key:item
for pair in \
  delfland:Delfland \
  amstel_gooi_en_vecht:AmstelGooienVecht \
  hollands_noorderkwartier:HollandsNoorderkwartier \
  hollandse_delta:HollandseDelta \
  rijnland:Rijnland \
  rivierenland:Rivierenland \
  scheldestromen:Scheldestromen \
  schieland_en_de_krimpenerwaard:SchielandendeKrimpenerwaard \
  wetterskip_fryslan:WetterskipFryslan \
  zuiderzeeland:Zuiderzeeland
do
  key="${pair%%:*}"; item="${pair##*:}"
  cmds=$(chain \
    "$(py src/peilbeheerst_model/feedback/${item}.py)" \
    "$(py src/peilbeheerst_model/profiles/prep_cross_sections.py ${item})" \
    "$(py src/peilbeheerst_model/profiles/${item}.py)" \
    "$(py src/peilbeheerst_model/forcing/${item}.py)")
  BEGIN="now+$((STAGGER_COUNT * STAGGER))seconds"
  JID=$(submit "${key}" "${DEP}" ${TIME} "${cmds}")
  JOBIDS="${JOBIDS}:${JID}"
  STAGGER_COUNT=$((STAGGER_COUNT + 1))
done

# hws (hws_demand -> hws_transient chain)
HWS_CMDS=$(chain \
  "$(py notebooks/rijkswaterstaat/2_basins.py)" \
  "$(py notebooks/rijkswaterstaat/3_netwerk.py)" \
  "$(py notebooks/rijkswaterstaat/4_kunstwerken.py)" \
  "$(py notebooks/rijkswaterstaat/5_model_netwerk.py)" \
  "$(py notebooks/rijkswaterstaat/6_model_sturing.py)" \
  "$(py notebooks/rijkswaterstaat/7_model_onttrekkingen.py)" \
  "$(py notebooks/rijkswaterstaat/8a_update_state.py)" \
  "$(py notebooks/rijkswaterstaat/8b_update_bc.py)")
BEGIN="now+$((STAGGER_COUNT * STAGGER))seconds"
JID=$(submit hws "${DEP}" ${TIME} "${HWS_CMDS}")
JOBIDS="${JOBIDS}:${JID}"
STAGGER_COUNT=$((STAGGER_COUNT + 1))

# Step 3: samenvoegen (after all 22 complete)
unset BEGIN
JOB_SAMENVOEGEN=$(submit samenvoegen "afterok${JOBIDS}" ${TIME} "$(py notebooks/08_samenvoegen_modellen.py)")

# Step 4: koppelen (after samenvoegen)
JOB_KOPPELEN=$(submit koppelen "afterok:${JOB_SAMENVOEGEN}" ${TIME} "$(py notebooks/09_koppelen_modellen.py)")

# Step 5: register + upload all produced outputs to DVC once (serial).
JOB_COMMIT=$(submit commit "afterok:${JOB_KOPPELEN}" ${TIME} "pixi run dvc commit -f && pixi run dvc push")

# Log job IDs for run.sh to depend on
echo "samenvoegen	${JOB_SAMENVOEGEN}" > repro_jobs.txt
echo "koppelen	${JOB_KOPPELEN}" >> repro_jobs.txt
echo "commit	${JOB_COMMIT}" >> repro_jobs.txt
