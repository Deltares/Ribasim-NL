#!/bin/bash

# Submit the full Ribasim-NL pipeline as individual SLURM jobs.
# Run directly on the login node: ./repro.sh
# Uses `dvc repro` per stage; SLURM dependencies ensure correct ordering.

module load pixi

PARTITION=4vcpu
TIME=1-00:00:00
PIXI="module load pixi; cd $PWD"

submit() {
  local name=$1 dep=$2 time=$3; shift 3
  sbatch --parsable --dependency="${dep}" \
    --job-name="${name}" --partition=${PARTITION} --time="${time}" \
    --wrap="${PIXI}; $*"
}

repro() { echo "srun pixi run dvc repro -f -s $*"; }

# Step 1: shared dependency
JOB_RWZI=$(submit rwzi singleton ${TIME} "$(repro rwzi)")
DEP="afterok:${JOB_RWZI}"

# Step 2: 22 independent stage jobs
JOBIDS=""

# 11 dynamic stages (parameterized -> bergend -> dynamic chain)
for key in \
  aa_en_maas \
  brabantse_delta \
  de_dommel \
  drents_overijsselse_delta \
  hunze_en_aas \
  limburg \
  noorderzijlvest \
  rijn_en_ijssel \
  stichtse_rijnlanden \
  vallei_en_veluwe \
  vechtstromen
do
  JID=$(submit "${key}" "${DEP}" ${TIME} "$(repro parameterized@${key} bergend@${key} dynamic@${key})")
  JOBIDS="${JOBIDS}:${JID}"
done

# 10 peilbeheerst stages
for key in \
  delfland \
  amstel_gooi_en_vecht \
  hollands_noorderkwartier \
  hollandse_delta \
  rijnland \
  rivierenland \
  scheldestromen \
  schieland_en_de_krimpenerwaard \
  wetterskip_fryslan \
  zuiderzeeland
do
  JID=$(submit "${key}" "${DEP}" ${TIME} "$(repro peilbeheerst@${key})")
  JOBIDS="${JOBIDS}:${JID}"
done

# hws (hws_demand -> hws_transient chain)
JID=$(submit hws "${DEP}" ${TIME} "$(repro hws_demand hws_transient)")
JOBIDS="${JOBIDS}:${JID}"

# Step 3: samenvoegen (after all 22 complete)
JOB_SAMENVOEGEN=$(submit samenvoegen "afterok${JOBIDS}" ${TIME} "$(repro samenvoegen)")

# Step 4: koppelen (after samenvoegen)
JOB_KOPPELEN=$(submit koppelen "afterok:${JOB_SAMENVOEGEN}" ${TIME} "$(repro koppelen)")

# Log job IDs for run.sh to depend on
echo "samenvoegen	${JOB_SAMENVOEGEN}" > repro_jobs.txt
echo "koppelen	${JOB_KOPPELEN}" >> repro_jobs.txt
