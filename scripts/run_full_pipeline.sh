#!/usr/bin/env bash
# NOTE: Édite les variables PROJECT_ID et BUCKET en début de script avant exécution.
set -euo pipefail

# If a .env file exists in the repo root it will be sourced and can override defaults.
if [ -f ".env" ]; then
  source .env
fi

# Project info
PROJECT_ID="${PROJECT_ID:-YOUR_PROJECT_ID}"
BUCKET="${BUCKET:-YOUR_BUCKET_NAME}"
CLUSTER_NAME="${CLUSTER_NAME:-dataprocpagepank}"
REGION="${REGION:-europe-west1}"

ZONE="${ZONE:-}"
SINGLE_NODE="${SINGLE_NODE:-false}"
NUMBER_WORKERS=2
IMAGE_VERSION="${IMAGE_VERSION:-2.3-debian12}"

# Security options
NO_EXTERNAL_IP="${NO_EXTERNAL_IP:-false}"
SUBNET="${SUBNET:-}"

# Chemins vers les jobs
GCS_JOB_PATH_DF="gs://${BUCKET}/jobs/df_pagerank.py"
GCS_JOB_PATH_RDD="gs://${BUCKET}/jobs/rdd_pagerank.py"

GCS_INPUT="gs://${BUCKET}/data/wikilinks.csv"
GCS_OUTPUT_RDD_BASE="gs://${BUCKET}/outputs/wikilinks-rdd-$(date +"%Y-%m-%d_%H-%M-%S")"
GCS_OUTPUT_RDD_TIME="outputs/time-rdd-$(date +"%Y-%m-%d_%H-%M-%S").csv"

GCS_OUTPUT_DF_BASE="gs://${BUCKET}/outputs/wikilinks-df-$(date +"%Y-%m-%d_%H-%M-%S")"
GCS_OUTPUT_DF_TIME="outputs/time-df-$(date +"%Y-%m-%d_%H-%M-%S").csv"

LOCAL_OUT_DIR="outputs/wikilinks"

TMPDIR="$(mktemp -d)"
CLUSTER_CREATED=false

# Conditions des experiences
LIMIT_SIZE_CSV=500
NUMBER_ITERATIONS=3


cleanup() {
  rm -rf "$TMPDIR"jobs/

  if [ "$CLUSTER_CREATED" = true ]; then
    echo "Suppression du cluster ${CLUSTER_NAME}..."
    gcloud dataproc clusters delete "$CLUSTER_NAME" --region="$REGION" --project="$PROJECT_ID" --quiet || true

  fi
}
trap cleanup EXIT


#======================================================================================================================
# Vérifications rapides
command -v gcloud >/dev/null 2>&1 || { echo "gcloud introuvable. Installe et configure le SDK Google Cloud."; exit 1; }
command -v gsutil >/dev/null 2>&1 || { echo "gsutil introuvable. Installe et configure le SDK Google Cloud."; exit 1; }

if [ "$PROJECT_ID" = "YOUR_PROJECT_ID" ] || [ "$BUCKET" = "YOUR_BUCKET_NAME" ]; then
  echo "Veuillez éditer ${BASH_SOURCE[0]} et renseigner PROJECT_ID et BUCKET avant d'exécuter."
  exit 1
fi

echo "Vérification du bucket gs://${BUCKET}..."
if ! gsutil ls -b "gs://${BUCKET}" >/dev/null 2>&1; then
  echo "Bucket gs://${BUCKET} introuvable. Création..."
  gsutil mb -p "$PROJECT_ID" -l "$REGION" "gs://${BUCKET}"
else
  echo "Bucket existe."
fi

#======================================================================================================================
# Téléchargement de wikilinks si absent
if gsutil -q stat "$GCS_INPUT"; then
  echo "Wikilinks déjà présent dans ${GCS_INPUT}."
else
  echo "Téléchargement des liens wikilinks"
  python src/data_fetcher.py $LIMIT_SIZE_CSV
fi

#======================================================================================================================
# Experience avec les dataframes

echo "Upload du job src/df_pagerank.py -> ${GCS_JOB_PATH_DF}"
gsutil cp -n "src/df_pagerank.py" "$GCS_JOB_PATH_DF" || echo "Le job existe déjà sur GCS ou l'upload a été ignoré (option -n)."

# Création du cluster Dataproc
echo "Création du cluster Dataproc ${CLUSTER_NAME} (region=${REGION})..."
ZONE_ARG=""
if [ -n "$ZONE" ]; then
  ZONE_ARG="--zone=$ZONE"
fi

SUBNET_ARG=""
if [ -n "$SUBNET" ]; then
  SUBNET_ARG="--subnet=$SUBNET"
fi

NO_ADDRESS_ARG=""
if [ "$NO_EXTERNAL_IP" = true ]; then
  NO_ADDRESS_ARG="--no-address"
fi

if [ "$SINGLE_NODE" = true ]; then
  gcloud dataproc clusters create "$CLUSTER_NAME" \
    --project="$PROJECT_ID" \
    --region="$REGION" \
    $ZONE_ARG \
    $SUBNET_ARG \
    --single-node $NO_ADDRESS_ARG \
    --image-version="$IMAGE_VERSION"
else
  gcloud dataproc clusters create "$CLUSTER_NAME" \
    --project="$PROJECT_ID" \
    --region="$REGION" \
    $ZONE_ARG \
    $SUBNET_ARG \
    --master-machine-type=n4-standard-4 \
    --worker-machine-type=n4-standard-4 \
    --master-boot-disk-size=50GB \
    --worker-boot-disk-size=50GB \
    --num-workers=$NUMBER_WORKERS \
    $NO_ADDRESS_ARG \
    --properties=yarn:yarn.scheduler.maximum-allocation-mb=14336,yarn:yarn.nodemanager.resource.memory-mb=14336 \
    --image-version="$IMAGE_VERSION"
fi

CLUSTER_CREATED=true


# Soumission du job
echo "Soumission du job pyspark..."
gcloud dataproc jobs submit pyspark "$GCS_JOB_PATH_DF" \
  --cluster="$CLUSTER_NAME" \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  -- $NUMBER_ITERATIONS $GCS_INPUT $GCS_OUTPUT_DF_BASE $PROJECT_ID $BUCKET $GCS_OUTPUT_DF_TIME

# Récupération des résultats
echo "Téléchargement des résultats depuis ${GCS_OUTPUT_DF_BASE} vers ${LOCAL_OUT_DIR}"
mkdir -p "$LOCAL_OUT_DIR"
gsutil -m cp -r "${GCS_OUTPUT_DF_BASE}" "$LOCAL_OUT_DIR/"
gsutil -m cp -r "gs://${BUCKET}/${GCS_OUTPUT_DF_TIME}" "$LOCAL_OUT_DIR/"


echo "Résultats téléchargés dans ${LOCAL_OUT_DIR}"

#======================================================================================================================
# Experience avec les rdd
echo "Upload du job src/rdd_pagerank.py -> ${GCS_JOB_PATH_RDD}"
gsutil cp -n "src/rdd_pagerank.py" "$GCS_JOB_PATH_RDD" || echo "Le job existe déjà sur GCS ou l'upload a été ignoré (option -n)."

# Soumission du job
echo "Soumission du job pyspark..."
gcloud dataproc jobs submit pyspark "$GCS_JOB_PATH_RDD" \
  --cluster="$CLUSTER_NAME" \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  -- $NUMBER_ITERATIONS $GCS_INPUT $GCS_OUTPUT_RDD_BASE $PROJECT_ID $BUCKET $GCS_OUTPUT_RDD_TIME

# Récupération des résultats
echo "Téléchargement des résultats depuis ${GCS_OUTPUT_RDD_BASE} vers ${LOCAL_OUT_DIR}"
mkdir -p "$LOCAL_OUT_DIR"
gsutil -m cp -r "${GCS_OUTPUT_RDD_BASE}" "$LOCAL_OUT_DIR/"
gsutil -m cp -r "gs://${BUCKET}/${GCS_OUTPUT_RDD_TIME}" "$LOCAL_OUT_DIR/"

echo "Résultats téléchargés dans ${LOCAL_OUT_DIR}"

#======================================================================================================================
# Suppression du cluster
echo "Suppression explicite du cluster ${CLUSTER_NAME}..."
gcloud dataproc jobs submit pyspark "$GCS_JOB_PATH_DF" \
  --cluster="$CLUSTER_NAME" \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  -- \
  --input "$GCS_INPUT" --output "$GCS_OUTPUT
CLUSTER_CREATED=false

echo "Pipeline terminé avec succès. Les résultats sont disponibles dans ${LOCAL_OUT_DIR}."
