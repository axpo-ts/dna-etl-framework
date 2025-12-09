#!/usr/bin/env bash
set -euo pipefail

BUNDLE_FILE="${1:-databricks.yml}"
TARGET="${2:-personal_dev}"

if ! command -v yq >/dev/null 2>&1; then
  echo "yq is required on PATH"; exit 1
fi

if ! command -v databricks >/dev/null 2>&1; then
  echo "databricks CLI is required on PATH"; exit 1
fi

# 1. Resolve ENV_CATALOG_IDENTIFIER and ENV_SCHEMA_PREFIX_WITH_DEV
ENV_CATALOG_IDENTIFIER=$(yq '.targets.'"${TARGET}"'.variables.ENV_CATALOG_IDENTIFIER // ""' "$BUNDLE_FILE")
ENV_SCHEMA_PREFIX_WITH_DEV=$(yq '.targets.'"${TARGET}"'.variables.ENV_SCHEMA_PREFIX_WITH_DEV // ""' "$BUNDLE_FILE")

if [[ -z "$ENV_CATALOG_IDENTIFIER" ]]; then
  echo "ENV_CATALOG_IDENTIFIER not set for target ${TARGET}"; exit 1
fi

CATALOG="${ENV_CATALOG_IDENTIFIER}_staging"
SCHEMA="wheel_schema"
VOLUME="wheel_volume"

echo "Resolved:"
echo "  Catalog: ${CATALOG}"
echo "  Schema : ${SCHEMA}"
echo "  Volume : ${VOLUME}"
echo "  Path   : /Volumes/${CATALOG}/${SCHEMA}/${VOLUME}"

# 2. Ensure schema exists
if ! databricks schemas get "${CATALOG}.${SCHEMA}" >/dev/null 2>&1; then
  echo "Creating schema ${CATALOG}.${SCHEMA}"
  databricks schemas create "${SCHEMA}" "${CATALOG}" \
    --comment "Wheel artifacts schema"
else
  echo "Schema ${CATALOG}.${SCHEMA} already exists"
fi

# 3. Ensure volume exists
if ! databricks volumes read "${CATALOG}.${SCHEMA}.${VOLUME}" >/dev/null 2>&1; then
  echo "Creating managed volume ${CATALOG}.${SCHEMA}.${VOLUME}"
  databricks volumes create "${CATALOG}" "${SCHEMA}" "${VOLUME}" MANAGED
else
  echo "Volume ${CATALOG}.${SCHEMA}.${VOLUME} already exists"
fi

echo "Done. Wheel artifacts volume is created at /Volumes/${CATALOG}/${SCHEMA}/${VOLUME}"

# 4. Build the wheel
pdm build -p .
echo "Built wheel artifact."

# 5. Upload wheel to volume
WHL_FILE=$(ls dist/*.whl | head -n 1)
echo "Found wheel to upload : $WHL_FILE"

databricks fs cp "${WHL_FILE}" "dbfs:/Volumes/${CATALOG}/${SCHEMA}/${VOLUME}/" --overwrite
echo "Artifact copied to /Volumes/${CATALOG}/${SCHEMA}/${VOLUME}/{$WHL_FILE}"

exit 0