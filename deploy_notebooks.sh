#!/bin/bash
# deploy_notebooks.sh
# Run from repo root: bash deploy_notebooks.sh dev
# Deploys notebooks, config, and test files to the Databricks workspace.

set -e

TARGET=${1:-dev}
NOTEBOOK_PATH="//Workspace/Users/benjaminstringer1994@gmail.com/ironclad-hr-notebooks/${TARGET}"

echo "Deploying to ${NOTEBOOK_PATH}..."

databricks workspace mkdirs "${NOTEBOOK_PATH}/notebooks"
databricks workspace mkdirs "${NOTEBOOK_PATH}/config"
databricks workspace mkdirs "${NOTEBOOK_PATH}/tests/unit"
databricks workspace mkdirs "${NOTEBOOK_PATH}/tests/integration"

# Notebooks
for notebook in notebooks/*.py; do
  name=$(basename "$notebook" .py)
  dest="${NOTEBOOK_PATH}/notebooks/${name}"
  echo "  Importing $name"
  databricks workspace import \
    --file "$notebook" \
    --format SOURCE \
    --language PYTHON \
    --overwrite \
    "$dest"
done

# Config (JSON — import as plain file via workspace)
databricks workspace import \
  --file "config/quality_rules.json" \
  --format AUTO \
  --overwrite \
  "${NOTEBOOK_PATH}/config/quality_rules.json" 2>/dev/null || \
  databricks fs cp "config/quality_rules.json" \
    "dbfs:/ironclad-hr/${TARGET}/config/quality_rules.json" --overwrite

echo "Deployment complete to ${NOTEBOOK_PATH}"
