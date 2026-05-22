#!/bin/bash
# deploy_notebooks.sh
# Deploys notebooks to Databricks workspace as proper notebook objects
# Run from repo root: bash deploy_notebooks.sh dev

TARGET=${1:-dev}
NOTEBOOK_PATH="Workspace/Users/benjaminstringer1994@gmail.com/ironclad-hr-notebooks/${TARGET}"

echo "Deploying notebooks to /${NOTEBOOK_PATH}..."

databricks workspace mkdirs "/${NOTEBOOK_PATH}"

for notebook in notebooks/*.py; do
  name=$(basename "$notebook" .py)
  dest="/${NOTEBOOK_PATH}/${name}"
  echo "  Importing $name -> $dest"
  databricks workspace import \
    --file "$notebook" \
    --format SOURCE \
    --language PYTHON \
    --overwrite \
    "$dest"
done

echo "Notebook deployment complete."
