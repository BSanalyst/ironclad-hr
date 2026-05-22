#!/bin/bash
# deploy_notebooks.sh
# Deploys notebooks to Databricks workspace as proper notebook objects
# Run from repo root before databricks bundle deploy

TARGET=${1:-dev}

case $TARGET in
  dev)
    NOTEBOOK_PATH="/Workspace/Users/benjaminstringer1994@gmail.com/ironclad-hr-notebooks/dev"
    ;;
  test)
    NOTEBOOK_PATH="/Workspace/Users/benjaminstringer1994@gmail.com/ironclad-hr-notebooks/test"
    ;;
  prod)
    NOTEBOOK_PATH="/Workspace/Users/benjaminstringer1994@gmail.com/ironclad-hr-notebooks/prod"
    ;;
  *)
    echo "Unknown target: $TARGET. Use dev, test, or prod."
    exit 1
    ;;
esac

echo "Deploying notebooks to $NOTEBOOK_PATH..."

databricks workspace mkdirs "$NOTEBOOK_PATH"

for notebook in notebooks/*.py; do
  name=$(basename "$notebook" .py)
  echo "  Importing $name..."
  databricks workspace import \
    --format SOURCE \
    --language PYTHON \
    --overwrite \
    "$notebook" \
    "$NOTEBOOK_PATH/$name"
done

echo "Notebook deployment complete."
