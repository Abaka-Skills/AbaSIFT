#!/usr/bin/env bash
# Create (or update) the abasift conda env and install the package in editable mode.
#   bash setup.sh          # then: conda activate abasift
set -euo pipefail
cd "$(dirname "$0")"

ENV_NAME=abasift

if conda env list | awk '{print $1}' | grep -qx "$ENV_NAME"; then
  echo ">> updating env $ENV_NAME"
  conda env update -n "$ENV_NAME" -f environment.yml --prune
else
  echo ">> creating env $ENV_NAME"
  conda env create -f environment.yml
fi

echo ">> installing abasift (editable)"
conda run -n "$ENV_NAME" python -m pip install -e . --no-deps

echo
echo "done. next:  conda activate $ENV_NAME && pytest"
