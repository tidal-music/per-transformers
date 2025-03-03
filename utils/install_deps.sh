#!/bin/bash

# Install requirements using codeartifact. It requires to generate a temporarily url

PROFILE="tidal-backoffice-production--personalization"

TIDAL_PYPI_INDEX_URL=$(sh generate_codeartifact_url.sh $PROFILE)

uv pip install --extra-index-url "$TIDAL_PYPI_INDEX_URL" -r ../pyproject.toml
