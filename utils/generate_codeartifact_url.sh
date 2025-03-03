#!/bin/bash

# use sent profile if it is sent as argument

if [ -n "$1" ]
  then
    PROFILE="--profile $1"
  else
    PROFILE=""
fi

TIDAL_PYPI_INDEX_TOKEN=`aws $PROFILE --region eu-west-1 codeartifact get-authorization-token --duration-seconds 1800 --domain tidal --domain-owner 498346215557 --query authorizationToken --output text`
TIDAL_PYPI_INDEX_URL=https://aws:"${TIDAL_PYPI_INDEX_TOKEN}"@tidal-498346215557.d.codeartifact.eu-west-1.amazonaws.com/pypi/python/simple/

echo $TIDAL_PYPI_INDEX_URL
