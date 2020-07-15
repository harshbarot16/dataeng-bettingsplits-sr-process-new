#!/bin/bash

if [[ -z "${ENVIRONMENT}" ]]; then
  if [[ -z "${CODEBUILD_WEBHOOK_TRIGGER}" ]]; then
    STAGE="dev"
  else
    STAGE="${CODEBUILD_WEBHOOK_TRIGGER}"
  fi
else
  STAGE="${ENVIRONMENT}"
fi

serverless deploy --stage $STAGE --verbose --aws-s3-accelerate --config ./serverless.yml
