#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
for cmd in terraform aws; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Install Terraform and AWS CLI: brew install terraform awscli" >&2; exit 1
  fi
done
if [[ ! -x .venv/bin/python ]]; then
  echo "Create the project environment first: uv sync" >&2; exit 1
fi
if [[ ! -f .env ]]; then
  echo "Copy .env.example to .env and fill in the AWS archive settings first" >&2; exit 1
fi
set -a
source .env
set +a
: "${AWS_ADMIN_PROFILE:?set AWS_ADMIN_PROFILE to an existing admin profile}"
export AWS_PROFILE="${AWS_PROFILE:-crypto-archiver}"
if [[ "$AWS_PROFILE" == "$AWS_ADMIN_PROFILE" ]]; then
  echo "AWS_PROFILE must differ from AWS_ADMIN_PROFILE so the archive job uses its own limited profile" >&2; exit 1
fi
: "${AWS_REGION:?set AWS_REGION}"
: "${S3_BUCKET:?set S3_BUCKET}"
: "${TF_VAR_bucket_name:?set TF_VAR_bucket_name}"
: "${TF_VAR_region:?set TF_VAR_region}"
: "${TF_VAR_budget_email:?set TF_VAR_budget_email}"
if [[ "$S3_BUCKET" != "$TF_VAR_bucket_name" || "$AWS_REGION" != "$TF_VAR_region" ]]; then
  echo "TF_VAR_bucket_name/region must match S3_BUCKET/AWS_REGION" >&2; exit 1
fi
AWS_PROFILE="$AWS_ADMIN_PROFILE" terraform -chdir=infra/terraform init
AWS_PROFILE="$AWS_ADMIN_PROFILE" terraform -chdir=infra/terraform apply
if [[ -z "$(aws configure get aws_access_key_id --profile "$AWS_PROFILE" || true)" ]]; then
  user_name="$(AWS_PROFILE="$AWS_ADMIN_PROFILE" terraform -chdir=infra/terraform output -raw archiver_user_name)"
  aws iam create-access-key --user-name "$user_name" --profile "$AWS_ADMIN_PROFILE" |
    .venv/bin/python -c '
import json
import os
import subprocess
import sys

key = json.load(sys.stdin)["AccessKey"]
for setting, field in (("aws_secret_access_key", "SecretAccessKey"), ("aws_access_key_id", "AccessKeyId")):
    result = subprocess.run(
        ["aws", "configure", "set", setting, key[field], "--profile", os.environ["AWS_PROFILE"]],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    if result.returncode:
        raise SystemExit("Could not configure the archiver profile")
'
fi
aws configure set region "$AWS_REGION" --profile "$AWS_PROFILE"
