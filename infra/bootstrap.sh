#!/usr/bin/env bash
# One-time GCP project bootstrap for pinpad-processor infrastructure.
# Run this before `terraform init` to enable APIs and create state bucket.
#
# Usage: bash infra/bootstrap.sh <PROJECT_ID> <GITHUB_REPO>
# Example: bash infra/bootstrap.sh my-project owner/repo-name

set -euo pipefail

PROJECT_ID="${1:?Usage: $0 <PROJECT_ID> <GITHUB_REPO>}"
GITHUB_REPO="${2:?Usage: $0 <PROJECT_ID> <GITHUB_REPO>}"
REGION="${3:-us-central1}"
STATE_BUCKET="${PROJECT_ID}-tfstate"

echo "==> Configuring project: ${PROJECT_ID}"
gcloud config set project "${PROJECT_ID}"

echo "==> Enabling required APIs..."
gcloud services enable \
  run.googleapis.com \
  pubsub.googleapis.com \
  secretmanager.googleapis.com \
  artifactregistry.googleapis.com \
  iam.googleapis.com \
  storage.googleapis.com \
  cloudbuild.googleapis.com \
  iamcredentials.googleapis.com

echo "==> Creating Terraform state bucket: gs://${STATE_BUCKET}"
if gsutil ls -b "gs://${STATE_BUCKET}" 2>/dev/null; then
  echo "    Bucket already exists, skipping."
else
  gsutil mb -p "${PROJECT_ID}" -l "${REGION}" "gs://${STATE_BUCKET}"
  gsutil versioning set on "gs://${STATE_BUCKET}"
fi

echo ""
echo "=== Bootstrap complete ==="
echo ""
echo "Next steps:"
echo "  1. Copy infra/terraform/terraform.tfvars.example to terraform.tfvars and fill in values"
echo "  2. cd infra/terraform"
echo "  3. terraform init -backend-config=\"bucket=${STATE_BUCKET}\""
echo "  4. terraform plan"
echo "  5. terraform apply"
echo ""
echo "For CI/CD, set these GitHub Actions secrets:"
echo "  GCP_PROJECT_ID=${PROJECT_ID}"
echo "  TF_STATE_BUCKET=${STATE_BUCKET}"
echo "  WIF_PROVIDER=  (from terraform output wif_provider)"
echo "  WIF_SA_EMAIL=  (from terraform output github_sa_email)"
