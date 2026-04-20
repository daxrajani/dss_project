#!/bin/bash
# Run this if GCP authentication is not working in your Codespace.
# Usage: bash auth_setup.sh
#
# Prerequisites: the GCP_SERVICE_ACCOUNT_KEY repository secret must be set.
# See README for instructions.
set -e

CRED_FILE="$HOME/.config/gcloud/application_default_credentials.json"
PROJECT="project-a0b2f7d8-d4bf-4557-923"

if [[ -z "${GCP_SERVICE_ACCOUNT_KEY}" ]]; then
    echo "ERROR: GCP_SERVICE_ACCOUNT_KEY environment variable is not set."
    echo ""
    echo "Ask the project owner to add it as a repository Codespaces secret at:"
    echo "  https://github.com/daxrajani/dss_project/settings/secrets/codespaces"
    echo ""
    echo "Once added, rebuild your Codespace and this will run automatically."
    exit 1
fi

echo ">>> Writing GCP credentials..."
mkdir -p "$HOME/.config/gcloud"
echo "${GCP_SERVICE_ACCOUNT_KEY}" > "$CRED_FILE"

echo ">>> Setting GCP project..."
source "$HOME/google-cloud-sdk/path.bash.inc" 2>/dev/null || true
gcloud config set project "$PROJECT" --quiet 2>/dev/null || true

echo ">>> Fetching access token..."
export CLOUDSDK_AUTH_ACCESS_TOKEN=$(python3 -c "
import google.auth, google.auth.transport.requests
creds, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
creds.refresh(google.auth.transport.requests.Request())
print(creds.token)
")

echo ">>> Verifying..."
python3 -c "import google.auth; creds, proj = google.auth.default(); print('ADC OK, project:', proj)"
gcloud dataproc clusters list --region=us-central1

echo ""
echo "Auth complete. Run: bash dashboard.sh"
