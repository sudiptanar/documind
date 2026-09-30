#!/usr/bin/env bash
# Generates the RS256 key pair the auth service signs JWTs with.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p secrets
if [[ -f secrets/jwt-private.pem ]]; then
  echo "secrets/jwt-private.pem already exists; delete it first to rotate keys."
  exit 0
fi
openssl genpkey -algorithm RSA -out secrets/jwt-private.pem -pkeyopt rsa_keygen_bits:2048
openssl rsa -in secrets/jwt-private.pem -pubout -out secrets/jwt-public.pem
echo "Wrote secrets/jwt-private.pem and secrets/jwt-public.pem"
