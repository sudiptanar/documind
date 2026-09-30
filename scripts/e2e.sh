#!/usr/bin/env bash
# End-to-end check through the gateway: register -> login -> upload -> wait for INDEXED -> ask -> assert a citation.
# Usage: BASE_URL=http://localhost:8080 scripts/e2e.sh
# Needs only bash + curl (no jq), so it runs in Git Bash, macOS, Linux and CI.
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
PDF="${PDF:-$(dirname "$0")/../samples/sample.pdf}"
QUESTION="${QUESTION:-How many days notice is required to terminate the agreement?}"
TIMEOUT="${TIMEOUT:-180}"
EMAIL="e2e-$(date +%s)-$RANDOM@example.com"
PASSWORD="Passw0rd!e2e"

pass() { printf '  \033[32mPASS\033[0m %s\n' "$1"; }
fail() { printf '  \033[31mFAIL\033[0m %s\n' "$1"; exit 1; }
field() { sed -n "s/.*\"$1\":\"\\([^\"]*\\)\".*/\\1/p" | head -1; }   # first string value of a JSON key

echo "DocuMind e2e against $BASE_URL"

code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE_URL/api/documents")
[[ "$code" == "401" ]] && pass "protected route without token -> 401" || fail "expected 401 without token, got $code"

code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE_URL/auth/register" -H 'Content-Type: application/json' \
  -d "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\",\"fullName\":\"E2E Test\"}")
[[ "$code" == "201" ]] && pass "register $EMAIL" || fail "register returned $code"

TOKEN=$(curl -s -X POST "$BASE_URL/auth/login" -H 'Content-Type: application/json' \
  -d "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\"}" | field accessToken)
[[ -n "$TOKEN" ]] && pass "login returned a JWT" || fail "login returned no token"
AUTH=(-H "Authorization: Bearer $TOKEN")

UPLOAD=$(curl -s "${AUTH[@]}" -F "file=@$PDF;type=application/pdf" "$BASE_URL/api/documents")
DOC_ID=$(echo "$UPLOAD" | field id)
[[ -n "$DOC_ID" ]] && pass "upload accepted, document $DOC_ID" || fail "upload failed: $UPLOAD"

printf '  .... waiting for indexing'
STATUS=""
for ((i = 0; i < TIMEOUT; i += 2)); do
  DOC=$(curl -s "${AUTH[@]}" "$BASE_URL/api/documents/$DOC_ID")
  STATUS=$(echo "$DOC" | field status)
  [[ "$STATUS" == "INDEXED" || "$STATUS" == "FAILED" ]] && break
  printf '.'; sleep 2
done
echo
if [[ "$STATUS" == "FAILED" ]]; then
  fail "document FAILED: $(echo "$DOC" | field error)"
fi
[[ "$STATUS" == "INDEXED" ]] && pass "document INDEXED" || fail "document still '$STATUS' after ${TIMEOUT}s"

ANSWER=$(curl -s "${AUTH[@]}" -H 'Content-Type: application/json' -X POST "$BASE_URL/api/query" \
  -d "{\"question\":\"$QUESTION\"}")
echo "  Q: $QUESTION"
echo "  A: $(echo "$ANSWER" | field answer)"
echo "$ANSWER" | grep -q '"fileName":"sample.pdf"' && pass "answer cites sample.pdf" || fail "no citation in: $ANSWER"

code=$(curl -s -o /dev/null -w '%{http_code}' "${AUTH[@]}" -X DELETE "$BASE_URL/api/documents/$DOC_ID")
[[ "$code" == "204" ]] && pass "cleanup: document deleted" || fail "delete returned $code"

echo "All end-to-end checks passed."
