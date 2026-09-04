#!/usr/bin/env bash
# WORKLOAD §0.7 / §5 made executable: the zero-leftover account, for cloud resources.
#
#   ./teardown.sh list      what carries Project=rtdb right now (READ ONLY — the default)
#   ./teardown.sh destroy   terraform destroy, then re-list and FAIL if anything survived
#
# It can only ever remove what Terraform created, and every one of those carries Project=rtdb via
# the provider's default_tags. Nothing here deletes by id, by name, or by hand — an untagged
# resource is another project's and is not ours to touch.
set -euo pipefail

REGION="${AWS_REGION:-ap-south-1}"
cd "$(dirname "$0")"

# The tagging API answers "what exists with this tag" across every service in one call, which is
# exactly the question. If it is not permitted, say so loudly rather than reporting an empty list —
# "I could not look" and "there is nothing there" must never render the same.
#
# TWO regions, and the second one is not optional: the tagging API is regional, but GLOBAL services
# index into us-east-1. Querying only ap-south-1 hid a Route53 zone that existed and was correctly
# tagged, and would hide the IAM roles too — a teardown that reports "clean" while the zone it just
# created is still billing is the exact false-green this script exists to prevent.
inventory() {
  local rc=0
  for r in "$REGION" us-east-1; do
    if ! aws resourcegroupstaggingapi get-resources \
          --region "$r" --tag-filters Key=Project,Values=rtdb \
          --query 'ResourceTagMappingList[].ResourceARN' --output text 2>/tmp/rtdb-tagapi.err; then
      echo "!! could not query the tagging API in $r — this is NOT a clean result:" >&2
      sed 's/^/   /' /tmp/rtdb-tagapi.err >&2
      rc=2
    fi
  done
  return $rc
}

case "${1:-list}" in
  list)
    echo "Resources tagged Project=rtdb in $REGION + us-east-1 (global services index there):"
    inventory | tr '\t' '\n' | sed 's/^/  /'
    ;;

  destroy)
    echo "Before:"; inventory | tr '\t' '\n' | sed 's/^/  /'
    # The Route53 zone is the one resource with a manual counterpart: destroying it invalidates the
    # NS records pasted at Cloudflare, and a recreated zone gets DIFFERENT nameservers. Iterating on
    # compute should leave it alone.
    if terraform state list 2>/dev/null | grep -qx 'aws_route53_zone.endpoint'; then
      echo
      echo "NOTE: this also destroys the delegated zone for the endpoint. The next apply will emit"
      echo "      four NEW nameservers that must be re-pasted at Cloudflare before TLS works."
      echo "      To keep it, destroy everything else instead:"
      echo "        terraform destroy \$(terraform state list | grep -v route53 | sed 's/^/-target=/')"
      echo
    fi
    terraform destroy "${@:2}"

    # The teeth. `terraform destroy` reporting success proves Terraform believes its state is empty;
    # it does not prove the account is. Ask the account.
    left="$(inventory | tr '\t' '\n' | grep -v '^$' || true)"
    if [ -n "$left" ]; then
      echo "!! LEFTOVERS — these still carry Project=rtdb after destroy:" >&2
      echo "$left" | sed 's/^/   /' >&2
      exit 1
    fi
    echo "Clean: nothing in $REGION carries Project=rtdb."
    ;;

  *)
    echo "usage: $0 [list|destroy]" >&2; exit 2 ;;
esac
