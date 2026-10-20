#!/usr/bin/env bash
# Runs the ```bash blocks of Markdown files in order, in one shell, so later
# blocks see the variables earlier ones set, and fails on the first command
# that fails. An HTML comment just before a block changes what happens:
#
#   <!-- doctest: skip -->           do not run it (it builds or starts a server)
#   <!-- doctest: expect TEXT -->    its output must contain TEXT
#
#   scripts/check-docs.sh docs/getting-started.md README.md
set -euo pipefail

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

n=0
for doc in "$@"; do
	# Split the file into one script per bash block, with a sidecar for its marker.
	awk -v dir="$work" -v doc="$doc" -v start="$n" '
		/^<!-- doctest: / { marker = $0; next }
		/^```bash$/ && !inblock { inblock = 1; start++; file = sprintf("%s/%04d.sh", dir, start)
			printf "" > file; print doc > (file ".doc"); if (marker != "") print marker > (file ".marker"); marker = ""; next }
		/^```/ && inblock { inblock = 0; close(file); next }
		inblock { print >> file; next }
		/[^[:space:]]/ { marker = "" }
	' "$doc"
	n=$(ls "$work"/*.sh 2>/dev/null | wc -l | tr -d ' ')
done

ran=0 skipped=0
for block in "$work"/*.sh; do
	[ -e "$block" ] || continue
	doc=$(cat "$block.doc")
	marker=$(cat "$block.marker" 2>/dev/null || true)
	case "$marker" in
	*"doctest: skip"*)
		skipped=$((skipped + 1))
		continue
		;;
	esac
	echo "== $doc, block $(basename "$block" .sh)"
	sed 's/^/  $ /' "$block"
	if ! . "$block" >"$block.out" 2>&1; then
		cat "$block.out"
		echo "FAILED: a command in this block failed" >&2
		exit 1
	fi
	awk '{ print "  " $0 }' "$block.out" # always ends with a newline
	want=$(printf '%s' "$marker" | sed -n 's/.*doctest: expect \(.*\) -->.*/\1/p')
	if [ -n "$want" ] && ! grep -qF -- "$want" "$block.out"; then
		echo "FAILED: output does not contain: $want" >&2
		exit 1
	fi
	ran=$((ran + 1))
done
echo "docs: $ran blocks ran as written, $skipped skipped"
