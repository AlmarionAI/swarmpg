#!/usr/bin/env bash
# SwarmPG kernel: upstream PostgreSQL at the commit in kernel/BASE plus the
# patch series in kernel/patches.
#
#   kernel/build.sh fetch    clone upstream at BASE into $SWARMPG_SRC and apply the series
#   kernel/build.sh build    configure (when needed), compile and install into $PREFIX
#   kernel/build.sh check    run the upstream regression suite (needs build first on macOS)
#   kernel/build.sh export   rewrite kernel/patches from the commits on top of BASE
#
# Environment: SWARMPG_SRC (default ./swarmpg-core), PREFIX (default ./.pginstall),
# CONFIGURE_FLAGS (default: debug + assertions, for development).
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
root=$(dirname "$here")
src=${SWARMPG_SRC:-$root/swarmpg-core}
prefix=${PREFIX:-$root/.pginstall}
flags=${CONFIGURE_FLAGS---enable-debug --enable-cassert}
base=$(cat "$here/BASE")
upstream=https://github.com/postgres/postgres.git
jobs=$(nproc 2>/dev/null || sysctl -n hw.ncpu)

fetch() {
	if [ -e "$src" ]; then
		echo "$src already exists; remove it or set SWARMPG_SRC" >&2
		exit 1
	fi
	git init -q "$src"
	git -C "$src" fetch -q --depth 1 "$upstream" "$base"
	git -C "$src" checkout -q FETCH_HEAD
	git -C "$src" -c user.name=swarmpg -c user.email=swarmpg@localhost \
		am -q --keep-non-patch --committer-date-is-author-date "$here"/patches/*.patch
	git -C "$src" log --oneline "$base"..HEAD
}

build() {
	# Re-run configure only when the source dir, prefix or flags change.
	stamp="$src/.swarmpg-configure"
	want="$src|$prefix|$flags"
	if [ ! -f "$stamp" ] || [ "$(cat "$stamp")" != "$want" ]; then
		(cd "$src" && ./configure --prefix="$prefix" --without-icu $flags)
		echo "$want" >"$stamp"
	fi
	make -C "$src" -j"$jobs" -s
	make -C "$src" -s install
}

check() {
	make -C "$src" -s check
}

export_patches() {
	rm -f "$here"/patches/*.patch
	git -C "$src" format-patch -q --no-signature --zero-commit -o "$here/patches" "$base"..HEAD
	ls "$here/patches"
}

case "${1:-build}" in
fetch) fetch ;;
build) build ;;
check) check ;;
export) export_patches ;;
*)
	echo "usage: $0 fetch|build|check|export" >&2
	exit 2
	;;
esac
