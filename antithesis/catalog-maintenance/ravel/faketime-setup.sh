#!/bin/sh
# One-shot setup before every other container (config/docker-compose.yaml,
# service `faketime`): put libfaketime and the run wrapper on the shared
# volume, so the rustfs image, which ships neither, loads them from there,
# and write the starting offset. An offset already on the volume is kept: a
# rerun after the driver's clock jump must not move the clocks back.
set -eu
dir=/faketime
for f in libfaketimeMT.so.1 run; do
    case "$f" in
        run) src=/usr/local/bin/ravel-antithesis-faketime-run; mode=0755 ;;
        *) src=/usr/local/lib/faketime/$f; mode=0644 ;;
    esac
    cp "$src" "$dir/$f.next"
    chmod "$mode" "$dir/$f.next"
    mv "$dir/$f.next" "$dir/$f"
done
if [ ! -e "$dir/offset" ]; then
    printf '+0\n' > "$dir/offset.next"
    chmod 0644 "$dir/offset.next"
    mv "$dir/offset.next" "$dir/offset"
    echo "faketime: wrote the starting offset +0"
else
    echo "faketime: kept the offset $(cat "$dir/offset")"
fi
