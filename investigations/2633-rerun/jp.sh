#!/bin/bash
cd /var/lib/fleet/work/db9e7a3c-73f0-4fd9-ac9b-8bcb2327aa8d/.gate-logs
B=$PWD/ravel-server-5378afd2; R=run; O=jp; mkdir -p $O
P=$R/jeprof.3546400.852.i852.heap; BASE=$R/jeprof.3546400.138.i138.heap; MU=$R/jeprof.3546400.2106.i2106.heap; END=$R/jeprof.3546400.3141.i3141.heap
jeprof --text --show_bytes $B $P > $O/jeprof-peak-text.txt 2>$O/err1 &
jeprof --text --show_bytes --cum $B $P > $O/jeprof-peak-cum.txt 2>$O/err2 &
jeprof --text --show_bytes --base=$BASE $B $P > $O/jeprof-diff-text.txt 2>$O/err3 &
jeprof --text --show_bytes --cum --base=$BASE $B $P > $O/jeprof-diff-cum.txt 2>$O/err4 &
jeprof --text --lines --show_bytes --cum $B $P > $O/jeprof-peak-lines-cum.txt 2>$O/err5 &
jeprof --text --lines --show_bytes --cum --base=$BASE $B $P > $O/jeprof-diff-lines-cum.txt 2>$O/err6 &
jeprof --text --show_bytes $B $BASE > $O/jeprof-base-text.txt 2>$O/err7 &
jeprof --text --show_bytes $B $MU > $O/jeprof-maxlive-text.txt 2>$O/err8 &
jeprof --text --show_bytes $B $END > $O/jeprof-end-text.txt 2>$O/err9 &
jeprof --collapsed --show_bytes $B $P > $O/c-peak.txt 2>$O/err10 &
jeprof --collapsed --show_bytes --base=$BASE $B $P > $O/c-diff.txt 2>$O/err11 &
jeprof --collapsed --show_bytes $B $BASE > $O/c-base.txt 2>$O/err12 &
jeprof --collapsed --show_bytes $B $MU > $O/c-maxlive.txt 2>$O/err13 &
jeprof --collapsed --show_bytes $B $END > $O/c-end.txt 2>$O/err14 &
wait
wc -l $O/*.txt
