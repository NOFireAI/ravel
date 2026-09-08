# VHS tape for docs/demo.gif: the kill-and-recover demonstration.
#
# The GIF in the README is a recording of a passing run of
# demo/kill-and-recover.sh (ADR-0081 decision 9). This tape is checked in so the
# recording is reproducible rather than a one-take screen capture: anyone with
# `vhs` and a running compose stack can regenerate the exact same GIF.
#
#   docker compose -f deploy/docker-compose/ravel.yml up -d
#   vhs demo/demo.tape
#
# The script is the artifact under test and CI runs its assertions; this tape
# only records it. If the script fails, the recording shows it failing, which is
# the correct outcome -- never edit the tape to hide a red run.

Output docs/demo.gif

Set Shell bash
Set FontSize 15
Set Width 1200
Set Height 760
Set Padding 18
Set Theme "Builtin Dark"
Set TypingSpeed 28ms
Set PlaybackSpeed 1.0

# Start on a clean prompt so the first frame is not a half-drawn shell.
Hide
Type "clear"
Enter
Sleep 500ms
Show

Sleep 1s

# One line of context, so a viewer who lands on the GIF with no other reading
# knows what claim is being tested before the output starts scrolling.
Type "# Ravel: object storage is the only durable component."
Enter
Sleep 800ms
Type "# Ingest, hard-kill the server, restart it empty, read the data back."
Enter
Sleep 1200ms

Type "./demo/kill-and-recover.sh"
Enter

# A measured sleep, not a guessed one. `Wait` is the better primitive in
# principle, but neither form works here: bare `Wait` only ever inspects the
# last line, which by the time the script finishes is the shell prompt rather
# than the banner, and `Wait+Screen` does not settle against the long JSON the
# query result prints. A measured run of the script on this stack completes in
# about 38 seconds, so this is that plus margin for a slower disk or a colder
# image cache.
#
# Re-time this if the script grows a step. Too short truncates the only part
# that matters; too long freezes the tail of the recording on a static frame,
# which is what a first attempt at 75s produced (half the GIF was a dead
# prompt).
Sleep 48s

Sleep 2s
Type 'echo "exit status: $?"'
Enter
Sleep 4s
