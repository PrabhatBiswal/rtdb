#!/bin/bash
# rider-rung.sh — one "rider" sample: a real phone writes to production, a SECOND real phone is
# watched until the change appears on its screen. Built for §5.34's load ladder, where the phones
# are the human-scale witness while the gateway is pushed to failure.
#
#   usage: WRITER=<serial> READER=<serial> scripts/rider-rung.sh "<label>"
#   out:   "<label> | <kind> Committed(rev=N) | arrived on READER ~Ns (+/-2s) | ..."
#
# Arrival is the time until the READER's rendered value pane CHANGES, polled on a fixed 2 s
# cadence, so the resolution is +/-2 s and "~2 s" means "at or under the poll floor".
#
# ---------------------------------------------------------------- why it is built this way
# Every clause below is a bug that was paid for during §5.33/§5.34. Do not "simplify" them back.
#
# 1. ONE FIXED CHILD, never a new child per rung. The demo renders the mirror in INSERTION order,
#    not sorted: a key chosen to sort before the others STILL renders last (proved on a phone whose
#    pane was at the top). So an appended child lands below the visible pane and a screen-diff
#    detector goes blind — reporting "nothing arrived" while delivery is perfectly healthy.
#
# 2. THE VALUE MUST CHANGE EVERY RUNG, so the write ALTERNATES a string and a typed object at that
#    same child. An identical rewrite renders NOTHING: RtdbClient.kt:415 skips a subscription whose
#    subtree value is unchanged (deliberate, Gate C ruling Q2). A repeated identical SET therefore
#    commits a rev and moves the server counters while the glass never changes — indistinguishable
#    from a lost rider unless you know this.
#
# 3. THE READER MUST BE A PHONE NOBODY HAS SCROLLED. A ScrollView keeps its scroll position and
#    never resets, so one stray swipe permanently blinds that phone as a witness to anything at the
#    top of the pane.
#
# 4. NEVER send a blind KEYCODE_BACK "to dismiss the keyboard". With no IME showing, BACK leaves the
#    ACTIVITY, and every later tap lands on the home screen: no write, no render, and the rung
#    reports a loss that BOTH witnesses agree on. Dismiss only when mInputShown=true.
#
# 5. ALWAYS -s <serial>. With more than one phone attached an unqualified adb call is a coin flip.
#
# 6. uiautomator dump CANNOT be trusted for text: it reports an EditText as empty while the field
#    visibly holds text (the text sits in the IME composing span), and on long screens it truncates
#    before the children list. Bounds are fine; text is not. Screenshot and look.
#
# 7. READER-ALIVE GUARD. If the reader stops returning a screen (a dying battery will do it), that
#    is a LOST WITNESS, not a lost rider, and it exits 2 rather than reporting a delivery failure.
set -u

ADB=${ADB:-~/android/sdk/platform-tools/adb}
WRITER=${WRITER:?set WRITER=<serial>   (adb devices -l)}
READER=${READER:?set READER=<serial>   must be a phone nobody has scrolled}
LABEL=${1:-rung}
LOSS_AFTER=${LOSS_AFTER:-60}
PKG=com.hobostays.rtdb.demo

# Tap targets, writer phone, portrait. Defaults are a 1080x2400 moto g67; override for other sizes
# by reading the bounds out of a uiautomator dump (bounds ARE reliable, text is not).
TAP_SET=${TAP_SET:-207 2219}
TAP_TYPED=${TAP_TYPED:-843 2219}
# Reader crop: the value pane. Override per device; must EXCLUDE the status bar, whose clock would
# otherwise change the hash once a minute and fake an arrival.
CROP=${CROP:-600 1080 700 0}

STATE=${STATE_FILE:-${TMPDIR:-/tmp}/.rider-alt}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

shot () {  # hash the reader's value pane; echoes "<md5> <bytes>"
  $ADB -s "$READER" exec-out screencap -p > "$WORK/s.png" 2>/dev/null
  sips -c $CROP "$WORK/s.png" --out "$WORK/s.crop.png" >/dev/null 2>&1
  echo "$(md5 -q "$WORK/s.crop.png" 2>/dev/null) $(stat -f%z "$WORK/s.crop.png" 2>/dev/null || echo 0)"
}

$ADB -s "$WRITER" shell am start -n $PKG/.DemoActivity >/dev/null 2>&1; sleep 2
if $ADB -s "$WRITER" shell dumpsys input_method 2>/dev/null | grep -q "mInputShown=true"; then
  $ADB -s "$WRITER" shell input keyevent KEYCODE_BACK; sleep 1
fi

# alternate the shape so the value always differs from last rung (see 2)
if [ "$(cat "$STATE" 2>/dev/null || echo typed)" = "typed" ]; then
  BTN="$TAP_SET";   KIND="SET(string)"; echo set   > "$STATE"
else
  BTN="$TAP_TYPED"; KIND="TYPED(object)"; echo typed > "$STATE"
fi

read BASE SZ <<< "$(shot)"
if [ "${SZ:-0}" -lt 10000 ]; then
  echo "$LABEL | READER UNAVAILABLE: $READER returned no usable screen — the WITNESS is gone, this is NOT a rider loss"
  exit 2
fi

# the writer's state before clearing: a fast arrival can beat the demo's 5 s state poll, leaving
# the post-write log with no state line at all
WPRE=$($ADB -s "$WRITER" logcat -d -v time RtdbDemo:I '*:S' 2>/dev/null | grep -oE "state=[A-Z]+" | tail -1)
$ADB -s "$WRITER" logcat -c
T0=$(date +%s)
$ADB -s "$WRITER" shell input tap $BTN

ARRIVED=""
while :; do
  sleep 2
  EL=$(( $(date +%s) - T0 ))
  read H SZ2 <<< "$(shot)"
  if [ "${SZ2:-0}" -lt 10000 ]; then
    echo "$LABEL | READER LOST MID-POLL at ${EL}s — the WITNESS died, NOT a rider loss"; exit 2
  fi
  [ "$H" != "$BASE" ] && { ARRIVED=$EL; break; }
  [ "$EL" -ge "$LOSS_AFTER" ] && break
done

REV=$($ADB -s "$WRITER" logcat -d -v time RtdbDemo:I '*:S' 2>/dev/null | grep -oE "Committed\(rev=[0-9]+\)" | tail -1)
WST=$($ADB -s "$WRITER" logcat -d -v time RtdbDemo:I '*:S' 2>/dev/null | grep -oE "state=[A-Z]+" | tail -1)
[ -z "$WST" ] && WST="$WPRE (pre-write)"
RST=$($ADB -s "$READER" logcat -d -v time RtdbDemo:I '*:S' 2>/dev/null | grep -oE "state=[A-Z]+" | tail -1)

if [ -n "$ARRIVED" ]; then
  echo "$LABEL | $KIND ${REV:-<no rev>} | arrived on reader ~${ARRIVED}s (+/-2s) | writer $WST | reader $RST"
else
  echo "$LABEL | $KIND ${REV:-<no rev>} | RIDER LOST: nothing rendered on reader within ${LOSS_AFTER}s (t0=$(date -r $T0 +%T)) | writer $WST | reader $RST"
fi
