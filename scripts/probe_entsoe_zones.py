#!/usr/bin/env python3
"""Probe ENTSO-E directly to settle two ABL-35 questions. Read-only, no DB.

Both questions are about *which zone key* to ask with, both were blocked on the
Transparency Platform being under scheduled maintenance on 2026-08-06, and both
are answered by running this once it is back up.

**Q1 (defect 1) -- why did GR and IE net position stop?**
Both zones stop dead at `2025-09-30 21:00`, the last hour of the CEST market day
2025-09-30, in exact lockstep out of 22. Since then only four isolated market
days landed, and every GR value in them is exactly 0.0 while GR's own
crossborder flows show a median net export of 1,142 MW over the same hours.
Two hypotheses, and this tells them apart:

  (a) ENTSO-E stopped publishing A25 for these zones under the keys we use.
      Note we query plain `IE` (10YIE-1001A00010, the old Irish TSO area), not
      the SDAC bidding zone `IE_SEM` (10Y1001A1001A59C). If IE_SEM returns data
      for a window where IE returns none, the fix is a bidding-zone map --
      exactly the shape NET_POSITION_BIDDING_ZONES already has for DE.
  (b) Something changed platform-wide on 2025-10-01, which is the SDAC
      15-minute-MTU go-live. The `--straddle` window brackets that date, and the
      `resolution` column in the output is what shows it.

**Q2 (defect 2) -- why are DE's DK/SE/NO borders absent?**
`COUNTRY_TO_NEIGHBOURS_KEYS["DE"]` is fixed (DE_AT_LU -> DE_LU, committed), but
that was only half the story: DK_1, DK_2 and SE_4 were ALREADY in the old list
and still returned nothing, while BE/CH/CZ/FR/NL/PL work. The suspect is the
query DOMAIN -- `query_crossborder_all` passes the 2-letter code, and entsoe-py
resolves 'DE' to the control-area EIC 10Y1001A1001A83F rather than the bidding
zone DE_LU (10Y1001A1001A82H). `--flows` asks both domains for the same border
and prints which one answers.

That change is deliberately NOT made blind: switching the domain could regress
the six DE borders that do work today. Run this first.

Usage:
    python scripts/probe_entsoe_zones.py --netpos
    python scripts/probe_entsoe_zones.py --netpos --straddle
    python scripts/probe_entsoe_zones.py --flows
"""
from __future__ import annotations

import argparse
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import config

BASE = "https://web-api.tp.entsoe.eu/api"

# EIC codes, from entsoe-py's Area enum. Spelled out rather than imported so
# the output names the code that was actually on the wire.
AREAS = {
    "BE": "10YBE----------2",
    "GR": "10YGR-HTSO-----Y",
    "IE": "10YIE-1001A00010",       # the old Irish TSO area -- what we query
    "IE_SEM": "10Y1001A1001A59C",   # the SDAC bidding zone -- the suspect
    "DE": "10Y1001A1001A83F",       # German CONTROL AREA -- what we query
    "DE_LU": "10Y1001A1001A82H",    # German BIDDING ZONE -- the suspect
    "DK_1": "10YDK-1--------W",
    "SE_4": "10Y1001A1001A47J",
    "FR": "10YFR-RTE------C",
}


def _request(params: dict) -> tuple[int, str]:
    url = BASE + "?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=60) as resp:
            return resp.status, resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", "replace")
    except Exception as exc:  # network / DNS
        return -1, f"{type(exc).__name__}: {exc}"


def _summarise(label: str, status: int, body: str) -> None:
    if status == 503 and "maintenance" in body.lower():
        print(f"  {label:26s} HTTP 503 -- platform under scheduled maintenance")
        return
    points = len(re.findall(r"<quantity>", body))
    resolutions = sorted(set(re.findall(r"<resolution>([^<]+)</resolution>", body)))
    reason = re.findall(r"<text>([^<]+)</text>", body)
    note = f"  reason={reason[0][:60]}" if reason else ""
    print(
        f"  {label:26s} HTTP {status}  points={points:5d}  "
        f"resolution={','.join(resolutions) or '-':10s}{note}"
    )


def probe_netpos(zones: list[str], start: str, end: str) -> None:
    print(f"A25 day-ahead net position, {start[:8]}..{end[:8]}")
    for zone in zones:
        status, body = _request({
            "securityToken": config.ENTSOE_API_KEY,
            "documentType": "A25",
            "businessType": "B09",
            "contract_MarketAgreement.Type": "A01",
            "in_Domain": AREAS[zone],
            "out_Domain": AREAS[zone],
            "periodStart": start,
            "periodEnd": end,
        })
        _summarise(f"{zone} ({AREAS[zone]})", status, body)


def probe_flows(pairs: list[tuple[str, str]], start: str, end: str) -> None:
    print(f"A11 physical flows, {start[:8]}..{end[:8]}")
    for out_zone, in_zone in pairs:
        status, body = _request({
            "securityToken": config.ENTSOE_API_KEY,
            "documentType": "A11",
            "out_Domain": AREAS[out_zone],
            "in_Domain": AREAS[in_zone],
            "periodStart": start,
            "periodEnd": end,
        })
        _summarise(f"{out_zone} -> {in_zone}", status, body)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--netpos", action="store_true", help="Q1: GR/IE net position")
    parser.add_argument("--flows", action="store_true", help="Q2: DE border domain")
    parser.add_argument("--straddle", action="store_true",
                        help="also probe a window bracketing 2025-10-01")
    parser.add_argument("--start", default="202608010000")
    parser.add_argument("--end", default="202608050000")
    args = parser.parse_args()

    if not config.ENTSOE_API_KEY:
        print("No ENTSO-E API key configured (config.ENTSOE_API_KEY).", file=sys.stderr)
        return 2
    if not (args.netpos or args.flows):
        parser.error("pass --netpos and/or --flows")

    if args.netpos:
        # BE is the control: if BE is empty too, the platform is the problem,
        # not the zone key.
        zones = ["BE", "GR", "IE", "IE_SEM"]
        probe_netpos(zones, args.start, args.end)
        print()
        print("Before the break, for the same zones:")
        probe_netpos(zones, "202509200000", "202509240000")
        if args.straddle:
            print()
            print("Straddling the 2025-10-01 SDAC 15-minute-MTU go-live "
                  "(watch the resolution column):")
            probe_netpos(["BE", "GR", "IE", "IE_SEM"], "202509280000", "202510040000")

    if args.flows:
        if args.netpos:
            print()
        # Same border, two domains. DK_1 and SE_4 are the ones absent from the
        # database; FR is the control that works today under the plain code.
        probe_flows(
            [("DE", "FR"), ("DE_LU", "FR"),
             ("DE", "DK_1"), ("DE_LU", "DK_1"),
             ("DE", "SE_4"), ("DE_LU", "SE_4")],
            args.start,
            args.end,
        )
        print()
        print("If DE_LU answers where DE does not, the fix is a crossborder "
              "bidding-zone map in query_crossborder_all -- and it must keep "
              "DE->FR working, which is why that pair is probed too.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
