#!/usr/bin/env bash
# Add the macro/commodity ETFs that were missing from the companies
# table after the 2026-05-16 audit:
#
#   UUP            US Dollar bullish (Invesco)
#   USO/UCO/SCO    US Oil Fund + 2x bull/bear
#   UNG/BOIL/KOLD  US Natural Gas Fund + 2x bull/bear
#   UGA            US Gasoline Fund
#   DBA/DBC        Invesco Agriculture / Broad Commodity
#   GBTC           Grayscale Bitcoin Trust
#   PALL/PPLT      Aberdeen Palladium / Platinum
#
# After this lands, the next `sawa index-update` (or coldstart's
# populate_index_constituents) will automatically join them to
# `us_active`, since they're active ARCX-listed ETFs that Polygon
# returns in the broad scan.
#
# Idempotent: `sawa add-symbol` uses ON CONFLICT (ticker) DO UPDATE.

set -euo pipefail

sawa add-symbol \
    UUP \
    USO UCO SCO \
    UNG BOIL KOLD \
    UGA \
    DBA DBC \
    GBTC \
    PALL PPLT
