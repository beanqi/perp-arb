#!/usr/bin/env python3
"""
Seed pair strategies for every symbol shared by Binance USD-M and Bybit linear.

The script uses public market metadata to find common USDT perpetual symbols,
then calls the perp-arb admin API to upsert strategies. It is intentionally
dependency-free so it can run on a bare server with Python 3.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


BINANCE_EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BYBIT_INSTRUMENTS_URL = "https://api.bybit.com/v5/market/instruments-info"


@dataclass(frozen=True)
class SymbolPair:
    symbol: str
    binance_symbol: str
    bybit_symbol: str


def request_json(
    method: str,
    url: str,
    payload: dict[str, Any] | None = None,
    timeout: float = 15.0,
) -> Any:
    body = None
    headers = {"accept": "application/json", "user-agent": "perp-arb-seed/1.0"}
    if payload is not None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        headers["content-type"] = "application/json"

    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
    except urllib.error.HTTPError as error:
        error_body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {url} failed: HTTP {error.code}: {error_body}") from error
    except urllib.error.URLError as error:
        raise RuntimeError(f"{method} {url} failed: {error.reason}") from error

    if not raw:
        return None
    return json.loads(raw.decode("utf-8"))


def fetch_binance_symbols(quote: str, timeout: float) -> set[str]:
    data = request_json("GET", BINANCE_EXCHANGE_INFO_URL, timeout=timeout)
    symbols = set()
    for item in data.get("symbols", []):
        if item.get("status") != "TRADING":
            continue
        if item.get("contractType") != "PERPETUAL":
            continue
        if item.get("quoteAsset") != quote:
            continue
        symbols.add(str(item["symbol"]).upper())
    return symbols


def fetch_bybit_symbols(quote: str, timeout: float) -> set[str]:
    symbols = set()
    cursor = ""
    while True:
        query = {
            "category": "linear",
            "limit": "1000",
        }
        if cursor:
            query["cursor"] = cursor
        url = f"{BYBIT_INSTRUMENTS_URL}?{urllib.parse.urlencode(query)}"
        data = request_json("GET", url, timeout=timeout)
        if data.get("retCode") != 0:
            raise RuntimeError(f"Bybit instruments request failed: {data}")
        result = data.get("result", {})
        for item in result.get("list", []):
            if item.get("status") != "Trading":
                continue
            if item.get("contractType") != "LinearPerpetual":
                continue
            if item.get("quoteCoin") != quote:
                continue
            symbols.add(str(item["symbol"]).upper())

        next_cursor = result.get("nextPageCursor") or ""
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor
    return symbols


def parse_levels(values: list[str], label: str) -> list[dict[str, float]]:
    levels = []
    previous_spread = None
    for value in values:
        try:
            spread_raw, notional_raw = value.split(":", 1)
            spread = float(spread_raw)
            notional = float(notional_raw)
        except ValueError as error:
            raise SystemExit(f"{label} must use SPREAD_PCT:NOTIONAL_USD, got {value!r}") from error

        if spread <= 0 or notional <= 0:
            raise SystemExit(f"{label} values must be positive, got {value!r}")
        if previous_spread is not None and spread <= previous_spread:
            raise SystemExit(f"{label} spread_pct values must be strictly increasing")
        previous_spread = spread
        levels.append({"spread_pct": spread, "notional_usd": notional})

    return levels


def parse_symbol_filter(value: str) -> set[str] | None:
    items = [item.strip().upper() for item in value.split(",") if item.strip()]
    return set(items) if items else None


def strategy_id(prefix: str, symbol: str, direction: str) -> str:
    normalized_symbol = symbol.lower()
    if direction == "binance-long":
        suffix = "binance-long-bybit-short"
    elif direction == "bybit-long":
        suffix = "bybit-long-binance-short"
    else:
        raise ValueError(f"unsupported direction: {direction}")
    return f"{prefix}-{normalized_symbol}-{suffix}"


def build_payload(
    pair: SymbolPair,
    direction: str,
    args: argparse.Namespace,
    open_levels: list[dict[str, float]],
    close_levels: list[dict[str, float]],
) -> dict[str, Any]:
    if direction == "binance-long":
        long_leg = {
            "exchange": "binance_usd_m",
            "symbol": pair.binance_symbol,
            "account_id": args.binance_account_id,
        }
        short_leg = {
            "exchange": "bybit_linear",
            "symbol": pair.bybit_symbol,
            "account_id": args.bybit_account_id,
        }
        name = f"{pair.symbol} Binance long / Bybit short"
    elif direction == "bybit-long":
        long_leg = {
            "exchange": "bybit_linear",
            "symbol": pair.bybit_symbol,
            "account_id": args.bybit_account_id,
        }
        short_leg = {
            "exchange": "binance_usd_m",
            "symbol": pair.binance_symbol,
            "account_id": args.binance_account_id,
        }
        name = f"{pair.symbol} Bybit long / Binance short"
    else:
        raise ValueError(f"unsupported direction: {direction}")

    return {
        "id": strategy_id(args.strategy_prefix, pair.symbol, direction),
        "name": name,
        "enabled": args.enabled,
        "long_leg": long_leg,
        "short_leg": short_leg,
        "open_levels": open_levels,
        "close_levels": close_levels,
        "max_total_notional": args.max_total_notional,
        "max_open_orders": args.max_open_orders,
        "stale_order_query_ms": args.stale_order_query_ms,
    }


def api_url(api_base: str, path: str) -> str:
    return f"{api_base.rstrip('/')}{path}"


def check_accounts(args: argparse.Namespace) -> None:
    accounts = request_json("GET", api_url(args.api_base, "/api/accounts"), timeout=args.timeout)
    by_id = {account.get("id"): account for account in accounts}
    missing = [
        account_id
        for account_id in [args.binance_account_id, args.bybit_account_id]
        if account_id not in by_id
    ]
    if missing:
        raise SystemExit(
            "missing account(s) in admin API: "
            + ", ".join(missing)
            + ". Create accounts first, or pass matching --binance-account-id/--bybit-account-id."
        )

    if args.enabled:
        disabled = [
            account_id
            for account_id in [args.binance_account_id, args.bybit_account_id]
            if not by_id[account_id].get("enabled")
        ]
        if disabled:
            raise SystemExit(
                "strategy creation uses --enabled, but account(s) are disabled: "
                + ", ".join(disabled)
            )


def existing_strategy_ids(args: argparse.Namespace) -> set[str]:
    strategies = request_json("GET", api_url(args.api_base, "/api/strategies"), timeout=args.timeout)
    return {strategy.get("id") for strategy in strategies}


def directions_from_args(value: str) -> list[str]:
    if value == "both":
        return ["binance-long", "bybit-long"]
    return [value]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create perp-arb strategies for Binance/Bybit common symbols."
    )
    parser.add_argument("--api-base", default="http://127.0.0.1:3000")
    parser.add_argument("--binance-account-id", default="binance-main")
    parser.add_argument("--bybit-account-id", default="bybit-main")
    parser.add_argument("--strategy-prefix", default="auto")
    parser.add_argument("--quote", default="USDT")
    parser.add_argument(
        "--directions",
        choices=["binance-long", "bybit-long", "both"],
        default="both",
        help="Strategy direction(s) to create.",
    )
    parser.add_argument(
        "--enabled",
        action="store_true",
        help="Create enabled strategies. Without this flag, strategies are created disabled.",
    )
    parser.add_argument("--limit", type=int, help="Only create the first N matched symbols.")
    parser.add_argument(
        "--symbols",
        default="",
        help="Comma-separated symbol allowlist, for example BTCUSDT,ETHUSDT.",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Do not upsert strategies whose ids already exist.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print summary and sample payloads without calling the admin API.",
    )
    parser.add_argument("--open-level", action="append")
    parser.add_argument("--close-level", action="append")
    parser.add_argument("--max-total-notional", type=float, default=500.0)
    parser.add_argument("--max-open-orders", type=int, default=4)
    parser.add_argument("--stale-order-query-ms", type=int, default=3000)
    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument(
        "--sleep-ms",
        type=int,
        default=0,
        help="Sleep between API writes; useful when creating enabled strategies.",
    )
    parser.add_argument(
        "--skip-account-check",
        action="store_true",
        help="Skip the /api/accounts preflight check.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    quote = args.quote.upper()
    open_levels = parse_levels(args.open_level or ["1.0:100.0"], "--open-level")
    close_levels = parse_levels(args.close_level or ["0.5:100.0"], "--close-level")
    wanted_symbols = parse_symbol_filter(args.symbols)

    print(f"fetching Binance USD-M {quote} perpetual symbols...", file=sys.stderr)
    binance_symbols = fetch_binance_symbols(quote, args.timeout)
    print(f"fetching Bybit linear {quote} perpetual symbols...", file=sys.stderr)
    bybit_symbols = fetch_bybit_symbols(quote, args.timeout)

    common = sorted(binance_symbols & bybit_symbols)
    if wanted_symbols is not None:
        common = [symbol for symbol in common if symbol in wanted_symbols]
    if args.limit is not None:
        common = common[: args.limit]

    pairs = [
        SymbolPair(symbol=symbol, binance_symbol=symbol, bybit_symbol=symbol)
        for symbol in common
    ]
    directions = directions_from_args(args.directions)
    payloads = [
        build_payload(pair, direction, args, open_levels, close_levels)
        for pair in pairs
        for direction in directions
    ]

    print(
        f"matched_symbols={len(common)} directions={len(directions)} "
        f"strategies={len(payloads)} enabled={args.enabled}"
    )
    print(f"binance_symbols={len(binance_symbols)} bybit_symbols={len(bybit_symbols)}")

    if payloads:
        print("sample payload:")
        print(json.dumps(payloads[0], ensure_ascii=False, indent=2))

    if args.dry_run:
        return 0

    if not args.skip_account_check:
        check_accounts(args)

    skip_ids = existing_strategy_ids(args) if args.skip_existing else set()
    created = 0
    skipped = 0
    failed = 0
    total = len(payloads)

    for index, payload in enumerate(payloads, start=1):
        if payload["id"] in skip_ids:
            skipped += 1
            print(f"[{index}/{total}] skip existing {payload['id']}")
            continue

        try:
            request_json(
                "POST",
                api_url(args.api_base, "/api/strategies"),
                payload=payload,
                timeout=args.timeout,
            )
        except RuntimeError as error:
            failed += 1
            print(f"[{index}/{total}] failed {payload['id']}: {error}", file=sys.stderr)
            continue

        created += 1
        print(f"[{index}/{total}] upserted {payload['id']}")
        if args.sleep_ms > 0:
            time.sleep(args.sleep_ms / 1000.0)

    print(f"done created_or_updated={created} skipped={skipped} failed={failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
