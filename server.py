"""NIFTY 500 supply/demand scanner API.

This service intentionally uses only Python's standard library. It retrieves
the NIFTY 500 constituent list from NSE Indices and daily OHLCV candles from
Yahoo Finance. No synthetic or placeholder market values are generated.
"""

from __future__ import annotations

import csv
import io
import json
import math
import os
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from statistics import mean
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, urlparse
from urllib.request import Request, urlopen


SERVICE_NAME = "nifty500-supply-demand-scanner"
SCANNER_RULES = [
    "Close(0) > Close(1)",
    "Close(0) > Close(2)",
    "Close(0) > Close(3)",
    "High(0) crosses above High(-1)",
    "Supply: 1st break → sideways → 2nd break = UP Achievement",
    "Demand: 1st break → sideways → 2nd break = DOWN Achievement",
]
CONSTITUENTS_URL = os.getenv(
    "NIFTY500_CONSTITUENTS_URL",
    "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv",
)
YAHOO_CHART_URL = os.getenv(
    "YAHOO_CHART_URL",
    "https://query1.finance.yahoo.com/v8/finance/chart",
)
PORT = int(os.getenv("PORT", "8080"))
CACHE_TTL_SECONDS = int(os.getenv("MARKET_DATA_CACHE_SECONDS", "300"))
CONSTITUENTS_CACHE_TTL_SECONDS = int(
    os.getenv("NIFTY500_CONSTITUENTS_CACHE_SECONDS", "21600")
)
CONSTITUENTS_CACHE_FILE = Path(
    os.getenv(
        "NIFTY500_CONSTITUENTS_CACHE_FILE",
        str(Path(__file__).resolve().parent.parent / "data" / "nifty500.csv"),
    )
)
STATIC_HTML_FILE = Path(__file__).resolve().parent / "index.html"
HTTP_TIMEOUT_SECONDS = float(os.getenv("MARKET_DATA_TIMEOUT_SECONDS", "20"))
CONSTITUENTS_TIMEOUT_SECONDS = float(
    os.getenv("NIFTY500_CONSTITUENTS_TIMEOUT_SECONDS", "60")
)
MAX_WORKERS = int(os.getenv("MARKET_DATA_WORKERS", "12"))
USER_AGENT = (
    "Mozilla/5.0 (compatible; NIFTY500-Supply-Demand-Scanner/1.0; "
    "+https://www.niftyindices.com/)"
)
_constituents_cache: tuple[float, list[dict[str, str]]] | None = None
_constituents_cache_lock = threading.Lock()
_constituents_cache_source = "not_loaded"


class UpstreamDataError(RuntimeError):
    """Raised when an upstream market-data request cannot provide real data."""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_timestamp(value: int | float | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()


def finite_number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def round_number(value: float | None, digits: int = 2) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def fetch_bytes(
    url: str,
    timeout: float = HTTP_TIMEOUT_SECONDS,
    attempts: int = 2,
) -> bytes:
    last_error: Exception | None = None
    for attempt in range(attempts):
        request = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "*/*"})
        try:
            with urlopen(request, timeout=timeout) as response:
                body = response.read()
            if not body:
                raise UpstreamDataError(f"Upstream returned an empty response for {url}")
            return body
        except (HTTPError, URLError, TimeoutError, UpstreamDataError) as exc:
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(0.5 * (attempt + 1))
    raise UpstreamDataError(
        f"Upstream request failed for {url}: {last_error}"
    ) from last_error


def fetch_official_constituents_csv() -> str:
    """Download only the official NSE Indices CSV with retry support.

    NSE Indices intermittently stalls with Python's urllib reader in hosted
    runtimes while responding normally to curl. The transport change does not
    introduce a data fallback: the URL is still the official NSE source and
    the response is validated before it can be cached.
    """

    timeout = str(max(10, int(CONSTITUENTS_TIMEOUT_SECONDS)))
    command = [
        "/usr/bin/curl",
        "--fail",
        "--silent",
        "--show-error",
        "--location",
        "--ipv4",
        "--http1.1",
        "--retry",
        "3",
        "--retry-delay",
        "1",
        "--connect-timeout",
        "10",
        "--max-time",
        timeout,
        "--user-agent",
        USER_AGENT,
        CONSTITUENTS_URL,
    ]
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            check=False,
            timeout=CONSTITUENTS_TIMEOUT_SECONDS + 15,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise UpstreamDataError(
            f"Official NIFTY 500 constituent download failed: {exc}"
        ) from exc
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        raise UpstreamDataError(
            "Official NIFTY 500 constituent download failed"
            + (f": {detail}" if detail else "")
        )
    return result.stdout.decode("utf-8-sig")


def parse_constituents(raw_csv: str) -> list[dict[str, str]]:
    rows = csv.DictReader(io.StringIO(raw_csv))
    constituents: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in rows:
        symbol = (row.get("Symbol") or "").strip().upper()
        company_name = (row.get("Company Name") or symbol).strip()
        # NSE's current CSV includes a non-security placeholder row for a
        # temporary corporate-action adjustment. It is not a tradable Yahoo
        # Finance symbol and must not be presented as market data.
        if symbol.startswith("DUMMY") or company_name.lower().startswith("dummy"):
            continue
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        constituents.append(
            {
                "symbol": symbol,
                "name": company_name,
                "sector": (row.get("Industry") or "Unknown").strip(),
            }
        )
    if len(constituents) < 450:
        raise UpstreamDataError("NSE Indices returned no valid NIFTY 500 constituents")
    return constituents


def load_constituents() -> list[dict[str, str]]:
    global _constituents_cache, _constituents_cache_source
    now = time.monotonic()
    with _constituents_cache_lock:
        if (
            _constituents_cache is not None
            and now - _constituents_cache[0] < CONSTITUENTS_CACHE_TTL_SECONDS
        ):
            return list(_constituents_cache[1])

    stale_disk_cache: list[dict[str, str]] | None = None
    if CONSTITUENTS_CACHE_FILE.exists():
        try:
            cache_age = time.time() - CONSTITUENTS_CACHE_FILE.stat().st_mtime
            cached = parse_constituents(
                CONSTITUENTS_CACHE_FILE.read_text(encoding="utf-8-sig")
            )
            stale_disk_cache = cached
            if 0 <= cache_age < CONSTITUENTS_CACHE_TTL_SECONDS:
                with _constituents_cache_lock:
                    _constituents_cache = (now, list(cached))
                    _constituents_cache_source = "disk_cache"
                return cached
        except (OSError, UnicodeError, UpstreamDataError):
            # Invalid cache content is never treated as market data.
            stale_disk_cache = None

    try:
        raw_csv = fetch_official_constituents_csv()
    except UpstreamDataError:
        if stale_disk_cache is None:
            raise
        with _constituents_cache_lock:
            _constituents_cache = (now, list(stale_disk_cache))
            _constituents_cache_source = "stale_disk_cache"
        return stale_disk_cache
    constituents = parse_constituents(raw_csv)
    try:
        CONSTITUENTS_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        temporary_file = CONSTITUENTS_CACHE_FILE.with_suffix(".tmp")
        temporary_file.write_text(raw_csv, encoding="utf-8")
        temporary_file.replace(CONSTITUENTS_CACHE_FILE)
    except OSError:
        # Serving a validated live response is still correct on a read-only
        # filesystem; disk caching is an optimization.
        pass
    with _constituents_cache_lock:
        _constituents_cache = (now, list(constituents))
        _constituents_cache_source = "live"
    return constituents


def fetch_history(symbol: str, lookback: int) -> list[dict[str, Any]]:
    # A trading-day lookback needs calendar-day padding for weekends and
    # exchange holidays. Two and a half years safely covers 500 sessions.
    calendar_days = max(365, int(lookback * 2.5))
    period_start = int((utc_now() - timedelta(days=calendar_days)).timestamp())
    period_end = int(utc_now().timestamp())
    yahoo_symbol = f"{symbol}.NS"
    url = (
        f"{YAHOO_CHART_URL.rstrip('/')}/{quote(yahoo_symbol, safe='')}"
        f"?period1={period_start}&period2={period_end}"
        "&interval=1d&events=history&includeAdjustedClose=true"
    )
    payload = json.loads(fetch_bytes(url).decode("utf-8"))
    chart = payload.get("chart") or {}
    error = chart.get("error")
    if error:
        raise UpstreamDataError(
            f"Yahoo Finance rejected {yahoo_symbol}: "
            f"{error.get('description') or error.get('code') or 'unknown error'}"
        )
    result = (chart.get("result") or [None])[0]
    if not result:
        raise UpstreamDataError(f"Yahoo Finance returned no history for {yahoo_symbol}")

    timestamps = result.get("timestamp") or []
    quote_block = ((result.get("indicators") or {}).get("quote") or [None])[0] or {}
    adjusted = (
        ((result.get("indicators") or {}).get("adjclose") or [None])[0] or {}
    ).get("adjclose") or []
    candles: list[dict[str, Any]] = []
    for index, timestamp in enumerate(timestamps):
        close = finite_number((quote_block.get("close") or [])[index])
        if close is None:
            continue
        open_value = finite_number((quote_block.get("open") or [])[index])
        high = finite_number((quote_block.get("high") or [])[index])
        low = finite_number((quote_block.get("low") or [])[index])
        volume = finite_number((quote_block.get("volume") or [])[index])
        adjusted_close = finite_number(adjusted[index]) if index < len(adjusted) else None
        candles.append(
            {
                "date": datetime.fromtimestamp(
                    timestamp, tz=timezone.utc
                ).date().isoformat(),
                "timestamp": iso_timestamp(timestamp),
                "open": round_number(open_value),
                "high": round_number(high),
                "low": round_number(low),
                "close": round_number(close),
                "adjustedClose": round_number(adjusted_close),
                "volume": int(volume) if volume is not None else None,
            }
        )
    if not candles:
        raise UpstreamDataError(f"Yahoo Finance returned no usable candles for {yahoo_symbol}")
    return candles[-lookback:]


def average_true_range(candles: list[dict[str, Any]], period: int = 14) -> float:
    ranges: list[float] = []
    for index, candle in enumerate(candles):
        high = candle["high"]
        low = candle["low"]
        if high is None or low is None:
            continue
        previous_close = candles[index - 1]["close"] if index else None
        if previous_close is None:
            ranges.append(high - low)
        else:
            ranges.append(max(high - low, abs(high - previous_close), abs(low - previous_close)))
    recent = ranges[-period:]
    return mean(recent) if recent else 0.0


def detect_zones(candles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Find demand/supply areas from actual pivot reactions.

    This is a screening heuristic, not a prediction. A zone is only returned
    when price moved away from a pivot by at least 1.5 recent ATRs.
    """

    if len(candles) < 30:
        return []
    atr = average_true_range(candles)
    if atr <= 0:
        return []
    candidates: list[dict[str, Any]] = []
    window = 3
    move_window = 10
    for index in range(window, len(candles) - move_window):
        current = candles[index]
        lows = [c["low"] for c in candles[index - window : index + window + 1]]
        highs = [c["high"] for c in candles[index - window : index + window + 1]]
        if any(value is None for value in lows + highs):
            continue
        future_highs = [c["high"] for c in candles[index + 1 : index + move_window + 1]]
        future_lows = [c["low"] for c in candles[index + 1 : index + move_window + 1]]
        pivot_low = current["low"]
        pivot_high = current["high"]
        if pivot_low == min(lows) and future_highs:
            move = max(future_highs) - pivot_low
            if move >= 1.5 * atr:
                zone_low = min(
                    c["low"] for c in candles[max(0, index - 2) : index + 1] if c["low"] is not None
                )
                zone_high = max(
                    c["open"] or c["close"]
                    for c in candles[max(0, index - 2) : index + 1]
                    if (c["open"] or c["close"]) is not None
                )
                candidates.append(
                    {
                        "type": "demand",
                        "low": round_number(zone_low),
                        "high": round_number(zone_high),
                        "strength": round_number(min(100, 50 + (move / atr) * 10), 1),
                        "baseDate": current["date"],
                        "reactionMovePct": round_number(move / pivot_low * 100, 2),
                    }
                )
        if pivot_high == max(highs) and future_lows:
            move = pivot_high - min(future_lows)
            if move >= 1.5 * atr:
                zone_low = min(
                    c["open"] or c["close"]
                    for c in candles[max(0, index - 2) : index + 1]
                    if (c["open"] or c["close"]) is not None
                )
                zone_high = max(
                    c["high"] for c in candles[max(0, index - 2) : index + 1] if c["high"] is not None
                )
                candidates.append(
                    {
                        "type": "supply",
                        "low": round_number(zone_low),
                        "high": round_number(zone_high),
                        "strength": round_number(min(100, 50 + (move / atr) * 10), 1),
                        "baseDate": current["date"],
                        "reactionMovePct": round_number(move / pivot_high * 100, 2),
                    }
                )

    # Prefer recent zones, then stronger reactions. Limit output so the HTML
    # scanner can render the entire universe without a massive payload.
    candidates.sort(key=lambda zone: (zone["baseDate"], zone["strength"]), reverse=True)
    selected: list[dict[str, Any]] = []
    for zone_type in ("demand", "supply"):
        selected.extend([zone for zone in candidates if zone["type"] == zone_type][:3])
    return sorted(selected, key=lambda zone: zone["baseDate"], reverse=True)


def build_symbol_record(constituent: dict[str, str], candles: list[dict[str, Any]]) -> dict[str, Any]:
    last = candles[-1]
    previous = candles[-2] if len(candles) > 1 else None
    close = last["close"]
    previous_close = previous["close"] if previous else None
    change = close - previous_close if close is not None and previous_close is not None else None
    change_pct = (change / previous_close * 100) if change is not None and previous_close else None
    volumes = [c["volume"] for c in candles[-20:] if c["volume"] is not None]
    volume_average = mean(volumes) if volumes else None
    return {
        **constituent,
        "exchange": "NSE",
        "currency": "INR",
        "last": round_number(close),
        "previousClose": round_number(previous_close),
        "change": round_number(change),
        "changePct": round_number(change_pct),
        "volume": last["volume"],
        "averageVolume20d": int(volume_average) if volume_average is not None else None,
        "asOf": last["timestamp"],
        "zones": detect_zones(candles),
        "candles": candles,
    }


def build_scanner_candidates(
    records: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    """Apply the six rules shown in the scanner UI to real candle records.

    The first four rules are direct comparisons on the latest four candles.
    The final two rules select the existing detected supply/demand zone branch;
    zone detection itself remains the backend's established candle-based
    detector and is not augmented with new filters here.
    """

    candidates: list[dict[str, Any]] = []
    candidate_symbols: set[str] = set()
    for record in records:
        candles = record.get("candles") or []
        if len(candles) < 4:
            continue
        current, previous_1, previous_2, previous_3 = candles[-1], candles[-2], candles[-3], candles[-4]
        current_close = current.get("close")
        close_values = [
            previous_1.get("close"),
            previous_2.get("close"),
            previous_3.get("close"),
        ]
        close_rules_match = (
            current_close is not None
            and all(value is not None and current_close > value for value in close_values)
        )
        high_rules_match = (
            current.get("high") is not None
            and previous_1.get("high") is not None
            and current["high"] > previous_1["high"]
        )
        if not close_rules_match or not high_rules_match:
            continue

        for zone in record.get("zones") or []:
            zone_type = zone.get("type")
            if zone_type not in ("supply", "demand"):
                continue
            pattern_rule = SCANNER_RULES[4] if zone_type == "supply" else SCANNER_RULES[5]
            candidate_symbols.add(record["symbol"])
            candidates.append(
                {
                    "symbol": record["symbol"],
                    "name": record.get("name"),
                    "last": record.get("last"),
                    "changePct": record.get("changePct"),
                    "zone": {
                        "type": zone_type,
                        "low": zone.get("low"),
                        "high": zone.get("high"),
                        "strength": zone.get("strength"),
                        "baseDate": zone.get("baseDate"),
                        "reactionMovePct": zone.get("reactionMovePct"),
                    },
                    "pattern": pattern_rule,
                    "matchedRules": [
                        SCANNER_RULES[0],
                        SCANNER_RULES[1],
                        SCANNER_RULES[2],
                        SCANNER_RULES[3],
                        pattern_rule,
                    ],
                }
            )
    candidates.sort(
        key=lambda candidate: (
            candidate["symbol"],
            candidate["zone"].get("baseDate") or "",
            candidate["zone"].get("type") or "",
        ),
        reverse=False,
    )
    return candidates, len(candidate_symbols)


class MarketDataService:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cache: dict[tuple[str, int], tuple[float, dict[str, Any]]] = {}
        self._last_error: str | None = None
        self._last_refresh: str | None = None

    def status(self) -> dict[str, Any]:
        with self._lock:
            cache_entries = len(self._cache)
            last_refresh = self._last_refresh
            last_error = self._last_error
        with _constituents_cache_lock:
            constituents_cache_source = _constituents_cache_source
        return {
            "status": "ok" if not last_error else "degraded",
            "service": SERVICE_NAME,
            "marketData": {
                "provider": "Yahoo Finance chart API",
                "constituents": "NSE Indices NIFTY 500 list",
                "universe": "NIFTY500",
                "realDataOnly": True,
                "cacheEntries": cache_entries,
                "constituentsCacheSource": constituents_cache_source,
                "constituentsCacheTtlSeconds": CONSTITUENTS_CACHE_TTL_SECONDS,
                "lastRefresh": last_refresh,
                "lastError": last_error,
            },
        }

    def get_market_data(self, universe: str, lookback: int) -> dict[str, Any]:
        cache_key = (universe, lookback)
        now = time.monotonic()
        with self._lock:
            cached = self._cache.get(cache_key)
            if cached and now - cached[0] < CACHE_TTL_SECONDS:
                response = dict(cached[1])
                response["meta"] = {**response["meta"], "cached": True}
                return response

        constituents = load_constituents()
        records: list[dict[str, Any]] = []
        failures: list[dict[str, str]] = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(fetch_history, item["symbol"], lookback): item
                for item in constituents
            }
            for future in as_completed(futures):
                constituent = futures[future]
                try:
                    candles = future.result()
                    records.append(build_symbol_record(constituent, candles))
                except (UpstreamDataError, ValueError, json.JSONDecodeError) as exc:
                    failures.append({"symbol": constituent["symbol"], "error": str(exc)})

        records.sort(key=lambda record: record["symbol"])
        if not records:
            message = (
                "No real market data was returned by Yahoo Finance. "
                "Synthetic data is disabled."
            )
            with self._lock:
                self._last_error = message
            raise UpstreamDataError(message)

        as_of_values = [record["asOf"] for record in records if record["asOf"]]
        zone_count = sum(len(record["zones"]) for record in records)
        candidates, candidate_symbol_count = build_scanner_candidates(records)
        response = {
            "ok": True,
            "meta": {
                "service": SERVICE_NAME,
                "source": "Yahoo Finance chart API",
                "constituentsSource": "NSE Indices",
                "constituentsCacheSource": _constituents_cache_source,
                "universe": universe,
                "lookback": lookback,
                "requestedSymbols": len(constituents),
                "returnedSymbols": len(records),
                "failedSymbols": len(failures),
                "latestSession": max(as_of_values) if as_of_values else None,
                "fetchedAt": utc_now().isoformat(),
                "cached": False,
                "realDataOnly": True,
                "scannerRules": SCANNER_RULES,
                "delayed": True,
                "disclaimer": (
                    "Market data is sourced from Yahoo Finance and may be delayed. "
                    "This scanner is for research and is not investment advice."
                ),
            },
            "summary": {
                "symbolsRequested": len(constituents),
                "symbolsReturned": len(records),
                "symbolsFailed": len(failures),
                "zonesDetected": zone_count,
                "candidateSymbols": candidate_symbol_count,
                "candidateMatches": len(candidates),
            },
            "data": records,
            "candidates": candidates,
            "warnings": failures[:100],
        }
        with self._lock:
            self._cache[cache_key] = (now, response)
            self._last_error = None if not failures else f"{len(failures)} symbols failed to load"
            self._last_refresh = response["meta"]["fetchedAt"]
        return response


market_data = MarketDataService()


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "NIFTY500Scanner/1.0"

    def log_message(self, format: str, *args: Any) -> None:
        # Keep workflow logs useful without leaking full query strings.
        print(f"{self.command} {self.path.split('?')[0]} - {format % args}", flush=True)

    def send_json(self, status: int, body: dict[str, Any]) -> None:
        encoded = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        try:
            self.wfile.write(encoded)
        except BrokenPipeError:
            # The frontend or a probe can time out while the full universe is
            # being fetched. Do not turn that expected disconnect into a
            # server traceback.
            return

    def send_html(self) -> None:
        try:
            encoded = STATIC_HTML_FILE.read_bytes()
        except OSError:
            self.send_json(
                HTTPStatus.NOT_FOUND,
                {
                    "ok": False,
                    "error": {
                        "code": "frontend_not_found",
                        "message": "Scanner frontend is not installed",
                    },
                },
            )
            return
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        try:
            self.wfile.write(encoded)
        except BrokenPipeError:
            return

    def do_OPTIONS(self) -> None:
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self) -> None:
        path = urlparse(self.path)
        if path.path in ("/favicon.ico", "/api/favicon.ico"):
            self.send_response(HTTPStatus.NO_CONTENT)
            self.end_headers()
            return
        if path.path in ("/", "/api", "/api/"):
            self.send_html()
            return
        if path.path in ("/api/healthz", "/healthz"):
            self.send_json(HTTPStatus.OK, {"status": "ok"})
            return
        if path.path in ("/api/live/status", "/live/status"):
            self.send_json(HTTPStatus.OK, market_data.status())
            return
        if path.path in ("/api/live/market-data", "/live/market-data"):
            self.handle_market_data(path.query)
            return
        self.send_json(
            HTTPStatus.NOT_FOUND,
            {
                "ok": False,
                "error": {
                    "code": "not_found",
                    "message": "Route not found",
                },
            },
        )

    def handle_market_data(self, query_string: str) -> None:
        query = parse_qs(query_string)
        universe = query.get("universe", ["NIFTY500"])[0].upper()
        if universe != "NIFTY500":
            self.send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "ok": False,
                    "error": {
                        "code": "unsupported_universe",
                        "message": "Only universe=NIFTY500 is currently supported",
                    },
                },
            )
            return
        raw_lookback = query.get("lookback", ["500"])[0]
        try:
            lookback = int(raw_lookback)
        except ValueError:
            self.send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "ok": False,
                    "error": {
                        "code": "invalid_lookback",
                        "message": "lookback must be an integer between 30 and 1000",
                    },
                },
            )
            return
        if not 30 <= lookback <= 1000:
            self.send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "ok": False,
                    "error": {
                        "code": "invalid_lookback",
                        "message": "lookback must be an integer between 30 and 1000",
                    },
                },
            )
            return
        try:
            response = market_data.get_market_data(universe, lookback)
            self.send_json(HTTPStatus.OK, response)
        except UpstreamDataError as exc:
            self.send_json(
                HTTPStatus.BAD_GATEWAY,
                {
                    "ok": False,
                    "error": {
                        "code": "market_data_unavailable",
                        "message": str(exc),
                        "syntheticData": False,
                    },
                },
            )
        except Exception as exc:
            self.send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {
                    "ok": False,
                    "error": {
                        "code": "market_data_error",
                        "message": str(exc),
                        "syntheticData": False,
                    },
                },
            )


def main() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", PORT), ApiHandler)
    print(
        f"{SERVICE_NAME} listening on port {PORT}; "
        "real market data only, synthetic fallback disabled",
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
