#!/usr/bin/env python3
"""Augment Belgium address CSV with Adressenregister adresmatch results."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import tempfile
import time
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode

import requests

DEFAULT_API_URL = "https://api.basisregisters.vlaanderen.be/v2/adresmatch"
DEFAULT_RATE_LIMIT = 25.0
NEW_COLUMNS = [
    "adresmatch_status",
    "adresmatch_score",
    "adresmatch_adres_uri",
    "adresmatch_adres_id",
    "adresmatch_identificator_namespace",
    "adresmatch_identificator_version",
    "adresmatch_gemeente",
    "adresmatch_straatnaam",
    "adresmatch_huisnummer",
    "adresmatch_busnummer",
    "adresmatch_postcode",
    "adresmatch_toevoeging",
    "adresmatch_pos_method",
    "adresmatch_pos_lon",
    "adresmatch_pos_lat",
    "adresmatch_error",
]


class AdresmatchError(RuntimeError):
    """Raised when the adresmatch API call fails."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "csv_path",
        help="Path to the input CSV that will be updated in place unless --output is set.",
    )
    parser.add_argument(
        "--api-url",
        default=DEFAULT_API_URL,
        help=f"Adresmatch endpoint to call (default: {DEFAULT_API_URL}).",
    )
    parser.add_argument(
        "--auth-token",
        help="Optional Bearer token to include in the Authorization header.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="HTTP timeout per request, in seconds (default: 20).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Extra sleep duration between calls after the rate limiter (default: 0).",
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=DEFAULT_RATE_LIMIT,
        help="Maximum number of requests per second (default: 25).",
    )
    parser.add_argument(
        "--output",
        help="Optional path for the enriched CSV. Defaults to overwriting the input file.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-query rows that already contain adresmatch_status values.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        help="Limit how many rows are processed (useful for dry-run/testing).",
    )
    return parser.parse_args()


def load_rows(csv_path: str) -> tuple[List[Dict[str, str]], List[str]]:
    with open(csv_path, newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        fieldnames = reader.fieldnames or []
    return rows, fieldnames


def ensure_field_order(fieldnames: List[str]) -> List[str]:
    result = list(fieldnames)
    for column in NEW_COLUMNS:
        if column not in result:
            result.append(column)
    return result


class RateLimiter:
    """Simple time-based limiter to keep requests under a threshold."""

    def __init__(self, max_per_second: Optional[float]):
        if max_per_second and max_per_second > 0:
            self.min_interval = 1.0 / max_per_second
        else:
            self.min_interval = 0.0
        self._last_call = 0.0

    def wait(self) -> None:
        if not self.min_interval:
            return
        now = time.perf_counter()
        elapsed = now - self._last_call
        sleep_for = self.min_interval - elapsed
        if sleep_for > 0:
            time.sleep(sleep_for)
            now = time.perf_counter()
        self._last_call = now


def parse_gml_coordinates(gml: object) -> Tuple[str, str]:
    """Return x/y values extracted from a simple GML pos string."""
    if not isinstance(gml, str):
        return "", ""
    match = re.search(r"<gml:pos>([^<]+)</gml:pos>", gml)
    if not match:
        return "", ""
    parts = match.group(1).strip().split()
    if len(parts) >= 2:
        return parts[0], parts[1]
    if parts:
        return parts[0], ""
    return "", ""


def build_query_params(row: Dict[str, str]) -> Optional[Dict[str, str]]:
    municipal = (row.get("LOM_MUN_NM") or "").strip() or None
    street = (row.get("LOM_ROAD_NM") or "").strip() or None
    housenumber = (row.get("LOM_HNR_FULL") or "").strip() or None
    bus = (row.get("LOM_BOXNR") or "").strip() or None
    postal = (row.get("LOM_POSTAL_CD") or "").strip() or None

    if not municipal and not postal:
        return None
    if not street or not housenumber:
        return None

    params: Dict[str, str] = {"straatnaam": street, "huisnummer": housenumber}
    if municipal:
        params["gemeentenaam"] = municipal
    if bus:
        params["busnummer"] = bus
    if postal:
        params["postcode"] = postal

    return params


def get_adresmatch(
    url: str,
    params: Dict[str, str],
    timeout: float,
    auth_token: Optional[str] = None,
) -> Dict[str, object]:
    headers = {"Accept": "application/json"}
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    response = requests.get(url, headers=headers, params=params, timeout=timeout)
    if response.status_code >= 400:
        raise AdresmatchError(
            f"Adresmatch request failed with HTTP {response.status_code}: {response.text[:200]}"
        )

    try:
        return response.json()
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise AdresmatchError("Invalid JSON response from adresmatch API") from exc


def extract_spelling(value: Optional[Dict[str, object]]) -> str:
    if not isinstance(value, dict):
        return ""
    geo_name = value.get("geografischeNaam")
    if isinstance(geo_name, dict):
        spelling = geo_name.get("spelling")
        if isinstance(spelling, str):
            return spelling
    spelling = value.get("spelling")
    return spelling if isinstance(spelling, str) else ""


def pick_best_match(payload: Dict[str, object]) -> Dict[str, object]:
    matches = payload.get("adresMatches")
    if isinstance(matches, list) and matches:
        return matches[0]
    return {}


def update_row_with_match(row: Dict[str, str], match: Dict[str, object]) -> None:
    score = match.get("score")
    adres_obj = match.get("adres") if isinstance(match, dict) else None
    if not isinstance(adres_obj, dict) and isinstance(match, dict):
        adres_obj = match

    if isinstance(score, (int, float)):
        row["adresmatch_score"] = f"{score:.4f}"
    else:
        row["adresmatch_score"] = ""

    if isinstance(adres_obj, dict):
        identificator = adres_obj.get("identificator")
        if isinstance(identificator, dict):
            adres_uri = identificator.get("id") or adres_obj.get("detail") or ""
            row["adresmatch_adres_uri"] = str(adres_uri)
            row["adresmatch_adres_id"] = str(
                identificator.get("objectId") or identificator.get("lokaleId") or ""
            )
            row["adresmatch_identificator_namespace"] = str(
                identificator.get("naamruimte") or identificator.get("namespace") or ""
            )
            row["adresmatch_identificator_version"] = str(
                identificator.get("versieId") or identificator.get("versie") or ""
            )
        else:
            row["adresmatch_adres_uri"] = str(adres_obj.get("detail") or "")
            row["adresmatch_adres_id"] = ""
            row["adresmatch_identificator_namespace"] = ""
            row["adresmatch_identificator_version"] = ""

        row["adresmatch_gemeente"] = extract_spelling(adres_obj.get("gemeentenaam"))
        row["adresmatch_straatnaam"] = extract_spelling(adres_obj.get("straatnaam"))
        row["adresmatch_huisnummer"] = str(adres_obj.get("huisnummer") or "")
        row["adresmatch_busnummer"] = str(adres_obj.get("busnummer") or "")
        postinfo = adres_obj.get("postinfo")
        if isinstance(postinfo, dict):
            row["adresmatch_postcode"] = str(postinfo.get("postnummer") or "")
        else:
            row["adresmatch_postcode"] = ""

        addition = adres_obj.get("toevoeging")
        row["adresmatch_toevoeging"] = str(addition or "")

        positie = adres_obj.get("adresPositie") or adres_obj.get("positie")
        if isinstance(positie, dict):
            method = positie.get("positieGeometrieMethode") or positie.get("methode")
            row["adresmatch_pos_method"] = str(method or "")
            lon = lat = ""
            geometrie = positie.get("geometrie")
            if isinstance(geometrie, dict):
                lon, lat = parse_gml_coordinates(geometrie.get("gml"))
                if (not lon or not lat) and isinstance(geometrie.get("coordinates"), (list, tuple)):
                    coords = geometrie["coordinates"]
                    if len(coords) >= 2:
                        lon = lon or str(coords[0])
                        lat = lat or str(coords[1])
            if not lon or not lat:
                punt = positie.get("punt")
                if isinstance(punt, dict):
                    lon = lon or str(punt.get("xcoordinaat") or "")
                    lat = lat or str(punt.get("ycoordinaat") or "")
            row["adresmatch_pos_lon"] = lon
            row["adresmatch_pos_lat"] = lat
        else:
            row["adresmatch_pos_method"] = ""
            row["adresmatch_pos_lon"] = ""
            row["adresmatch_pos_lat"] = ""
    else:
        row["adresmatch_adres_uri"] = ""
        row["adresmatch_adres_id"] = ""
        row["adresmatch_identificator_namespace"] = ""
        row["adresmatch_identificator_version"] = ""
        row["adresmatch_gemeente"] = ""
        row["adresmatch_straatnaam"] = ""
        row["adresmatch_huisnummer"] = ""
        row["adresmatch_busnummer"] = ""
        row["adresmatch_postcode"] = ""
        row["adresmatch_toevoeging"] = ""
        row["adresmatch_pos_method"] = ""
        row["adresmatch_pos_lon"] = ""
        row["adresmatch_pos_lat"] = ""

    row["adresmatch_error"] = ""
    row["adresmatch_status"] = "matched" if match else "no_match"


def write_rows(path: str, rows: Iterable[Dict[str, str]], fieldnames: List[str]) -> None:
    target_dir = os.path.dirname(os.path.abspath(path))
    os.makedirs(target_dir, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        delete=False,
        newline="",
        encoding="utf-8-sig",
        dir=target_dir,
    ) as tmp:
        writer = csv.DictWriter(tmp, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        temp_name = tmp.name
    os.replace(temp_name, path)


def should_skip_row(row: Dict[str, str], force: bool) -> bool:
    if force:
        return False
    status = row.get("adresmatch_status")
    return bool(status)


def process_rows(
    rows: List[Dict[str, str]],
    args: argparse.Namespace,
) -> None:
    processed = 0
    limiter = RateLimiter(args.rate_limit)
    for idx, row in enumerate(rows, start=1):
        if args.max_rows is not None and processed >= args.max_rows:
            break

        if should_skip_row(row, args.force):
            continue

        params = build_query_params(row)
        if params is None:
            row["adresmatch_status"] = "missing_input"
            row["adresmatch_error"] = "Missing municipality/postcode, street, or house number"
            continue

        try:
            # print(
            #     "[adresmatch] GET",
            #     f"{args.api_url}?{urlencode(params)}",
            # )
            limiter.wait()
            response_json = get_adresmatch(
                url=args.api_url,
                params=params,
                timeout=args.timeout,
                auth_token=args.auth_token,
            )
            # print(
            #     "[adresmatch] RESPONSE",
            #     json.dumps(response_json, ensure_ascii=False, separators=(",", ":")),
            # )
        except Exception as exc:  # pragma: no cover - strictly runtime handling
            row["adresmatch_status"] = "error"
            row["adresmatch_error"] = str(exc)
            continue

        match = pick_best_match(response_json)
        update_row_with_match(row, match)

        processed += 1
        if args.delay:
            time.sleep(args.delay)


def main() -> None:
    args = parse_args()
    rows, fieldnames = load_rows(args.csv_path)
    ordered_fields = ensure_field_order(fieldnames)
    process_rows(rows, args)
    output_path = args.output or args.csv_path
    write_rows(output_path, rows, ordered_fields)
    print(f"Processed {len(rows)} rows. Updated file saved to {output_path}.")


if __name__ == "__main__":
    try:
        main()
    except AdresmatchError as exc:
        print(f"Adresmatch failed: {exc}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        sys.exit(130)
