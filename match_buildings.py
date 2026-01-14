#!/usr/bin/env python3
"""Enrich adres CSV rows with gebouwenregister footprints via the REST API."""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import sys
import tempfile
import time
from typing import Dict, Iterable, List, Optional

import requests

DEFAULT_GEBOUWEN_URL = "https://api.basisregisters.vlaanderen.be/v2/gebouwen"
DEFAULT_GEBOUWEENHEDEN_URL = "https://api.basisregisters.vlaanderen.be/v2/gebouweenheden"
DEFAULT_ADRES_ID_FIELD = "adresmatch_adres_id"
DEFAULT_RATE_LIMIT = 5.0
NEW_COLUMNS = [
    "gebouwregister_status",
    "gebouwregister_id",
    "gebouwregister_wkt",
    "gebouwregister_error",
]
STATUS_PRIORITY = {
    "gerealiseerd": 0,
    "inGebruik": 1,
    "inAanbouw": 2,
    "ontwerp": 3,
    "inAanvraag": 4,
    "gehistoreerd": 9,
}


class BuildingLookupError(RuntimeError):
    """Raised when the gebouwenregister API call fails."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path", help="Input CSV enriched with adresmatch results.")
    parser.add_argument(
        "--output",
        help="Path for the CSV with gebouwregister matches. Defaults to <input>_gebouwen.csv",
    )
    parser.add_argument(
        "--gebouwen-url",
        default=DEFAULT_GEBOUWEN_URL,
        help="Base URL for the gebouwenregister endpoint (default: %(default)s).",
    )
    parser.add_argument(
        "--gebouweenheden-url",
        default=DEFAULT_GEBOUWEENHEDEN_URL,
        help="Base URL for the gebouweenheden endpoint (default: %(default)s).",
    )
    parser.add_argument(
        "--adres-id-field",
        default=DEFAULT_ADRES_ID_FIELD,
        help="Column containing the adres ID from adresmatch (default: %(default)s).",
    )
    parser.add_argument(
        "--building-limit",
        type=int,
        default=5,
        help="Maximum number of gebouweenheden to request per adres (default: %(default)s).",
    )
    parser.add_argument(
        "--include-historic",
        action="store_true",
        help="Allow gehistoreerde gebouwen if no active ones are found.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="HTTP timeout per API call in seconds (default: %(default)s).",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Number of retries for failed API calls (default: %(default)s).",
    )
    parser.add_argument(
        "--retry-wait",
        type=float,
        default=1.0,
        help="Seconds to wait between retries (default: %(default)s).",
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=DEFAULT_RATE_LIMIT,
        help="Max requests per second (default: %(default)s).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Extra sleep in seconds after each processed row (default: %(default)s).",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        help="Limit how many rows are processed (useful for dry-runs).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-query rows that already contain gebouwregister_status values.",
    )
    parser.add_argument(
        "--auth",
        help="Optional Authorization header value (e.g. 'Bearer <token>').",
    )
    return parser.parse_args()


def load_rows(csv_path: str) -> tuple[List[Dict[str, str]], List[str]]:
    with open(csv_path, newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        header = reader.fieldnames or []
    return rows, header


def ensure_columns(fieldnames: List[str]) -> List[str]:
    names = list(fieldnames)
    for column in NEW_COLUMNS:
        if column not in names:
            names.append(column)
    return names


class RateLimiter:
    def __init__(self, max_per_second: Optional[float]):
        self.min_interval = 1.0 / max_per_second if max_per_second and max_per_second > 0 else 0.0
        self._last_call = 0.0

    def wait(self) -> None:
        if not self.min_interval:
            return
        now = time.perf_counter()
        sleep_for = self.min_interval - (now - self._last_call)
        if sleep_for > 0:
            time.sleep(sleep_for)
            now = time.perf_counter()
        self._last_call = now


def format_coord(value: float) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    return ("%.6f" % value).rstrip("0").rstrip(".")


def ring_to_wkt(ring: List[List[float]]) -> str:
    parts = []
    for coord in ring:
        if not isinstance(coord, (list, tuple)) or len(coord) < 2:
            continue
        try:
            x = float(coord[0])
            y = float(coord[1])
        except (TypeError, ValueError):
            continue
        parts.append(f"{format_coord(x)} {format_coord(y)}")
    return ", ".join(parts)


def polygon_to_wkt(coords: List[List[List[float]]]) -> str:
    rings = []
    for ring in coords:
        seq = ring_to_wkt(ring)
        if seq:
            rings.append(f"({seq})")
    joined = ", ".join(rings) if rings else ""
    return f"({joined})" if joined else ""


gml_polygon_re = re.compile(r"<gml:Polygon[^>]*>(.*?)</gml:Polygon>", re.IGNORECASE | re.DOTALL)
gml_poslist_re = re.compile(r"<gml:posList>([^<]+)</gml:posList>", re.IGNORECASE | re.DOTALL)


def parse_gml_pos_list(text: str) -> List[List[float]]:
    items = text.strip().split()
    coords: List[List[float]] = []
    for idx in range(0, len(items) - 1, 2):
        try:
            coords.append([float(items[idx]), float(items[idx + 1])])
        except ValueError:
            continue
    return coords


def gml_to_wkt(gml: str) -> str:
    polygons = []
    polygon_matches = gml_polygon_re.findall(gml)
    if not polygon_matches:
        polygon_matches = [gml]
    for block in polygon_matches:
        rings = []
        for pos_list in gml_poslist_re.findall(block):
            ring = parse_gml_pos_list(pos_list)
            if ring:
                rings.append(ring)
        if rings:
            polygons.append(rings)
    if not polygons:
        return gml
    if len(polygons) == 1:
        body = polygon_to_wkt(polygons[0])
        return f"POLYGON {body}" if body else ""
    bodies: List[str] = []
    for poly in polygons:
        body = polygon_to_wkt(poly)
        if body:
            bodies.append(body)
    joined = ", ".join(bodies)
    return f"MULTIPOLYGON ({joined})" if joined else ""


def geometry_to_wkt(geometry: Optional[Dict[str, object]]) -> str:
    if not isinstance(geometry, dict):
        return ""
    geom_type = geometry.get("type")
    coords = geometry.get("coordinates")

    if geom_type == "Polygon" and isinstance(coords, list):
        body = polygon_to_wkt(coords)
        return f"POLYGON {body}" if body else ""
    if geom_type == "MultiPolygon" and isinstance(coords, list):
        bodies = []
        for poly in coords:
            body = polygon_to_wkt(poly)
            if body:
                bodies.append(body)
        joined = ", ".join(bodies)
        return f"MULTIPOLYGON ({joined})" if joined else ""
    if geom_type == "Point" and isinstance(coords, (list, tuple)) and len(coords) >= 2:
        try:
            x = float(coords[0])
            y = float(coords[1])
        except (TypeError, ValueError):
            return ""
        return f"POINT ({format_coord(x)} {format_coord(y)})"

    gml = geometry.get("gml")
    if isinstance(gml, str):
        return gml_to_wkt(gml)

    return json.dumps(geometry, ensure_ascii=False)


def http_get_json(
    url: str,
    args: argparse.Namespace,
    *,
    params: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    headers = {"Accept": "application/json"}
    if args.auth:
        headers["Authorization"] = args.auth

    attempts = max(1, (args.retries or 0) + 1)
    last_error: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(url, params=params, headers=headers, timeout=args.timeout)
        except requests.RequestException as exc:
            last_error = exc
            if attempt == attempts:
                raise BuildingLookupError(f"Request to {url} failed: {exc}") from exc
            time.sleep(args.retry_wait)
            continue

        if response.status_code >= 500 and attempt < attempts:
            time.sleep(args.retry_wait)
            continue
        if response.status_code >= 400:
            snippet = response.text[:1000]
            raise BuildingLookupError(
                f"Request to {url} failed with HTTP {response.status_code}: {snippet}"
            )
        try:
            return response.json()
        except json.JSONDecodeError as exc:
            raise BuildingLookupError("Invalid JSON returned by gebouwenregister API") from exc

    if last_error:
        raise BuildingLookupError(f"Request failed after retries: {last_error}") from last_error
    raise BuildingLookupError("Request failed for an unknown reason")


def fetch_units_for_address(adres_id: str, args: argparse.Namespace) -> List[Dict[str, object]]:
    params = {"adresobjectId": adres_id, "limit": max(1, args.building_limit)}
    data = http_get_json(args.gebouweenheden_url, args, params=params)
    units = data.get("gebouweenheden")
    return units if isinstance(units, list) else []


def select_unit(units: List[Dict[str, object]], include_historic: bool) -> Optional[Dict[str, object]]:
    if not units:
        return None
    if not include_historic:
        for unit in units:
            status_value = str(
                unit.get("gebouweenheidStatus") or unit.get("status") or ""
            ).lower()
            if status_value not in {"gehistoreerd", "afgeschaft"}:
                return unit
    return units[0]


def fetch_unit_detail(unit: Dict[str, object], args: argparse.Namespace) -> Dict[str, object]:
    detail_url = unit.get("detail")
    if not detail_url:
        ident = unit.get("identificator")
        object_id = None
        if isinstance(ident, dict):
            object_id = ident.get("objectId") or ident.get("objectid")
        if object_id:
            detail_url = f"{args.gebouweenheden_url.rstrip('/')}/{object_id}"
    if not detail_url:
        raise BuildingLookupError("Gebouweenheid detail URL missing")
    return http_get_json(detail_url, args)


def extract_building_id(unit_detail: Dict[str, object]) -> str:
    direct = unit_detail.get("gebouwId") or unit_detail.get("gebouwid")
    if direct:
        return str(direct)
    gebouw = unit_detail.get("gebouw")
    if isinstance(gebouw, dict):
        ident = gebouw.get("identificator")
        if isinstance(ident, dict):
            return str(ident.get("objectId") or ident.get("objectid") or "")
        gid = (
            gebouw.get("id")
            or gebouw.get("objectId")
            or gebouw.get("objectid")
        )
        if gid:
            return str(gid)
    relatie = unit_detail.get("relatie")
    if isinstance(relatie, dict):
        gid = relatie.get("gebouwId") or relatie.get("gebouwid")
        if gid:
            return str(gid)
    return ""


def fetch_building_detail_by_id(gebouw_id: str, args: argparse.Namespace) -> Dict[str, object]:
    if not gebouw_id:
        raise BuildingLookupError("Gebouw ID ontbreekt")
    url = f"{args.gebouwen_url.rstrip('/')}/{gebouw_id}"
    return http_get_json(url, args)


def extract_geometry_wkt(detail: Dict[str, object]) -> str:
    polygon = detail.get("gebouwPolygoon")
    if isinstance(polygon, dict):
        wkt = geometry_to_wkt(polygon.get("geometrie"))
        if wkt:
            return wkt
    line = detail.get("gebouwLijn")
    if isinstance(line, dict):
        wkt = geometry_to_wkt(line.get("geometrie"))
        if wkt:
            return wkt
    point = detail.get("gebouwPunt")
    if isinstance(point, dict):
        wkt = geometry_to_wkt(point.get("geometrie"))
        if wkt:
            return wkt
    return ""


def should_skip(row: Dict[str, str], force: bool) -> bool:
    if force:
        return False
    status = row.get("gebouwregister_status")
    return bool(status)


def write_rows(path: str, rows: Iterable[Dict[str, str]], fieldnames: List[str]) -> None:
    target_dir = os.path.dirname(os.path.abspath(path))
    os.makedirs(target_dir, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", delete=False, newline="", encoding="utf-8-sig", dir=target_dir
    ) as tmp:
        writer = csv.DictWriter(tmp, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        temp_name = tmp.name
    os.replace(temp_name, path)


def process_rows(rows: List[Dict[str, str]], args: argparse.Namespace) -> None:
    limiter = RateLimiter(args.rate_limit)
    processed = 0
    for row in rows:
        if should_skip(row, args.force):
            continue

        adres_id = (row.get(args.adres_id_field) or "").strip()
        if not adres_id:
            row["gebouwregister_status"] = "missing_adres_id"
            row["gebouwregister_error"] = f"Missing {args.adres_id_field}"
            row["gebouwregister_id"] = ""
            row["gebouwregister_wkt"] = ""
            continue

        try:
            limiter.wait()
            units = fetch_units_for_address(adres_id, args)
        except Exception as exc:
            row["gebouwregister_status"] = "error"
            row["gebouwregister_error"] = str(exc)
            row["gebouwregister_id"] = ""
            row["gebouwregister_wkt"] = ""
            continue

        unit_candidate = select_unit(units, args.include_historic)
        if not unit_candidate:
            row["gebouwregister_status"] = "no_match"
            row["gebouwregister_error"] = "Geen gebouweenheid gevonden voor adres"
            row["gebouwregister_id"] = ""
            row["gebouwregister_wkt"] = ""
            continue

        try:
            limiter.wait()
            unit_detail = fetch_unit_detail(unit_candidate, args)
        except Exception as exc:
            row["gebouwregister_status"] = "error"
            row["gebouwregister_error"] = str(exc)
            row["gebouwregister_id"] = ""
            row["gebouwregister_wkt"] = ""
            continue

        gebouw_id = extract_building_id(unit_detail)
        if not gebouw_id:
            row["gebouwregister_status"] = "error"
            row["gebouwregister_error"] = "Geen gebouwId gevonden voor deze gebouweenheid"
            row["gebouwregister_id"] = ""
            row["gebouwregister_wkt"] = ""
            continue

        try:
            limiter.wait()
            gebouw_detail = fetch_building_detail_by_id(gebouw_id, args)
        except Exception as exc:
            row["gebouwregister_status"] = "error"
            row["gebouwregister_error"] = str(exc)
            row["gebouwregister_id"] = ""
            row["gebouwregister_wkt"] = ""
            continue

        wkt = extract_geometry_wkt(gebouw_detail)
        row["gebouwregister_status"] = "matched" if wkt else "matched_no_geometry"
        row["gebouwregister_error"] = "" if wkt else "Gebouw gevonden maar geen geometrie beschikbaar"
        row["gebouwregister_id"] = gebouw_id
        row["gebouwregister_wkt"] = wkt

        processed += 1
        if args.max_rows is not None and processed >= args.max_rows:
            break

        if args.delay:
            time.sleep(args.delay)


def main() -> None:
    args = parse_args()
    output_path = args.output
    if not output_path:
        base, ext = os.path.splitext(args.csv_path)
        output_path = f"{base}_gebouwen{ext or '.csv'}"

    rows, header = load_rows(args.csv_path)
    fieldnames = ensure_columns(header)
    process_rows(rows, args)
    write_rows(output_path, rows, fieldnames)
    print(f"Processed {len(rows)} rows. Output written to {output_path}.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        sys.exit(130)
    except BuildingLookupError as exc:
        print(f"Gebouwenregister error: {exc}", file=sys.stderr)
        sys.exit(1)
