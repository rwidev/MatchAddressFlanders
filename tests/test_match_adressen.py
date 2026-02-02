import pytest
from argparse import Namespace

import match_adressen
from match_adressen import (
    parse_gml_coordinates,
    _populate_identificator_fields,
    _populate_position_fields,
    pick_best_match,
    update_row_with_match,
    _process_single_row,
)


def test_parse_gml_coordinates():
    assert parse_gml_coordinates("<gml:pos>4.1 51.2</gml:pos>") == ("4.1", "51.2")
    assert parse_gml_coordinates("nope") == ("", "")


def test_pick_best_match():
    assert pick_best_match({"adresMatches": [{"a": 1}, {"a": 2}]}) == {"a": 1}
    assert pick_best_match({}) == {}


def test_populate_identificator_fields():
    row = {}
    adres_obj = {
        "identificator": {"id": "uri", "objectId": "42", "naamruimte": "ns", "versieId": "v1"}
    }
    _populate_identificator_fields(row, adres_obj)

    assert row["adresmatch_adres_uri"] == "uri"
    assert row["adresmatch_adres_id"] == "42"
    assert row["adresmatch_identificator_namespace"] == "ns"
    assert row["adresmatch_identificator_version"] == "v1"


def test_populate_position_fields_with_gml():
    row = {}
    positie = {"positieGeometrieMethode": "method", "geometrie": {"gml": "<gml:pos>4.1 51.2</gml:pos>"}}
    _populate_position_fields(row, positie)

    assert row["adresmatch_pos_method"] == "method"
    assert row["adresmatch_pos_lon"] == "4.1"
    assert row["adresmatch_pos_lat"] == "51.2"


def test_populate_position_fields_with_coordinates():
    row = {}
    positie = {"methode": "m", "geometrie": {"coordinates": [5.1, 52.2]}}
    _populate_position_fields(row, positie)

    assert row["adresmatch_pos_method"] == "m"
    assert row["adresmatch_pos_lon"] == "5.1"
    assert row["adresmatch_pos_lat"] == "52.2"


def test_update_row_with_match_no_match():
    row = {}
    update_row_with_match(row, {})
    assert row["adresmatch_status"] == "no_match"
    assert row["adresmatch_score"] == ""


def test_process_single_row_success(monkeypatch):
    row = {"LOM_ROAD_NM": "Main", "LOM_SOURCE_HNR": "1", "LOM_POSTAL_CD": "1000"}
    args = Namespace(api_url="http://example", timeout=1, auth_token=None, delay=0, force=False)

    def fake_get(url, params, timeout, auth_token=None):
        return {"adresMatches": [{"score": 0.9, "adres": {"straatnaam": {"spelling": "Main"}, "huisnummer": "1"}}]}

    monkeypatch.setattr(match_adressen, "get_adresmatch", fake_get)

    processed = _process_single_row(row, args, match_adressen.RateLimiter(None))
    assert processed is True
    assert row["adresmatch_status"] == "matched"
    assert row["adresmatch_score"] != ""
