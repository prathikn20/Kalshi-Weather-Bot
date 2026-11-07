import csv
import gzip
import io
import json
import runpy
import tarfile
from pathlib import Path

import pytest
import zstandard

DERIVE = runpy.run_path(str(Path(__file__).parents[1] / "scripts/derive_nws_stations.py"))
FIXTURES = Path(__file__).parent / "fixtures/nws_derivation"


def envelopes():
    return list(DERIVE["archived_envelopes"](FIXTURES))


def write_envelopes(root, records):
    root.mkdir(parents=True, exist_ok=True)
    for i, (_, record) in enumerate(records):
        (root / f"{i}.json.gz").write_bytes(gzip.compress(json.dumps(record).encode()))


def test_derivation_reports_metadata_discovery_and_archived_market_coverage_separately():
    report = DERIVE["derive"](FIXTURES)
    (snapshot,) = report["series_snapshots"]
    assert snapshot["metadata_series_count"] == 11
    assert snapshot["collector_matched_count"] == 8
    assert snapshot["unmatched_high_temperature_candidates"] == ["KXHIGHTATL", "KXHIGHTEGLL"]
    assert report["collector_matched_series_count"] == 8
    assert report["archived_market_series_count"] == 7
    assert report["matched_without_archived_markets"] == ["KXHIGHHOU"]
    assert report["archived_market_series_not_matched"] == []
    assert report["unresolved_series"] == {}
    assert report["skipped_non_us_series"] == {}
    assert {site["cli_code"] for site in report["sites"]} == {
        "CLIAUS",
        "CLIMDW",
        "CLIDEN",
        "CLILAX",
        "CLIMIA",
        "CLINYC",
        "CLIPHL",
    }


def test_settlement_sources_are_copied_verbatim_from_archived_rules_not_cli_links(tmp_path):
    records = envelopes()
    expected = {}
    for _, envelope in records:
        payload = json.loads(envelope["body"])
        if "markets" in payload:
            (market,) = payload["markets"]
            market["rules_primary"] = market["rules_primary"].replace(
                "The Weather Company",
                "The Weather Company, Inc.",
            )
            expected[market["ticker"]] = market["rules_primary"]
            envelope["body"] = json.dumps(payload)
    write_envelopes(tmp_path, records)
    for site in DERIVE["derive"](tmp_path)["sites"]:
        assert site["settlement_source"] == "The Weather Company, Inc."
        assert f"according to {site['settlement_source']}, then" in expected[site["market_ticker"]]


def test_the_checked_in_config_matches_sites_and_verbatim_sources_in_real_rules():
    report = DERIVE["derive"](FIXTURES)
    with Path("config/nws_stations.csv").open(newline="") as source:
        rows = list(csv.DictReader(source))
    assert len(rows) == len(report["sites"]) == 7
    for site in report["sites"]:
        row = next(row for row in rows if row["series_ticker"] == site["series_ticker"])
        assert row["cli_code"] == site["cli_code"]
        assert row["settlement_source"] == site["settlement_source"]
        assert row["market_ticker"] == site["market_ticker"]
        assert Path(row["archive_path"]).name == Path(site["archive_path"]).name


def test_derivation_reads_compacted_and_gzipped_history_equivalently(tmp_path):
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        for i, (_, envelope) in enumerate(envelopes()):
            body = json.dumps(envelope).encode()
            info = tarfile.TarInfo(f"{i}.json")
            info.size = len(body)
            archive.addfile(info, io.BytesIO(body))
    (tmp_path / "2026-10-07T14.tar.zst").write_bytes(
        zstandard.ZstdCompressor().compress(buffer.getvalue())
    )
    compressed, originals = DERIVE["derive"](tmp_path), DERIVE["derive"](FIXTURES)
    for report in (compressed, originals):
        for site in report["sites"]:
            del site["archive_path"]
        for snapshot in report["series_snapshots"]:
            del snapshot["archive_path"]
    assert compressed == originals


def test_unverified_sites_are_reported_and_cannot_silently_enter_the_config(tmp_path):
    records = envelopes()
    for _, envelope in records:
        envelope["body"] = envelope["body"].replace("CLIAUS", "CLIZZZ")
    write_envelopes(tmp_path, records)
    report = DERIVE["derive"](tmp_path)
    assert report["unresolved_series"]["KXHIGHAUS"]["cli_code"] == "CLIZZZ"
    with pytest.raises(ValueError, match="Unverified sites"):
        DERIVE["write_config"](report, tmp_path / "config.csv", tmp_path / "nws")
    assert not (tmp_path / "config.csv").exists()


def test_explicitly_verified_non_us_sites_are_reported_and_excluded(tmp_path, monkeypatch):
    records = envelopes()
    for _, envelope in records:
        payload = json.loads(envelope["body"])
        if "markets" in payload:
            (market,) = payload["markets"]
            market["ticker"] = "KXHIGHTEGLL-26OCT08-T80"
            market["rules_primary"] = "Highest temperature at London Heathrow."
            envelope["body"] = json.dumps(payload)
            break
    monkeypatch.setitem(
        DERIVE["VERIFIED_NON_US_SERIES"],
        "KXHIGHTEGLL",
        "London Heathrow, UK; outside NWS coverage",
    )
    write_envelopes(tmp_path, records)
    report = DERIVE["derive"](tmp_path)
    assert report["skipped_non_us_series"] == {
        "KXHIGHTEGLL": "London Heathrow, UK; outside NWS coverage",
    }
    assert len(report["sites"]) == 6
    assert not report["unresolved_series"]


def test_conflicting_sites_or_settlement_sources_raise_instead_of_choosing_one(tmp_path):
    records = envelopes()
    _, envelope = next((path, env) for path, env in records if "/markets?" in env["request_url"])
    duplicate = dict(envelope)
    duplicate["body"] = envelope["body"].replace("The Weather Company", "Another Provider")
    records.append(("conflict", duplicate))
    write_envelopes(tmp_path, records)
    with pytest.raises(ValueError, match="Conflicting settlement sites or sources"):
        DERIVE["derive"](tmp_path)
