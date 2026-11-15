"""Required extraction invariants, including a byte-preserved NOAA GRIB message."""

import csv
import hashlib
import inspect
import json
import subprocess
import tomllib
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from kwb.parse.ndfd import (
    MaxTPeriod,
    NoQualifyingIssuance,
    ObjectInfo,
    Station,
    climate_day,
    containing_cell,
    native_fahrenheit,
    period_for_day,
    select_forecast,
    select_objects,
)
from kwb.parse.ndfd_decode import decode_grib, extract_points, grid_spec

FIXTURE = Path(__file__).parent / "fixtures/ndfd_points/maxt.grib2"
PROVENANCE = json.loads(FIXTURE.with_name("provenance.json").read_text())
OBJ = ObjectInfo(PROVENANCE["key"], FIXTURE.stat().st_size, "fixture-subset")
LAG = timedelta(hours=2)


@pytest.fixture
def stations():
    with Path("config/station_timezones.csv").open() as stream:
        zones = {r["station"]: r["timezone"] for r in csv.DictReader(stream)}
    with Path("config/nws_stations.csv").open() as stream:
        rows = list(csv.DictReader(stream))
    assert set(zones) == {r["station_id"] for r in rows}
    return tuple(
        sorted(
            (
                Station(
                    r["station_id"],
                    float(r["latitude"]),
                    float(r["longitude"]),
                    zones[r["station_id"]],
                )
                for r in rows
            ),
            key=lambda s: s.station,
        )
    )


def obj(name):
    return ObjectInfo("wmo/maxt/2026/11/06/" + name + "_KWBN_202611060016", 1, '"x"')


def test_only_operational_conus_maxt_objects_are_selected_and_every_exclusion_is_counted():
    selection = select_objects(
        [
            obj(p)
            for p in [
                "YGUZ98",
                "YGUZ97",
                "YGUZ87",
                "YGUZ88",
                "YGAZ98",
                "YGRZ97",
                "YGSZ98",
                "YGTZ97",
            ]
        ]
    )
    assert [r.key.split("/")[-1][:6] for r in selection.selected] == ["YGUZ98", "YGUZ97"]
    assert selection.excluded == {"legacy_conus": 2, "other_domain": 4}
    assert len(selection.selected) + sum(selection.excluded.values()) == 8
    for name in ("YGUZ99", "YGUZ96", "YGUZ98_BAD"):
        with pytest.raises(ValueError, match="Unknown"):
            select_objects([obj(name)])
    with pytest.raises(ValueError, match="Duplicate"):
        select_objects([obj("YGUZ98"), obj("YGUZ98")])


@pytest.mark.parametrize("station_id", ["KAUS", "KDEN", "KLAX", "KMDW", "KMIA", "KNYC", "KPHL"])
def test_every_configured_station_is_inside_its_selected_grib_cell_and_unsupported_scanning_raises(
    stations, station_id
):
    import grib2io
    from pyproj import Proj

    station = next(s for s in stations if s.station == station_id)
    with grib2io.open(str(FIXTURE), save_index=False, use_index=False) as messages:
        grid = grid_spec(messages[0])
    cell = containing_cell(grid, station.latitude, station.longitude)
    p = Proj(grid.projection)
    x, y = p(station.longitude, station.latitude)
    cx, cy = p(cell.centre_longitude, cell.centre_latitude)
    assert abs(x - cx) <= grid.dx / 2
    assert abs(y - cy) <= grid.dy / 2
    # Independent known cells catch decoder-orientation and projection regressions.
    expected = {
        "KAUS": (332, 986),
        "KDEN": (774, 752),
        "KLAX": (571, 232),
        "KMDW": (857, 1336),
        "KMIA": (170, 1666),
        "KNYC": (860, 1811),
        "KPHL": (813, 1774),
    }
    assert (cell.row, cell.column) == expected[station_id]
    for i in range(8):
        flags = list(grid.scan_flags)
        flags[i] = 1 - flags[i]
        with pytest.raises(ValueError, match="scanning"):
            containing_cell(
                replace(grid, scan_flags=tuple(flags)), station.latitude, station.longitude
            )
    with pytest.raises(ValueError, match="outside"):
        containing_cell(grid, 0, 0)


@pytest.mark.parametrize("day", [date(2026, 3, 8), date(2026, 11, 1), date(2026, 7, 1)])
def test_each_climate_day_has_exactly_one_twelve_hour_period_and_standard_time_never_shifts(
    stations, day
):
    offsets = {"KAUS": -6, "KMDW": -6, "KDEN": -7, "KLAX": -8, "KMIA": -5, "KNYC": -5, "KPHL": -5}
    period = MaxTPeriod(
        datetime.combine(day, datetime.min.time(), tzinfo=UTC) + timedelta(hours=12),
        datetime.combine(day, datetime.min.time(), tzinfo=UTC) + timedelta(days=1),
    )
    for station in stations:
        assert station.standard_offset(day.year) == timedelta(hours=offsets[station.station])
        start, end = climate_day(station, day)
        assert start.hour == -offsets[station.station]
        assert end - start == timedelta(days=1)
        assert period_for_day([period], station, day) == period
        for periods in ([], [period, period]):
            with pytest.raises(ValueError, match="exactly one"):
                period_for_day(periods, station, day)
        crossing = MaxTPeriod(start - timedelta(seconds=1), start + timedelta(hours=12, seconds=-1))
        with pytest.raises(ValueError):
            period_for_day([crossing], station, day)
    with pytest.raises(ValueError, match="twelve"):
        MaxTPeriod(period.start, period.end + timedelta(hours=1))


def test_forecasts_use_the_latest_eligible_issuance_covering_the_day_and_require_a_lag(
    stations,
):
    rows = list(decode_grib(FIXTURE, OBJ, stations, lag=LAG))
    row = rows[0]
    day = row.period.start.date()
    old_obj = ObjectInfo(OBJ.key.replace("202611060016", "202611052316"), OBJ.size, OBJ.etag)
    old = replace(row, obj=old_obj, available_at=old_obj.issued_at + LAG)
    as_of = row.available_at
    assert select_forecast([old, row], row.station.station, day, as_of, lag=LAG) == row
    assert (
        select_forecast(
            [old, row], row.station.station, day, as_of - timedelta(microseconds=1), lag=LAG
        )
        == old
    )
    tomorrow = replace(
        row,
        period=MaxTPeriod(row.period.start + timedelta(days=1), row.period.end + timedelta(days=1)),
    )
    assert select_forecast([old, tomorrow], row.station.station, day, as_of, lag=LAG) == old
    with pytest.raises(NoQualifyingIssuance):
        select_forecast([row], row.station.station, day, as_of - timedelta(seconds=1), lag=LAG)
    with pytest.raises(ValueError, match="duplicate"):
        select_forecast([row, row], row.station.station, day, as_of, lag=LAG)
    for function in (decode_grib, extract_points, select_forecast):
        assert inspect.signature(function).parameters["lag"].default is inspect.Parameter.empty
    with pytest.raises(ValueError, match="UTC"):
        select_forecast([row], row.station.station, day, as_of.replace(tzinfo=None), lag=LAG)


def test_point_values_preserve_kelvin_and_reject_missing_or_ambiguous_native_fahrenheit(stations):
    import grib2io
    import numpy as np

    rows = list(decode_grib(FIXTURE, OBJ, stations, lag=LAG))
    with grib2io.open(str(FIXTURE), save_index=False, use_index=False) as messages:
        message = messages[0]
        field = message.data
        for row in rows:
            assert row.kelvin == float(field[row.cell.row, row.cell.column])
            assert row.fahrenheit == (row.kelvin - 273.15) * 1.8 + 32
            assert row.native_fahrenheit == native_fahrenheit(row.kelvin, row.packing_quantum_k)
        station = stations[0]
        cell = rows[0].cell
        field[cell.row, cell.column] = np.nan
        with pytest.raises(ValueError, match="Missing"):
            list(extract_points(message, OBJ, [station], lag=LAG))
    for value, quantum, masked in [
        (float("nan"), 0.1, False),
        (300, 0.1, True),
        (300, 5, False),
        (9999, 0.1, False),
        (295.15, 0.001, False),
    ]:
        with pytest.raises(ValueError):
            native_fahrenheit(value, quantum, masked=masked)
    # Exercise the real extraction boundary with a bitmap-masked finite value.
    with grib2io.open(str(FIXTURE), save_index=False, use_index=False) as messages:
        message = messages[0]
        _ = message.data
        message.bitmap = np.ones((message.ny, message.nx), dtype=np.int32)
        message.bitmap[rows[0].cell.row, rows[0].cell.column] = 0
        with pytest.raises(ValueError, match="Missing"):
            list(extract_points(message, OBJ, [stations[0]], lag=LAG))


def test_the_real_grib_fixture_decodes_to_the_recorded_station_value_sha256(stations):
    assert FIXTURE.stat().st_size == PROVENANCE["size"]
    assert hashlib.sha256(FIXTURE.read_bytes()).hexdigest() == PROVENANCE["sha256"]
    rows = list(decode_grib(FIXTURE, OBJ, stations, lag=LAG))
    values = [
        [r.station.station, r.cell.row, r.cell.column, r.kelvin.hex(), r.native_fahrenheit]
        for r in rows
    ]
    digest = hashlib.sha256(json.dumps(values, separators=(",", ":")).encode("ascii")).hexdigest()
    assert digest == PROVENANCE["station_values_sha256"]


def test_default_install_excludes_grib2io_and_notebooks_include_the_ndfd_group():
    exported = subprocess.check_output(
        ["uv", "export", "--locked", "--no-default-groups", "--no-hashes"], text=True
    )
    assert "grib2io==" not in exported
    default = subprocess.check_output(["uv", "export", "--locked", "--no-hashes"], text=True)
    assert "grib2io==" not in default
    settings = tomllib.loads(Path("pyproject.toml").read_text())
    assert {"include-group": "ndfd"} in settings["dependency-groups"]["notebooks"]
    assert "ndfd" not in settings.get("tool", {}).get("uv", {}).get("default-groups", ["dev"])


def test_importing_ndfd_boundaries_does_not_load_the_native_decoder():
    subprocess.run(
        [
            "uv",
            "run",
            "--locked",
            "--group",
            "ndfd",
            "python",
            "-c",
            "import sys; import kwb.ops.ndfd_point_check; "
            "assert 'grib2io' not in sys.modules; assert 'numpy' not in sys.modules",
        ],
        check=True,
    )


def test_the_decoder_links_to_the_stable_machine_library_outside_every_checkout():
    import grib2io

    library = Path(grib2io.__config__.g2c_library).resolve()
    assert library.parent == Path.home() / ".local/share/kwb/g2c-2.3.0/lib"
    assert library.is_file()
    assert not grib2io.__config__.has_interpolation
