"""grib2io I/O boundary. Decode one field once for all configured stations."""

from collections.abc import Iterator, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

import grib2io
import numpy as np

from kwb.parse.ndfd import (
    GridSpec,
    MaxTPeriod,
    ObjectInfo,
    PointForecast,
    Station,
    containing_cell,
    native_fahrenheit,
    validate_lag,
)


def grid_spec(message) -> GridSpec:
    return GridSpec(
        dict(message.projParameters),
        message.latitudeFirstGridpoint,
        message.longitudeFirstGridpoint,
        message.gridlengthXDirection,
        message.gridlengthYDirection,
        int(message.nx),
        int(message.ny),
        tuple(message.scanModeFlags),
        int(message.gdtn),
    )


def message_period(message) -> MaxTPeriod:
    if (
        message.shortName != "TMAX"
        or message.units != "K"
        or int(message.pdtn) != 8
        or int(message.statisticalProcess) != 2
    ):
        raise ValueError("Expected Kelvin MaxT PDT 4.8 maximum")
    end = datetime(
        message.yearOfEndOfTimePeriod,
        message.monthOfEndOfTimePeriod,
        message.dayOfEndOfTimePeriod,
        message.hourOfEndOfTimePeriod,
        message.minuteOfEndOfTimePeriod,
        message.secondOfEndOfTimePeriod,
        tzinfo=UTC,
    )
    return MaxTPeriod(end - message.duration, end)


def inspect_periods(path: Path) -> tuple[MaxTPeriod, ...]:
    with grib2io.open(str(path), save_index=False, use_index=False) as messages:
        return tuple(message_period(message) for message in messages)


def extract_points(message, obj: ObjectInfo, stations: Sequence[Station], *, lag: timedelta):
    validate_lag(lag)
    period, grid = message_period(message), grid_spec(message)
    if int(message.drtn) not in (2, 3):
        raise ValueError("Unsupported MaxT packing; toolchain supports complex packing only")
    cells = [containing_cell(grid, s.latitude, s.longitude) for s in stations]
    field = message.data
    if field.shape != (grid.ny, grid.nx):
        raise ValueError("Decoded field shape differs from grid")
    quantum = 2.0**message.binScaleFactor * 10.0 ** (-message.decScaleFactor)
    try:
        for station, cell in zip(stations, cells, strict=True):
            value = field[cell.row, cell.column]
            masked = bool(np.ma.is_masked(value))
            # grib2io applies bitmaps and complex-packing missing values to NaN.
            if message.bitmap is not None and not message.bitmap[cell.row, cell.column]:
                masked = True
            kelvin = float(value) if not masked else float("nan")
            native = native_fahrenheit(kelvin, quantum, masked=masked)
            yield PointForecast(
                station,
                obj,
                period,
                cell,
                kelvin,
                (kelvin - 273.15) * 1.8 + 32,
                native,
                quantum,
                obj.issued_at + lag,
            )
    finally:
        message.flush_data()


def decode_grib(
    path: Path,
    obj: ObjectInfo,
    stations: Sequence[Station],
    *,
    lag: timedelta,
    periods: Sequence[MaxTPeriod] | None = None,
) -> Iterator[PointForecast]:
    validate_lag(lag)
    with grib2io.open(str(path), save_index=False, use_index=False) as messages:
        for message in messages:
            period = message_period(message)
            if periods is None or period in periods:
                yield from extract_points(message, obj, stations, lag=lag)
