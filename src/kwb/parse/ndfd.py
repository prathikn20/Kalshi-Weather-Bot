"""Pure NDFD MaxT boundaries; no store, network, or notebook dependencies."""

import math
import re
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class Station:
    station: str
    latitude: float
    longitude: float
    zone: str

    def standard_offset(self, year: int) -> timedelta:
        local = datetime(year, 1, 15, 12, tzinfo=ZoneInfo(self.zone))
        offset, dst = local.utcoffset(), local.dst()
        if offset is None or dst is None:
            raise ValueError("Timezone has no standard offset")
        return offset - dst


@dataclass(frozen=True)
class ObjectInfo:
    key: str
    size: int
    etag: str

    @property
    def issued_at(self) -> datetime:
        match = re.fullmatch(r"YGUZ(?:97|98)_KWBN_(\d{12})", self.key.rsplit("/", 1)[-1])
        if not match:
            raise ValueError(f"Not an operational CONUS MaxT object: {self.key}")
        return datetime.strptime(match[1], "%Y%m%d%H%M").replace(tzinfo=UTC)


@dataclass(frozen=True)
class ObjectSelection:
    selected: tuple[ObjectInfo, ...]
    excluded: dict[str, int]


def select_objects(objects: Iterable[ObjectInfo]) -> ObjectSelection:
    selected, excluded, seen = [], Counter(), set()
    for obj in objects:
        if obj.key in seen:
            raise ValueError(f"Duplicate object: {obj.key}")
        seen.add(obj.key)
        if not re.fullmatch(r"wmo/maxt/\d{4}/\d{2}/\d{2}/[^/]+", obj.key):
            raise ValueError(f"Unexpected MaxT key: {obj.key}")
        name = obj.key.rsplit("/", 1)[-1]
        if re.fullmatch(r"YGUZ(?:97|98)_KWBN_\d{12}", name):
            _ = obj.issued_at  # Validate the timestamp too.
            selected.append(obj)
        elif re.fullmatch(r"YGUZ(?:87|88)_KWBN_\d{12}", name):
            excluded["legacy_conus"] += 1
        elif re.fullmatch(r"YG[ARST]Z(?:87|88|97|98)_KWBN_\d{12}", name):
            excluded["other_domain"] += 1
        else:
            raise ValueError(f"Unknown MaxT product: {obj.key}")
    return ObjectSelection(tuple(selected), dict(excluded))


@dataclass(frozen=True)
class GridSpec:
    projection: dict
    first_latitude: float
    first_longitude: float
    dx: float
    dy: float
    nx: int
    ny: int
    scan_flags: tuple[int, ...]
    template: int


@dataclass(frozen=True)
class Cell:
    row: int
    column: int
    centre_latitude: float
    centre_longitude: float


def containing_cell(grid: GridSpec, latitude: float, longitude: float) -> Cell:
    from pyproj import Proj

    if grid.template != 30 or grid.scan_flags != (0, 1, 0, 1, 0, 0, 0, 0):
        raise ValueError("Unsupported CONUS grid/scanning flags")
    if not all(math.isfinite(v) for v in (latitude, longitude, grid.dx, grid.dy)):
        raise ValueError("Nonfinite coordinate or spacing")
    if grid.dx <= 0 or grid.dy <= 0:
        raise ValueError("Grid spacing must be positive")
    projection = Proj(grid.projection)
    x0, y0 = projection(grid.first_longitude, grid.first_latitude)
    x, y = projection(longitude, latitude)
    fx, fy = (x - x0) / grid.dx, (y - y0) / grid.dy
    column, row = math.floor(fx + 0.5), math.floor(fy + 0.5)
    if not (0 <= row < grid.ny and 0 <= column < grid.nx):
        raise ValueError("Station outside CONUS grid")
    if not (column - 0.5 <= fx < column + 0.5 and row - 0.5 <= fy < row + 0.5):
        raise ValueError("Selected cell does not contain station")
    lon, lat = projection(x0 + column * grid.dx, y0 + row * grid.dy, inverse=True)
    return Cell(row, column, lat, lon)


@dataclass(frozen=True)
class MaxTPeriod:
    start: datetime
    end: datetime

    def __post_init__(self):
        require_utc(self.start)
        require_utc(self.end)
        if self.end - self.start != timedelta(hours=12):
            raise ValueError("MaxT period must be twelve hours")


def require_utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("Timestamp must be aware UTC")


def climate_day(station: Station, target_date: date) -> tuple[datetime, datetime]:
    standard = timezone(station.standard_offset(target_date.year))
    start = datetime.combine(target_date, time(), tzinfo=standard).astimezone(UTC)
    return start, start + timedelta(days=1)


def period_for_day(
    periods: Sequence[MaxTPeriod], station: Station, target_date: date
) -> MaxTPeriod:
    start, end = climate_day(station, target_date)
    candidates = [p for p in periods if start <= p.start and p.end <= end]
    if len(candidates) != 1:
        raise ValueError(
            f"Expected exactly one MaxT period for {station.station} {target_date}; "
            f"got {len(candidates)}"
        )
    return candidates[0]


def native_fahrenheit(kelvin: float, quantum_k: float, *, masked: bool = False) -> int:
    if masked or not math.isfinite(kelvin) or not 150 < kelvin < 350:
        raise ValueError("Missing or invalid station temperature")
    if not math.isfinite(quantum_k) or quantum_k <= 0:
        raise ValueError("Invalid packing quantum")
    fahrenheit = (kelvin - 273.15) * 1.8 + 32
    # float32 decoder rounding is separate from the GRIB packing quantum.
    bound = quantum_k * 0.5 * 1.8 + abs(kelvin) * 2**-23 * 1.8
    low, high = math.ceil(fahrenheit - bound), math.floor(fahrenheit + bound)
    if low != high:
        raise ValueError("Native Fahrenheit is ambiguous or outside packing precision")
    return low


@dataclass(frozen=True)
class PointForecast:
    station: Station
    obj: ObjectInfo
    period: MaxTPeriod
    cell: Cell
    kelvin: float
    fahrenheit: float
    native_fahrenheit: int
    packing_quantum_k: float
    available_at: datetime


class NoQualifyingIssuance(ValueError):
    """No issuance was available and covered this climate day."""


def validate_lag(lag: timedelta) -> None:
    if not isinstance(lag, timedelta) or lag < timedelta(0):
        raise ValueError("Lag must be an explicit nonnegative timedelta")


def select_forecast(
    forecasts: Iterable[PointForecast],
    station: str,
    target_date: date,
    as_of: datetime,
    *,
    lag: timedelta,
) -> PointForecast:
    require_utc(as_of)
    validate_lag(lag)
    eligible = []
    for row in forecasts:
        if row.station.station != station:
            continue
        availability = row.obj.issued_at + lag
        if row.available_at != availability:
            raise ValueError("Forecast availability does not match explicit lag")
        start, end = climate_day(row.station, target_date)
        if availability <= as_of and start <= row.period.start and row.period.end <= end:
            eligible.append(row)
    if not eligible:
        raise NoQualifyingIssuance(f"No qualifying issuance for {station} {target_date} at {as_of}")
    latest = max(r.obj.issued_at for r in eligible)
    chosen = [r for r in eligible if r.obj.issued_at == latest]
    if len(chosen) != 1:
        raise ValueError("Latest issuance has duplicate or ambiguous periods")
    return chosen[0]
