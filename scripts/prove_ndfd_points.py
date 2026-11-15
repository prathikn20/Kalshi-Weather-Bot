"""Break each required property, observe pytest fail, restore, and verify it passes."""

import hashlib
import importlib.util
import json
import os
import subprocess
from pathlib import Path

POINTS = "tests/test_ndfd_points.py::"
CACHE = "tests/test_ndfd_cache_check.py::"
CASES = [
    (
        1,
        "src/kwb/parse/ndfd.py",
        'r"YGUZ(?:97|98)_KWBN_\\d{12}"',
        'r"YGUZ(?:87|88|97|98)_KWBN_\\d{12}"',
        POINTS
        + "test_only_operational_conus_maxt_objects_are_selected_and_every_exclusion_is_counted",
    ),
    (
        2,
        "src/kwb/parse/ndfd.py",
        "column, row = math.floor(fx + 0.5), math.floor(fy + 0.5)",
        "column, row = math.floor(fx + 0.5) + 1, math.floor(fy + 0.5)",
        POINTS + "test_every_configured_station_is_inside_its_selected_grib_cell_"
        "and_unsupported_scanning_raises",
    ),
    (
        3,
        "src/kwb/parse/ndfd.py",
        "standard = timezone(station.standard_offset(target_date.year))",
        "standard = ZoneInfo(station.zone)",
        POINTS
        + "test_each_climate_day_has_exactly_one_twelve_hour_period_and_standard_time_never_shifts",
    ),
    (
        4,
        "src/kwb/parse/ndfd.py",
        "availability <= as_of and",
        "True and",
        POINTS
        + "test_forecasts_use_the_latest_eligible_issuance_covering_the_day_and_require_a_lag",
    ),
    (
        5,
        "src/kwb/parse/ndfd_decode.py",
        'kelvin = float(value) if not masked else float("nan")',
        'kelvin = float(value) + 1 if not masked else float("nan")',
        POINTS
        + "test_point_values_preserve_kelvin_and_reject_missing_or_ambiguous_native_fahrenheit",
    ),
    (
        6,
        "src/kwb/parse/ndfd_decode.py",
        "value = field[cell.row, cell.column]",
        "value = field[cell.row, cell.column + 1]",
        POINTS + "test_the_real_grib_fixture_decodes_to_the_recorded_station_value_sha256",
    ),
    (
        7,
        "src/kwb/collectors/ndfd_cache.py",
        'if hashlib.file_digest(path.open("rb"), "sha256").hexdigest() != row[2]:',
        "if False:",
        CACHE + "test_cached_objects_are_reused_only_when_the_complete_manifest_and_sha256_match",
    ),
    (
        8,
        "src/kwb/collectors/ndfd_cache.py",
        "self.monotonic() + 1.000001",
        "self.monotonic()",
        CACHE + "test_downloads_are_unsigned_paced_and_respect_retry_policy_and_retry_after",
    ),
    (
        9,
        "src/kwb/ops/ndfd_point_check.py",
        "if len(keys) != len(set(keys)) or set(keys) != expected:",
        "if False:",
        CACHE + "test_every_station_day_has_exactly_one_accounting_status",
    ),
    (
        "land_accuracy",
        "src/kwb/ops/ndfd_point_check.py",
        "if abs(signed_distance_m) <= accuracy_m:",
        "if False:",
        CACHE + "test_land_classification_is_indeterminate_within_the_shoreline_accuracy",
    ),
    (
        "age_fallback",
        "src/kwb/ops/ndfd_point_check.py",
        'sum(r.get("fallback", False) for r in rows)',
        "0",
        CACHE + "test_selected_issuance_age_distributions_and_older_fallbacks_are_reported",
    ),
]


def run(test):
    result = subprocess.run(
        ["uv", "run", "--locked", "--group", "ndfd", "pytest", "-q", test],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    return result.returncode, result.stdout + result.stderr


def main():
    output = Path("state/ndfd-point-check-2026-11-15")
    output.mkdir(parents=True, exist_ok=True)
    evidence = []
    for property_id, filename, before, after, test in CASES:
        path = Path(filename)
        original = path.read_bytes()
        source = original.decode()
        if source.count(before) != 1:
            raise ValueError(f"Mutation must have exactly one target: {property_id}")
        try:
            path.write_text(source.replace(before, after))
            Path(importlib.util.cache_from_source(str(path))).unlink(missing_ok=True)
            code, failure = run(test)
            if code != 1 or "FAILED" not in failure:
                raise AssertionError(
                    f"Mutation {property_id} did not produce a test failure:\n{failure}"
                )
        finally:
            path.write_bytes(original)
            Path(importlib.util.cache_from_source(str(path))).unlink(missing_ok=True)
        restored_code, restored_output = run(test)
        if restored_code:
            raise AssertionError(f"Restoration failed:\n{restored_output}")
        record = {
            "property": property_id,
            "test": test,
            "mutation": {"before": before, "after": after},
            "failure_output": failure,
            "restored_output": restored_output,
            "source_sha256_restored": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
        evidence.append(record)
        (output / "mutation-evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
        print(
            f"Property {property_id}: failed under mutation; passed after restoration", flush=True
        )


if __name__ == "__main__":
    main()
