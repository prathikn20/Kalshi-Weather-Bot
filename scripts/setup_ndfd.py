"""Install the pinned decoder once per machine, then sync this checkout's NDFD group."""

import ctypes
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import tomllib
from pathlib import Path

import tomli_w

COMMIT = "acbccb8a894255cd30056b722781a7c9f7b1fe3c"
PREFIX = Path.home() / ".local/share/kwb/g2c-2.3.0"
LIBRARY = PREFIX / "lib" / ("libg2c.0.dylib" if sys.platform == "darwin" else "libg2c.so.0")


def run(*args):
    subprocess.run(args, check=True)


def verified():
    try:
        alias = PREFIX / "lib" / ("libg2c.dylib" if sys.platform == "darwin" else "libg2c.so")
        if alias.resolve() != LIBRARY.resolve() or not (PREFIX / "include/grib2.h").is_file():
            return False
        manifest = json.loads((PREFIX / "install.json").read_text())
        if manifest["commit"] != COMMIT:
            return False
        for name, digest in manifest["files"].items():
            if hashlib.sha256((PREFIX / name).read_bytes()).hexdigest() != digest:
                return False
        return ctypes.CDLL(str(LIBRARY)).g2_free is not None
    except (OSError, ValueError, KeyError, AttributeError):
        return False


def main():
    started = time.monotonic()
    if not verified():
        PREFIX.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="kwb-g2c-", dir=PREFIX.parent) as directory:
            source = Path(directory) / "source"
            build = Path(directory) / "build"
            run(
                "git",
                "clone",
                "--depth",
                "1",
                "--branch",
                "v2.3.0",
                "https://github.com/NOAA-EMC/NCEPLIBS-g2c.git",
                str(source),
            )
            actual = subprocess.check_output(
                ["git", "-C", str(source), "rev-parse", "HEAD"], text=True
            ).strip()
            if actual != COMMIT:
                raise ValueError(f"g2c tag changed: {actual}")
            # Remove only our invalid versioned installation; no other native installs are touched.
            if PREFIX.exists():
                shutil.rmtree(PREFIX)
            run(
                "cmake",
                "-S",
                str(source),
                "-B",
                str(build),
                f"-DCMAKE_INSTALL_PREFIX={PREFIX}",
                "-DCMAKE_INSTALL_LIBDIR=lib",
                "-DBUILD_SHARED_LIBS=ON",
                "-DBUILD_STATIC_LIBS=OFF",
                "-DUSE_PNG=OFF",
                "-DUSE_Jasper=OFF",
                "-DUSE_OpenJPEG=OFF",
                "-DUSE_AEC=OFF",
                "-DBUILD_TESTING=OFF",
                "-DUTILS=OFF",
            )
            run("cmake", "--build", str(build), "--parallel", "2")
            run("cmake", "--install", str(build))
            files = {
                str(p.relative_to(PREFIX)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in PREFIX.rglob("*")
                if p.is_file() and not p.is_symlink()
            }
            (PREFIX / "install.json").write_text(
                json.dumps({"commit": COMMIT, "files": files}, indent=2)
            )
        if not verified():
            raise RuntimeError("Installed g2c failed hash/load verification")
        print("Built and verified", LIBRARY, flush=True)
    else:
        print("Reused verified", LIBRARY, flush=True)
    # User-level, package-specific build variables are in uv's cache key. This one
    # install is shared by all checkouts; preserve every other user setting.
    config = Path(os.environ.get("XDG_CONFIG_HOME", str(Path.home() / ".config"))) / "uv/uv.toml"
    settings = tomllib.loads(config.read_text()) if config.exists() else {}
    settings.setdefault("extra-build-variables", {}).setdefault("grib2io", {})["G2C_DIR"] = str(
        PREFIX
    )
    config.parent.mkdir(parents=True, exist_ok=True)
    config.write_text(tomli_w.dumps(settings))
    print("uv cache-key build variable G2C_DIR:", PREFIX, flush=True)
    run("uv", "sync", "--locked", "--group", "ndfd")
    run(
        "uv",
        "run",
        "--locked",
        "--group",
        "ndfd",
        "python",
        "-c",
        "from pathlib import Path; import grib2io; "
        "assert Path(grib2io.__config__.g2c_library).resolve().parent == "
        "Path.home() / '.local/share/kwb/g2c-2.3.0/lib'; "
        "grib2io.show_config()",
    )
    print(f"NDFD setup seconds: {time.monotonic() - started:.3f}", flush=True)


if __name__ == "__main__":
    main()
