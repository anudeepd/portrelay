"""Install every wheel in a clean virtual environment and smoke-test its binary."""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys
import tempfile
import venv


def main() -> int:
    wheel_dir = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "dist")
    wheels = sorted(wheel_dir.glob("*.whl"))
    if not wheels:
        raise SystemExit(f"no wheels found in {wheel_dir}")

    with tempfile.TemporaryDirectory(prefix="portrelay-wheel-") as directory:
        environment = pathlib.Path(directory) / "venv"
        venv.EnvBuilder(with_pip=True, clear=True).create(environment)
        python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        executable = environment / ("Scripts/portrelay.exe" if os.name == "nt" else "bin/portrelay")
        for wheel in wheels:
            subprocess.run(
                [str(python), "-m", "pip", "install", "--force-reinstall", "--no-deps", str(wheel)],
                check=True,
            )
            subprocess.run([str(executable), "--version"], check=True)
            subprocess.run([str(executable), "--help"], check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
