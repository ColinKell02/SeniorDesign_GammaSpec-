"""
install.py — Dependency installer for the Planetary Data Viz scripts.

Just run:   python install.py

This will install every third-party package needed by:
    Ceres_Plotter.py
    data_fetcher.py
    LunarProspector_WebApp.py
    CuriosityVis.py
    build_abundance_library.py
    build_spatial_library.py

By default it installs into whatever Python interpreter you ran this with
(so if you're inside a venv, it installs there; if not, it installs to your
user site-packages with --user to avoid permission errors).

Pass --venv to instead create a fresh .venv folder next to this script
and install everything inside it.
"""

import subprocess
import sys
import os
import venv
from pathlib import Path

# --- Packages required across all scripts ---
REQUIREMENTS = [
    # Core scientific stack
    "numpy>=1.23",
    "pandas>=1.5",
    "scipy>=1.10",
    # Plotting and imaging
    "matplotlib>=3.6",
    "plotly>=5.18",
    "Pillow>=9.4",
    # Dash web app + 3D VTK rendering
    "dash>=2.14",
    "dash-vtk>=0.0.9",
    # PDS4 planetary data parsing
    "pds4_tools>=1.3",
    # NASA SPICE toolkit
    "spiceypy>=6.0",
]


def run(cmd):
    """Run a command and stream its output. Raise on failure."""
    print(f"\n>>> {' '.join(cmd)}")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        raise SystemExit(f"Command failed with exit code {result.returncode}")


def check_python_version():
    if sys.version_info < (3, 9):
        raise SystemExit(
            f"Python 3.9 or newer is required. You have {sys.version.split()[0]}."
        )
    print(f"Python {sys.version.split()[0]} detected — OK.")


def install_into_current(use_user_flag):
    """Install packages into the current interpreter."""
    pip_cmd = [sys.executable, "-m", "pip", "install", "--upgrade", "pip"]
    run(pip_cmd)

    install_cmd = [sys.executable, "-m", "pip", "install"]
    if use_user_flag:
        install_cmd.append("--user")
    install_cmd.extend(REQUIREMENTS)
    run(install_cmd)


def install_into_venv():
    """Create a .venv next to this script and install packages inside it."""
    venv_dir = Path(__file__).resolve().parent / ".venv"

    if not venv_dir.exists():
        print(f"Creating virtual environment at {venv_dir} ...")
        venv.create(venv_dir, with_pip=True)
    else:
        print(f"Reusing existing virtual environment at {venv_dir}")

    # Locate the venv's Python executable
    if os.name == "nt":
        venv_python = venv_dir / "Scripts" / "python.exe"
        activate_hint = r".venv\Scripts\activate"
    else:
        venv_python = venv_dir / "bin" / "python"
        activate_hint = "source .venv/bin/activate"

    if not venv_python.exists():
        raise SystemExit(f"Could not find Python inside venv at {venv_python}")

    run([str(venv_python), "-m", "pip", "install", "--upgrade", "pip"])
    run([str(venv_python), "-m", "pip", "install", *REQUIREMENTS])

    print("\n=== Done! ===")
    print(f"To use this environment, activate it with:\n    {activate_hint}")


def in_virtualenv():
    """True if we're already running inside a venv or conda env."""
    return (
        hasattr(sys, "real_prefix")
        or sys.base_prefix != sys.prefix
        or os.environ.get("CONDA_PREFIX") is not None
    )


def main():
    print("=== Planetary Data Viz: Dependency Installer ===")
    check_python_version()

    use_venv = "--venv" in sys.argv

    if use_venv:
        install_into_venv()
        return

    if in_virtualenv():
        print("Detected active virtual environment — installing into it.")
        install_into_current(use_user_flag=False)
    else:
        print("No virtual environment detected — installing with --user.")
        print("(Re-run with `python install.py --venv` to create a .venv instead.)")
        install_into_current(use_user_flag=True)

    print("\n=== Done! ===")
    print("All dependencies installed successfully.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(130)