"""
============================================================
Script 00 — Environment Setup & Folder Creation
============================================================
PURPOSE:
    - Checks that Python version is correct (3.8 or higher)
    - Checks that all required libraries are installed
    - Creates every folder this project needs
    - Creates a blank audit log so other scripts can write to it
    - Prints a clear setup summary at the end

RUN THIS FIRST before any other script.
Command: python scripts/00_setup_environment.py
============================================================
"""

import sys          # Lets us check Python version and exit with errors
import os           # Lets us create folders and check if they exist
import importlib    # Lets us check if a Python library is installed
import csv          # Lets us create the audit log CSV file
from datetime import datetime  # Gets the current date and time


# ============================================================
# CONFIGURATION
# ============================================================

# Root folder of the project (one level above /scripts)
# os.path.dirname(__file__) = the folder this script is in (/scripts)
# os.path.join(..., '..') = go up one level to the project root
PROJECT_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
PROJECT_ROOT = os.path.abspath(PROJECT_ROOT)  # Remove the '..' from the path

# Minimum Python version required by the libraries we use
MIN_PYTHON_VERSION = (3, 8)

# All folders that must exist for the project to work
REQUIRED_FOLDERS = [
    "data/raw/cms_physician",    # CMS Medicare physician data
    "data/raw/hcup",             # HCUP hospital inpatient data
    "data/raw/meps",             # MEPS ambulatory care data
    "data/raw/cms_hospital",     # CMS Hospital Compare data
    "data/raw/benchmarks",       # National benchmark reference data
    "data/processed",            # Cleaned, analysis-ready data
    "scripts",                   # All Python scripts
    "dashboard/instructions",    # Power BI setup guide
    "outputs/reports",           # CSV reports that feed Power BI
    "outputs/memos",             # Auto-generated executive Word memo
    "outputs/logs",              # Audit trail logs
]

# Libraries we need — format: ('import_name', 'pip_name')
# import_name = what you type in Python: import X
# pip_name    = what you type to install: pip install X
REQUIRED_LIBRARIES = [
    ('requests',      'requests'),
    ('tqdm',          'tqdm'),
    ('pandas',        'pandas'),
    ('numpy',         'numpy'),
    ('openpyxl',      'openpyxl'),
    ('scipy',         'scipy'),
    ('statsmodels',   'statsmodels'),
    ('sklearn',       'scikit-learn'),
    ('matplotlib',    'matplotlib'),
    ('seaborn',       'seaborn'),
    ('plotly',        'plotly'),
    ('docx',          'python-docx'),
    ('colorama',      'colorama'),
    ('chardet',       'chardet'),
]

# Audit log file — every script records what it did here
AUDIT_LOG_PATH = os.path.join(PROJECT_ROOT, 'outputs', 'logs', 'audit_trail.csv')


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def print_header():
    """Prints a banner when the script starts."""
    print("=" * 65)
    print("  JOHNS HOPKINS UROLOGY ANALYTICS — Environment Setup")
    print("=" * 65)
    print(f"  Script : 00_setup_environment.py")
    print(f"  Time   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Folder : {PROJECT_ROOT}")
    print("=" * 65)
    print()


def check_python_version():
    """
    Checks that the installed Python is version 3.8 or higher.
    Stops the script with a clear error if Python is too old.
    """
    print("[STEP 1] Checking Python version...")

    current = sys.version_info  # Returns (major, minor, micro)
    current_str = f"{current.major}.{current.minor}.{current.micro}"

    if current < MIN_PYTHON_VERSION:
        print(f"  ERROR: Python {current_str} detected.")
        print(f"  This project requires Python {MIN_PYTHON_VERSION[0]}.{MIN_PYTHON_VERSION[1]}+")
        print("  Download latest Python from: https://www.python.org/downloads/")
        sys.exit(1)  # Stop here with error code 1

    print(f"  OK — Python {current_str} (need {MIN_PYTHON_VERSION[0]}.{MIN_PYTHON_VERSION[1]}+)")
    print()


def check_libraries():
    """
    Tries to import each required library.
    Prints a clear pip install command for any that are missing.
    Returns True if all OK, False if any missing.
    """
    print("[STEP 2] Checking required libraries...")

    missing = []  # Collect names of missing libraries

    for import_name, pip_name in REQUIRED_LIBRARIES:
        try:
            importlib.import_module(import_name)
            print(f"  OK      — {pip_name}")
        except ImportError:
            print(f"  MISSING — {pip_name}")
            missing.append(pip_name)

    if missing:
        print()
        print("  ACTION REQUIRED: Install missing libraries with:")
        print()
        print(f"    pip install {' '.join(missing)}")
        print()
        print("  Or install everything at once with:")
        print("    pip install -r requirements.txt")
        print()
        return False

    print("  All libraries are installed!")
    print()
    return True


def create_folders():
    """
    Creates all required project folders.
    Skips folders that already exist — safe to run multiple times.
    """
    print("[STEP 3] Creating project folders...")

    for folder in REQUIRED_FOLDERS:
        full_path = os.path.join(PROJECT_ROOT, folder)

        if os.path.exists(full_path):
            print(f"  EXISTS  — {folder}")
        else:
            os.makedirs(full_path)  # Creates the folder and any parent folders
            print(f"  CREATED — {folder}")

    print()


def create_audit_log():
    """
    Creates the audit trail CSV file if it does not exist yet.

    Every script in this project writes one row to this file when it runs.
    This creates a permanent record of:
      - What ran and when
      - What data was processed
      - Whether it succeeded or failed

    This is standard practice in healthcare analytics for compliance.
    """
    print("[STEP 4] Initializing audit log...")

    if os.path.exists(AUDIT_LOG_PATH):
        print(f"  EXISTS  — audit_trail.csv (will append to existing log)")
    else:
        # Create the CSV file with column headers
        with open(AUDIT_LOG_PATH, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'timestamp',     # When the script ran
                'script_name',   # Which script ran
                'data_source',   # What data source was used
                'rows_in',       # Raw records received
                'rows_out',      # Records after filtering/cleaning
                'status',        # SUCCESS or ERROR
                'notes'          # Extra context or error message
            ])
        print(f"  CREATED — audit_trail.csv")

    # Log this setup run
    append_audit_log(
        script_name = '00_setup_environment.py',
        data_source = 'N/A — Setup Script',
        rows_in     = 0,
        rows_out    = 0,
        status      = 'SUCCESS',
        notes       = 'Environment setup completed'
    )
    print()


def append_audit_log(script_name, data_source, rows_in, rows_out, status, notes):
    """
    Adds one row to the audit trail CSV.
    This function is imported and used by ALL other scripts in this project.

    Parameters:
      script_name : str — Name of the script (e.g. '01_download_cms_data.py')
      data_source : str — Where data came from (e.g. 'CMS Medicare API 2022')
      rows_in     : int — Raw records before processing
      rows_out    : int — Records after cleaning and filtering
      status      : str — 'SUCCESS' or 'ERROR'
      notes       : str — Any extra detail, warning, or error message
    """
    with open(AUDIT_LOG_PATH, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            script_name,
            data_source,
            rows_in,
            rows_out,
            status,
            notes
        ])


def print_summary(all_ok):
    """Prints the final pass/fail summary."""
    print("=" * 65)
    if all_ok:
        print("  SETUP COMPLETE — Ready to run Script 01")
        print()
        print("  Next step:")
        print("    python scripts/01_download_cms_data.py")
    else:
        print("  SETUP INCOMPLETE — Fix missing libraries above first,")
        print("  then re-run: python scripts/00_setup_environment.py")
    print("=" * 65)


# ============================================================
# MAIN — Runs when you execute this script
# ============================================================

if __name__ == "__main__":

    print_header()

    check_python_version()            # Step 1: Check Python version
    libraries_ok = check_libraries()  # Step 2: Check libraries
    create_folders()                  # Step 3: Create folders
    create_audit_log()                # Step 4: Create audit log

    print_summary(libraries_ok)
