#!/usr/bin/env python3
"""
Set up macOS launchd scheduler to run daily_sync.py every day at 6 AM.
Also creates a convenience shell script for manual runs.
"""
import os, sys, stat

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PLIST_NAME = "com.clapstore.dailysync"
PLIST_PATH = os.path.expanduser(f"~/Library/LaunchAgents/{PLIST_NAME}.plist")
PYTHON = sys.executable or "/usr/bin/python3"
SCRIPT_PATH = os.path.join(BASE_DIR, "daily_sync.py")
LOG_DIR = os.path.join(BASE_DIR, "logs")
RUNNER_PATH = os.path.join(BASE_DIR, "run_sync.sh")


def create_runner_script():
    """Create a shell script for easy manual runs."""
    content = f"""#!/bin/zsh
# Run daily sync manually
# Usage:
#   ./run_sync.sh              # Sync yesterday
#   ./run_sync.sh 2026-03-10   # Sync specific date
#   ./run_sync.sh --status     # Show status
#   ./run_sync.sh --import     # Import historical JSON data
#   ./run_sync.sh --rebuild    # Rebuild dashboard from DB

cd "{BASE_DIR}"
{PYTHON} daily_sync.py "$@"
"""
    with open(RUNNER_PATH, "w") as f:
        f.write(content)
    os.chmod(RUNNER_PATH, os.stat(RUNNER_PATH).st_mode | stat.S_IEXEC)
    print(f"Created: {RUNNER_PATH}")


def create_plist():
    """Create launchd plist for daily 6 AM execution."""
    os.makedirs(LOG_DIR, exist_ok=True)

    plist_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{PLIST_NAME}</string>

    <key>ProgramArguments</key>
    <array>
        <string>{PYTHON}</string>
        <string>{SCRIPT_PATH}</string>
    </array>

    <key>WorkingDirectory</key>
    <string>{BASE_DIR}</string>

    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>6</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>

    <key>StandardOutPath</key>
    <string>{LOG_DIR}/sync.log</string>

    <key>StandardErrorPath</key>
    <string>{LOG_DIR}/sync_error.log</string>

    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin</string>
    </dict>
</dict>
</plist>
"""
    with open(PLIST_PATH, "w") as f:
        f.write(plist_content)
    print(f"Created: {PLIST_PATH}")
    return PLIST_PATH


def show_instructions():
    print(f"""
{'='*60}
  DAILY SYNC SETUP COMPLETE
{'='*60}

Files created:
  1. {RUNNER_PATH}
     → Manual sync: ./run_sync.sh
  2. {PLIST_PATH}
     → Auto-runs at 6:00 AM daily

STEP 1 — Import your existing historical data:
  cd "{BASE_DIR}"
  ./run_sync.sh --import

STEP 2 — Test with yesterday's data:
  ./run_sync.sh

STEP 3 — Enable daily auto-sync:
  launchctl load {PLIST_PATH}

To check if it's running:
  launchctl list | grep clapstore

To disable:
  launchctl unload {PLIST_PATH}

To view logs:
  tail -f {LOG_DIR}/sync.log

Other commands:
  ./run_sync.sh --status              # Show DB status
  ./run_sync.sh --rebuild             # Rebuild dashboard from DB
  ./run_sync.sh 2026-03-10            # Sync specific date
  ./run_sync.sh --backfill 2026-03-01 2026-03-10
  ./run_sync.sh --shiprocket-only     # Skip Amazon
{'='*60}
""")


def main():
    print("Setting up Clapstore Daily Sync...\n")
    create_runner_script()
    create_plist()
    show_instructions()


if __name__ == "__main__":
    main()
