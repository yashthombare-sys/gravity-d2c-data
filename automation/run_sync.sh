#!/bin/zsh
# Run daily sync manually
# Usage:
#   ./run_sync.sh              # Sync yesterday
#   ./run_sync.sh 2026-03-10   # Sync specific date
#   ./run_sync.sh --status     # Show status
#   ./run_sync.sh --import     # Import historical JSON data
#   ./run_sync.sh --rebuild    # Rebuild dashboard from DB

cd "/Users/yashthombare/Desktop/Gravity/Shiprocket D2C data/automation"
/Library/Developer/CommandLineTools/usr/bin/python3 daily_sync.py "$@"
