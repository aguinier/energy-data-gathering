#!/bin/bash
#
# Setup hourly cron job for ENTSO-E data updates
#
# Usage:
#   bash scripts/scheduler_setup.sh
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo "ENTSO-E Data Pipeline Scheduler Setup"
echo "=========================================="
echo

# Get absolute path to project directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "Project directory: $PROJECT_DIR"
echo

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}ERROR: python3 not found${NC}"
    echo "Please install Python 3 before running this script"
    exit 1
fi

PYTHON_PATH=$(which python3)
echo "Python path: $PYTHON_PATH"
echo

# Check if required Python packages are installed
echo "Checking Python dependencies..."
cd "$PROJECT_DIR"

if [ ! -f "requirements.txt" ]; then
    echo -e "${RED}ERROR: requirements.txt not found${NC}"
    exit 1
fi

# Try to import required modules
$PYTHON_PATH -c "import entsoe" 2>/dev/null || {
    echo -e "${YELLOW}WARNING: Required packages not installed${NC}"
    echo "Installing dependencies from requirements.txt..."
    $PYTHON_PATH -m pip install -r requirements.txt || {
        echo -e "${RED}ERROR: Failed to install dependencies${NC}"
        exit 1
    }
}

echo -e "${GREEN}✓ Dependencies OK${NC}"
echo

# Create cron job entry
UPDATE_SCRIPT="$PROJECT_DIR/scripts/update.py"
LOG_FILE="$PROJECT_DIR/logs/cron_update.log"

# Cron job to run hourly at minute 15
CRON_ENTRY="15 * * * * cd $PROJECT_DIR && $PYTHON_PATH $UPDATE_SCRIPT >> $LOG_FILE 2>&1"

echo "Proposed cron job:"
echo "  $CRON_ENTRY"
echo

# Ask user for confirmation
read -p "Do you want to add this cron job? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Setup cancelled"
    exit 0
fi

# Backup existing crontab
echo "Backing up existing crontab..."
crontab -l > /tmp/crontab_backup_$(date +%Y%m%d_%H%M%S).txt 2>/dev/null || true

# Check if cron job already exists
if crontab -l 2>/dev/null | grep -q "$UPDATE_SCRIPT"; then
    echo -e "${YELLOW}WARNING: Cron job already exists${NC}"
    read -p "Do you want to replace it? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Setup cancelled"
        exit 0
    fi

    # Remove existing entry
    (crontab -l 2>/dev/null | grep -v "$UPDATE_SCRIPT") | crontab -
    echo "Removed existing cron job"
fi

# Add new cron job
(crontab -l 2>/dev/null; echo "$CRON_ENTRY") | crontab -

echo -e "${GREEN}✓ Cron job added successfully${NC}"
echo

# Verify cron job was added
echo "Current crontab entries for this project:"
crontab -l | grep "$UPDATE_SCRIPT" || echo "No entries found"
echo

# Setup log rotation (optional)
echo "=========================================="
echo "Log Rotation Setup (Optional)"
echo "=========================================="
echo
echo "The pipeline logs will grow over time. You can set up log rotation"
echo "to automatically compress and archive old logs."
echo

read -p "Do you want to set up log rotation? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    LOGROTATE_CONF="/etc/logrotate.d/entsoe-pipeline"

    echo "Creating logrotate configuration..."

    # Create logrotate config (requires sudo)
    sudo tee "$LOGROTATE_CONF" > /dev/null <<EOF
$PROJECT_DIR/logs/*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    create 644 $USER $USER
}
EOF

    echo -e "${GREEN}✓ Log rotation configured${NC}"
    echo "Logs will be rotated daily and kept for 30 days"
else
    echo "Skipping log rotation setup"
fi

echo
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo
echo "The update pipeline will run hourly at minute 15."
echo "Next run: $(date -v +1H '+%Y-%m-%d %H:15:00')"
echo
echo "Useful commands:"
echo "  View cron jobs:        crontab -l"
echo "  Remove cron job:       crontab -e (then delete the line)"
echo "  View update logs:      tail -f $LOG_FILE"
echo "  Test update manually:  python $UPDATE_SCRIPT"
echo
echo -e "${GREEN}✓ All done!${NC}"
