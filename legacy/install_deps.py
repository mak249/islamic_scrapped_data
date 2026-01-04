#!/usr/bin/env python3
"""
Installation script for dependencies.
Run this if you encounter import errors.
"""

import subprocess
import sys

def install_dependencies():
    """Install required Python packages."""
    packages = [
        'pyyaml',
        'scrapy',
        'scrapy-playwright',
        'playwright',
        'requests',
        'beautifulsoup4',
        'lxml'
    ]

    print("Installing required packages...")
    for package in packages:
        try:
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])
            print(f"✓ Installed {package}")
        except subprocess.CalledProcessError as e:
            print(f"✗ Failed to install {package}: {e}")

    # Try to install browser for Playwright
    try:
        subprocess.check_call([sys.executable, '-m', 'playwright', 'install', 'chromium'])
        print("✓ Installed Playwright browser")
    except subprocess.CalledProcessError:
        print("⚠ Playwright browser installation failed - you may need to run 'playwright install' manually")

    print("\nInstallation complete! You can now run: python main.py status")

if __name__ == "__main__":
    install_dependencies()
