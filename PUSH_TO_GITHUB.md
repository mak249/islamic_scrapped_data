# Instructions to Push to GitHub

## Step 1: Install Git LFS (if not already installed)
```powershell
# Download and install from: https://git-lfs.github.com/
# Or use winget:
winget install --id=Git.Git-LFS -e
```

## Step 2: Initialize Git LFS in this repository
```powershell
cd "d:\web scraping"
git lfs install
```

## Step 3: Add files and commit
```powershell
# Add .gitattributes (already created)
git add .gitattributes

# Add all database files (they will be tracked by LFS)
git add *.db

# Add all other files
git add .

# Commit everything
git commit -m "Add all scrape data with Git LFS for databases"
```

## Step 4: Push to GitHub
```powershell
# Push to GitHub (you'll need to authenticate)
git push -u origin main
```

**Note:** If you get authentication errors:
1. Create a Personal Access Token at: https://github.com/settings/tokens
2. Use the token as your password when prompted
3. Or configure Git credential helper

## Step 5: Verify LFS files
```powershell
# Check that LFS is tracking files correctly
git lfs ls-files
```

---

## For Future Scraping and Pushing

### Continue Scraping
```powershell
# Continue from last question_id (scrapes next 10,000 by default)
python continue_scraping.py

# Or with custom range
python continue_scraping.py --start-id 50000 --end-id 60000
```

### After Scraping, Push Updates
```powershell
# Add changes
git add .

# Commit
git commit -m "Update: Scraped question_ids X to Y"

# Push
git push origin main
```

The resume functionality automatically tracks the last question_id, so you can always continue from where you left off!
