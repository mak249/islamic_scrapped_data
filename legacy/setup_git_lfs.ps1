# PowerShell script to set up Git LFS and prepare for push
# Run this script to set up everything for pushing to GitHub

Write-Host "🔧 Setting up Git LFS for database files..." -ForegroundColor Cyan

# Check if Git LFS is installed
try {
    $lfsVersion = git lfs version 2>&1
    Write-Host "✅ Git LFS is installed: $lfsVersion" -ForegroundColor Green
} catch {
    Write-Host "❌ Git LFS is not installed!" -ForegroundColor Red
    Write-Host "Please install Git LFS from: https://git-lfs.github.com/" -ForegroundColor Yellow
    Write-Host "Or run: winget install --id=Git.Git-LFS -e" -ForegroundColor Yellow
    exit 1
}

# Initialize Git LFS in this repository
Write-Host "`n📦 Initializing Git LFS..." -ForegroundColor Cyan
git lfs install

# Track database files
Write-Host "`n📝 Setting up .gitattributes for LFS tracking..." -ForegroundColor Cyan
if (Test-Path ".gitattributes") {
    Write-Host "✅ .gitattributes already exists" -ForegroundColor Green
} else {
    Write-Host "❌ .gitattributes not found!" -ForegroundColor Red
    exit 1
}

# Add .gitattributes first
Write-Host "`n➕ Adding .gitattributes..." -ForegroundColor Cyan
git add .gitattributes

# Force add database files with LFS
Write-Host "`n➕ Adding database files with LFS..." -ForegroundColor Cyan
git add -f *.db

# Add all other files
Write-Host "`n➕ Adding all other files..." -ForegroundColor Cyan
git add .

# Show status
Write-Host "`n📊 Git Status:" -ForegroundColor Cyan
git status --short | Select-Object -First 20

# Check LFS files
Write-Host "`n📦 Files tracked by Git LFS:" -ForegroundColor Cyan
git lfs ls-files

Write-Host "`n✅ Setup complete!" -ForegroundColor Green
Write-Host "`nNext steps:" -ForegroundColor Yellow
Write-Host "1. Review the changes: git status" -ForegroundColor White
Write-Host "2. Commit: git commit -m 'Add all scrape data with Git LFS'" -ForegroundColor White
Write-Host "3. Push: git push -u origin main" -ForegroundColor White
Write-Host "`nNote: You may need to authenticate with GitHub (use Personal Access Token)" -ForegroundColor Yellow
