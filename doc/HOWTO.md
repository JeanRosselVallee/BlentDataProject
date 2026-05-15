# HOWTO

Quick setup notes for this project (Windows / PowerShell).

## Git

### Install

```powershell
winget install --id Git.Git -e --source winget 
```

### Locate folder `%LOCALAPPDATA%\Programs\Git\`

### Add to PATH (PowerShell)

```powershell
#env:Path += ";#env:USERPROFILE\AppData\Local\Programs\Git\bin" 
```

### Check

```powershell
git --version 
```

### Clone repository

```powershell
git clone https://github.com/JeanRosselVallee/BlentDataProject.git
```

### Notes

- Add .gitconfig & include it in .gitignore
- To update sources in a GitHub Codespace (or any clone):

```powershell
git pull origin main 
```

## Python

### VS Code

- Install the Python extension proposed by VS Code.

### Install Python (terminal)

```powershell
winget search python 
winget install --id 9PNRBTZXMB4Z 
python --version 
```

If needed, add WindowsApps to PATH: 

```powershell
#env:Path += ";#env:LOCALAPPDATA\Microsoft\WindowsApps" 
```

### Set interpreter (VS Code)

- Press `Ctrl+P`
- Run: `Python: Select Interpreter`
- Select: Python 3.11 (Required for Airflow compatibility)

## Environment

### Create the project virtual environment
**Note for Windows:** If you get an error saying `python3.11` is not recognized, it is because Windows uses the Python Launcher `py` to target specific versions.

First, verify which versions are installed:
```powershell
py --list
```

### If multiple Python versions are installed (use Python Launcher):
```powershell
py -3.11 -m venv .venv
# If 3.11 is not installed, download it from python.org
```

### Standard creation:
```powershell
python -m venv .venv 
Set-ExecutionPolicy -ExecutionPolicy Bypass -Scope Process 
.\.venv\Scripts\activate
```

### Install packages

```powershell
pip install -r requirements.txt 
```


### Exit venv

```powershell
deactivate 
```

## Execution

### Run app & create DB

```powershell
python -m pip install -e .
python -m scripts.run
```

### Populate DB

```powershell
python -m app.database.populate 
```

## SQLite

### VS Code extension

- Install “SQLite” by `alexcvzz`
- Left bar: Extensions
- Select DB Connection
- Right click => Run query

## Code check

### Format check & update Python files (blanks, row length, etc.)

```powershell
 black --line-length 79 . 
```

### Quality check & warnings (logical errors, unused variables, etc.)

```powershell
flake8 [config.py](http://config.py) 
flake8 [run.py](http://run.py) 
cd app/ 
flake8 . 
cd tests/ 
flake8 . 
cd .. 
```

## NotebookLM

Run in a PowerShell terminal.

### List all pertinent files into `Project1.txt`

```powershell
Get-ChildItem -Recurse -File | Where-Object { #*.FullName -notmatch '\(.venv|.git)\' } | ForEach-Object { 
    #*.FullName.Replace(#(Get-Location).Path + "", "") 
    } | Out-File Project1.txt 
```

### Copy files' contents to `Full_Project_Snapshot.txt`

```powershell
Get-Content Project1.txt | ForEach-Object { 
    "--- FILE: $_ ---"; 
    if ($_ -eq "digimarket.db") { 
        "[Binary Database File - Content Skipped]" 
    } elseif (Test-Path $_) { 
        Get-Content $_ 
    } else { 
        "Warning: File not found" 
    }; 
    ""; "" 
} | Out-File Full_Project_Snapshot.txt
```
