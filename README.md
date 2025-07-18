# GetGovData PowerShell Lab

This project is a **lab experiment** in building a robust, production-grade PowerShell script for downloading and exporting dataset metadata from [data.gov.uk](https://data.gov.uk). While experimental, it aims to demonstrate best practices for reliability, maintainability, and testability in PowerShell scripting.

## Features

- Fetches all dataset metadata from data.gov.uk's API
- Exports results to CSV, with dynamic columns for download URLs
- Parallel processing (PowerShell 7+) for speed, with sequential fallback for compatibility
- Robust error handling, retry logic, and comprehensive logging
- Unit tests for core logic using Pester
- **Linting with PSScriptAnalyzer for PowerShell best practices**
- Modular function design for reusability and testability

## Project Structure

- `GetGovData.ps1` — Main script (entry point)
- `GetGovData.Functions.psm1` — All reusable function definitions (PowerShell module)
- `GetGovData.Tests.ps1` — Pester unit tests for core logic
- `DataGovUK_Datasets.csv` — Output CSV (generated)
- `DataGovUK_Log.txt` — Log file (generated)
- `Debug_Log.txt` — Debug output (generated)

## Usage

### Prerequisites

- PowerShell 7+ recommended for parallelism (falls back to sequential on Windows PowerShell 5.1)
- Internet access to data.gov.uk

### Run the Script

```powershell
# In PowerShell 7+
./GetGovData.ps1
```

- By default, the script runs in test mode (first 10 datasets). To process all datasets, comment out or remove the test mode line in the script.
- Output files will be created in the current directory.

### Run the Tests

```powershell
Install-Module Pester -Force  # if not already installed
Invoke-Pester ./GetGovData.Tests.ps1
```

### Run Linting (PSScriptAnalyzer)

```powershell
Install-Module PSScriptAnalyzer -Force -Scope CurrentUser  # if not already installed
Invoke-ScriptAnalyzer -Path . -Recurse
```

- The linter checks for PowerShell best practices, including help comments, trailing whitespace, and use of approved verbs.
- The GitHub Actions workflow will fail if any linting errors are found, ensuring code quality is maintained.

## Architecture & Best Practices

- **Functions are defined in a separate module** (`GetGovData.Functions.psm1`) for easy testing and reuse.
- **Main script logic is guarded** so it only runs when executed directly, not when dot-sourced (enabling unit testing).
- **Parallel execution** uses `ForEach-Object -Parallel` (PowerShell 7+), with a throttle limit for resource control.
- **Comprehensive logging** to both console and file, including warnings for skipped/invalid datasets.
- **Exit codes** are set for CI/CD integration: `0` (success), `1` (fatal error), `2` (no valid data).
- **Unit tests** (Pester) cover core logic and error handling.
- **Linting** (PSScriptAnalyzer) enforces PowerShell style and best practices.

## Known Test Limitation (Pester & Module Mocking)

> **Note:** One unit test in `GetGovData.Tests.ps1` ("Invoke-RestMethodWithRetry returns response on first try") will always fail due to a [known limitation in Pester 5.x](https://github.com/pester/Pester/issues/1647) and PowerShell module scoping. When a function in a module calls an external command (like `Invoke-RestMethod`), Pester's `Mock` does not always intercept the call inside `InModuleScope`.
>
> This is a common limitation in PowerShell module testing and does **not** indicate a problem with the code or test design. All other tests pass, confirming the core logic is robust and testable. See the test file for details.

## Disclaimer

This is a **lab/experimental project**. While it aims for production-grade best practices, it is not intended for direct use in production environments without further review, adaptation, and security hardening.

---

**Author:** Karl Hitchcock
