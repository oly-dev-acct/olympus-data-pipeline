# =============================================================================
# Invoke-DatabaseBackup.ps1
# Automates SQL Server database backups and cleans up files older than 2 days.
# Designed for use with Windows Task Scheduler (no SQL Agent / SSIS required).
# =============================================================================

# ---------------------------------------------------------------------------
# CONFIGURATION  –  Edit these values to match your environment
# ---------------------------------------------------------------------------
$SqlServer       = "CCPOLYSQL01\SQLEXPRESS"   # e.g. "MYSERVER" or "MYSERVER\SQLEXPRESS"
$SqlUser         = ""                            # Leave blank to use Windows Auth (recommended)
$SqlPassword     = ""                            # Leave blank to use Windows Auth
$BackupRoot      = "\\ccpolyfs01\revenuemanagement\SQLServer Backups"
$RetentionDays   = 2                             # Delete .bak files older than this many days
$LogFile         = "$PSScriptRoot\BackupLog.txt" # Log path (same folder as this script)

$Databases = @(
    "Olympus_Property_Staging",
    "Olympus_Property_Analytics"
)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# LOGGING HELPER
# ---------------------------------------------------------------------------
function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[$timestamp] [$Level] $Message"
    Add-Content -Path $LogFile -Value $line
    Write-Host $line
}

# ---------------------------------------------------------------------------
# BUILD SQL CONNECTION STRING
# ---------------------------------------------------------------------------
function Get-ConnectionString {
    if ($SqlUser -ne "") {
        return "Server=$SqlServer;User Id=$SqlUser;Password=$SqlPassword;"
    } else {
        return "Server=$SqlServer;Integrated Security=True;"
    }
}

# ---------------------------------------------------------------------------
# EXECUTE SQL BACKUP FOR ONE DATABASE
# Returns $true on success, $false on failure.
# ---------------------------------------------------------------------------
function Invoke-Backup {
    param([string]$DbName)

    $dateTime  = (Get-Date -Format "yyyy-MM-dd_HHmmss")
    $bakFile   = Join-Path $BackupRoot "$($DbName)_$dateTime.bak"

    $sql = @"
BACKUP DATABASE [$DbName]
TO DISK = N'$bakFile'
WITH NOFORMAT, NOINIT,
     NAME  = N'$DbName-Full Database Backup',
     SKIP, NOREWIND, NOUNLOAD, STATS = 10;
"@

    Write-Log "Starting backup: $DbName  -->  $bakFile"

    try {
        $conn = New-Object System.Data.SqlClient.SqlConnection (Get-ConnectionString)
        $conn.Open()

        $cmd = $conn.CreateCommand()
        $cmd.CommandText    = $sql
        $cmd.CommandTimeout = 3600   # 1-hour timeout for large DBs

        # Capture STATS = 10 progress messages
        $handler = [System.Data.SqlClient.SqlInfoMessageEventHandler] {
            param($src, $e)
            Write-Log "  $($e.Message)" "PROG"
        }
        $conn.add_InfoMessage($handler)
        $conn.FireInfoMessageEventOnUserErrors = $true

        $cmd.ExecuteNonQuery() | Out-Null
        $conn.Close()

        Write-Log "Backup COMPLETED: $DbName" "OK"
        return $true

    } catch {
        Write-Log "Backup FAILED for $DbName : $_" "ERROR"
        if ($conn.State -eq 'Open') { $conn.Close() }
        return $false
    }
}

# ---------------------------------------------------------------------------
# DELETE .BAK FILES OLDER THAN $RetentionDays ON THE BACKUP SHARE
# Only runs when ALL backups above succeeded.
# ---------------------------------------------------------------------------
function Remove-OldBackups {
    Write-Log "Scanning for .bak files older than $RetentionDays day(s) in: $BackupRoot"

    $cutoff   = (Get-Date).AddDays(-$RetentionDays)
    $allFiles = Get-ChildItem -Path $BackupRoot -Filter "*.bak" -File -ErrorAction Stop
    Write-Log "Total .bak files found on share: $($allFiles.Count)"

    $oldFiles = $allFiles | Where-Object { $_.LastWriteTime -lt $cutoff }
    Write-Log "Files older than $RetentionDays day(s): $($oldFiles.Count)"

    if ($oldFiles.Count -eq 0) {
        Write-Log "No old backup files to delete."
        return
    }

    foreach ($file in $oldFiles) {
        try {
            Write-Log "Deleting: $($file.Name)  [LastWrite: $($file.LastWriteTime)]"
            Remove-Item -Path $file.FullName -Force
            Write-Log "Deleted: $($file.Name)" "OK"
        } catch {
            Write-Log "Could not delete $($file.Name): $_" "WARN"
        }
    }
}

# ===========================================================================
# MAIN
# ===========================================================================
Write-Log "===== Backup job started ====="

$allSucceeded = $true

foreach ($db in $Databases) {
    $result = Invoke-Backup -DbName $db
    if (-not $result) {
        $allSucceeded = $false
    }
}

if ($allSucceeded) {
    Write-Log "All backups succeeded - proceeding with old-file cleanup."
    Remove-OldBackups
} else {
    Write-Log "One or more backups FAILED - skipping cleanup to avoid data loss." "WARN"
}

Write-Log "===== Backup job finished ====="