# # # -----------------------------
# # # PostgreSQL Database Backup
# # # -----------------------------

# # # PostgreSQL credentials
# # $PG_HOST = "10.200.155.5"
# # $PG_DB = "Olympus_Property"
# # $PG_USER = "pwalgude"
# # $PG_PASSWORD = "Re@donly123"

# # # Path to pg_dump
# # $pgDumpPath = "C:\Program Files\PostgreSQL\18\bin\pg_dump.exe"

# # # Backup directory
# # $BACKUP_DIR = "Z:\postgressql-db-backup"

# # # Create directory if it doesn't exist
# # if (!(Test-Path $BACKUP_DIR)) {
# #     New-Item -ItemType Directory -Path $BACKUP_DIR | Out-Null
# # }

# # # Timestamp for backup file
# # $DATE = Get-Date -Format "yyyyMMdd_HHmmss"

# # # Backup file path
# # $BACKUP_FILE = "$BACKUP_DIR\$PG_DB`_backup_$DATE.dump"

# # # Set environment variables
# # $env:PGPASSWORD = $PG_PASSWORD
# # $env:PGSSLMODE = "disable"

# # Write-Host "Starting backup..."

# # # Run pg_dump
# # & "$pgDumpPath" `
# # -h $PG_HOST `
# # -U $PG_USER `
# # -d $PG_DB `
# # -F c `
# # -b `
# # -v `
# # -f "$BACKUP_FILE"

# # # Remove password from environment
# # Remove-Item Env:\PGPASSWORD

# # Write-Host "Backup completed: $BACKUP_FILE"

# # # -----------------------------
# # # Delete backups older than 7 days
# # # -----------------------------

# # Get-ChildItem "$BACKUP_DIR\$PG_DB`_backup_*.dump" |
# # Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-7) } |
# # Remove-Item -Force

# # Write-Host "Old backups older than 7 days deleted (if any)."




# # -----------------------------
# # PostgreSQL Database Backup
# # -----------------------------

# # # PostgreSQL credentials
# # $PG_HOST = "10.200.155.5"
# # $PG_DB = "Olympus_Property"
# # # $PG_USER = "postgres"
# # # $PG_PASSWORD = "Olympus4"
# # $PG_USER = "pwalgude"
# # $PG_PASSWORD = "Re@donly123"



# # PostgreSQL credentials
# $PG_HOST = "10.200.155.5"
# $PG_DB = "Olympus_Property"
# $PG_USER = "postgres"
# $PG_PASSWORD = "Olympus321"

# # Path to pg_dump
# $pgDumpPath = "C:\Program Files\PostgreSQL\18\bin\pg_dump.exe"

# # Backup directory (temporary local folder first)
# $LOCAL_DIR = "C:\Temp\Postgres_Backup"
# $BACKUP_DIR = "Z:\postgressql-db-backup"

# # Create directories if they don't exist
# foreach ($dir in @($LOCAL_DIR, $BACKUP_DIR)) {
#     if (!(Test-Path $dir)) {
#         New-Item -ItemType Directory -Path $dir | Out-Null
#     }
# }

# # Timestamp for backup file
# $DATE = Get-Date -Format "yyyyMMdd_HHmmss"
# $TEMP_BACKUP_FILE = "$LOCAL_DIR\$PG_DB`_backup_$DATE.dump"
# $FINAL_BACKUP_FILE = "$BACKUP_DIR\$PG_DB`_backup_$DATE.dump"

# # Set environment variables for pg_dump
# $env:PGPASSWORD = $PG_PASSWORD
# $env:PGSSLMODE = "disable"

# Write-Host "Starting backup for database '$PG_DB'..."

# # Run pg_dump
# & "$pgDumpPath" `
#     -h $PG_HOST `
#     -U $PG_USER `
#     -d $PG_DB `
#     -F c `
#     -b `
#     -v `
#     -f "$TEMP_BACKUP_FILE"

# # Remove password from environment
# Remove-Item Env:\PGPASSWORD

# # Check if backup file was created and has size > 0
# if ((Test-Path $TEMP_BACKUP_FILE) -and ((Get-Item $TEMP_BACKUP_FILE).Length -gt 0)) {

#     # Move to final backup location
#     Move-Item $TEMP_BACKUP_FILE $FINAL_BACKUP_FILE -Force
#     Write-Host "Backup SUCCESSFUL: $FINAL_BACKUP_FILE"

#     # -----------------------------
#     # Delete backups older than 7 days
#     # -----------------------------
#     Get-ChildItem "$BACKUP_DIR\$PG_DB`_backup_*.dump" |
#         Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-7) } |
#         Remove-Item -Force

#     Write-Host "Old backups older than 7 days deleted (if any)."

# } else {
#     Write-Host "ERROR: Backup failed or dump file is empty!"
#     if (Test-Path $TEMP_BACKUP_FILE) { Remove-Item $TEMP_BACKUP_FILE -Force }
# }




# -----------------------------
# PostgreSQL Database Backup Script
# -----------------------------

# PostgreSQL credentials
$PG_HOST = "10.200.155.5"
$PG_DB = "Olympus_Property"
$PG_USER = "postgres"
$PG_PASSWORD = "Olympus321"

# Path to pg_dump
$pgDumpPath = "C:\Program Files\PostgreSQL\18\bin\pg_dump.exe"

# Backup directories
$LOCAL_DIR = "C:\Temp\Postgres_Backup"
$BACKUP_DIR = "Z:\postgressql-db-backup"

# Create directories if they do not exist
foreach ($dir in @($LOCAL_DIR, $BACKUP_DIR)) {
    if (!(Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir | Out-Null
    }
}

# Timestamp
$DATE = Get-Date -Format "yyyyMMdd_HHmmss"

# Backup file names
$TEMP_DUMP_FILE = "$LOCAL_DIR\$PG_DB`_backup_$DATE.dump"
$FINAL_DUMP_FILE = "$BACKUP_DIR\$PG_DB`_backup_$DATE.dump"

$TEMP_SQL_FILE = "$LOCAL_DIR\$PG_DB`_backup_$DATE.sql"
$FINAL_SQL_FILE = "$BACKUP_DIR\$PG_DB`_backup_$DATE.sql"

# Set environment variables
$env:PGPASSWORD = $PG_PASSWORD
$env:PGSSLMODE = "disable"

Write-Host "Starting PostgreSQL backup for database '$PG_DB'..."

# -------------------------------------------------
# 1. Create Binary Backup (.dump)
# -------------------------------------------------

Write-Host "Creating binary backup (.dump)..."

& "$pgDumpPath" `
    -h $PG_HOST `
    -U $PG_USER `
    -d $PG_DB `
    -F c `
    -b `
    -v `
    -f "$TEMP_DUMP_FILE"

# -------------------------------------------------
# 2. Create SQL Backup (.sql) with DDL + DML + Procedures
# -------------------------------------------------

Write-Host "Creating SQL backup (.sql)..."

& "$pgDumpPath" `
    -h $PG_HOST `
    -U $PG_USER `
    -d $PG_DB `
    -F p `
    -v `
    -f "$TEMP_SQL_FILE"

# Remove password from environment
Remove-Item Env:\PGPASSWORD

# -------------------------------------------------
# 3. Validate and move binary backup
# -------------------------------------------------

if ((Test-Path $TEMP_DUMP_FILE) -and ((Get-Item $TEMP_DUMP_FILE).Length -gt 0)) {

    Move-Item $TEMP_DUMP_FILE $FINAL_DUMP_FILE -Force
    Write-Host "Binary Backup SUCCESSFUL: $FINAL_DUMP_FILE"

} else {

    Write-Host "ERROR: Binary backup failed!"
    if (Test-Path $TEMP_DUMP_FILE) { Remove-Item $TEMP_DUMP_FILE -Force }

}

# -------------------------------------------------
# 4. Validate and move SQL backup
# -------------------------------------------------

if ((Test-Path $TEMP_SQL_FILE) -and ((Get-Item $TEMP_SQL_FILE).Length -gt 0)) {

    Move-Item $TEMP_SQL_FILE $FINAL_SQL_FILE -Force
    Write-Host "SQL Backup SUCCESSFUL: $FINAL_SQL_FILE"

} else {

    Write-Host "ERROR: SQL backup failed!"
    if (Test-Path $TEMP_SQL_FILE) { Remove-Item $TEMP_SQL_FILE -Force }

}

# -------------------------------------------------
# 5. Delete backups older than 7 days
# -------------------------------------------------

Write-Host "Cleaning old backups..."

Get-ChildItem "$BACKUP_DIR\$PG_DB`_backup_*" |
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-7) } |
    Remove-Item -Force

Write-Host "Old backups older than 7 days deleted."

Write-Host "PostgreSQL Backup Process Completed."