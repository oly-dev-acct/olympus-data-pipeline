$dumpFile = "Z:\postgressql-db-backup\Olympus_Property_backup_20260311_062507.dump"
$pgRestore = "C:\Program Files\PostgreSQL\18\bin\pg_restore.exe"

# Use & to call the executable
& $pgRestore --list $dumpFile