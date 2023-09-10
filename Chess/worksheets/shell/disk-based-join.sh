mcsSetConfig HashJoin AllowDiskBasedJoin Y
# Enables disk-based joins
# Valid values are Y and N
# Default value is N

mcsSetConfig HashJoin TempFileCompression Y
# Enables compression for temporary files used by disk-based joins
# Valid values are Y and N
# Default value is N

mcsSetConfig SystemConfig SystemTempFileDir /mariadb/tmp
# Configures the directory used for temporary files used by disk-based joins and aggregations
# Default value is /tmp/columnstore_tmp_files
