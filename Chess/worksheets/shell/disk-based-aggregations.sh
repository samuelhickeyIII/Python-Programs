mcsSetConfig RowAggregation AllowDiskBasedAggregation Y
# Enables disk-based aggregations
# Valid values are Y and N
# Default value is N

mcsSetConfig RowAggregation Compression SNAPPY
# Enables compression of temporary files used for disk-based aggregations
# Default value is SNAPPY

mcsSetConfig SystemConfig SystemTempFileDir /tmp/columnstore_tmp_files
# Configures the directory used for temporary files used by disk-based joins and aggregations
# Default value is /tmp/columnstore_tmp_files
