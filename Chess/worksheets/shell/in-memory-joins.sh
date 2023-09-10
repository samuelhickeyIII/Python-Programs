mcsSetConfig HashJoin PmMaxMemorySmallSide 2G
# Configures the amount of memory available for a single join. 
# Valid values are from 0 to 4 GB.
# Default value is 1 GB.

mcsSetConfig HashJoin TotalUmMemory '85%'
# Configures the amount of memory available for all joins.
# Values can be specified as a percentage of total system memory or as a specific amount of memory.
# Valid percentage values are from 0 to 100%.
# Default value is 25%.
