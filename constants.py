VALID_PAGE_SIZES = (1, 512, 1024, 2048, 4096, 8192, 16384, 32768)

SQLITE_TABLE_COLUMNS = {
    'sqlite_master': ('type', 'name', 'tbl_name', 'rootpage', 'sql',),
    'sqlite_sequence': ('name', 'seq',),
    'sqlite_stat1': ('tbl', 'idx', 'stat',),
    'sqlite_stat2': ('tbl', 'idx', 'sampleno', 'sample'),
    'sqlite_stat3': ('tbl', 'idx', 'nEq', 'nLt', 'nDLt', 'sample'),
    'sqlite_stat4': ('tbl', 'idx', 'nEq', 'nLt', 'nDLt', 'sample'),
}
