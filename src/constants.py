# MIT License
#
# Copyright (c) 2017 Matt Boyer
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

VALID_PAGE_SIZES = (1, 512, 1024, 2048, 4096, 8192, 16384, 32768)

SQLITE_TABLE_COLUMNS = {
    'sqlite_master': ('type', 'name', 'tbl_name', 'rootpage', 'sql',),
    'sqlite_sequence': ('name', 'seq',),
    'sqlite_stat1': ('tbl', 'idx', 'stat',),
    'sqlite_stat2': ('tbl', 'idx', 'sampleno', 'sample'),
    'sqlite_stat3': ('tbl', 'idx', 'nEq', 'nLt', 'nDLt', 'sample'),
    'sqlite_stat4': ('tbl', 'idx', 'nEq', 'nLt', 'nDLt', 'sample'),
}

# These are the integers used in ptrmap entries to designate the kind of page
# for which a given ptrmap entry holds a notional "child to parent" pointer
BTREE_ROOT_PAGE = 1
FREELIST_PAGE = 2
FIRST_OFLOW_PAGE = 3
NON_FIRST_OFLOW_PAGE = 4
BTREE_NONROOT_PAGE = 5

PTRMAP_PAGE_TYPES = (
    BTREE_ROOT_PAGE,
    FREELIST_PAGE,
    FIRST_OFLOW_PAGE,
    NON_FIRST_OFLOW_PAGE,
    BTREE_NONROOT_PAGE,
)

OVERFLOW_PAGE_TYPES = (
    FIRST_OFLOW_PAGE,
    NON_FIRST_OFLOW_PAGE,
)

# These are identifiers used internally to keep track of page types *before*
# specialised objects can be instantiated
FREELIST_TRUNK_PAGE = 'freelist_trunk'
FREELIST_LEAF_PAGE = 'freelist_leaf'
PTRMAP_PAGE = 'ptrmap_page'
UNKNOWN_PAGE = 'unknown'

FREELIST_PAGE_TYPES = (
    FREELIST_TRUNK_PAGE,
    FREELIST_LEAF_PAGE,
)

NON_BTREE_PAGE_TYPES = (
    FREELIST_TRUNK_PAGE,
    FIRST_OFLOW_PAGE,
    NON_FIRST_OFLOW_PAGE,
    PTRMAP_PAGE,
)
