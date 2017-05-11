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

import collections


SQLite_header = collections.namedtuple('SQLite_header', (
    'magic',
    'page_size',
    'write_format',
    'read_format',
    'reserved_length',
    'max_payload_fraction',
    'min_payload_fraction',
    'leaf_payload_fraction',
    'file_change_counter',
    'size_in_pages',
    'first_freelist_trunk',
    'freelist_pages',
    'schema_cookie',
    'schema_format',
    'default_page_cache_size',
    'largest_btree_page',
    'text_encoding',
    'user_version',
    'incremental_vacuum',
    'application_id',
    'version_valid',
    'sqlite_version',
))


SQLite_btree_page_header = collections.namedtuple('SQLite_btree_page_header', (
    'page_type',
    'first_freeblock_offset',
    'num_cells',
    'cell_content_offset',
    'num_fragmented_free_bytes',
    'right_most_page_idx',
))


SQLite_ptrmap_info = collections.namedtuple('SQLite_ptrmap_info', (
    'page_idx',
    'page_type',
    'page_ptr',
))


SQLite_record_field = collections.namedtuple('SQLite_record_field', (
    'col_type',
    'col_type_descr',
    'field_length',
    'field_bytes',
))


SQLite_master_record = collections.namedtuple('SQLite_master_record', (
    'type',
    'name',
    'tbl_name',
    'rootpage',
    'sql',
))


type_specs = {
    'INTEGER': int,
    'TEXT': str,
    'VARCHAR': str,
    'LONGVARCHAR': str,
    'REAL': float,
    'FLOAT': float,
    'LONG': int,
    'BLOB': bytes,
}
