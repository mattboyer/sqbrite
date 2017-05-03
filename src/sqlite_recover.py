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

from . import constants
from . import PROJECT_NAME, PROJECT_DESCRIPTION, USER_YAML_PATH, BUILTIN_YAML

import argparse
import collections
import csv
import logging
import os
import os.path
import pdb
import pkg_resources
import re
import shutil
import sqlite3
import stat
import struct
import tempfile
import yaml


logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s')
_LOGGER = logging.getLogger('SQLite recovery')
_LOGGER.setLevel(logging.INFO)


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


heuristics = {}
signatures = {}


def heuristic_factory(magic, offset):
    assert(isinstance(magic, bytes))
    assert(isinstance(offset, int))
    assert(offset >= 0)

    # We only need to compile the regex once
    magic_re = re.compile(magic)

    def generic_heuristic(freeblock_bytes):
        all_matches = [match for match in magic_re.finditer(freeblock_bytes)]
        for magic_match in all_matches[::-1]:
            header_start = magic_match.start()-offset
            if header_start < 0:
                _LOGGER.debug("Header start outside of freeblock!")
                break
            yield header_start
    return generic_heuristic


def load_heuristics():

    def _load_from_yaml(yaml_string):
        if isinstance(yaml_string, bytes):
            yaml_string = yaml_string.decode('utf-8')

        raw_yaml = yaml.load(yaml_string)
        for table_grouping, tables in raw_yaml.items():
            _LOGGER.info(
                "Loading raw_yaml for table grouping \"%s\"",
                table_grouping
            )
            for table_name, table_props in tables.items():
                heuristics[table_name] = heuristic_factory(
                    table_props['magic'], table_props['offset']
                )
                _LOGGER.debug("Loaded heuristics for \"%s\"", table_name)

    with pkg_resources.resource_stream(PROJECT_NAME, BUILTIN_YAML) as builtin:
        try:
            _load_from_yaml(builtin.read())
        except KeyError:
            raise SystemError("Malformed builtin magic file")

    if not os.path.exists(USER_YAML_PATH):
        return
    with open(USER_YAML_PATH, 'r') as user_yaml:
        try:
            _load_from_yaml(user_yaml.read())
        except KeyError:
            raise SystemError("Malformed user magic file")


class IndexDict(dict):
    def __iter__(self):
        for k in sorted(self.keys()):
            yield k


class SQLite_DB(object):
    def __init__(self, path):
        self._path = path
        self._page_types = {}
        self._header = self.parse_header()

        self._page_cache = None
        # Actual page objects go here
        self._pages = {}
        self.build_page_cache()

        self._ptrmap = {}

        # TODO Do we need all of these?
        self._table_roots = {}
        self._page_tables = {}
        self._tables = {}
        self._table_columns = {}
        self._freelist_leaves = []
        self._freelist_btree_pages = []

    @property
    def ptrmap(self):
        return self._ptrmap

    @property
    def header(self):
        return self._header

    @property
    def pages(self):
        return self._pages

    @property
    def tables(self):
        return self._tables

    @property
    def freelist_leaves(self):
        return self._freelist_leaves

    @property
    def table_columns(self):
        return self._table_columns

    def page_bytes(self, page_idx):
        try:
            return self._page_cache[page_idx]
        except KeyError:
            raise ValueError("No cache for page %d", page_idx)

    def map_table_page(self, page_idx, table):
        assert isinstance(page_idx, int)
        assert isinstance(table, Table)
        self._page_tables[page_idx] = table

    def get_page_table(self, page_idx):
        assert isinstance(page_idx, int)
        try:
            return self._page_tables[page_idx]
        except KeyError:
            return None

    def __repr__(self):
        return '<SQLite DB, page count: {} | page size: {}>'.format(
            self.header.size_in_pages,
            self.header.page_size
        )

    def parse_header(self):
        header_bytes = None
        file_size = None
        with open(self._path, 'br') as sqlite:
            header_bytes = sqlite.read(100)
            file_size = os.fstat(sqlite.fileno())[stat.ST_SIZE]

        if not header_bytes:
            raise ValueError("Couldn't read SQLite header")
        assert isinstance(header_bytes, bytes)
        # This DB header is always big-endian
        fields = SQLite_header(*struct.unpack(
            r'>16sHBBBBBBIIIIIIIIIIII20xII',
            header_bytes[:100]
        ))
        assert fields.page_size in constants.VALID_PAGE_SIZES
        db_size = fields.page_size * fields.size_in_pages
        assert db_size <= file_size
        assert (fields.page_size > 0) and \
            (fields.file_change_counter == fields.version_valid)

        if file_size < 1073741824:
            _LOGGER.debug("No lock-byte page in this file!")

        if fields.first_freelist_trunk > 0:
            self._page_types[fields.first_freelist_trunk] = \
                constants.FREELIST_TRUNK_PAGE
        _LOGGER.debug(fields)
        return fields

    def build_page_cache(self):
        # The SQLite docs use a numbering convention for pages where the
        # first page (the one that has the header) is page 1, with the next
        # ptrmap page being page 2, etc.
        page_cache = [None, ]
        with open(self._path, 'br') as sqlite:
            for page_idx in range(self._header.size_in_pages):
                page_offset = page_idx * self._header.page_size
                sqlite.seek(page_offset, os.SEEK_SET)
                page_cache.append(sqlite.read(self._header.page_size))
        self._page_cache = page_cache
        for page_idx in range(1, len(self._page_cache)):
            # We want these to be temporary objects, to be replaced with
            # more specialised objects as parsing progresses
            self._pages[page_idx] = Page(page_idx, self)

    def populate_freelist_pages(self):
        if 0 == self._header.first_freelist_trunk:
            _LOGGER.debug("This database has no freelist trunk page")
            return

        _LOGGER.info("Parsing freelist pages")
        parsed_trunks = 0
        parsed_leaves = 0
        freelist_trunk_idx = self._header.first_freelist_trunk

        while freelist_trunk_idx != 0:
            _LOGGER.debug(
                "Parsing freelist trunk page %d",
                freelist_trunk_idx
            )
            trunk_bytes = bytes(self.pages[freelist_trunk_idx])

            next_freelist_trunk_page_idx, num_leaf_pages = struct.unpack(
                r'>II',
                trunk_bytes[:8]
            )

            # Now that we know how long the array of freelist page pointers is,
            # let's read it again
            trunk_array = struct.unpack(
                r'>{count}I'.format(count=2+num_leaf_pages),
                trunk_bytes[:(4*(2+num_leaf_pages))]
            )

            # We're skipping the first entries as they are realy the next trunk
            # index and the leaf count
            # TODO Fix that
            leaves_in_trunk = []
            for page_idx in trunk_array[2:]:
                # Let's prepare a specialised object for this freelist leaf
                # page
                leaf_page = FreelistLeafPage(
                    page_idx, self, freelist_trunk_idx
                )
                leaves_in_trunk.append(leaf_page)
                self._freelist_leaves.append(page_idx)
                self._pages[page_idx] = leaf_page

                self._page_types[page_idx] = constants.FREELIST_LEAF_PAGE

            trunk_page = FreelistTrunkPage(
                freelist_trunk_idx,
                self,
                leaves_in_trunk
            )
            self._pages[freelist_trunk_idx] = trunk_page
            # We've parsed this trunk page
            parsed_trunks += 1
            # ...And every leaf in it
            parsed_leaves += num_leaf_pages

            freelist_trunk_idx = next_freelist_trunk_page_idx

        assert (parsed_trunks + parsed_leaves) == self._header.freelist_pages
        _LOGGER.info(
            "Freelist summary: %d trunk pages, %d leaf pages",
            parsed_trunks,
            parsed_leaves
        )

    def populate_overflow_pages(self):
        # Knowledge of the overflow pages can come from the pointer map (easy),
        # or the parsing of individual cells in table leaf pages (hard)
        #
        # For now, assume we already have a page type dict populated from the
        # ptrmap
        _LOGGER.info("Parsing overflow pages")
        overflow_count = 0
        for page_idx in sorted(self._page_types):
            page_type = self._page_types[page_idx]
            if page_type not in constants.OVERFLOW_PAGE_TYPES:
                continue
            overflow_page = OverflowPage(page_idx, self)
            self.pages[page_idx] = overflow_page
            overflow_count += 1

        _LOGGER.info("Overflow summary: %d pages", overflow_count)

    def populate_ptrmap_pages(self):
        if self._header.largest_btree_page == 0:
            # We don't have ptrmap pages in this DB. That sucks.
            _LOGGER.warning("%r does not have ptrmap pages!", self)
            for page_idx in range(1, self._header.size_in_pages):
                self._page_types[page_idx] = constants.UNKNOWN_PAGE
            return

        _LOGGER.info("Parsing ptrmap pages")

        ptrmap_page_idx = 2
        usable_size = self._header.page_size - self._header.reserved_length
        num_ptrmap_entries_in_page = usable_size // 5
        ptrmap_page_indices = []

        ptrmap_page_idx = 2
        while ptrmap_page_idx <= self._header.size_in_pages:
            page_bytes = self._page_cache[ptrmap_page_idx]
            ptrmap_page_indices.append(ptrmap_page_idx)
            self._page_types[ptrmap_page_idx] = constants.PTRMAP_PAGE
            page_ptrmap_entries = {}

            ptrmap_bytes = page_bytes[:5 * num_ptrmap_entries_in_page]
            for entry_idx in range(num_ptrmap_entries_in_page):
                ptr_page_idx = ptrmap_page_idx + entry_idx + 1
                page_type, page_ptr = struct.unpack(
                    r'>BI',
                    ptrmap_bytes[5*entry_idx:5*(entry_idx+1)]
                )
                if page_type == 0:
                    break

                ptrmap_entry = SQLite_ptrmap_info(
                    ptr_page_idx, page_type, page_ptr
                )
                assert ptrmap_entry.page_type in constants.PTRMAP_PAGE_TYPES
                if page_type == constants.BTREE_ROOT_PAGE:
                    assert page_ptr == 0
                    self._page_types[ptr_page_idx] = page_type

                elif page_type == constants.FREELIST_PAGE:
                    # Freelist pages are assumed to be known already
                    assert self._page_types[ptr_page_idx] in \
                        constants.FREELIST_PAGE_TYPES
                    assert page_ptr == 0

                elif page_type == constants.FIRST_OFLOW_PAGE:
                    assert page_ptr != 0
                    self._page_types[ptr_page_idx] = page_type

                elif page_type == constants.NON_FIRST_OFLOW_PAGE:
                    assert page_ptr != 0
                    self._page_types[ptr_page_idx] = page_type

                elif page_type == constants.BTREE_NONROOT_PAGE:
                    assert page_ptr != 0
                    self._page_types[ptr_page_idx] = page_type

                # _LOGGER.debug("%r", ptrmap_entry)
                self._ptrmap[ptr_page_idx] = ptrmap_entry
                page_ptrmap_entries[ptr_page_idx] = ptrmap_entry

            page = PtrmapPage(ptrmap_page_idx, self, page_ptrmap_entries)
            self._pages[ptrmap_page_idx] = page
            _LOGGER.debug("%r", page)
            ptrmap_page_idx += num_ptrmap_entries_in_page + 1

        _LOGGER.info(
            "Ptrmap summary: %d pages, %r",
            len(ptrmap_page_indices), ptrmap_page_indices
        )

    def populate_btree_pages(self):
        # TODO Should this use table information instead of scanning all pages?
        page_idx = 1
        while page_idx <= self._header.size_in_pages:
            try:
                if self._page_types[page_idx] in \
                        constants.NON_BTREE_PAGE_TYPES:
                    page_idx += 1
                    continue
            except KeyError:
                pass

            try:
                page_obj = BTreePage(page_idx, self)
            except ValueError:
                # This page isn't a valid btree page. This can happen if we
                # don't have a ptrmap to guide us
                _LOGGER.warning(
                    "Page %d (%s) is not a btree page",
                    page_idx,
                    self._page_types[page_idx]
                )
                page_idx += 1
                continue

            page_obj.parse_cells()
            self._page_types[page_idx] = page_obj.page_type
            self._pages[page_idx] = page_obj
            page_idx += 1

    def _parse_master_leaf_page(self, page):
        for cell_idx in page.cells:
            _, master_record = page.cells[cell_idx]
            assert isinstance(master_record, Record)
            fields = [
                master_record.fields[idx].value for idx in master_record.fields
            ]
            master_record = SQLite_master_record(*fields)
            if 'table' != master_record.type:
                continue

            self._table_roots[master_record.name] = \
                self.pages[master_record.rootpage]

            # This record describes a table in the schema, which means it
            # includes a SQL statement that defines the table's columns
            # We need to parse the field names out of that statement
            assert master_record.sql.startswith('CREATE TABLE')
            columns_re = re.compile(r'^CREATE TABLE (\S+) \((.*)\)$')
            match = columns_re.match(master_record.sql)
            if match:
                assert match.group(1) == master_record.name
                column_list = match.group(2)
                csl_between_parens_re = re.compile(r'\([^)]+\)')
                expunged = csl_between_parens_re.sub('', column_list)

                cols = [
                    statement.strip() for statement in expunged.split(',')
                ]
                cols = [
                    statement for statement in cols if not (
                        statement.startswith('PRIMARY') or
                        statement.startswith('UNIQUE')
                    )
                ]
                columns = [col.split()[0] for col in cols]
                signature = []

                # Some column definitions lack a type
                for col_def in cols:
                    def_tokens = col_def.split()
                    try:
                        col_type = def_tokens[1]
                    except IndexError:
                        signature.append(object)
                        continue

                    _LOGGER.debug(
                        "Column \"%s\" is defined as \"%s\"",
                        def_tokens[0], col_type
                    )
                    try:
                        signature.append(type_specs[col_type])
                    except KeyError:
                        _LOGGER.warning("No native type for \"%s\"", col_def)
                        signature.append(object)
                _LOGGER.info(
                    "Signature for table \"%s\": %r",
                    master_record.name, signature
                )
                signatures[master_record.name] = signature

                _LOGGER.info(
                    "Columns for table \"%s\": %r",
                    master_record.name, columns
                )
                self._table_columns[master_record.name] = columns

    def map_tables(self):
        first_page = self.pages[1]
        assert isinstance(first_page, BTreePage)

        master_table = Table('sqlite_master', self, first_page)
        self._table_columns.update(constants.SQLITE_TABLE_COLUMNS)

        for master_leaf in master_table.leaves:
            self._parse_master_leaf_page(master_leaf)

        assert all(
            isinstance(root, BTreePage) for root in self._table_roots.values()
        )
        assert all(
            root.parent is None for root in self._table_roots.values()
        )

        self.map_table_page(1, master_table)
        self._table_roots['sqlite_master'] = self.pages[1]

        for table_name, rootpage in self._table_roots.items():
            try:
                table_obj = Table(table_name, self, rootpage)
            except Exception as ex:  # pylint:disable=W0703
                pdb.set_trace()
                _LOGGER.warning(
                    "Caught %r while instantiating table object for \"%s\"",
                    ex, table_name
                )
            else:
                self._tables[table_name] = table_obj

    def reparent_orphaned_table_leaf_pages(self):
        reparented_pages = []
        for page in self.pages.values():
            if not isinstance(page, BTreePage):
                continue
            if page.page_type != "Table Leaf":
                continue

            table = page.table
            if not table:
                parent = page
                root_table = None
                while parent:
                    root_table = parent.table
                    parent = parent.parent
                if root_table is None:
                    self._freelist_btree_pages.append(page)

                if root_table is None:
                    if not page.cells:
                        continue

                    first_record = page.cells[0][1]
                    matches = []
                    for table_name in signatures:
                        # All records within a given page are for the same
                        # table
                        if self.tables[table_name].check_signature(
                                first_record):
                            matches.append(self.tables[table_name])
                    if not matches:
                        _LOGGER.error(
                            "Couldn't find a matching table for %r",
                            page
                        )
                        continue
                    if len(matches) > 1:
                        _LOGGER.error(
                            "Multiple matching tables for %r: %r",
                            page, matches
                        )
                        continue
                    elif len(matches) == 1:
                        root_table = matches[0]

                _LOGGER.debug(
                    "Reparenting %r to table \"%s\"",
                    page, root_table.name
                )
                root_table.add_leaf(page)
                self.map_table_page(page.idx, root_table)
                reparented_pages.append(page)

        if reparented_pages:
            _LOGGER.info(
                "Reparented %d pages: %r",
                len(reparented_pages), [p.idx for p in reparented_pages]
            )

    def grep(self, needle):
        match_found = False
        page_idx = 1
        needle_re = re.compile(needle.encode('utf-8'))
        while (page_idx <= self.header.size_in_pages):
            page = self.pages[page_idx]
            page_offsets = []
            for match in needle_re.finditer(bytes(page)):
                needle_offset = match.start()
                page_offsets.append(needle_offset)
            if page_offsets:
                _LOGGER.info(
                    "Found search term in page %r @ offset(s) %s",
                    page, ', '.join(str(offset) for offset in page_offsets)
                )
            page_idx += 1
        if not match_found:
            _LOGGER.warning(
                "Search term not found",
            )


class Table(object):
    def __init__(self, name, db, rootpage):
        self._name = name
        self._db = db
        assert(isinstance(rootpage, BTreePage))
        self._root = rootpage
        self._leaves = []
        try:
            self._columns = self._db.table_columns[self.name]
        except KeyError:
            self._columns = None

        # We want this to be a list of leaf-type pages, sorted in the order of
        # their smallest rowid
        self._populate_pages()

    @property
    def name(self):
        return self._name

    def add_leaf(self, leaf_page):
        self._leaves.append(leaf_page)

    @property
    def columns(self):
        return self._columns

    def __repr__(self):
        return "<SQLite table \"{}\", root: {}, leaves: {}>".format(
            self.name, self._root.idx, len(self._leaves)
        )

    def _populate_pages(self):
        _LOGGER.info("Page %d is root for %s", self._root.idx, self.name)
        table_pages = [self._root]

        if self._root.btree_header.right_most_page_idx is not None:
            rightmost_idx = self._root.btree_header.right_most_page_idx
            rightmost_page = self._db.pages[rightmost_idx]
            if rightmost_page is not self._root:
                _LOGGER.info(
                    "Page %d is rightmost for %s",
                    rightmost_idx, self.name
                )
                table_pages.append(rightmost_page)

        page_queue = list(table_pages)
        while page_queue:
            table_page = page_queue.pop(0)
            # table_pages is initialised with the table's rootpage, which
            # may be a leaf page for a very small table
            if table_page.page_type != 'Table Interior':
                self._leaves.append(table_page)
                continue

            for cell_idx in table_page.cells:
                page_ptr, max_row_in_page = table_page.cells[cell_idx]

                page = self._db.pages[page_ptr]
                _LOGGER.debug("B-Tree cell: (%r, %d)", page, max_row_in_page)
                table_pages.append(page)
                if page.page_type == 'Table Interior':
                    page_queue.append(page)
                elif page.page_type == 'Table Leaf':
                    self._leaves.append(page)

        assert(all(p.page_type == 'Table Leaf' for p in self._leaves))
        for page in table_pages:
            self._db.map_table_page(page.idx, self)

    @property
    def leaves(self):
        for leaf_page in self._leaves:
            yield leaf_page

    def recover_records(self):
        for page in self.leaves:
            assert isinstance(page, BTreePage)
            if not page.freeblocks:
                continue

            _LOGGER.info("%r", page)
            page.recover_freeblock_records()
            page.print_recovered_records()

    def csv_dump(self, out_dir):
        csv_path = os.path.join(out_dir, self.name + '.csv')
        if os.path.exists(csv_path):
            raise ValueError("Output file {} exists!".format(csv_path))

        _LOGGER.info("Dumping table \"%s\" to CSV", self.name)
        with tempfile.TemporaryFile('w+', newline='') as csv_temp:
            writer = csv.DictWriter(csv_temp, fieldnames=self._columns)
            writer.writeheader()

            for leaf_page in self.leaves:
                for cell_idx in leaf_page.cells:
                    rowid, record = leaf_page.cells[cell_idx]
                    # assert(self.check_signature(record))

                    _LOGGER.debug('Record %d: %r', rowid, record.header)
                    fields_iter = (
                        repr(record.fields[idx]) for idx in record.fields
                    )
                    _LOGGER.debug(', '.join(fields_iter))

                    values_iter = (
                        record.fields[idx].value for idx in record.fields
                    )
                    writer.writerow(dict(zip(self._columns, values_iter)))

                if not leaf_page.recovered_records:
                    continue

                # Recovered records are in an unordered set because their rowid
                # has been lost, making sorting impossible
                for record in leaf_page.recovered_records:
                    values_iter = (
                        record.fields[idx].value for idx in record.fields
                    )
                    writer.writerow(dict(zip(self._columns, values_iter)))

            if csv_temp.tell() > 0:
                csv_temp.seek(0)
                with open(csv_path, 'w') as csv_file:
                    csv_file.write(csv_temp.read())

    def build_insert_SQL(self, record):
        column_placeholders = (
            ':' + col_name for col_name in self._columns
        )
        insert_statement = 'INSERT INTO {} VALUES ({})'.format(
            self.name,
            ', '.join(c for c in column_placeholders),
        )
        value_kwargs = {}
        for col_idx, col_name in enumerate(self._columns):
            try:
                if record.fields[col_idx].value is None:
                    value_kwargs[col_name] = None
                else:
                    value_kwargs[col_name] = record.fields[col_idx].value
            except KeyError:
                value_kwargs[col_name] = None

        return insert_statement, value_kwargs

    def check_signature(self, record):
        assert isinstance(record, Record)
        try:
            sig = signatures[self.name]
        except KeyError:
            # The sqlite schema tables don't have a signature (or need one)
            return True
        if len(record.fields) > len(self.columns):
            return False

        # It's OK for a record to have fewer fields than there are columns in
        # this table, this is seen when NULLable or default-valued columns are
        # added in an ALTER TABLE statement.
        for field_idx, field in record.fields.items():
            # NULL can be a value for any column type
            if field.value is None:
                continue
            if not isinstance(field.value, sig[field_idx]):
                return False
        return True


class Page(object):
    def __init__(self, page_idx, db):
        self._page_idx = page_idx
        self._db = db
        self._bytes = db.page_bytes(self.idx)

    @property
    def idx(self):
        return self._page_idx

    @property
    def usable_size(self):
        return self._db.header.page_size - self._db.header.reserved_length

    def __bytes__(self):
        return self._bytes

    @property
    def parent(self):
        try:
            parent_idx = self._db.ptrmap[self.idx].page_ptr
        except KeyError:
            return None

        if 0 == parent_idx:
            return None
        else:
            return self._db.pages[parent_idx]

    def __repr__(self):
        return "<SQLite Page {0}>".format(self.idx)


class FreelistTrunkPage(Page):
    # XXX Maybe it would make sense to expect a Page instance as constructor
    # argument?
    def __init__(self, page_idx, db, leaves):
        super().__init__(page_idx, db)
        self._leaves = leaves

    def __repr__(self):
        return "<SQLite Freelist Trunk Page {0}: {1} leaves>".format(
            self.idx, len(self._leaves)
        )


class FreelistLeafPage(Page):
    # XXX Maybe it would make sense to expect a Page instance as constructor
    # argument?
    def __init__(self, page_idx, db, trunk_idx):
        super().__init__(page_idx, db)
        self._trunk = self._db.pages[trunk_idx]

    def __repr__(self):
        return "<SQLite Freelist Leaf Page {0}. Trunk: {1}>".format(
            self.idx, self._trunk.idx
        )


class PtrmapPage(Page):
    # XXX Maybe it would make sense to expect a Page instance as constructor
    # argument?
    def __init__(self, page_idx, db, ptr_array):
        super().__init__(page_idx, db)
        self._pointers = ptr_array

    @property
    def pointers(self):
        return self._pointers

    def __repr__(self):
        return "<SQLite Ptrmap Page {0}. {1} pointers>".format(
            self.idx, len(self.pointers)
        )


class OverflowPage(Page):
    # XXX Maybe it would make sense to expect a Page instance as constructor
    # argument?
    def __init__(self, page_idx, db):
        super().__init__(page_idx, db)
        self._parse()

    def _parse(self):
        # TODO We should have parsing here for the next page index in the
        # overflow chain
        pass

    def __repr__(self):
        return "<SQLite Overflow Page {0}. Continuation of {1}>".format(
            self.idx, self.parent.idx
        )


class BTreePage(Page):
    btree_page_types = {
        0x02:   "Index Interior",
        0x05:   "Table Interior",
        0x0A:   "Index Leaf",
        0x0D:   "Table Leaf",
    }

    def __init__(self, page_idx, db):
        # XXX We don't know a page's type until we've had a look at the header.
        # Or do we?
        super().__init__(page_idx, db)
        self._header_size = 8
        page_header_bytes = self._get_btree_page_header()
        self._btree_header = SQLite_btree_page_header(
            # Set the right-most page index to None in the 1st pass
            *struct.unpack(r'>BHHHB', page_header_bytes), None
        )
        self._cell_ptr_array = []
        self._freeblocks = IndexDict()
        self._cells = IndexDict()
        self._recovered_records = set()
        self._overflow_threshold = self.usable_size - 35

        if self._btree_header.page_type not in BTreePage.btree_page_types:
            # pdb.set_trace()
            raise ValueError

        # We have a twelve-byte header, need to read it again
        if self._btree_header.page_type in (0x02, 0x05):
            self._header_size = 12
            page_header_bytes = self._get_btree_page_header()
            self._btree_header = SQLite_btree_page_header(*struct.unpack(
                r'>BHHHBI', page_header_bytes
            ))

        # Page 1 (and page 2, but that's the 1st ptrmap page) does not have a
        # ptrmap entry.
        # The first ptrmap page will contain back pointer information for pages
        # 3 through J+2, inclusive.
        if self._db.ptrmap:
            if self.idx >= 3 and self.idx not in self._db.ptrmap:
                _LOGGER.warning(
                    "BTree page %d doesn't have ptrmap entry!", self.idx
                )

        if self._btree_header.num_cells > 0:
            cell_ptr_bytes = self._get_btree_ptr_array(
                self._btree_header.num_cells
            )
            self._cell_ptr_array = struct.unpack(
                r'>{count}H'.format(count=self._btree_header.num_cells),
                cell_ptr_bytes
            )
            smallest_cell_offset = min(self._cell_ptr_array)
            if self._btree_header.cell_content_offset != smallest_cell_offset:
                _LOGGER.warning(
                    (
                        "Inconsistent cell ptr array in page %d! Cell content "
                        "starts at offset %d, but min cell pointer is %d"
                    ),
                    self.idx,
                    self._btree_header.cell_content_offset,
                    smallest_cell_offset
                )

    @property
    def btree_header(self):
        return self._btree_header

    @property
    def page_type(self):
        try:
            return self.btree_page_types[self._btree_header.page_type]
        except KeyError:
            pdb.set_trace()
            _LOGGER.warning(
                "Unknown B-Tree page type: %d", self._btree_header.page_type
            )
            raise

    @property
    def freeblocks(self):
        return self._freeblocks

    @property
    def cells(self):
        return self._cells

    def __repr__(self):
        # TODO Include table in repr, where available
        return "<SQLite B-Tree Page {0} ({1}) {2} cells>".format(
            self.idx, self.page_type, len(self._cell_ptr_array)
        )

    @property
    def table(self):
        return self._db.get_page_table(self.idx)

    def _get_btree_page_header(self):
        header_offset = 0
        if self.idx == 1:
            header_offset += 100
        return bytes(self)[header_offset:self._header_size + header_offset]

    def _get_btree_ptr_array(self, num_cells):
        array_offset = self._header_size
        if self.idx == 1:
            array_offset += 100
        return bytes(self)[array_offset:2 * num_cells + array_offset]

    def parse_cells(self):
        if self.btree_header.page_type == 0x05:
            self.parse_table_interior_cells()
        elif self.btree_header.page_type == 0x0D:
            self.parse_table_leaf_cells()
        self.parse_freeblocks()

    def parse_table_interior_cells(self):
        if self.btree_header.page_type != 0x05:
            assert False

        _LOGGER.debug("Parsing cells in table interior cell %d", self.idx)
        for cell_idx, offset in enumerate(self._cell_ptr_array):
            _LOGGER.debug("Parsing cell %d @ offset %d", cell_idx, offset)
            left_ptr_bytes = bytes(self)[offset:offset + 4]
            left_ptr, = struct.unpack(r'>I', left_ptr_bytes)

            offset += 4
            integer_key = Varint(bytes(self)[offset:offset+9])
            self._cells[cell_idx] = (left_ptr, int(integer_key))

    def parse_table_leaf_cells(self):
        if self.btree_header.page_type != 0x0d:
            assert False

        _LOGGER.debug("Parsing cells in table leaf cell %d", self.idx)
        for cell_idx, cell_offset in enumerate(self._cell_ptr_array):
            _LOGGER.debug("Parsing cell %d @ offset %d", cell_idx, cell_offset)

            # This is the total size of the payload, which may include overflow
            offset = cell_offset
            payload_length_varint = Varint(bytes(self)[offset:offset+9])
            total_payload_size = int(payload_length_varint)

            overflow = False
            # Let X be U-35. If the payload size P is less than or equal to X
            # then the entire payload is stored on the b-tree leaf page. Let M
            # be ((U-12)*32/255)-23 and let K be M+((P-M)%(U-4)). If P is
            # greater than X then the number of bytes stored on the table
            # b-tree leaf page is K if K is less or equal to X or M otherwise.
            # The number of bytes stored on the leaf page is never less than M.
            cell_payload_size = 0
            if total_payload_size > self._overflow_threshold:
                m = int(((self.usable_size - 12) * 32/255)-23)
                k = m + ((total_payload_size - m) % (self.usable_size - 4))
                if k <= self._overflow_threshold:
                    cell_payload_size = k
                else:
                    cell_payload_size = m
                overflow = True
            else:
                cell_payload_size = total_payload_size

            offset += len(payload_length_varint)

            integer_key = Varint(bytes(self)[offset:offset+9])
            offset += len(integer_key)

            overflow_bytes = bytes()
            if overflow:
                first_oflow_page_bytes = bytes(self)[
                    offset + cell_payload_size:offset + cell_payload_size + 4
                ]
                if not first_oflow_page_bytes:
                    continue

                first_oflow_idx, = struct.unpack(
                    r'>I', first_oflow_page_bytes
                )
                next_oflow_idx = first_oflow_idx
                while next_oflow_idx != 0:
                    oflow_page_bytes = self._db.page_bytes(next_oflow_idx)

                    len_overflow = min(
                        len(oflow_page_bytes) - 4,
                        (
                            total_payload_size - cell_payload_size +
                            len(overflow_bytes)
                        )
                    )
                    overflow_bytes += oflow_page_bytes[4:4 + len_overflow]

                    first_four_bytes = oflow_page_bytes[:4]
                    next_oflow_idx, = struct.unpack(
                        r'>I', first_four_bytes
                    )

            try:
                cell_data = bytes(self)[offset:offset + cell_payload_size]
                if overflow_bytes:
                    cell_data += overflow_bytes

                # All payload bytes should be accounted for
                assert len(cell_data) == total_payload_size

                record_obj = Record(cell_data)
                _LOGGER.debug("Created record: %r", record_obj)

            except TypeError as ex:
                _LOGGER.warning(
                    "Caught %r while instantiating record %d",
                    ex, int(integer_key)
                )
                pdb.set_trace()
                raise

            self._cells[cell_idx] = (int(integer_key), record_obj)

    def parse_freeblocks(self):
        # The first 2 bytes of a freeblock are a big-endian integer which is
        # the offset in the b-tree page of the next freeblock in the chain, or
        # zero if the freeblock is the last on the chain. The third and fourth
        # bytes of each freeblock form a big-endian integer which is the size
        # of the freeblock in bytes, including the 4-byte header. Freeblocks
        # are always connected in order of increasing offset. The second field
        # of the b-tree page header is the offset of the first freeblock, or
        # zero if there are no freeblocks on the page. In a well-formed b-tree
        # page, there will always be at least one cell before the first
        # freeblock.
        #
        # TODO But what about deleted records that exceeded the overflow
        # threshold in the past?
        block_offset = self.btree_header.first_freeblock_offset
        while block_offset != 0:
            freeblock_header = bytes(self)[block_offset:block_offset + 4]
            # Freeblock_size includes the 4-byte header
            next_freeblock_offset, freeblock_size = struct.unpack(
                r'>HH',
                freeblock_header
            )
            freeblock_bytes = bytes(self)[
                block_offset + 4:block_offset + freeblock_size - 4
            ]
            self._freeblocks[block_offset] = freeblock_bytes
            block_offset = next_freeblock_offset

    def print_cells(self):
        for cell_idx in self.cells.keys():
            rowid, record = self.cells[cell_idx]
            _LOGGER.info(
                "Cell %d, rowid: %d, record: %r",
                cell_idx, rowid, record
            )
            record.print_fields(table=self.table)

    def recover_freeblock_records(self):
        # If we're lucky (i.e. if no overwriting has taken place), we should be
        # able to find whole record headers in freeblocks.
        # We need to start from the end of the freeblock and work our way back
        # to the start. That means we don't know where a cell header will
        # start, but I suppose we can take a guess
        table = self.table
        if not table or table.name not in heuristics:
            return

        _LOGGER.info("Attempting to recover records from freeblocks")
        for freeblock_idx, freeblock_offset in enumerate(self._freeblocks):
            freeblock_bytes = self._freeblocks[freeblock_offset]
            if 0 == len(freeblock_bytes):
                continue
            _LOGGER.debug(
                "Freeblock %d/%d in page, offset %d, %d bytes",
                1 + freeblock_idx,
                len(self._freeblocks),
                freeblock_offset,
                len(freeblock_bytes)
            )

            recovered_bytes = 0
            recovered_in_freeblock = 0

            # TODO Maybe we need to guess the record header lengths rather than
            # try and read them from the freeblocks
            for header_start in heuristics[table.name](freeblock_bytes):
                _LOGGER.debug(
                    (
                        "Trying potential record header start at "
                        "freeblock offset %d/%d"
                    ),
                    header_start, len(freeblock_bytes)
                )
                _LOGGER.debug("%r", freeblock_bytes)
                try:
                    # We don't know how to handle overflow in deleted records,
                    # so we'll have to truncate the bytes object used to
                    # instantiate the Record object
                    record_bytes = freeblock_bytes[
                        header_start:header_start+self._overflow_threshold
                    ]
                    record_obj = Record(record_bytes)
                except MalformedRecord:
                    # This isn't a well-formed record, let's move to the next
                    # candidate
                    continue

                field_lengths = sum(
                    len(field_obj) for field_obj in record_obj.fields.values()
                )
                record_obj.truncate(field_lengths + len(record_obj.header))
                self._recovered_records.add(record_obj)

                recovered_bytes += len(bytes(record_obj))
                recovered_in_freeblock += 1

            _LOGGER.info(
                (
                    "Recovered %d record(s): %d bytes out of %d "
                    "freeblock bytes @ offset %d"
                ),
                recovered_in_freeblock,
                recovered_bytes,
                len(freeblock_bytes),
                freeblock_offset,
            )

    @property
    def recovered_records(self):
        return self._recovered_records

    def print_recovered_records(self):
        if not self._recovered_records:
            return

        for record_obj in self._recovered_records:
            _LOGGER.info("Recovered record: %r", record_obj)
            _LOGGER.info("Recovered record header: %s", record_obj.header)
            record_obj.print_fields(table=self.table)


class Record(object):

    column_types = {
        0: (0, "NULL"),
        1: (1, "8-bit twos-complement integer"),
        2: (2, "big-endian 16-bit twos-complement integer"),
        3: (3, "big-endian 24-bit twos-complement integer"),
        4: (4, "big-endian 32-bit twos-complement integer"),
        5: (6, "big-endian 48-bit twos-complement integer"),
        6: (8, "big-endian 64-bit twos-complement integer"),
        7: (8, "Floating point"),
        8: (0, "Integer 0"),
        9: (0, "Integer 1"),
    }

    def __init__(self, record_bytes):
        self._bytes = record_bytes
        self._header_bytes = None
        self._fields = IndexDict()
        self._parse()

    def __bytes__(self):
        return self._bytes

    @property
    def header(self):
        return self._header_bytes

    @property
    def fields(self):
        return self._fields

    def truncate(self, new_length):
        self._bytes = self._bytes[:new_length]
        self._parse()

    def _parse(self):
        header_offset = 0

        header_length_varint = Varint(
            # A varint is encoded on *at most* 9 bytes
            bytes(self)[header_offset:9 + header_offset]
        )

        # Let's keep track of how many bytes of the Record header (including
        # the header length itself) we've succesfully parsed
        parsed_header_bytes = len(header_length_varint)

        if len(bytes(self)) < int(header_length_varint):
            raise MalformedRecord(
                "Not enough bytes to fully read the record header!"
            )

        header_offset += len(header_length_varint)
        self._header_bytes = bytes(self)[:int(header_length_varint)]

        col_idx = 0
        field_offset = int(header_length_varint)
        while header_offset < int(header_length_varint):
            serial_type_varint = Varint(
                bytes(self)[header_offset:9 + header_offset]
            )
            serial_type = int(serial_type_varint)
            col_length = None

            try:
                col_length, _ = self.column_types[serial_type]
            except KeyError:
                if serial_type >= 13 and (1 == serial_type % 2):
                    col_length = (serial_type - 13) // 2
                elif serial_type >= 12 and (0 == serial_type % 2):
                    col_length = (serial_type - 12) // 2
                else:
                    raise ValueError(
                        "Unknown serial type {}".format(serial_type)
                    )

            try:
                field_obj = Field(
                    col_idx,
                    serial_type,
                    bytes(self)[field_offset:field_offset + col_length]
                )
            except MalformedField as ex:
                _LOGGER.warning(
                    "Caught %r while instantiating field %d (%d)",
                    ex, col_idx, serial_type
                )
                raise MalformedRecord
            except Exception as ex:
                _LOGGER.warning(
                    "Caught %r while instantiating field %d (%d)",
                    ex, col_idx, serial_type
                )
                pdb.set_trace()
                raise

            self._fields[col_idx] = field_obj
            col_idx += 1
            field_offset += col_length

            parsed_header_bytes += len(serial_type_varint)
            header_offset += len(serial_type_varint)

            if field_offset > len(bytes(self)):
                raise MalformedRecord

        # assert(parsed_header_bytes == int(header_length_varint))

    def print_fields(self, table=None):
        for field_idx in self._fields:
            field_obj = self._fields[field_idx]
            if not table or table.columns is None:
                _LOGGER.info(
                    "\tField %d (%d bytes), type %d: %s",
                    field_obj.index,
                    len(field_obj),
                    field_obj.serial_type,
                    field_obj.value
                )
            else:
                _LOGGER.info(
                    "\t%s: %s",
                    table.columns[field_obj.index],
                    field_obj.value
                )

    def __repr__(self):
        return '<Record {} fields, {} bytes, header: {} bytes>'.format(
            len(self._fields), len(bytes(self)), len(self.header)
        )


class MalformedField(Exception):
    pass


class MalformedRecord(Exception):
    pass


class Field(object):
    def __init__(self, idx, serial_type, serial_bytes):
        self._index = idx
        self._type = serial_type
        self._bytes = serial_bytes
        self._value = None
        self._parse()

    def _check_length(self, expected_length):
        if len(self) != expected_length:
            raise MalformedField

    # TODO Raise a specific exception when bad bytes are encountered for the
    # fields and then use this to weed out bad freeblock records
    def _parse(self):
        if self._type == 0:
            self._value = None
        # Integer types
        elif self._type == 1:
            self._check_length(1)
            self._value = decode_twos_complement(bytes(self)[0:1], 8)
        elif self._type == 2:
            self._check_length(2)
            self._value = decode_twos_complement(bytes(self)[0:2], 16)
        elif self._type == 3:
            self._check_length(3)
            self._value = decode_twos_complement(bytes(self)[0:3], 24)
        elif self._type == 4:
            self._check_length(4)
            self._value = decode_twos_complement(bytes(self)[0:4], 32)
        elif self._type == 5:
            self._check_length(6)
            self._value = decode_twos_complement(bytes(self)[0:6], 48)
        elif self._type == 6:
            self._check_length(8)
            self._value = decode_twos_complement(bytes(self)[0:8], 64)

        elif self._type == 7:
            self._value = struct.unpack(r'>d', bytes(self)[0:8])[0]
        elif self._type == 8:
            self._value = 0
        elif self._type == 9:
            self._value = 1
        elif self._type >= 13 and (1 == self._type % 2):
            try:
                self._value = bytes(self).decode('utf-8')
            except UnicodeDecodeError:
                raise MalformedField

        elif self._type >= 12 and (0 == self._type % 2):
            self._value = bytes(self)

    def __bytes__(self):
        return self._bytes

    def __repr__(self):
        return "<Field {}: {} ({} bytes)>".format(
            self._index, self._value, len(bytes(self))
        )

    def __len__(self):
        return len(bytes(self))

    @property
    def index(self):
        return self._index

    @property
    def value(self):
        return self._value

    @property
    def serial_type(self):
        return self._type


class Varint(object):
    def __init__(self, varint_bytes):
        self._bytes = varint_bytes
        self._len = 0
        self._value = 0

        varint_bits = []
        for b in self._bytes:
            self._len += 1
            if b & 0x80:
                varint_bits.append(b & 0x7F)
            else:
                varint_bits.append(b)
                break

        varint_twos_complement = 0
        for position, b in enumerate(varint_bits[::-1]):
            varint_twos_complement += b * (1 << (7*position))

        self._value = decode_twos_complement(
            int.to_bytes(varint_twos_complement, 4, byteorder='big'), 64
        )

    def __int__(self):
        return self._value

    def __len__(self):
        return self._len

    def __repr__(self):
        return "<Varint {} ({} bytes)>".format(int(self), len(self))


def decode_twos_complement(encoded, bit_length):
    assert(0 == bit_length % 8)
    encoded_int = int.from_bytes(encoded, byteorder='big')
    mask = 2**(bit_length - 1)
    value = -(encoded_int & mask) + (encoded_int & ~mask)
    return value


def gen_output_dir(db_path):
    db_abspath = os.path.abspath(db_path)
    db_dir, db_name = os.path.split(db_abspath)

    munged_name = db_name.replace('.', '_')
    out_dir = os.path.join(db_dir, munged_name)
    if not os.path.exists(out_dir):
        return out_dir
    suffix = 1
    while suffix <= 10:
        out_dir = os.path.join(db_dir, "{}_{}".format(munged_name, suffix))
        if not os.path.exists(out_dir):
            return out_dir
        suffix += 1
    raise SystemError(
        "Unreasonable number of output directories for {}".format(db_path)
    )


def _load_db(sqlite_path):
    _LOGGER.info("Processing %s", sqlite_path)

    load_heuristics()

    db = SQLite_DB(sqlite_path)
    _LOGGER.info("Database: %r", db)

    db.populate_freelist_pages()
    db.populate_ptrmap_pages()
    db.populate_overflow_pages()

    # Should we aim to instantiate specialised b-tree objects here, or is the
    # use of generic btree page objects acceptable?
    db.populate_btree_pages()

    db.map_tables()

    # We need a first pass to process tables that are disconnected
    # from their table's root page
    db.reparent_orphaned_table_leaf_pages()

    # All pages should now be represented by specialised objects
    assert(all(isinstance(p, Page) for p in db.pages.values()))
    assert(not any(type(p) is Page for p in db.pages.values()))
    return db


def dump_to_csv(args):
    out_dir = args.output_dir or gen_output_dir(args.sqlite_path)
    db = _load_db(args.sqlite_path)

    if os.path.exists(out_dir):
        raise ValueError("Output directory {} exists!".format(out_dir))
    os.mkdir(out_dir)

    for table_name in sorted(db.tables):
        table = db.tables[table_name]
        _LOGGER.info("Table \"%s\"", table)
        table.recover_records()
        table.csv_dump(out_dir)


def undelete(args):
    db_abspath = os.path.abspath(args.sqlite_path)
    db = _load_db(db_abspath)

    output_path = os.path.abspath(args.output_path)
    if os.path.exists(output_path):
        raise ValueError("Output file {} exists!".format(output_path))

    shutil.copyfile(db_abspath, output_path)
    with sqlite3.connect(output_path) as output_db_connection:
        cursor = output_db_connection.cursor()
        for table_name in sorted(db.tables):
            table = db.tables[table_name]
            _LOGGER.info("Table \"%s\"", table)
            table.recover_records()

            failed_inserts = 0
            constraint_violations = 0
            successful_inserts = 0
            for leaf_page in table.leaves:
                if not leaf_page.recovered_records:
                    continue

                for record in leaf_page.recovered_records:
                    insert_statement, values = table.build_insert_SQL(record)

                    try:
                        cursor.execute(insert_statement, values)
                    except sqlite3.IntegrityError:
                        # We gotta soldier on, there's not much we can do if a
                        # constraint is violated by this insert
                        constraint_violations += 1
                    except (
                                sqlite3.ProgrammingError,
                                sqlite3.OperationalError,
                                sqlite3.InterfaceError
                            ) as insert_ex:
                        _LOGGER.warning(
                            (
                                "Caught %r while executing INSERT statement "
                                "in \"%s\""
                            ),
                            insert_ex,
                            table
                        )
                        failed_inserts += 1
                        # pdb.set_trace()
                    else:
                        successful_inserts += 1
            if failed_inserts > 0:
                _LOGGER.warning(
                    "%d failed INSERT statements in \"%s\"",
                    failed_inserts, table
                )
            if constraint_violations > 0:
                _LOGGER.warning(
                    "%d constraint violations statements in \"%s\"",
                    constraint_violations, table
                )
            _LOGGER.info(
                "%d successful INSERT statements in \"%s\"",
                successful_inserts, table
            )


def find_in_db(args):
    db = _load_db(args.sqlite_path)
    db.grep(args.needle)


subcmd_actions = {
    'csv':  dump_to_csv,
    'grep': find_in_db,
    'undelete': undelete,
}


def subcmd_dispatcher(arg_ns):
    return subcmd_actions[arg_ns.subcmd](arg_ns)


def main():

    verbose_parser = argparse.ArgumentParser(add_help=False)
    verbose_parser.add_argument(
        '-v', '--verbose',
        action='count',
        help='Give *A LOT* more output.',
    )

    cli_parser = argparse.ArgumentParser(
        description=PROJECT_DESCRIPTION,
        parents=[verbose_parser],
    )

    subcmd_parsers = cli_parser.add_subparsers(
        title='Subcommands',
        description='%(prog)s implements the following subcommands:',
        dest='subcmd',
    )

    csv_parser = subcmd_parsers.add_parser(
        'csv',
        parents=[verbose_parser],
        help='Dumps visible and recovered records to CSV files',
        description=(
            'Recovers as many records as possible from the database passed as '
            'argument and outputs all visible and recovered records to CSV '
            'files in output_dir'
        ),
    )
    csv_parser.add_argument(
        'sqlite_path',
        help='sqlite3 file path'
    )
    csv_parser.add_argument(
        'output_dir',
        nargs='?',
        default=None,
        help='Output directory'
    )

    grep_parser = subcmd_parsers.add_parser(
        'grep',
        parents=[verbose_parser],
        help='Matches a string in one or more pages of the database',
        description='Bar',
    )
    grep_parser.add_argument(
        'sqlite_path',
        help='sqlite3 file path'
    )
    grep_parser.add_argument(
        'needle',
        help='String to match in the database'
    )

    undelete_parser = subcmd_parsers.add_parser(
        'undelete',
        parents=[verbose_parser],
        help='Inserts recovered records into a copy of the database',
        description=(
            'Recovers as many records as possible from the database passed as '
            'argument and inserts all recovered records into a copy of'
            'the database.'
        ),
    )
    undelete_parser.add_argument(
        'sqlite_path',
        help='sqlite3 file path'
    )
    undelete_parser.add_argument(
        'output_path',
        help='Output database path'
    )

    cli_args = cli_parser.parse_args()
    if cli_args.verbose:
        _LOGGER.setLevel(logging.DEBUG)

    if cli_args.subcmd:
        subcmd_dispatcher(cli_args)
    else:
        # No subcommand specified, print the usage and bail
        cli_parser.print_help()
