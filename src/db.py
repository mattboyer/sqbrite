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

import os
import re
import stat
import struct

from . import constants
from . import _LOGGER
from .record import Record
from .pages import (
    Page, OverflowPage, FreelistLeafPage, FreelistTrunkPage, BTreePage,
    PtrmapPage
)
from .table import Table
from .tuples import (
    SQLite_header, SQLite_ptrmap_info, SQLite_master_record, type_specs
)


signatures = {}


class SQLite_DB(object):
    def __init__(self, path, heuristics_registry):
        self._path = path
        self._page_types = {}
        self._header = self.parse_header()
        self._registry = heuristics_registry

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
        except KeyError as ex:
            raise ValueError(f"No cache for page {page_idx}") from ex

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

            # Set _page_types value for this page
            self._page_types[freelist_trunk_idx] = \
                constants.FREELIST_TRUNK_PAGE

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
                # We need to pass in the singleton registry instance
                page_obj = BTreePage(page_idx, self, self._registry)
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

        master_table = Table('sqlite_master', self, first_page, signatures)
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
                table_obj = Table(table_name, self, rootpage, signatures)
            except Exception as ex:  # pylint:disable=W0703
                # pdb.set_trace()
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
