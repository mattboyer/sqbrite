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

import struct

from . import _LOGGER
from .record import (Record, MalformedRecord)
from .tuples import SQLite_btree_page_header
from .utils import (Varint, IndexDict)


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

    def __init__(self, page_idx, db, heuristics):
        # XXX We don't know a page's type until we've had a look at the header.
        # Or do we?
        super().__init__(page_idx, db)
        self._heuristics = heuristics
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
            # pdb.set_trace()
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
                            total_payload_size - cell_payload_size -
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
                # pdb.set_trace()
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

    def recover_freeblock_records(self, grouping):
        # If we're lucky (i.e. if no overwriting has taken place), we should be
        # able to find whole record headers in freeblocks.
        # We need to start from the end of the freeblock and work our way back
        # to the start. That means we don't know where a cell header will
        # start, but I suppose we can take a guess

        if not self.table:
            return

        try:
            table_heuristic = self._heuristics.get_heuristic(
                self.table, grouping
            )
        except ValueError as ex:
            _LOGGER.error(str(ex))
            return

        _LOGGER.info(
            "Using heuristic %r on table \"%s\"",
            table_heuristic, self.table,
        )

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
            for header_start in table_heuristic(freeblock_bytes):
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
