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

import csv
import os
import tempfile

from . import _LOGGER
from .record import Record
from .pages import BTreePage


class Table(object):
    def __init__(self, name, db, rootpage, signatures):
        self._name = name
        self._db = db
        self._signatures = signatures
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

    def recover_records(self, grouping):
        for page in self.leaves:
            assert isinstance(page, BTreePage)
            if not page.freeblocks:
                continue

            _LOGGER.info("%r", page)
            page.recover_freeblock_records(grouping)
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
                with open(csv_path, 'w', encoding='UTF8') as csv_file:
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
            sig = self._signatures[self.name]
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
