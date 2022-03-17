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

from . import _LOGGER
from .field import (Field, MalformedField)
from .utils import (Varint, IndexDict)


class MalformedRecord(Exception):
    pass


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
            except KeyError as col_type_ex:
                if serial_type >= 13 and (1 == serial_type % 2):
                    col_length = (serial_type - 13) // 2
                elif serial_type >= 12 and (0 == serial_type % 2):
                    col_length = (serial_type - 12) // 2
                else:
                    raise ValueError(
                        "Unknown serial type {}".format(serial_type)
                    ) from col_type_ex

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
                raise MalformedRecord from ex
            except Exception as ex:
                _LOGGER.warning(
                    "Caught %r while instantiating field %d (%d)",
                    ex, col_idx, serial_type
                )
                # pdb.set_trace()
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
