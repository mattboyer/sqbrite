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

from .utils import decode_twos_complement


class MalformedField(Exception):
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
            except UnicodeDecodeError as ex:
                raise MalformedField from ex

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
