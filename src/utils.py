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


class IndexDict(dict):
    def __iter__(self):
        for k in sorted(self.keys()):
            yield k


def decode_twos_complement(encoded, bit_length):
    assert(0 == bit_length % 8)
    encoded_int = int.from_bytes(encoded, byteorder='big')
    mask = 2**(bit_length - 1)
    value = -(encoded_int & mask) + (encoded_int & ~mask)
    return value
