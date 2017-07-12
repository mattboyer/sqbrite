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
import pkg_resources
import re
import yaml

from . import _LOGGER
from . import PROJECT_NAME, USER_YAML_PATH, BUILTIN_YAML

heuristics = {}


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
            _LOGGER.debug(
                "Loading raw_yaml for table grouping \"%s\"",
                table_grouping
            )
            grouping_tables = {}
            for table_name, table_props in tables.items():
                grouping_tables[table_name] = heuristic_factory(
                    table_props['magic'], table_props['offset']
                )
                _LOGGER.debug("Loaded heuristics for \"%s\"", table_name)
            heuristics[table_grouping] = grouping_tables

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


def iter_heur():
    for db_name in sorted(heuristics.keys()):
        yield db_name
