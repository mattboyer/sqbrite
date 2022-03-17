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
from pkg_resources import resource_stream
import re
import yaml

from . import _LOGGER
from . import PROJECT_NAME, USER_YAML_PATH, BUILTIN_YAML


class Heuristic(object):
    def __init__(self, magic, offset, grouping, table, name_regex=None):
        self._offset = offset
        self._table_name = table
        self._grouping = grouping
        self._magic_re = re.compile(magic)

        self._table_name_regex = None
        if name_regex is not None:
            self._table_name_regex = re.compile(name_regex)

    def __repr__(self):
        return "<Record heuristic for table \"{0}\"({1})>".format(
            self._table_name, self._grouping
        )

    def __call__(self, freeblock_bytes):
        # We need to unwind the full set of matches so we can traverse it
        # in reverse
        all_matches = [
            match for match in self._magic_re.finditer(freeblock_bytes)
        ]
        for magic_match in all_matches[::-1]:
            header_start = magic_match.start() - self._offset
            if header_start < 0:
                _LOGGER.debug("Header start outside of freeblock!")
                break
            yield header_start

    def match(self, table):
        if self._table_name_regex is not None:
            return bool(self._table_name_regex.match(table.name))
        else:
            return self._table_name == table.name


class HeuristicsRegistry(dict):

    def __init__(self):
        super().__init__(self)

    @staticmethod
    def check_heuristic(magic, offset):
        assert(isinstance(magic, bytes))
        assert(isinstance(offset, int))
        assert(offset >= 0)

    def _load_from_yaml(self, yaml_string):
        if isinstance(yaml_string, bytes):
            yaml_string = yaml_string.decode('utf-8')

        raw_yaml = yaml.load(yaml_string, Loader=yaml.CLoader)
        # TODO Find a more descriptive term than "table grouping"
        for table_grouping, tables in raw_yaml.items():
            _LOGGER.debug(
                "Loading YAML data for table grouping \"%s\"",
                table_grouping
            )
            grouping_tables = {}
            for table_name, table_props in tables.items():
                self.check_heuristic(
                    table_props['magic'], table_props['offset']
                )
                grouping_tables[table_name] = Heuristic(
                    table_props['magic'], table_props['offset'],
                    table_grouping, table_name,
                    name_regex=table_props.get('name_regex')
                )
                _LOGGER.debug("Loaded heuristics for \"%s\"", table_name)
            self[table_grouping] = grouping_tables

    def load_heuristics(self):
        with resource_stream(PROJECT_NAME, BUILTIN_YAML) as builtin:
            try:
                self._load_from_yaml(builtin.read())
            except KeyError as ex:
                raise SystemError("Malformed builtin magic file") from ex

        if not os.path.exists(USER_YAML_PATH):
            return
        with open(USER_YAML_PATH, 'r', encoding='UTF8') as user_yaml:
            try:
                self._load_from_yaml(user_yaml.read())
            except KeyError as ex:
                raise SystemError("Malformed user magic file") from ex

    @property
    def groupings(self):
        for db_name in sorted(self.keys()):
            yield db_name

    @property
    def all_tables(self):
        for db in self.groupings:
            for table in self[db].keys():
                yield (db, table)

    def _get_heuristic_in_grouping(self, db_table, grouping):
        heuristic_name = None
        if grouping in self:
            for heuristic_name in self[grouping]:
                if self[grouping][heuristic_name].match(db_table):
                    break
            else:
                # We haven't found a match within the grouping... what
                # shall we do?
                raise ValueError("No heuristic found")

            return self[grouping][heuristic_name]

        else:
            raise ValueError(
                "No heuristic defined for table \"%s\" in grouping \"%s\"" %
                (db_table.name, grouping)
            )

    def _get_heuristic_in_all_groupings(self, db_table):
        grouping = None
        heuristic_name = None
        for grouping, heuristic_name in self.all_tables:
            if self[grouping][heuristic_name].match(db_table):
                break
        else:
            raise ValueError(
                "No heuristic defined for table \"%s\" in any grouping" %
                (db_table.name,)
            )

        return self[grouping][heuristic_name]

    def get_heuristic(self, db_table, grouping):
        if grouping is not None:
            return self._get_heuristic_in_grouping(db_table, grouping)
        else:
            return self._get_heuristic_in_all_groupings(db_table)
