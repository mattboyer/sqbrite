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

import argparse
import logging
import os.path
import shutil
import sqlite3

from . import PROJECT_DESCRIPTION, PROJECT_NAME
from . import _LOGGER
from .db import SQLite_DB
from .heuristics import HeuristicsRegistry
from .pages import Page


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
    registry = HeuristicsRegistry()
    registry.load_heuristics()

    db = SQLite_DB(sqlite_path, registry)
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
        table.recover_records(args.database_name)
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
            table.recover_records(args.database_name)

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


def list_supported(args):  # pylint:disable=W0613
    registry = HeuristicsRegistry()
    registry.load_heuristics()
    for db in registry.groupings:
        print(db)


subcmd_actions = {
    'csv':  dump_to_csv,
    'grep': find_in_db,
    'undelete': undelete,
    'list': list_supported,
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
    csv_parser.add_argument(
        '-d', '--database-name',
        nargs='?',
        default=None,
        help='Database name'
    )

    list_parser = subcmd_parsers.add_parser(  # pylint:disable=W0612
        'list',
        parents=[verbose_parser],
        help='Displays supported DB types',
        description=(
            'Displays the names of all database types with table heuristics '
            'known to {}'.format(PROJECT_NAME)
        ),
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
    undelete_parser.add_argument(
        '-d', '--database-name',
        nargs='?',
        default=None,
        help='Database name'
    )

    cli_args = cli_parser.parse_args()
    if cli_args.verbose:
        _LOGGER.setLevel(logging.DEBUG)

    if cli_args.subcmd:
        subcmd_dispatcher(cli_args)
    else:
        # No subcommand specified, print the usage and bail
        cli_parser.print_help()
