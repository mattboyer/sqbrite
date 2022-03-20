Bring the shine back into your database with SQBrite!
=====================================================

.. image:: https://app.travis-ci.com/mattboyer/sqbrite.svg?branch=master
    :target: https://app.travis-ci.com/mattboyer/sqbrite
    :alt: Continuous Integration status

.. image:: https://scrutinizer-ci.com/g/mattboyer/sqbrite/badges/quality-score.png?b=master
    :target: https://scrutinizer-ci.com/g/mattboyer/sqbrite/?branch=master
    :alt: Scrutinizer Code Quality

.. image:: https://img.shields.io/pypi/v/sqbrite.svg
    :target: https://pypi.python.org/pypi/sqbrite/
    :alt: Latest Version

.. image:: https://img.shields.io/pypi/format/sqbrite.svg
    :target: https://pypi.python.org/pypi/sqbrite/
    :alt: Download format

.. image:: https://img.shields.io/pypi/pyversions/sqbrite.svg
    :target: https://pypi.python.org/pypi/sqbrite/
    :alt: Supported Python versions

SQBrite is a data recovery/forensics tool for `SQLite <https://www.sqlite.org/>`_ databases. It uses a Python 3 implementation of the `SQLite on-disk file format <https://www.sqlite.org/fileformat2.html>`_ to recover deleted table rows.

SQBrite's name is inspired by `PL Daniels' <https://github.com/inflex>`_ `undark <http://pldaniels.com/undark/>`_, but is a completely separate implementation.

.. image:: https://asciinema.org/a/118939.png
    :target: https://asciinema.org/a/118939
    :alt: SQBrite demo terminal recording

Installing SQBrite
------------------

SQBrite requires Python 3. To install, simply run:

.. code-block:: bash

    $ pip3 install --user sqbrite
    $ sqbrite --help

Background
----------

SQLite uses a paginated data model in which each database is a collection of same-size *pages*. There are several kinds of pages, of which one type (B-Tree Table Leaf pages) contains the starting point for actual data belonging to individual table rows.

When a row is deleted by means of a ``DELETE FROM table (...)`` statement, the space occupied by that row's data (a *record*) within the relevant B-Tree Table Leaf page is marked as free and may subsequently be used to store new records or update existing records. However, it is common to see freed space within a page (a *freeblock*, in SQLite parlance) left alone after rows are deleted. In that case, it ***may*** be possible to retrieve deleted row data from within the freeblock.

Heuristics
++++++++++

The SQLite file format doesn't keep track of where deleted records start and end within a leaf page's freeblocks. This means that SQBrite needs a mechanism to find out where record headers start. This is achieved through the use of byte-wise regular expressions specific to tables in known databases. These regular expressions and the offset that separates matches from the first byte in a well-formed header are stored in a user-editable YAML file.

SQBrite aims to ship with heuristics for popular SQLite databases, so **do** send pull requests if you've got good results with your heuristics.

Features
--------

- Export all records to CSV or reinject "undeleted" records into a copy of the database
- Extensible heuristics - just add entries to ``~/.local/share/sqbrite/sqbrite.yaml``!
- SQBrite can recover records from within active B-tree table leaf pages or from former table-leaf Freelist pages.
- Heuristics for iOS and Firefox databases

Limitations
-----------

- SQBrite works better when ``ptrmap`` pages are present
- The ``undelete`` subcommand may fail when re-inserting deleted rows into a table causes a constraint violation
- SQBrite cannot recover records deleted with the `SQLite secure_delete pragma <https://www.sqlite.org/pragma.html#pragma_secure_delete>`_ enabled
- Recovering data from overflow pages that have become Freelist leaf pages is not currently supported


Acknowledgments
---------------

Big thanks to `@tobraha <https://github.com/tobraha>`_ for contributing bugfixes in 2022.
