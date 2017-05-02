Bring that shine back into your database with SQBrite!
======================================================

.. image:: https://travis-ci.org/mattboyer/sqbrite.svg?branch=master
    :target: https://travis-ci.org/mattboyer/sqbrite
    :alt: Continuous Integration status

.. image:: https://scrutinizer-ci.com/g/mattboyer/sqbrite/badges/quality-score.png?b=master
    :target: https://scrutinizer-ci.com/g/mattboyer/sqbrite/?branch=master
    :alt: Scrutinizer Code Quality

``sqbrite`` is a data recovery/forensics tool for `SQLite <https://www.sqlite.org/>`_ databases. It uses a Python 3 implementation of the `SQLite on-disk file format <https://www.sqlite.org/fileformat2.html>`_ to recover deleted table rows.

SQBrite's name is inspired by `PL Daniels' <https://github.com/inflex>`_ `undark <http://pldaniels.com/undark/>`_, but is a completely separate implementation.

Background
----------

SQLite uses a paginated data model in which each database is a collection of same-size _pages_. There are several kinds of pages, of which one type (B-Tree Table Leaf pages) contains the starting point for actual data belonging to table rows. When a row is deleted by means of a ``DELETE FROM table (...)`` statement, the space occupied by that row's data within the relevant B-Tree Table Leaf page is marked as free and may subsequently be used to store new records or update existing records. However, it is common to see freed space within a page (a freeblock, in SQLite parlance) left alone after rows are deleted. In that case, it *may* be possible to retrieve deleted row data from within the freeblock.

Heuristics
++++++++++

The SQLite file format doesn't keep track of where deleted records start and end within a leaf page's freeblocks. This means that ``sqbrite`` needs a mechanism to find out where record headers start. This is achieved by means of regular expressions specific to tables in known databases. These regular expressions and the offset that separates matches from the first byte in a well-formed header are stored in a user-editable JSON file.
``sqbrite`` aims to ship with heuristics for popular SQLite databases, so *do* send pull requests if you've got good results with your heuristics.

Features
--------

- Export all records to CSV or reinject "undeleted" records into a copy of the database
- Extensible heuristics - just add entries to ``~/.local/share/sqbrite/sqbrite.json``!
- ``sqbrite`` can recover records from within active B-tree table leaf pages or from former table-leaf Freelist pages.

Limitations
-----------

- ``sqbrite`` works better when ``ptrmap`` pages are present
- The undelete subcommand may fail when re-inserting deleted rows into a table causes a constraint violation
- ``sqbrite`` cannot recover records deleted with the `SQLite secure_delete pragma <https://www.sqlite.org/pragma.html#pragma_secure_delete>`_ enabled
- Recovering data from overflow pages that have become Freelist leaf pages is not currently supported
