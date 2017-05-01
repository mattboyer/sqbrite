.. image:: https://travis-ci.org/mattboyer/sqbrite.svg?branch=master
    :target: https://travis-ci.org/mattboyer/sqbrite
    :alt: Continuous Integration status

.. image:: https://scrutinizer-ci.com/g/mattboyer/sqbrite/badges/quality-score.png?b=master
    :target: https://scrutinizer-ci.com/g/mattboyer/sqbrite/?branch=master
    :alt: Scrutinizer Code Quality

Bring that shine back into your database with SQBrite!
======================================================

``sqbrite`` is a data recovery/forensics tool for SQLite databases. It uses a Python 3 implementation of the `SQLite on-disk file format <https://www.sqlite.org/fileformat2.html>`_ to recover deleted table rows.

.. image:: https://asciinema.org/a/dq9j9oeje763429i9d9ypj7pd.png
    :target: https://asciinema.org/a/dq9j9oeje763429i9d9ypj7pd
    :alt: Termcast

Features
--------

- Export all records to CSV or reinject "undeleted" records into a copy of the database
- Extensible heuristics - just add entries to ``~/.local/share/sqbrite/sqbrite.json``

Limitations
-----------

- ``sqbrite`` works better when ``ptrmap`` pages are present
- ``sqbrite`` cannot recover records deleted with the `SQLite ``secure_delete pragma`` <https://www.sqlite.org/pragma.html#pragma_secure_delete>`_ enabled
