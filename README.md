# RebirthDB Python driver
[![Build Status](https://travis-ci.org/RebirthDB/rebirthdb-python.svg?branch=master)](https://travis-ci.org/RebirthDB/rebirthdb-python) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/2b5231a6f90a4a1ba2fc795f8466bbe4)](https://www.codacy.com/app/RebirthDB/rebirthdb-python?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=RebirthDB/rebirthdb-python&amp;utm_campaign=Badge_Grade) [![Codacy Badge](https://api.codacy.com/project/badge/Coverage/2b5231a6f90a4a1ba2fc795f8466bbe4)](https://www.codacy.com/app/RebirthDB/rebirthdb-python?utm_source=github.com&utm_medium=referral&utm_content=RebirthDB/rebirthdb-python&utm_campaign=Badge_Coverage)

## Overview

### What is RebirthDB?
RebirthDB is the fork of RethinkDB which is the first open-source scalable database built for realtime applications. It exposes a new database access model -- instead of polling for changes, the developer can tell the database to continuously push updated query results to applications in realtime. RebirthDB allows developers to build scalable realtime apps in a fraction of the time with less effort.

## Installation
```bash
$ pip install rebirthdb
```
*Note: this package is the extracted driver of RethinkDB's original python driver.*

## Quickstart
The main difference with the previous driver (except the name of the package) is we are **not** importing RebirthDB as `r`. If you would like to use `RebirthDB`'s python driver as a drop in replacement, you should do the following:

```python
from rebirthdb import RebirthDB

r = RebirthDB()
connection = r.connect(db='test')
```

## Example
Create a table, populate with data, and get every document.

```python
from rebirthdb import RebirthDB

r = RebirthDB()
connection = r.connect(db='test')

r.table_create('marvel').run(connection)
marvel_heroes = r.table('marvel')
marvel_heroes.insert({
    'id': 1,
    'name': 'Iron Man',
    'first_appearance': 'Tales of Suspense #39'
}).run(connection)

for hero in marvel_heroes.run(connection):
    print(hero['name'])
```

## Run tests
To ensure python driver works with python 2.7, 3.4, 3.5, 3.6 we are using `tox` and `pytest`. For testing (depending on what you would like to test) you can run `tox` or `pytest`.

## New features
Github's Issue tracker is **ONLY** used for reporting bugs. NO NEW FEATURE ACCEPTED! Use [spectrum](https://spectrum.chat/rebirthdb) for supporting features.

## Contributing
Hurray! You reached this section which means, that you would like to contribute. Please read our contributing guide lines and feel free to open a pull request.
