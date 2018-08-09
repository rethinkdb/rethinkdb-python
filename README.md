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
In the `Makefile` you can find three different test commands: `test-unit`, `test-integration` and `test-remote`. As RebirthDB dropping the support of Windows, we would like to ensure that those of us who are using Windows for development can still contribute. Because of this, we support running integration tests against Digital Ocean Droplets as well.

Before you run any test, make sure that you install the requirements.
```bash
$ pip install -r requirements.txt
```

### Running unit tests
```bash
$ make test-unit
```

### Running integration tests
*To run integration tests locally, make sure you intstalled RebirthDB*
```bash
$ make test-integration
```

### Running remote integration tests
*To run the remote tests, you need to have a Digital Ocean account and an API key.*

Remote test will create a new temporary SSH key and a Droplet for you until the tests are finished.

**Available environment variables**
| Variable name | Default value |
|---------------|---------------|
| DO_TOKEN      | N/A           |
| DO_SIZE       | 512MB         |
| DO_REGION     | sfo2          |

```bash
$ export DO_TOKEN=<YOUR_TOKEN>
$ make test-remote
```

## New features
Github's Issue tracker is **ONLY** used for reporting bugs. NO NEW FEATURE ACCEPTED! Use [spectrum](https://spectrum.chat/rebirthdb) for supporting features.

## Contributing
Hurray! You reached this section which means, that you would like to contribute. Please read our contributing guide lines and feel free to open a pull request.
