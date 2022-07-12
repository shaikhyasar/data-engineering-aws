# Command Line Interface for ETL Pipeline

This module is a project which I took it from upwork. you can able to see the pdf about this project. I have done this project using

- Typer
- Poetry (To make our module as package)

## To use this module, Follow the steps.

You should have install the package, Please check the requirements in the root directory

```
poetry install
```

The above line will setup everything.

# `eltcli`

**Usage**:

```console
$ eltcli [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--install-completion`: Install completion for the current shell.
* `--show-completion`: Show completion for the current shell, to copy it or customize the installation.
* `--help`: Show this message and exit.

**Commands**:

* `extract`: Extract command is used to extract the data...
* `load`: Load command is used to extract the data from...
* `transform`: transform command is used to perform some...

## `eltcli extract`

Extract command is used to extract the data from the database and save it as CSV to requested location

**Usage**:

```console
$ eltcli extract [OPTIONS]
```

**Options**:

* `--database TEXT`: Give the path of the database  [required]
* `--sql TEXT`: Enter the sql  [required]
* `--params TEXT`: provide the paramters  [required]
* `--target TEXT`: enter the target location  [required]
* `--help`: Show this message and exit.

## `eltcli load`

Load command is used to extract the data from the CSV and save it to database in the requested table

**Usage**:

```console
$ eltcli load [OPTIONS]
```

**Options**:

* `--input TEXT`: Give the path of the csv file  [required]
* `--database TEXT`: Give the path of the database  [required]
* `--target TEXT`: enter the target table  [required]
* `--help`: Show this message and exit.

## `eltcli transform`

transform command is used to perform some transformation on the loaded table

**Usage**:

```console
$ eltcli transform [OPTIONS]
```

**Options**:

* `--database TEXT`: Give the path of the database  [required]
* `--sql TEXT`: Enter the sql  [required]
* `--params TEXT`: provide the paramters  [required]
* `--target TEXT`: enter the target table  [required]
* `--help`: Show this message and exit.
