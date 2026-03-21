# Oracle2PostgreSQL Migration Tool

A comprehensive dual-mode (GUI and CLI) application built in Python to automate the migration of Oracle SQL schema and PL/SQL code to PostgreSQL standards.

## Features

- **Dual-Mode Interface**: Enjoy a straightforward Tkinter GUI or use the CLI for CI/CD integration and batch processing.
- **Automated Schema Conversion**:
  - Data Type translation (e.g. `NUMBER` -> `NUMERIC`, `VARCHAR2` -> `VARCHAR`)
  - PL/SQL to PL/pgSQL procedural code conversion
  - Sequence transformations
  - Synonym to view/search_path mappings
  - Oracle Packages to PostgreSQL Schemas
  - Trigger compatibility conversions
- **Reporting & Safety**: Automatically generates thorough migration reports and creates backups of original files before any modifications.
- **Flexible Configuration**: Choose your preferred encoding, define target file extensions, and selectively toggle specific conversion features.
- **Multi-language Interface**: Interfaces available in English, Japanese, and more.

## Installation

1. Ensure Python 3.8+ is installed.
2. Clone the repository and install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### GUI Mode
Launch the graphical interface by running the main script without any arguments:
```bash
python main.py
```

### CLI Mode
Execute headless migrations directly via the command line:
```bash
python main.py -i <input_directory> -o <output_directory> [options]
```

**Common CLI Options:**
- `-i, --input`: Input directory containing Oracle SQL files
- `-o, --output`: Output directory for PostgreSQL files
- `-e, --encoding`: File encoding (default: `utf-8`)
- `--extensions`: Comma-separated file extensions to process (default: `.sql,.pls,.pkb,.pks,.trg,.vw,.fnc,.prc`)
- `--lang`: Interface language (default: `en`)
- `--no-datatypes`, `--no-plsql`, `--no-sequences`, etc.: Disable specific conversion blocks if desired.

Run `python main.py --help` for the full list of arguments.

## Tests

Validate your conversion tool using the included test suite suite:
```bash
python test_conversion.py
```
