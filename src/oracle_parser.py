"""
Oracle2PostgreSQL - Oracle SQL/PL-SQL Parser
Parses Oracle SQL files into an AST for transformation.
"""

import re
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import List, Optional, Dict, Tuple


class StatementType(Enum):
    """Types of Oracle SQL statements."""
    # DDL
    CREATE_TABLE = auto()
    CREATE_INDEX = auto()
    CREATE_VIEW = auto()
    CREATE_MATERIALIZED_VIEW = auto()
    CREATE_SEQUENCE = auto()
    CREATE_SYNONYM = auto()
    CREATE_TYPE = auto()
    CREATE_TRIGGER = auto()
    CREATE_DATABASE_LINK = auto()
    ALTER_TABLE = auto()
    ALTER_INDEX = auto()
    ALTER_SEQUENCE = auto()
    DROP = auto()
    TRUNCATE = auto()
    COMMENT = auto()
    GRANT = auto()
    REVOKE = auto()
    # DML
    SELECT = auto()
    INSERT = auto()
    UPDATE = auto()
    DELETE = auto()
    MERGE = auto()
    # PL/SQL
    CREATE_PROCEDURE = auto()
    CREATE_FUNCTION = auto()
    CREATE_PACKAGE = auto()
    CREATE_PACKAGE_BODY = auto()
    DECLARE_BLOCK = auto()
    BEGIN_BLOCK = auto()
    # Other
    SET = auto()
    EXEC = auto()
    UNKNOWN = auto()


@dataclass
class ColumnDef:
    """Oracle column definition."""
    name: str
    data_type: str
    precision: Optional[str] = None
    scale: Optional[str] = None
    nullable: bool = True
    default_value: Optional[str] = None
    is_primary_key: bool = False
    constraints: List[str] = field(default_factory=list)
    original_line: str = ""


@dataclass
class ConstraintDef:
    """Table constraint definition."""
    name: Optional[str]
    constraint_type: str  # PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK
    columns: List[str] = field(default_factory=list)
    ref_table: Optional[str] = None
    ref_columns: List[str] = field(default_factory=list)
    condition: Optional[str] = None
    original_text: str = ""


@dataclass
class IndexDef:
    """Index definition."""
    name: str
    table_name: str
    columns: List[str] = field(default_factory=list)
    is_unique: bool = False
    is_bitmap: bool = False
    tablespace: Optional[str] = None
    original_text: str = ""


@dataclass
class SequenceDef:
    """Sequence definition."""
    name: str
    start_value: Optional[int] = None
    increment_by: Optional[int] = None
    min_value: Optional[int] = None
    max_value: Optional[int] = None
    cache_size: Optional[int] = None
    cycle: bool = False
    order: bool = False
    original_text: str = ""


@dataclass
class TableDef:
    """Oracle table definition."""
    name: str
    schema: Optional[str] = None
    columns: List[ColumnDef] = field(default_factory=list)
    constraints: List[ConstraintDef] = field(default_factory=list)
    tablespace: Optional[str] = None
    storage_params: Dict[str, str] = field(default_factory=dict)
    is_temporary: bool = False
    original_text: str = ""


@dataclass
class ViewDef:
    """View definition."""
    name: str
    schema: Optional[str] = None
    query: str = ""
    is_materialized: bool = False
    is_force: bool = False
    original_text: str = ""


@dataclass
class SynonymDef:
    """Synonym definition."""
    name: str
    target: str
    is_public: bool = False
    original_text: str = ""


@dataclass
class ParameterDef:
    """PL/SQL parameter definition."""
    name: str
    data_type: str
    direction: str = "IN"  # IN, OUT, IN OUT
    default_value: Optional[str] = None


@dataclass
class ProcedureDef:
    """PL/SQL procedure/function definition."""
    name: str
    parameters: List[ParameterDef] = field(default_factory=list)
    return_type: Optional[str] = None
    body: str = ""
    is_function: bool = False
    local_variables: List[Tuple[str, str]] = field(default_factory=list)
    original_text: str = ""


@dataclass
class PackageDef:
    """PL/SQL package definition."""
    name: str
    procedures: List[ProcedureDef] = field(default_factory=list)
    functions: List[ProcedureDef] = field(default_factory=list)
    types: List[str] = field(default_factory=list)
    variables: List[Tuple[str, str]] = field(default_factory=list)
    is_body: bool = False
    original_text: str = ""


@dataclass
class TriggerDef:
    """Trigger definition."""
    name: str
    table_name: str
    timing: str = "BEFORE"  # BEFORE, AFTER, INSTEAD OF
    events: List[str] = field(default_factory=list)  # INSERT, UPDATE, DELETE
    for_each_row: bool = False
    when_clause: Optional[str] = None
    body: str = ""
    original_text: str = ""


@dataclass
class OracleStatement:
    """A parsed Oracle SQL statement."""
    type: StatementType
    text: str
    line_number: int = 0
    table_def: Optional[TableDef] = None
    index_def: Optional[IndexDef] = None
    sequence_def: Optional[SequenceDef] = None
    view_def: Optional[ViewDef] = None
    synonym_def: Optional[SynonymDef] = None
    procedure_def: Optional[ProcedureDef] = None
    package_def: Optional[PackageDef] = None
    trigger_def: Optional[TriggerDef] = None
    comments: List[str] = field(default_factory=list)


@dataclass
class OracleSchema:
    """Root AST: represents an entire Oracle SQL file."""
    filename: str = ""
    statements: List[OracleStatement] = field(default_factory=list)
    tables: List[TableDef] = field(default_factory=list)
    indexes: List[IndexDef] = field(default_factory=list)
    sequences: List[SequenceDef] = field(default_factory=list)
    views: List[ViewDef] = field(default_factory=list)
    synonyms: List[SynonymDef] = field(default_factory=list)
    procedures: List[ProcedureDef] = field(default_factory=list)
    packages: List[PackageDef] = field(default_factory=list)
    triggers: List[TriggerDef] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    # Metadata
    has_plsql: bool = False
    has_packages: bool = False
    has_triggers: bool = False
    has_synonyms: bool = False
    has_sequences: bool = False
    has_dblinks: bool = False
    has_oracle_hints: bool = False
    has_connect_by: bool = False
    has_outer_join_plus: bool = False
    oracle_functions_used: List[str] = field(default_factory=list)


class OracleParser:
    """Parser for Oracle SQL/PL-SQL files."""

    def __init__(self, encoding: str = "utf-8"):
        self.encoding = encoding
        self._errors: List[str] = []

    def parse_file(self, filepath: str) -> OracleSchema:
        """Parse an Oracle SQL file into an OracleSchema AST."""
        try:
            with open(filepath, "r", encoding=self.encoding, errors="replace") as f:
                content = f.read()
        except (OSError, IOError) as e:
            schema = OracleSchema(filename=filepath)
            schema.errors.append(f"Failed to read file: {e}")
            return schema

        schema = self.parse_string(content)
        schema.filename = filepath
        return schema

    def parse_string(self, content: str) -> OracleSchema:
        """Parse Oracle SQL content string into an OracleSchema AST."""
        schema = OracleSchema()
        self._errors = []

        # Preprocess
        content = self._normalize_line_endings(content)
        raw_statements = self._split_statements(content)

        for line_no, stmt_text in raw_statements:
            stmt_text = stmt_text.strip()
            if not stmt_text:
                continue
            stmt = self._parse_statement(stmt_text, line_no)
            schema.statements.append(stmt)
            self._collect_into_schema(schema, stmt)

        # Detect Oracle-specific features
        self._detect_features(schema, content)
        schema.errors = self._errors
        return schema

    def _normalize_line_endings(self, content: str) -> str:
        return content.replace("\r\n", "\n").replace("\r", "\n")

    def _split_statements(self, content: str) -> List[Tuple[int, str]]:
        """Split SQL content into individual statements."""
        statements = []
        current = []
        current_line = 1
        start_line = 1
        in_plsql_block = False
        block_depth = 0
        in_string = False
        in_comment = False
        in_block_comment = False

        lines = content.split("\n")
        i = 0
        while i < len(lines):
            line = lines[i]
            stripped = line.strip()
            current_line = i + 1

            if not current:
                start_line = current_line

            # Skip empty lines between statements
            if not stripped and not current:
                i += 1
                continue

            # Handle single-line comments
            if stripped.startswith("--") and not in_plsql_block:
                current.append(line)
                i += 1
                continue

            # Detect PL/SQL block start
            upper = stripped.upper()
            if re.match(r"^CREATE\s+(?:OR\s+REPLACE\s+)?(?:PACKAGE|PROCEDURE|FUNCTION|TRIGGER)\b",
                        upper, re.IGNORECASE):
                in_plsql_block = True
                block_depth = 0

            current.append(line)

            if in_plsql_block:
                # Count BEGIN/END pairs
                clean_line = self._remove_strings_and_comments(stripped)
                clean_upper = clean_line.upper()

                begins = len(re.findall(r"\bBEGIN\b", clean_upper))
                ends = len(re.findall(r"\bEND\b", clean_upper))
                block_depth += begins - ends

                # PL/SQL block ends with / on its own line
                if stripped == "/" or (block_depth <= 0 and ends > 0
                                       and re.search(r"\bEND\s*;?\s*$", clean_upper)):
                    stmt_text = "\n".join(current)
                    if stmt_text.strip().endswith("/"):
                        stmt_text = stmt_text.strip()[:-1].strip()
                    if stmt_text.strip():
                        statements.append((start_line, stmt_text))
                    current = []
                    in_plsql_block = False
                    block_depth = 0
            else:
                # Non-PL/SQL: split on ;
                if ";" in stripped:
                    stmt_text = "\n".join(current)
                    # Handle multiple ; in one line
                    parts = self._split_on_semicolon(stmt_text)
                    for p in parts:
                        p = p.strip()
                        if p:
                            statements.append((start_line, p))
                    current = []

            i += 1

        # Remaining content
        if current:
            stmt_text = "\n".join(current).strip()
            if stmt_text and stmt_text != "/":
                statements.append((start_line, stmt_text))

        return statements

    def _split_on_semicolon(self, text: str) -> List[str]:
        """Split text on semicolons, respecting string literals."""
        parts = []
        current = []
        in_string = False
        i = 0
        chars = text
        while i < len(chars):
            c = chars[i]
            if c == "'" and not in_string:
                in_string = True
                current.append(c)
            elif c == "'" and in_string:
                # Check for escaped quote
                if i + 1 < len(chars) and chars[i + 1] == "'":
                    current.append("''")
                    i += 1
                else:
                    in_string = False
                    current.append(c)
            elif c == ";" and not in_string:
                parts.append("".join(current))
                current = []
            else:
                current.append(c)
            i += 1
        if current:
            parts.append("".join(current))
        return parts

    def _remove_strings_and_comments(self, text: str) -> str:
        """Remove string literals and comments for analysis."""
        # Remove string literals
        text = re.sub(r"'[^']*'", "''", text)
        # Remove single-line comments
        text = re.sub(r"--.*$", "", text)
        # Remove block comments
        text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
        return text

    def _parse_statement(self, text: str, line_number: int) -> OracleStatement:
        """Parse a single SQL statement into an OracleStatement."""
        upper = text.strip().upper()
        clean = self._remove_strings_and_comments(text).strip().upper()

        # Comment-only
        if clean.startswith("--") or (clean.startswith("/*") and clean.endswith("*/")):
            return OracleStatement(type=StatementType.UNKNOWN, text=text, line_number=line_number)

        stmt_type = self._detect_statement_type(clean)
        stmt = OracleStatement(type=stmt_type, text=text, line_number=line_number)

        try:
            if stmt_type == StatementType.CREATE_TABLE:
                stmt.table_def = self._parse_create_table(text)
            elif stmt_type == StatementType.CREATE_INDEX:
                stmt.index_def = self._parse_create_index(text)
            elif stmt_type == StatementType.CREATE_SEQUENCE:
                stmt.sequence_def = self._parse_create_sequence(text)
            elif stmt_type in (StatementType.CREATE_VIEW, StatementType.CREATE_MATERIALIZED_VIEW):
                stmt.view_def = self._parse_create_view(text, stmt_type == StatementType.CREATE_MATERIALIZED_VIEW)
            elif stmt_type == StatementType.CREATE_SYNONYM:
                stmt.synonym_def = self._parse_create_synonym(text)
            elif stmt_type in (StatementType.CREATE_PROCEDURE, StatementType.CREATE_FUNCTION):
                stmt.procedure_def = self._parse_create_procedure(text, stmt_type == StatementType.CREATE_FUNCTION)
            elif stmt_type in (StatementType.CREATE_PACKAGE, StatementType.CREATE_PACKAGE_BODY):
                stmt.package_def = self._parse_create_package(text, stmt_type == StatementType.CREATE_PACKAGE_BODY)
            elif stmt_type == StatementType.CREATE_TRIGGER:
                stmt.trigger_def = self._parse_create_trigger(text)
        except Exception as e:
            self._errors.append(f"Line {line_number}: Parse error in {stmt_type.name}: {e}")

        return stmt

    def _detect_statement_type(self, upper_text: str) -> StatementType:
        """Detect the type of SQL statement from its text."""
        t = upper_text.strip()

        if re.match(r"^CREATE\s+(?:OR\s+REPLACE\s+)?(?:GLOBAL\s+TEMPORARY\s+)?TABLE\b", t):
            return StatementType.CREATE_TABLE
        if re.match(r"^CREATE\s+(?:UNIQUE\s+)?(?:BITMAP\s+)?INDEX\b", t):
            return StatementType.CREATE_INDEX
        if re.match(r"^CREATE\s+MATERIALIZED\s+VIEW\b", t):
            return StatementType.CREATE_MATERIALIZED_VIEW
        if re.match(r"^CREATE\s+(?:OR\s+REPLACE\s+)?(?:FORCE\s+)?VIEW\b", t):
            return StatementType.CREATE_VIEW
        if re.match(r"^CREATE\s+SEQUENCE\b", t):
            return StatementType.CREATE_SEQUENCE
        if re.match(r"^CREATE\s+(?:OR\s+REPLACE\s+)?(?:PUBLIC\s+)?SYNONYM\b", t):
            return StatementType.CREATE_SYNONYM
        if re.match(r"^CREATE\s+(?:OR\s+REPLACE\s+)?PACKAGE\s+BODY\b", t):
            return StatementType.CREATE_PACKAGE_BODY
        if re.match(r"^CREATE\s+(?:OR\s+REPLACE\s+)?PACKAGE\b", t):
            return StatementType.CREATE_PACKAGE
        if re.match(r"^CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\b", t):
            return StatementType.CREATE_PROCEDURE
        if re.match(r"^CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\b", t):
            return StatementType.CREATE_FUNCTION
        if re.match(r"^CREATE\s+(?:OR\s+REPLACE\s+)?TRIGGER\b", t):
            return StatementType.CREATE_TRIGGER
        if re.match(r"^CREATE\s+(?:OR\s+REPLACE\s+)?TYPE\b", t):
            return StatementType.CREATE_TYPE
        if re.match(r"^CREATE\s+(?:PUBLIC\s+)?DATABASE\s+LINK\b", t):
            return StatementType.CREATE_DATABASE_LINK
        if re.match(r"^ALTER\s+TABLE\b", t):
            return StatementType.ALTER_TABLE
        if re.match(r"^ALTER\s+INDEX\b", t):
            return StatementType.ALTER_INDEX
        if re.match(r"^ALTER\s+SEQUENCE\b", t):
            return StatementType.ALTER_SEQUENCE
        if re.match(r"^DROP\b", t):
            return StatementType.DROP
        if re.match(r"^TRUNCATE\b", t):
            return StatementType.TRUNCATE
        if re.match(r"^COMMENT\s+ON\b", t):
            return StatementType.COMMENT
        if re.match(r"^GRANT\b", t):
            return StatementType.GRANT
        if re.match(r"^REVOKE\b", t):
            return StatementType.REVOKE
        if re.match(r"^SELECT\b", t):
            return StatementType.SELECT
        if re.match(r"^INSERT\b", t):
            return StatementType.INSERT
        if re.match(r"^UPDATE\b", t):
            return StatementType.UPDATE
        if re.match(r"^DELETE\b", t):
            return StatementType.DELETE
        if re.match(r"^MERGE\b", t):
            return StatementType.MERGE
        if re.match(r"^DECLARE\b", t):
            return StatementType.DECLARE_BLOCK
        if re.match(r"^BEGIN\b", t):
            return StatementType.BEGIN_BLOCK
        if re.match(r"^SET\b", t):
            return StatementType.SET
        if re.match(r"^EXEC\b", t):
            return StatementType.EXEC
        return StatementType.UNKNOWN

    # ------------------------------------------------------------------
    # DDL Parsers
    # ------------------------------------------------------------------
    def _parse_create_table(self, text: str) -> TableDef:
        """Parse CREATE TABLE statement."""
        table = TableDef(name="", original_text=text)

        # Extract table name
        m = re.search(
            r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:GLOBAL\s+TEMPORARY\s+)?TABLE\s+"
            r"(?:(\w+)\.)?(\w+)",
            text, re.IGNORECASE
        )
        if m:
            table.schema = m.group(1)
            table.name = m.group(2)

        if re.search(r"GLOBAL\s+TEMPORARY", text, re.IGNORECASE):
            table.is_temporary = True

        # Extract column definitions within parentheses
        paren_match = re.search(r"\(\s*(.*)\s*\)", text, re.DOTALL)
        if paren_match:
            body = paren_match.group(1)
            self._parse_table_body(table, body)

        # Extract storage parameters
        storage_m = re.search(r"TABLESPACE\s+(\w+)", text, re.IGNORECASE)
        if storage_m:
            table.tablespace = storage_m.group(1)

        return table

    def _parse_table_body(self, table: TableDef, body: str):
        """Parse the column/constraint definitions inside CREATE TABLE(...)."""
        parts = self._split_columns(body)

        for part in parts:
            part = part.strip()
            if not part:
                continue
            upper = part.strip().upper()

            # Constraint
            if (upper.startswith("CONSTRAINT") or upper.startswith("PRIMARY KEY")
                    or upper.startswith("FOREIGN KEY") or upper.startswith("UNIQUE")
                    or upper.startswith("CHECK")):
                constraint = self._parse_constraint(part)
                if constraint:
                    table.constraints.append(constraint)
            else:
                col = self._parse_column_def(part)
                if col:
                    table.columns.append(col)

    def _split_columns(self, body: str) -> List[str]:
        """Split column definitions by comma, respecting parentheses."""
        parts = []
        current = []
        depth = 0
        in_string = False

        for c in body:
            if c == "'" and not in_string:
                in_string = True
                current.append(c)
            elif c == "'" and in_string:
                in_string = False
                current.append(c)
            elif c == "(" and not in_string:
                depth += 1
                current.append(c)
            elif c == ")" and not in_string:
                depth -= 1
                current.append(c)
            elif c == "," and depth == 0 and not in_string:
                parts.append("".join(current))
                current = []
            else:
                current.append(c)

        if current:
            parts.append("".join(current))
        return parts

    def _parse_column_def(self, text: str) -> Optional[ColumnDef]:
        """Parse a single column definition."""
        text = text.strip()
        if not text:
            return None

        # Match: column_name DATA_TYPE[(precision[,scale])] [DEFAULT ...] [NOT NULL] [constraints]
        m = re.match(
            r'"?(\w+)"?\s+'
            r'([\w]+(?:\s*\([^)]*\))?(?:\s+WITH\s+(?:LOCAL\s+)?TIME\s+ZONE)?'
            r'(?:\s+BYTE)?)',
            text, re.IGNORECASE
        )
        if not m:
            return None

        col = ColumnDef(name=m.group(1), data_type=m.group(2).strip(), original_line=text)

        # Parse precision/scale from data type
        type_match = re.match(r"(\w+)\s*\(\s*(\d+)(?:\s*,\s*(\d+))?\s*(?:BYTE|CHAR)?\s*\)",
                              col.data_type, re.IGNORECASE)
        if type_match:
            col.data_type = type_match.group(1)
            col.precision = type_match.group(2)
            col.scale = type_match.group(3)

        # Default value
        default_m = re.search(r"\bDEFAULT\s+(.+?)(?:\s+(?:NOT\s+NULL|NULL|CONSTRAINT|PRIMARY|UNIQUE|CHECK|REFERENCES)\b|$)",
                              text, re.IGNORECASE)
        if default_m:
            col.default_value = default_m.group(1).strip()

        # NOT NULL
        if re.search(r"\bNOT\s+NULL\b", text, re.IGNORECASE):
            col.nullable = False

        # Inline PRIMARY KEY
        if re.search(r"\bPRIMARY\s+KEY\b", text, re.IGNORECASE):
            col.is_primary_key = True

        return col

    def _parse_constraint(self, text: str) -> Optional[ConstraintDef]:
        """Parse a table constraint definition."""
        constraint = ConstraintDef(name=None, constraint_type="", original_text=text)

        # Named constraint
        name_m = re.match(r"CONSTRAINT\s+(\w+)\s+", text, re.IGNORECASE)
        if name_m:
            constraint.name = name_m.group(1)
            text = text[name_m.end():]

        upper = text.strip().upper()
        if upper.startswith("PRIMARY KEY"):
            constraint.constraint_type = "PRIMARY KEY"
            cols_m = re.search(r"PRIMARY\s+KEY\s*\(([^)]+)\)", text, re.IGNORECASE)
            if cols_m:
                constraint.columns = [c.strip().strip('"') for c in cols_m.group(1).split(",")]
        elif upper.startswith("FOREIGN KEY"):
            constraint.constraint_type = "FOREIGN KEY"
            fk_m = re.search(
                r"FOREIGN\s+KEY\s*\(([^)]+)\)\s+REFERENCES\s+(\w+)\s*\(([^)]+)\)",
                text, re.IGNORECASE
            )
            if fk_m:
                constraint.columns = [c.strip().strip('"') for c in fk_m.group(1).split(",")]
                constraint.ref_table = fk_m.group(2)
                constraint.ref_columns = [c.strip().strip('"') for c in fk_m.group(3).split(",")]
        elif upper.startswith("UNIQUE"):
            constraint.constraint_type = "UNIQUE"
            cols_m = re.search(r"UNIQUE\s*\(([^)]+)\)", text, re.IGNORECASE)
            if cols_m:
                constraint.columns = [c.strip().strip('"') for c in cols_m.group(1).split(",")]
        elif upper.startswith("CHECK"):
            constraint.constraint_type = "CHECK"
            check_m = re.search(r"CHECK\s*\((.+)\)", text, re.IGNORECASE | re.DOTALL)
            if check_m:
                constraint.condition = check_m.group(1).strip()

        return constraint

    def _parse_create_index(self, text: str) -> IndexDef:
        """Parse CREATE INDEX statement."""
        idx = IndexDef(name="", table_name="", original_text=text)
        idx.is_unique = bool(re.search(r"\bUNIQUE\b", text, re.IGNORECASE))
        idx.is_bitmap = bool(re.search(r"\bBITMAP\b", text, re.IGNORECASE))

        m = re.search(r"INDEX\s+(?:(\w+)\.)?(\w+)\s+ON\s+(?:(\w+)\.)?(\w+)\s*\(([^)]+)\)",
                       text, re.IGNORECASE)
        if m:
            idx.name = m.group(2)
            idx.table_name = m.group(4)
            idx.columns = [c.strip() for c in m.group(5).split(",")]

        ts_m = re.search(r"TABLESPACE\s+(\w+)", text, re.IGNORECASE)
        if ts_m:
            idx.tablespace = ts_m.group(1)

        return idx

    def _parse_create_sequence(self, text: str) -> SequenceDef:
        """Parse CREATE SEQUENCE statement."""
        seq = SequenceDef(name="", original_text=text)
        m = re.search(r"CREATE\s+SEQUENCE\s+(?:(\w+)\.)?(\w+)", text, re.IGNORECASE)
        if m:
            seq.name = m.group(2)

        for pattern, attr, conv in [
            (r"START\s+WITH\s+(\d+)", "start_value", int),
            (r"INCREMENT\s+BY\s+(\d+)", "increment_by", int),
            (r"MINVALUE\s+(\d+)", "min_value", int),
            (r"MAXVALUE\s+(\d+)", "max_value", int),
            (r"CACHE\s+(\d+)", "cache_size", int),
        ]:
            pm = re.search(pattern, text, re.IGNORECASE)
            if pm:
                setattr(seq, attr, conv(pm.group(1)))

        seq.cycle = bool(re.search(r"\bCYCLE\b", text, re.IGNORECASE)
                          and not re.search(r"\bNOCYCLE\b", text, re.IGNORECASE))
        return seq

    def _parse_create_view(self, text: str, is_materialized: bool = False) -> ViewDef:
        """Parse CREATE VIEW / CREATE MATERIALIZED VIEW."""
        view = ViewDef(name="", original_text=text, is_materialized=is_materialized)
        view.is_force = bool(re.search(r"\bFORCE\b", text, re.IGNORECASE))

        if is_materialized:
            m = re.search(r"MATERIALIZED\s+VIEW\s+(?:(\w+)\.)?(\w+)", text, re.IGNORECASE)
        else:
            m = re.search(r"VIEW\s+(?:(\w+)\.)?(\w+)", text, re.IGNORECASE)
        if m:
            view.schema = m.group(1)
            view.name = m.group(2)

        as_m = re.search(r"\bAS\s+(.+)", text, re.IGNORECASE | re.DOTALL)
        if as_m:
            view.query = as_m.group(1).strip().rstrip(";")
        return view

    def _parse_create_synonym(self, text: str) -> SynonymDef:
        """Parse CREATE SYNONYM."""
        syn = SynonymDef(name="", target="", original_text=text)
        syn.is_public = bool(re.search(r"\bPUBLIC\b", text, re.IGNORECASE))

        m = re.search(r"SYNONYM\s+(?:(\w+)\.)?(\w+)\s+FOR\s+(?:(\w+)\.)?(\w+)",
                       text, re.IGNORECASE)
        if m:
            syn.name = m.group(2)
            target_parts = [p for p in [m.group(3), m.group(4)] if p]
            syn.target = ".".join(target_parts)
        return syn

    def _parse_create_procedure(self, text: str, is_function: bool = False) -> ProcedureDef:
        """Parse CREATE PROCEDURE / CREATE FUNCTION."""
        proc = ProcedureDef(name="", is_function=is_function, original_text=text)

        keyword = "FUNCTION" if is_function else "PROCEDURE"
        m = re.search(rf"{keyword}\s+(?:(\w+)\.)?(\w+)", text, re.IGNORECASE)
        if m:
            proc.name = m.group(2)

        # Extract parameters
        param_m = re.search(rf"{keyword}\s+\S+\s*\(([^)]*)\)", text, re.IGNORECASE | re.DOTALL)
        if param_m:
            proc.parameters = self._parse_parameters(param_m.group(1))

        # Return type for functions
        if is_function:
            ret_m = re.search(r"\bRETURN\s+(\S+)", text, re.IGNORECASE)
            if ret_m:
                proc.return_type = ret_m.group(1)

        # Extract body (everything after IS/AS)
        body_m = re.search(r"\b(?:IS|AS)\b\s*\n?(.*)", text, re.IGNORECASE | re.DOTALL)
        if body_m:
            proc.body = body_m.group(1).strip()

        return proc

    def _parse_parameters(self, param_text: str) -> List[ParameterDef]:
        """Parse procedure/function parameters."""
        params = []
        if not param_text.strip():
            return params

        for part in param_text.split(","):
            part = part.strip()
            if not part:
                continue

            # param_name [IN|OUT|IN OUT] data_type [DEFAULT value]
            m = re.match(
                r"(\w+)\s+"
                r"(?:(IN\s+OUT|IN|OUT)\s+)?"
                r"(\S+)"
                r"(?:\s+DEFAULT\s+(.+))?",
                part, re.IGNORECASE
            )
            if m:
                direction = (m.group(2) or "IN").upper().strip()
                params.append(ParameterDef(
                    name=m.group(1),
                    direction=direction,
                    data_type=m.group(3),
                    default_value=m.group(4),
                ))
        return params

    def _parse_create_package(self, text: str, is_body: bool = False) -> PackageDef:
        """Parse CREATE PACKAGE / CREATE PACKAGE BODY."""
        pkg = PackageDef(name="", is_body=is_body, original_text=text)

        keyword = "PACKAGE\\s+BODY" if is_body else "PACKAGE"
        m = re.search(rf"{keyword}\s+(?:(\w+)\.)?(\w+)", text, re.IGNORECASE)
        if m:
            pkg.name = m.group(2)

        # Extract procedure/function declarations in the package
        for proc_m in re.finditer(
            r"(?:PROCEDURE|FUNCTION)\s+(\w+)\s*(?:\([^)]*\))?",
            text, re.IGNORECASE
        ):
            name = proc_m.group(1)
            is_func = proc_m.group(0).strip().upper().startswith("FUNCTION")
            p = ProcedureDef(name=name, is_function=is_func)
            if is_func:
                pkg.functions.append(p)
            else:
                pkg.procedures.append(p)

        return pkg

    def _parse_create_trigger(self, text: str) -> TriggerDef:
        """Parse CREATE TRIGGER."""
        trig = TriggerDef(name="", table_name="", original_text=text)

        m = re.search(r"TRIGGER\s+(?:(\w+)\.)?(\w+)", text, re.IGNORECASE)
        if m:
            trig.name = m.group(2)

        timing_m = re.search(r"\b(BEFORE|AFTER|INSTEAD\s+OF)\b", text, re.IGNORECASE)
        if timing_m:
            trig.timing = timing_m.group(1).upper()

        events = []
        for event in ["INSERT", "UPDATE", "DELETE"]:
            if re.search(rf"\b{event}\b", text, re.IGNORECASE):
                events.append(event)
        trig.events = events

        on_m = re.search(r"\bON\s+(?:(\w+)\.)?(\w+)", text, re.IGNORECASE)
        if on_m:
            trig.table_name = on_m.group(2)

        trig.for_each_row = bool(re.search(r"\bFOR\s+EACH\s+ROW\b", text, re.IGNORECASE))

        when_m = re.search(r"\bWHEN\s*\((.+?)\)", text, re.IGNORECASE | re.DOTALL)
        if when_m:
            trig.when_clause = when_m.group(1).strip()

        body_m = re.search(r"\b(?:BEGIN|DECLARE)\b(.*)", text, re.IGNORECASE | re.DOTALL)
        if body_m:
            trig.body = body_m.group(0).strip()

        return trig

    # ------------------------------------------------------------------
    # Feature Detection
    # ------------------------------------------------------------------
    def _detect_features(self, schema: OracleSchema, content: str):
        """Detect Oracle-specific features used in the SQL content."""
        upper = content.upper()

        schema.has_plsql = bool(
            schema.procedures or schema.packages or schema.triggers
            or re.search(r"\bBEGIN\b", upper)
        )
        schema.has_packages = bool(schema.packages)
        schema.has_triggers = bool(schema.triggers)
        schema.has_synonyms = bool(schema.synonyms)
        schema.has_sequences = bool(schema.sequences)
        schema.has_dblinks = bool(re.search(r"\bDATABASE\s+LINK\b", upper))
        schema.has_oracle_hints = bool(re.search(r"/\*\+", content))
        schema.has_connect_by = bool(re.search(r"\bCONNECT\s+BY\b", upper))
        schema.has_outer_join_plus = bool(re.search(r"\(\+\)", content))

        # Detect Oracle-specific functions
        oracle_funcs = [
            "SYSDATE", "SYSTIMESTAMP", "NVL", "NVL2", "DECODE",
            "TO_DATE", "TO_CHAR", "TO_NUMBER", "SUBSTR", "INSTR",
            "ROWNUM", "ROWID", "SYS_GUID", "ADD_MONTHS",
            "MONTHS_BETWEEN", "TRUNC", "DBMS_OUTPUT", "DBMS_LOB",
            "UTL_FILE", "DBMS_SCHEDULER", "REGEXP_LIKE",
            "LISTAGG", "WM_CONCAT", "LENGTHB",
            "RAISE_APPLICATION_ERROR",
        ]
        found = []
        for func in oracle_funcs:
            if re.search(rf"\b{func}\b", upper):
                found.append(func)
        schema.oracle_functions_used = found

    def _collect_into_schema(self, schema: OracleSchema, stmt: OracleStatement):
        """Collect parsed definitions into the schema's typed lists."""
        if stmt.table_def:
            schema.tables.append(stmt.table_def)
        if stmt.index_def:
            schema.indexes.append(stmt.index_def)
        if stmt.sequence_def:
            schema.sequences.append(stmt.sequence_def)
        if stmt.view_def:
            schema.views.append(stmt.view_def)
        if stmt.synonym_def:
            schema.synonyms.append(stmt.synonym_def)
        if stmt.procedure_def:
            schema.procedures.append(stmt.procedure_def)
        if stmt.package_def:
            schema.packages.append(stmt.package_def)
        if stmt.trigger_def:
            schema.triggers.append(stmt.trigger_def)
