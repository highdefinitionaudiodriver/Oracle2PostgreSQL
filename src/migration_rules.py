"""
Oracle2PostgreSQL - Migration Rules Engine
Centralized rule registry for Oracle → PostgreSQL transformations.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class MigrationRule:
    """A single migration rule."""
    rule_id: str
    category: str       # DATATYPE, FUNCTION, SYNTAX, PLSQL, SEQUENCE, OBJECT, TRIGGER, SYNONYM
    severity: str       # AUTO, REVIEW, MANUAL
    old_pattern: str
    new_pattern: str
    description: str
    description_ja: str = ""
    auto_fix: bool = True
    severity_override: str = ""
    field_mapping: Dict[str, str] = field(default_factory=dict)
    notes: str = ""


class MigrationRules:
    """Central registry of all Oracle → PostgreSQL migration rules."""

    def __init__(self):
        self.rules: List[MigrationRule] = []
        self._init_datatype_rules()
        self._init_function_rules()
        self._init_syntax_rules()
        self._init_plsql_rules()
        self._init_sequence_rules()
        self._init_object_rules()
        self._init_trigger_rules()
        self._init_synonym_rules()
        self._init_join_rules()
        self._init_hint_rules()

    # ------------------------------------------------------------------
    # Data Type Rules
    # ------------------------------------------------------------------
    def _init_datatype_rules(self):
        rules = [
            MigrationRule(
                rule_id="DT_001", category="DATATYPE", severity="AUTO",
                old_pattern=r"\bNUMBER\b(?!\s*\()", new_pattern="NUMERIC",
                description="NUMBER (no precision) → NUMERIC",
                description_ja="NUMBER（精度なし）→ NUMERIC",
            ),
            MigrationRule(
                rule_id="DT_002", category="DATATYPE", severity="AUTO",
                old_pattern=r"\bNUMBER\s*\(\s*(\d+)\s*\)", new_pattern=r"NUMERIC(\1)",
                description="NUMBER(p) → NUMERIC(p)",
                description_ja="NUMBER(p) → NUMERIC(p)",
            ),
            MigrationRule(
                rule_id="DT_003", category="DATATYPE", severity="AUTO",
                old_pattern=r"\bNUMBER\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", new_pattern=r"NUMERIC(\1,\2)",
                description="NUMBER(p,s) → NUMERIC(p,s)",
                description_ja="NUMBER(p,s) → NUMERIC(p,s)",
            ),
            MigrationRule(
                rule_id="DT_004", category="DATATYPE", severity="AUTO",
                old_pattern=r"\bVARCHAR2\s*\(", new_pattern="VARCHAR(",
                description="VARCHAR2 → VARCHAR",
                description_ja="VARCHAR2 → VARCHAR",
            ),
            MigrationRule(
                rule_id="DT_005", category="DATATYPE", severity="AUTO",
                old_pattern=r"\bNVARCHAR2\s*\(", new_pattern="VARCHAR(",
                description="NVARCHAR2 → VARCHAR",
                description_ja="NVARCHAR2 → VARCHAR",
            ),
            MigrationRule(
                rule_id="DT_006", category="DATATYPE", severity="AUTO",
                old_pattern=r"\bCLOB\b", new_pattern="TEXT",
                description="CLOB → TEXT",
                description_ja="CLOB → TEXT",
            ),
            MigrationRule(
                rule_id="DT_007", category="DATATYPE", severity="AUTO",
                old_pattern=r"\bNCLOB\b", new_pattern="TEXT",
                description="NCLOB → TEXT",
                description_ja="NCLOB → TEXT",
            ),
            MigrationRule(
                rule_id="DT_008", category="DATATYPE", severity="AUTO",
                old_pattern=r"\bBLOB\b", new_pattern="BYTEA",
                description="BLOB → BYTEA",
                description_ja="BLOB → BYTEA",
            ),
            MigrationRule(
                rule_id="DT_009", category="DATATYPE", severity="AUTO",
                old_pattern=r"\bRAW\s*\(\s*\d+\s*\)", new_pattern="BYTEA",
                description="RAW(n) → BYTEA",
                description_ja="RAW(n) → BYTEA",
            ),
            MigrationRule(
                rule_id="DT_010", category="DATATYPE", severity="AUTO",
                old_pattern=r"\bLONG\s+RAW\b", new_pattern="BYTEA",
                description="LONG RAW → BYTEA",
                description_ja="LONG RAW → BYTEA",
            ),
            MigrationRule(
                rule_id="DT_011", category="DATATYPE", severity="AUTO",
                old_pattern=r"\bLONG\b", new_pattern="TEXT",
                description="LONG → TEXT",
                description_ja="LONG → TEXT",
            ),
            MigrationRule(
                rule_id="DT_012", category="DATATYPE", severity="AUTO",
                old_pattern=r"\bBINARY_FLOAT\b", new_pattern="REAL",
                description="BINARY_FLOAT → REAL",
                description_ja="BINARY_FLOAT → REAL",
            ),
            MigrationRule(
                rule_id="DT_013", category="DATATYPE", severity="AUTO",
                old_pattern=r"\bBINARY_DOUBLE\b", new_pattern="DOUBLE PRECISION",
                description="BINARY_DOUBLE → DOUBLE PRECISION",
                description_ja="BINARY_DOUBLE → DOUBLE PRECISION",
            ),
            MigrationRule(
                rule_id="DT_014", category="DATATYPE", severity="AUTO",
                old_pattern=r"\bDATE\b", new_pattern="TIMESTAMP",
                description="DATE → TIMESTAMP (Oracle DATE includes time)",
                description_ja="DATE → TIMESTAMP（OracleのDATEは時刻を含む）",
                notes="Oracle DATE includes time component, PostgreSQL DATE does not",
            ),
            MigrationRule(
                rule_id="DT_015", category="DATATYPE", severity="AUTO",
                old_pattern=r"\bTIMESTAMP\s*\(\s*(\d+)\s*\)\s+WITH\s+LOCAL\s+TIME\s+ZONE\b",
                new_pattern=r"TIMESTAMPTZ(\1)",
                description="TIMESTAMP WITH LOCAL TIME ZONE → TIMESTAMPTZ",
                description_ja="TIMESTAMP WITH LOCAL TIME ZONE → TIMESTAMPTZ",
            ),
            MigrationRule(
                rule_id="DT_016", category="DATATYPE", severity="AUTO",
                old_pattern=r"\bINTERVAL\s+YEAR\s*(?:\(\d+\))?\s+TO\s+MONTH\b",
                new_pattern="INTERVAL",
                description="INTERVAL YEAR TO MONTH → INTERVAL",
                description_ja="INTERVAL YEAR TO MONTH → INTERVAL",
            ),
            MigrationRule(
                rule_id="DT_017", category="DATATYPE", severity="AUTO",
                old_pattern=r"\bINTERVAL\s+DAY\s*(?:\(\d+\))?\s+TO\s+SECOND\s*(?:\(\d+\))?\b",
                new_pattern="INTERVAL",
                description="INTERVAL DAY TO SECOND → INTERVAL",
                description_ja="INTERVAL DAY TO SECOND → INTERVAL",
            ),
            MigrationRule(
                rule_id="DT_018", category="DATATYPE", severity="REVIEW",
                old_pattern=r"\bXMLTYPE\b", new_pattern="XML",
                description="XMLTYPE → XML",
                description_ja="XMLTYPE → XML",
                notes="Check XML operations compatibility",
            ),
            MigrationRule(
                rule_id="DT_019", category="DATATYPE", severity="AUTO",
                old_pattern=r"\bBFILE\b", new_pattern="TEXT",
                description="BFILE → TEXT (store path reference)",
                description_ja="BFILE → TEXT（パス参照として保存）",
                notes="BFILE stores external file reference; use TEXT to store path",
            ),
            MigrationRule(
                rule_id="DT_020", category="DATATYPE", severity="AUTO",
                old_pattern=r"\bCHAR\s*\((\d+)\s+BYTE\)", new_pattern=r"CHAR(\1)",
                description="CHAR(n BYTE) → CHAR(n)",
                description_ja="CHAR(n BYTE) → CHAR(n)",
            ),
            MigrationRule(
                rule_id="DT_021", category="DATATYPE", severity="AUTO",
                old_pattern=r"\bVARCHAR2\s*\((\d+)\s+BYTE\)", new_pattern=r"VARCHAR(\1)",
                description="VARCHAR2(n BYTE) → VARCHAR(n)",
                description_ja="VARCHAR2(n BYTE) → VARCHAR(n)",
            ),
            MigrationRule(
                rule_id="DT_022", category="DATATYPE", severity="AUTO",
                old_pattern=r"\bFLOAT\b(?!\s*\()", new_pattern="DOUBLE PRECISION",
                description="FLOAT → DOUBLE PRECISION",
                description_ja="FLOAT → DOUBLE PRECISION",
            ),
            MigrationRule(
                rule_id="DT_023", category="DATATYPE", severity="AUTO",
                old_pattern=r"\bNUMBER\s*\(\s*1\s*\)", new_pattern="BOOLEAN",
                description="NUMBER(1) → BOOLEAN (likely flag column)",
                description_ja="NUMBER(1) → BOOLEAN（フラグ列の可能性）",
                notes="Common Oracle pattern for boolean flags",
            ),
        ]
        self.rules.extend(rules)

    # ------------------------------------------------------------------
    # Function Rules
    # ------------------------------------------------------------------
    def _init_function_rules(self):
        rules = [
            MigrationRule(
                rule_id="FN_001", category="FUNCTION", severity="AUTO",
                old_pattern=r"\bSYSDATE\b", new_pattern="CURRENT_TIMESTAMP",
                description="SYSDATE → CURRENT_TIMESTAMP",
                description_ja="SYSDATE → CURRENT_TIMESTAMP",
            ),
            MigrationRule(
                rule_id="FN_002", category="FUNCTION", severity="AUTO",
                old_pattern=r"\bSYSTIMESTAMP\b", new_pattern="CURRENT_TIMESTAMP",
                description="SYSTIMESTAMP → CURRENT_TIMESTAMP",
                description_ja="SYSTIMESTAMP → CURRENT_TIMESTAMP",
            ),
            MigrationRule(
                rule_id="FN_003", category="FUNCTION", severity="AUTO",
                old_pattern=r"\bNVL\s*\(", new_pattern="COALESCE(",
                description="NVL() → COALESCE()",
                description_ja="NVL() → COALESCE()",
            ),
            MigrationRule(
                rule_id="FN_004", category="FUNCTION", severity="AUTO",
                old_pattern=r"\bNVL2\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)",
                new_pattern=r"CASE WHEN \1 IS NOT NULL THEN \2 ELSE \3 END",
                description="NVL2(expr,val1,val2) → CASE WHEN expr IS NOT NULL THEN val1 ELSE val2 END",
                description_ja="NVL2 → CASE WHEN式",
            ),
            MigrationRule(
                rule_id="FN_005", category="FUNCTION", severity="AUTO",
                old_pattern=r"\bDECODE\s*\(", new_pattern="/* DECODE→CASE */ CASE",
                description="DECODE() → CASE expression (requires manual restructuring)",
                description_ja="DECODE() → CASE式（手動での構造変更が必要）",
                auto_fix=False,
                severity_override="REVIEW",
            ),
            MigrationRule(
                rule_id="FN_006", category="FUNCTION", severity="AUTO",
                old_pattern=r"\bTO_DATE\s*\(\s*([^,]+)\s*,\s*'([^']+)'\s*\)",
                new_pattern=r"TO_TIMESTAMP(\1, '\2')",
                description="TO_DATE() → TO_TIMESTAMP() (Oracle DATE has time)",
                description_ja="TO_DATE() → TO_TIMESTAMP()（OracleのDATEは時刻含む）",
                notes="Adjust format string: Oracle→PG differences (e.g. MI→MI, HH24→HH24)",
            ),
            MigrationRule(
                rule_id="FN_007", category="FUNCTION", severity="AUTO",
                old_pattern=r"\bTO_CHAR\s*\(\s*([^,]+)\s*,\s*'([^']+)'\s*\)",
                new_pattern=r"TO_CHAR(\1, '\2')",
                description="TO_CHAR() — format string may need adjustment",
                description_ja="TO_CHAR() — フォーマット文字列の調整が必要な場合あり",
            ),
            MigrationRule(
                rule_id="FN_008", category="FUNCTION", severity="AUTO",
                old_pattern=r"\bTO_NUMBER\s*\(", new_pattern="CAST(",
                description="TO_NUMBER() → CAST(... AS NUMERIC) or ::NUMERIC",
                description_ja="TO_NUMBER() → CAST(... AS NUMERIC)",
                auto_fix=False, severity_override="REVIEW",
            ),
            MigrationRule(
                rule_id="FN_009", category="FUNCTION", severity="AUTO",
                old_pattern=r"\bSUBSTR\s*\(", new_pattern="SUBSTRING(",
                description="SUBSTR() → SUBSTRING()",
                description_ja="SUBSTR() → SUBSTRING()",
            ),
            MigrationRule(
                rule_id="FN_010", category="FUNCTION", severity="AUTO",
                old_pattern=r"\bINSTR\s*\(", new_pattern="POSITION(",
                description="INSTR() → POSITION() or STRPOS()",
                description_ja="INSTR() → POSITION()",
                notes="INSTR with 3+ args needs custom function",
                auto_fix=False, severity_override="REVIEW",
            ),
            MigrationRule(
                rule_id="FN_011", category="FUNCTION", severity="AUTO",
                old_pattern=r"\bLENGTHB\s*\(", new_pattern="OCTET_LENGTH(",
                description="LENGTHB() → OCTET_LENGTH()",
                description_ja="LENGTHB() → OCTET_LENGTH()",
            ),
            MigrationRule(
                rule_id="FN_012", category="FUNCTION", severity="AUTO",
                old_pattern=r"\|\|", new_pattern="||",
                description="String concatenation || (same in PostgreSQL)",
                description_ja="文字列結合 ||（PostgreSQLも同じ）",
                auto_fix=False,  # No change needed
            ),
            MigrationRule(
                rule_id="FN_013", category="FUNCTION", severity="AUTO",
                old_pattern=r"\bDBMS_OUTPUT\.PUT_LINE\s*\(", new_pattern="RAISE NOTICE '%', ",
                description="DBMS_OUTPUT.PUT_LINE() → RAISE NOTICE",
                description_ja="DBMS_OUTPUT.PUT_LINE() → RAISE NOTICE",
            ),
            MigrationRule(
                rule_id="FN_014", category="FUNCTION", severity="REVIEW",
                old_pattern=r"\bDBMS_LOB\.\w+", new_pattern="/* DBMS_LOB: use PostgreSQL large object functions */",
                description="DBMS_LOB.* → PostgreSQL large object functions",
                description_ja="DBMS_LOB.* → PostgreSQL大規模オブジェクト関数",
                auto_fix=False,
            ),
            MigrationRule(
                rule_id="FN_015", category="FUNCTION", severity="REVIEW",
                old_pattern=r"\bUTL_FILE\.\w+", new_pattern="/* UTL_FILE: use pg_read_file / COPY or external script */",
                description="UTL_FILE.* → pg_read_file/COPY or external processing",
                description_ja="UTL_FILE.* → pg_read_file/COPYまたは外部処理",
                auto_fix=False,
            ),
            MigrationRule(
                rule_id="FN_016", category="FUNCTION", severity="AUTO",
                old_pattern=r"\bROWNUM\b", new_pattern="/* ROWNUM→LIMIT */ LIMIT",
                description="ROWNUM → LIMIT/OFFSET or ROW_NUMBER()",
                description_ja="ROWNUM → LIMIT/OFFSETまたはROW_NUMBER()",
                auto_fix=False, severity_override="REVIEW",
            ),
            MigrationRule(
                rule_id="FN_017", category="FUNCTION", severity="AUTO",
                old_pattern=r"\bROWID\b", new_pattern="CTID",
                description="ROWID → CTID (not exact equivalent)",
                description_ja="ROWID → CTID（完全な等価ではない）",
                notes="CTID is not stable across VACUUM",
            ),
            MigrationRule(
                rule_id="FN_018", category="FUNCTION", severity="AUTO",
                old_pattern=r"\bUSER\b(?!\s*[\.(])", new_pattern="CURRENT_USER",
                description="USER → CURRENT_USER",
                description_ja="USER → CURRENT_USER",
            ),
            MigrationRule(
                rule_id="FN_019", category="FUNCTION", severity="AUTO",
                old_pattern=r"\bSYS_GUID\s*\(\s*\)", new_pattern="gen_random_uuid()",
                description="SYS_GUID() → gen_random_uuid()",
                description_ja="SYS_GUID() → gen_random_uuid()",
            ),
            MigrationRule(
                rule_id="FN_020", category="FUNCTION", severity="REVIEW",
                old_pattern=r"\bDBMS_SCHEDULER\.\w+", new_pattern="/* DBMS_SCHEDULER: use pg_cron extension */",
                description="DBMS_SCHEDULER → pg_cron extension",
                description_ja="DBMS_SCHEDULER → pg_cron拡張",
                auto_fix=False,
            ),
            MigrationRule(
                rule_id="FN_021", category="FUNCTION", severity="AUTO",
                old_pattern=r"\bREGEXP_LIKE\s*\(([^,]+),\s*([^)]+)\)",
                new_pattern=r"\1 ~ \2",
                description="REGEXP_LIKE(str, pattern) → str ~ pattern",
                description_ja="REGEXP_LIKE → ~ 演算子",
            ),
            MigrationRule(
                rule_id="FN_022", category="FUNCTION", severity="AUTO",
                old_pattern=r"\bLISTAGG\s*\(", new_pattern="STRING_AGG(",
                description="LISTAGG() → STRING_AGG()",
                description_ja="LISTAGG() → STRING_AGG()",
            ),
            MigrationRule(
                rule_id="FN_023", category="FUNCTION", severity="AUTO",
                old_pattern=r"\bWM_CONCAT\s*\(", new_pattern="STRING_AGG(",
                description="WM_CONCAT() → STRING_AGG()",
                description_ja="WM_CONCAT() → STRING_AGG()",
            ),
            MigrationRule(
                rule_id="FN_024", category="FUNCTION", severity="AUTO",
                old_pattern=r"\bADD_MONTHS\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)",
                new_pattern=r"\1 + INTERVAL '\2 months'",
                description="ADD_MONTHS(date, n) → date + INTERVAL 'n months'",
                description_ja="ADD_MONTHS() → INTERVAL加算",
            ),
            MigrationRule(
                rule_id="FN_025", category="FUNCTION", severity="AUTO",
                old_pattern=r"\bMONTHS_BETWEEN\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)",
                new_pattern=r"EXTRACT(EPOCH FROM AGE(\1, \2)) / 2592000",
                description="MONTHS_BETWEEN() → AGE() based calculation",
                description_ja="MONTHS_BETWEEN() → AGE()ベースの計算",
                severity_override="REVIEW",
            ),
            MigrationRule(
                rule_id="FN_026", category="FUNCTION", severity="AUTO",
                old_pattern=r"\bTRUNC\s*\(\s*([^,)]+)\s*\)", new_pattern=r"DATE_TRUNC('day', \1)",
                description="TRUNC(date) → DATE_TRUNC('day', date)",
                description_ja="TRUNC(date) → DATE_TRUNC('day', date)",
                notes="Only for date truncation; numeric TRUNC stays the same",
            ),
        ]
        self.rules.extend(rules)

    # ------------------------------------------------------------------
    # Syntax Rules
    # ------------------------------------------------------------------
    def _init_syntax_rules(self):
        rules = [
            MigrationRule(
                rule_id="SX_001", category="SYNTAX", severity="AUTO",
                old_pattern=r"SELECT\s+.*\bFROM\s+DUAL\b",
                new_pattern="SELECT ... /* removed FROM DUAL */",
                description="FROM DUAL → (remove, or use VALUES)",
                description_ja="FROM DUAL → 削除（またはVALUES使用）",
                auto_fix=False,  # Handled separately in transformer
            ),
            MigrationRule(
                rule_id="SX_002", category="SYNTAX", severity="AUTO",
                old_pattern=r"(\w+)\.NEXTVAL", new_pattern=r"nextval('\1')",
                description="sequence.NEXTVAL → nextval('sequence')",
                description_ja="sequence.NEXTVAL → nextval('sequence')",
            ),
            MigrationRule(
                rule_id="SX_003", category="SYNTAX", severity="AUTO",
                old_pattern=r"(\w+)\.CURRVAL", new_pattern=r"currval('\1')",
                description="sequence.CURRVAL → currval('sequence')",
                description_ja="sequence.CURRVAL → currval('sequence')",
            ),
            MigrationRule(
                rule_id="SX_004", category="SYNTAX", severity="AUTO",
                old_pattern=r"\bSELECT\s+(\w+)\.NEXTVAL\s+INTO\s+(\w+)\s+FROM\s+DUAL",
                new_pattern=r"\2 := nextval('\1')",
                description="SELECT seq.NEXTVAL INTO var FROM DUAL → var := nextval('seq')",
                description_ja="SELECT seq.NEXTVAL INTO var FROM DUAL → var := nextval('seq')",
            ),
            MigrationRule(
                rule_id="SX_005", category="SYNTAX", severity="REVIEW",
                old_pattern=r"\bCONNECT\s+BY\b", new_pattern="/* CONNECT BY → recursive CTE */",
                description="CONNECT BY → WITH RECURSIVE CTE",
                description_ja="CONNECT BY → WITH RECURSIVE CTE",
                auto_fix=False,
            ),
            MigrationRule(
                rule_id="SX_006", category="SYNTAX", severity="REVIEW",
                old_pattern=r"\bSTART\s+WITH\b", new_pattern="/* START WITH → recursive CTE base */",
                description="START WITH → WITH RECURSIVE base case",
                description_ja="START WITH → WITH RECURSIVE 基底条件",
                auto_fix=False,
            ),
            MigrationRule(
                rule_id="SX_007", category="SYNTAX", severity="AUTO",
                old_pattern=r"\bMINUS\b", new_pattern="EXCEPT",
                description="MINUS → EXCEPT",
                description_ja="MINUS → EXCEPT",
            ),
            MigrationRule(
                rule_id="SX_008", category="SYNTAX", severity="AUTO",
                old_pattern=r"(CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+\w+[^;]*)\bSTORAGE\s*\([^)]*\)",
                new_pattern=r"\1/* STORAGE clause removed */",
                description="STORAGE clause → removed (PG handles internally)",
                description_ja="STORAGE句 → 削除（PGが内部管理）",
            ),
            MigrationRule(
                rule_id="SX_009", category="SYNTAX", severity="AUTO",
                old_pattern=r"\bTABLESPACE\s+\w+", new_pattern="/* TABLESPACE removed */",
                description="TABLESPACE clause → removed or mapped",
                description_ja="TABLESPACE句 → 削除またはマッピング",
            ),
            MigrationRule(
                rule_id="SX_010", category="SYNTAX", severity="AUTO",
                old_pattern=r"\bPCTFREE\s+\d+", new_pattern="/* PCTFREE removed */",
                description="PCTFREE → removed",
                description_ja="PCTFREE → 削除",
            ),
            MigrationRule(
                rule_id="SX_011", category="SYNTAX", severity="AUTO",
                old_pattern=r"\bINITRANS\s+\d+", new_pattern="/* INITRANS removed */",
                description="INITRANS → removed",
                description_ja="INITRANS → 削除",
            ),
            MigrationRule(
                rule_id="SX_012", category="SYNTAX", severity="AUTO",
                old_pattern=r"\bLOGGING\b", new_pattern="/* LOGGING removed */",
                description="LOGGING → removed",
                description_ja="LOGGING → 削除",
            ),
            MigrationRule(
                rule_id="SX_013", category="SYNTAX", severity="AUTO",
                old_pattern=r"\bNOLOGGING\b", new_pattern="/* NOLOGGING → UNLOGGED (if needed) */",
                description="NOLOGGING → UNLOGGED (optional)",
                description_ja="NOLOGGING → UNLOGGED（オプション）",
            ),
            MigrationRule(
                rule_id="SX_014", category="SYNTAX", severity="AUTO",
                old_pattern=r"\bENABLE\b(?=\s)", new_pattern="/* ENABLE removed */",
                description="ENABLE (constraint) → removed (PG default)",
                description_ja="ENABLE → 削除（PGのデフォルト）",
            ),
            MigrationRule(
                rule_id="SX_015", category="SYNTAX", severity="AUTO",
                old_pattern=r"&(\w+)", new_pattern=r"/* substitution variable &\1 */",
                description="&variable → substitution variable (manual)",
                description_ja="&variable → 代入変数（手動対応）",
                auto_fix=False, severity_override="REVIEW",
            ),
        ]
        self.rules.extend(rules)

    # ------------------------------------------------------------------
    # PL/SQL → PL/pgSQL Rules
    # ------------------------------------------------------------------
    def _init_plsql_rules(self):
        rules = [
            MigrationRule(
                rule_id="PL_001", category="PLSQL", severity="AUTO",
                old_pattern=r"CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+(\w+)",
                new_pattern=r"CREATE OR REPLACE PROCEDURE \1",
                description="CREATE PROCEDURE → PostgreSQL syntax",
                description_ja="CREATE PROCEDURE → PostgreSQL構文",
            ),
            MigrationRule(
                rule_id="PL_002", category="PLSQL", severity="AUTO",
                old_pattern=r"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(\w+)",
                new_pattern=r"CREATE OR REPLACE FUNCTION \1",
                description="CREATE FUNCTION → PostgreSQL syntax",
                description_ja="CREATE FUNCTION → PostgreSQL構文",
            ),
            MigrationRule(
                rule_id="PL_003", category="PLSQL", severity="AUTO",
                old_pattern=r"\bIS\s*\n", new_pattern="AS $$\nDECLARE\n",
                description="IS (proc body) → AS $$ DECLARE",
                description_ja="IS（プロシージャ本体）→ AS $$ DECLARE",
            ),
            MigrationRule(
                rule_id="PL_004", category="PLSQL", severity="AUTO",
                old_pattern=r"\bEND\s+(\w+)\s*;", new_pattern=r"END; $$ LANGUAGE plpgsql;",
                description="END proc_name; → END; $$ LANGUAGE plpgsql;",
                description_ja="END proc_name; → END; $$ LANGUAGE plpgsql;",
            ),
            MigrationRule(
                rule_id="PL_005", category="PLSQL", severity="AUTO",
                old_pattern=r"\b(\w+)\s+IN\s+OUT\b", new_pattern=r"INOUT \1",
                description="param IN OUT → INOUT param",
                description_ja="param IN OUT → INOUT param",
            ),
            MigrationRule(
                rule_id="PL_006", category="PLSQL", severity="AUTO",
                old_pattern=r"\b(\w+)\s+OUT\s+(\w+)", new_pattern=r"OUT \1 \2",
                description="param OUT type → OUT param type",
                description_ja="param OUT type → OUT param type",
            ),
            MigrationRule(
                rule_id="PL_007", category="PLSQL", severity="AUTO",
                old_pattern=r"\b(\w+)%TYPE\b", new_pattern=r"\1%TYPE",
                description="%TYPE reference (same in PL/pgSQL)",
                description_ja="%TYPE参照（PL/pgSQLも同じ）",
                auto_fix=False,
            ),
            MigrationRule(
                rule_id="PL_008", category="PLSQL", severity="AUTO",
                old_pattern=r"\b(\w+)%ROWTYPE\b", new_pattern=r"\1%ROWTYPE",
                description="%ROWTYPE reference (same in PL/pgSQL)",
                description_ja="%ROWTYPE参照（PL/pgSQLも同じ）",
                auto_fix=False,
            ),
            MigrationRule(
                rule_id="PL_009", category="PLSQL", severity="AUTO",
                old_pattern=r"\bCURSOR\s+(\w+)\s+IS\b", new_pattern=r"\1 CURSOR FOR",
                description="CURSOR name IS → name CURSOR FOR",
                description_ja="CURSOR name IS → name CURSOR FOR",
            ),
            MigrationRule(
                rule_id="PL_010", category="PLSQL", severity="REVIEW",
                old_pattern=r"\bEXCEPTION\s+WHEN\s+(\w+)\s+THEN",
                new_pattern=r"EXCEPTION WHEN \1 THEN",
                description="Exception handling — verify exception names",
                description_ja="例外処理 — 例外名の確認が必要",
                notes="Map Oracle exceptions: NO_DATA_FOUND→same, TOO_MANY_ROWS→same, OTHERS→OTHERS",
            ),
            MigrationRule(
                rule_id="PL_011", category="PLSQL", severity="REVIEW",
                old_pattern=r"\bRAISE_APPLICATION_ERROR\s*\(\s*(-?\d+)\s*,",
                new_pattern=r"RAISE EXCEPTION 'Error %', ",
                description="RAISE_APPLICATION_ERROR → RAISE EXCEPTION",
                description_ja="RAISE_APPLICATION_ERROR → RAISE EXCEPTION",
            ),
            MigrationRule(
                rule_id="PL_012", category="PLSQL", severity="AUTO",
                old_pattern=r"\bFOR\s+(\w+)\s+IN\s+(\w+)\s*\.\.\s*(\w+)\s+LOOP",
                new_pattern=r"FOR \1 IN \2..\3 LOOP",
                description="FOR i IN a..b LOOP (same in PL/pgSQL)",
                description_ja="FOR i IN a..b LOOP（PL/pgSQLも同じ）",
                auto_fix=False,
            ),
            MigrationRule(
                rule_id="PL_013", category="PLSQL", severity="MANUAL",
                old_pattern=r"CREATE\s+(?:OR\s+REPLACE\s+)?PACKAGE\s+(?:BODY\s+)?(\w+)",
                new_pattern=r"/* PACKAGE \1 → split into schema + individual functions */",
                description="PACKAGE → Schema + individual functions (manual)",
                description_ja="PACKAGE → スキーマ + 個別関数（手動対応）",
                auto_fix=False,
            ),
            MigrationRule(
                rule_id="PL_014", category="PLSQL", severity="AUTO",
                old_pattern=r"\bRETURN\b(?=\s+\w+\s+(?:IS|AS)\b)", new_pattern="RETURNS",
                description="RETURN type (function declaration) → RETURNS type",
                description_ja="RETURN type（関数宣言）→ RETURNS type",
            ),
            MigrationRule(
                rule_id="PL_015", category="PLSQL", severity="REVIEW",
                old_pattern=r"\bPRAGMA\s+\w+", new_pattern="/* PRAGMA removed (no PG equivalent) */",
                description="PRAGMA → removed (no PostgreSQL equivalent)",
                description_ja="PRAGMA → 削除（PostgreSQL相当なし）",
            ),
            MigrationRule(
                rule_id="PL_016", category="PLSQL", severity="AUTO",
                old_pattern=r"\bTYPE\s+(\w+)\s+IS\s+TABLE\s+OF\b",
                new_pattern=r"\1 /* PL/SQL collection → PG array or temp table */",
                description="TYPE name IS TABLE OF → array[] or temp table",
                description_ja="TYPE name IS TABLE OF → array[]またはテンプテーブル",
                auto_fix=False, severity_override="REVIEW",
            ),
            MigrationRule(
                rule_id="PL_017", category="PLSQL", severity="AUTO",
                old_pattern=r"\bTYPE\s+(\w+)\s+IS\s+RECORD\b",
                new_pattern=r"/* TYPE \1 IS RECORD → CREATE TYPE \1 AS (...) */",
                description="TYPE IS RECORD → CREATE TYPE AS",
                description_ja="TYPE IS RECORD → CREATE TYPE AS",
                auto_fix=False, severity_override="REVIEW",
            ),
            MigrationRule(
                rule_id="PL_018", category="PLSQL", severity="AUTO",
                old_pattern=r"\bEXECUTE\s+IMMEDIATE\b", new_pattern="EXECUTE",
                description="EXECUTE IMMEDIATE → EXECUTE",
                description_ja="EXECUTE IMMEDIATE → EXECUTE",
            ),
            MigrationRule(
                rule_id="PL_019", category="PLSQL", severity="AUTO",
                old_pattern=r"\bSQL%ROWCOUNT\b", new_pattern="GET DIAGNOSTICS row_count = ROW_COUNT",
                description="SQL%ROWCOUNT → GET DIAGNOSTICS ... ROW_COUNT",
                description_ja="SQL%ROWCOUNT → GET DIAGNOSTICS ... ROW_COUNT",
                severity_override="REVIEW",
            ),
            MigrationRule(
                rule_id="PL_020", category="PLSQL", severity="AUTO",
                old_pattern=r"\bSQL%FOUND\b", new_pattern="FOUND",
                description="SQL%FOUND → FOUND",
                description_ja="SQL%FOUND → FOUND",
            ),
            MigrationRule(
                rule_id="PL_021", category="PLSQL", severity="AUTO",
                old_pattern=r"\bSQL%NOTFOUND\b", new_pattern="NOT FOUND",
                description="SQL%NOTFOUND → NOT FOUND",
                description_ja="SQL%NOTFOUND → NOT FOUND",
            ),
        ]
        self.rules.extend(rules)

    # ------------------------------------------------------------------
    # Sequence Rules
    # ------------------------------------------------------------------
    def _init_sequence_rules(self):
        rules = [
            MigrationRule(
                rule_id="SEQ_001", category="SEQUENCE", severity="AUTO",
                old_pattern=r"CREATE\s+SEQUENCE\s+(\w+)\s+MINVALUE\s+(\d+)\s+MAXVALUE\s+(\d+)",
                new_pattern=r"CREATE SEQUENCE \1 MINVALUE \2 MAXVALUE \3",
                description="CREATE SEQUENCE (basic syntax same)",
                description_ja="CREATE SEQUENCE（基本構文は同じ）",
            ),
            MigrationRule(
                rule_id="SEQ_002", category="SEQUENCE", severity="AUTO",
                old_pattern=r"\bNOCACHE\b", new_pattern="/* NOCACHE removed */",
                description="NOCACHE → removed (PG default is no cache)",
                description_ja="NOCACHE → 削除（PGデフォルトはキャッシュなし）",
            ),
            MigrationRule(
                rule_id="SEQ_003", category="SEQUENCE", severity="AUTO",
                old_pattern=r"\bNOORDER\b", new_pattern="/* NOORDER removed */",
                description="NOORDER → removed (no PG equivalent)",
                description_ja="NOORDER → 削除（PG相当なし）",
            ),
            MigrationRule(
                rule_id="SEQ_004", category="SEQUENCE", severity="AUTO",
                old_pattern=r"\bNOCYCLE\b", new_pattern="NO CYCLE",
                description="NOCYCLE → NO CYCLE",
                description_ja="NOCYCLE → NO CYCLE",
            ),
        ]
        self.rules.extend(rules)

    # ------------------------------------------------------------------
    # Object/DDL Rules
    # ------------------------------------------------------------------
    def _init_object_rules(self):
        rules = [
            MigrationRule(
                rule_id="OBJ_001", category="OBJECT", severity="REVIEW",
                old_pattern=r"CREATE\s+(?:OR\s+REPLACE\s+)?TYPE\s+(\w+)\s+AS\s+OBJECT",
                new_pattern=r"CREATE TYPE \1 AS (",
                description="CREATE TYPE AS OBJECT → CREATE TYPE AS composite",
                description_ja="CREATE TYPE AS OBJECT → CREATE TYPE AS 複合型",
            ),
            MigrationRule(
                rule_id="OBJ_002", category="OBJECT", severity="REVIEW",
                old_pattern=r"CREATE\s+(?:OR\s+REPLACE\s+)?TYPE\s+(\w+)\s+AS\s+VARRAY\s*\(\s*(\d+)\s*\)\s+OF\s+(\w+)",
                new_pattern=r"/* VARRAY(\2) OF \3 → \3[] (PostgreSQL array) */",
                description="VARRAY → PostgreSQL array[]",
                description_ja="VARRAY → PostgreSQL array[]",
            ),
            MigrationRule(
                rule_id="OBJ_003", category="OBJECT", severity="REVIEW",
                old_pattern=r"CREATE\s+(?:OR\s+REPLACE\s+)?TYPE\s+(\w+)\s+AS\s+TABLE\s+OF",
                new_pattern=r"/* nested table → PostgreSQL array[] or separate table */",
                description="Nested table type → array[] or separate table",
                description_ja="ネストされたテーブル型 → array[]または別テーブル",
            ),
            MigrationRule(
                rule_id="OBJ_004", category="OBJECT", severity="AUTO",
                old_pattern=r"CREATE\s+(?:BITMAP\s+)?INDEX\s+(\w+)\s+ON\s+(\w+)\s*\(([^)]+)\)",
                new_pattern=r"CREATE INDEX \1 ON \2 (\3)",
                description="CREATE INDEX (remove BITMAP keyword)",
                description_ja="CREATE INDEX（BITMAPキーワード削除）",
                notes="PostgreSQL has GIN/GiST instead of bitmap indexes",
            ),
            MigrationRule(
                rule_id="OBJ_005", category="OBJECT", severity="AUTO",
                old_pattern=r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:FORCE\s+)?VIEW",
                new_pattern="CREATE OR REPLACE VIEW",
                description="CREATE VIEW (remove FORCE keyword)",
                description_ja="CREATE VIEW（FORCEキーワード削除）",
            ),
            MigrationRule(
                rule_id="OBJ_006", category="OBJECT", severity="REVIEW",
                old_pattern=r"CREATE\s+MATERIALIZED\s+VIEW\s+(\w+).*?BUILD\s+(IMMEDIATE|DEFERRED)",
                new_pattern=r"CREATE MATERIALIZED VIEW \1",
                description="Materialized view (remove BUILD clause)",
                description_ja="マテリアライズドビュー（BUILD句削除）",
            ),
            MigrationRule(
                rule_id="OBJ_007", category="OBJECT", severity="REVIEW",
                old_pattern=r"REFRESH\s+(FAST|COMPLETE|FORCE)\s+ON\s+(COMMIT|DEMAND)",
                new_pattern="/* REFRESH → use pg_cron or REFRESH MATERIALIZED VIEW CONCURRENTLY */",
                description="Materialized view refresh → manual or pg_cron",
                description_ja="マテリアライズドビュー更新 → 手動またはpg_cron",
            ),
            MigrationRule(
                rule_id="OBJ_008", category="OBJECT", severity="AUTO",
                old_pattern=r"CREATE\s+(?:PUBLIC\s+)?DATABASE\s+LINK\b",
                new_pattern="/* DB LINK → use postgres_fdw extension */",
                description="DATABASE LINK → postgres_fdw",
                description_ja="DATABASE LINK → postgres_fdw",
                auto_fix=False,
            ),
        ]
        self.rules.extend(rules)

    # ------------------------------------------------------------------
    # Trigger Rules
    # ------------------------------------------------------------------
    def _init_trigger_rules(self):
        rules = [
            MigrationRule(
                rule_id="TR_001", category="TRIGGER", severity="REVIEW",
                old_pattern=r"CREATE\s+(?:OR\s+REPLACE\s+)?TRIGGER\s+(\w+)",
                new_pattern=r"CREATE OR REPLACE FUNCTION \1_fn() RETURNS TRIGGER",
                description="Trigger → trigger function + CREATE TRIGGER",
                description_ja="トリガー → トリガー関数 + CREATE TRIGGER",
                notes="PostgreSQL requires separate function for trigger body",
            ),
            MigrationRule(
                rule_id="TR_002", category="TRIGGER", severity="AUTO",
                old_pattern=r":NEW\.(\w+)", new_pattern=r"NEW.\1",
                description=":NEW.col → NEW.col",
                description_ja=":NEW.col → NEW.col",
            ),
            MigrationRule(
                rule_id="TR_003", category="TRIGGER", severity="AUTO",
                old_pattern=r":OLD\.(\w+)", new_pattern=r"OLD.\1",
                description=":OLD.col → OLD.col",
                description_ja=":OLD.col → OLD.col",
            ),
        ]
        self.rules.extend(rules)

    # ------------------------------------------------------------------
    # Synonym Rules
    # ------------------------------------------------------------------
    def _init_synonym_rules(self):
        rules = [
            MigrationRule(
                rule_id="SYN_001", category="SYNONYM", severity="REVIEW",
                old_pattern=r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:PUBLIC\s+)?SYNONYM\s+(\w+)\s+FOR\s+(\w+)",
                new_pattern=r"CREATE OR REPLACE VIEW \1 AS SELECT * FROM \2",
                description="SYNONYM → VIEW or SET search_path",
                description_ja="SYNONYM → VIEWまたはSET search_path",
                notes="Alternative: ALTER ROLE SET search_path TO include target schema",
            ),
        ]
        self.rules.extend(rules)

    # ------------------------------------------------------------------
    # Oracle-specific JOIN syntax
    # ------------------------------------------------------------------
    def _init_join_rules(self):
        rules = [
            MigrationRule(
                rule_id="JOIN_001", category="SYNTAX", severity="REVIEW",
                old_pattern=r"(\w+\.\w+)\s*=\s*(\w+\.\w+)\s*\(\+\)",
                new_pattern=r"\1 LEFT JOIN ... ON \1 = \2",
                description="Oracle (+) outer join → LEFT/RIGHT JOIN",
                description_ja="Oracle (+) 外部結合 → LEFT/RIGHT JOIN",
                auto_fix=False,
            ),
            MigrationRule(
                rule_id="JOIN_002", category="SYNTAX", severity="REVIEW",
                old_pattern=r"\(\+\)\s*=",
                new_pattern="/* (+) → ANSI JOIN syntax */",
                description="Oracle (+) join syntax → ANSI JOIN",
                description_ja="Oracle (+) 結合構文 → ANSI JOIN",
                auto_fix=False,
            ),
        ]
        self.rules.extend(rules)

    # ------------------------------------------------------------------
    # Optimizer Hint Rules
    # ------------------------------------------------------------------
    def _init_hint_rules(self):
        rules = [
            MigrationRule(
                rule_id="HINT_001", category="SYNTAX", severity="AUTO",
                old_pattern=r"/\*\+[^*]*\*/", new_pattern="/* Oracle hint removed */",
                description="Optimizer hints /*+ ... */ → removed",
                description_ja="オプティマイザヒント /*+ ... */ → 削除",
                notes="PostgreSQL uses pg_hint_plan extension if needed",
            ),
        ]
        self.rules.extend(rules)

    # ------------------------------------------------------------------
    # Query Methods
    # ------------------------------------------------------------------
    @property
    def datatype_rules(self) -> Dict[str, MigrationRule]:
        return {r.old_pattern: r for r in self.rules if r.category == "DATATYPE"}

    def get_rules_by_category(self, category: str) -> List[MigrationRule]:
        return [r for r in self.rules if r.category == category]

    def get_rules_by_severity(self, severity: str) -> List[MigrationRule]:
        return [r for r in self.rules if r.severity == severity]

    def get_auto_fixable_rules(self) -> List[MigrationRule]:
        return [r for r in self.rules if r.auto_fix]

    def get_all_categories(self) -> List[str]:
        return sorted(set(r.category for r in self.rules))

    def get_rule_by_id(self, rule_id: str) -> Optional[MigrationRule]:
        for r in self.rules:
            if r.rule_id == rule_id:
                return r
        return None

    def get_rule_count(self) -> Dict[str, int]:
        counts = {}
        for r in self.rules:
            counts[r.category] = counts.get(r.category, 0) + 1
        return counts
