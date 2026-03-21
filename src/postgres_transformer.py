"""
Oracle2PostgreSQL - PostgreSQL Transformer
Applies migration rules to transform Oracle SQL AST into PostgreSQL-compatible SQL.
"""

import re
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple

from .oracle_parser import (
    OracleSchema, OracleStatement, StatementType,
    TableDef, ColumnDef, ConstraintDef, IndexDef, SequenceDef,
    ViewDef, SynonymDef, ProcedureDef, PackageDef, TriggerDef,
)
from .migration_rules import MigrationRules, MigrationRule


@dataclass
class TransformOptions:
    """Configuration for the transformation."""
    convert_datatypes: bool = True
    convert_plsql: bool = True
    convert_sequences: bool = True
    convert_synonyms: bool = True
    convert_packages: bool = True
    convert_triggers: bool = True
    generate_report: bool = True
    create_backup: bool = True
    encoding: str = "utf-8"
    file_extensions: str = ".sql,.pls,.pkb,.pks,.trg,.vw,.fnc,.prc"


@dataclass
class ChangeRecord:
    """Record of a single change made during transformation."""
    rule_id: str
    category: str
    severity: str          # AUTO, REVIEW, MANUAL
    description: str
    description_ja: str
    old_text: str
    new_text: str
    line_number: int = 0
    file_name: str = ""


@dataclass
class TransformResult:
    """Result of transforming a single file."""
    filename: str
    original_text: str
    transformed_text: str
    changes: List[ChangeRecord] = field(default_factory=list)
    auto_converted: int = 0
    needs_review: int = 0
    manual_only: int = 0
    errors: List[str] = field(default_factory=list)

    @property
    def total_changes(self) -> int:
        return self.auto_converted + self.needs_review + self.manual_only


class PostgresTransformer:
    """Engine to transform Oracle SQL to PostgreSQL."""

    def __init__(self, options: TransformOptions = None):
        self.options = options or TransformOptions()
        self.rules = MigrationRules()

    def transform(self, schema: OracleSchema) -> TransformResult:
        """Transform an OracleSchema into PostgreSQL SQL."""
        result = TransformResult(
            filename=schema.filename,
            original_text="",
            transformed_text="",
        )

        transformed_statements = []

        for stmt in schema.statements:
            original = stmt.text
            result.original_text += original + "\n\n"

            transformed, changes = self._transform_statement(stmt, schema.filename)
            transformed_statements.append(transformed)

            for change in changes:
                result.changes.append(change)
                if change.severity == "AUTO":
                    result.auto_converted += 1
                elif change.severity == "REVIEW":
                    result.needs_review += 1
                elif change.severity == "MANUAL":
                    result.manual_only += 1

        result.transformed_text = "\n\n".join(transformed_statements)
        result.errors = schema.errors[:]
        return result

    def _transform_statement(self, stmt: OracleStatement, filename: str) -> Tuple[str, List[ChangeRecord]]:
        """Transform a single Oracle statement."""
        text = stmt.text
        changes: List[ChangeRecord] = []

        if stmt.type == StatementType.CREATE_TABLE and stmt.table_def:
            text, ch = self._transform_create_table(stmt.table_def, text, filename, stmt.line_number)
            changes.extend(ch)
        elif stmt.type == StatementType.CREATE_INDEX and stmt.index_def:
            text, ch = self._transform_create_index(stmt.index_def, text, filename, stmt.line_number)
            changes.extend(ch)
        elif stmt.type == StatementType.CREATE_SEQUENCE and stmt.sequence_def:
            text, ch = self._transform_create_sequence(stmt.sequence_def, text, filename, stmt.line_number)
            changes.extend(ch)
        elif stmt.type in (StatementType.CREATE_VIEW, StatementType.CREATE_MATERIALIZED_VIEW) and stmt.view_def:
            text, ch = self._transform_create_view(stmt.view_def, text, filename, stmt.line_number)
            changes.extend(ch)
        elif stmt.type == StatementType.CREATE_SYNONYM and stmt.synonym_def:
            text, ch = self._transform_create_synonym(stmt.synonym_def, text, filename, stmt.line_number)
            changes.extend(ch)
        elif stmt.type in (StatementType.CREATE_PROCEDURE, StatementType.CREATE_FUNCTION) and stmt.procedure_def:
            text, ch = self._transform_create_procedure(stmt.procedure_def, text, filename, stmt.line_number)
            changes.extend(ch)
        elif stmt.type in (StatementType.CREATE_PACKAGE, StatementType.CREATE_PACKAGE_BODY) and stmt.package_def:
            text, ch = self._transform_create_package(stmt.package_def, text, filename, stmt.line_number)
            changes.extend(ch)
        elif stmt.type == StatementType.CREATE_TRIGGER and stmt.trigger_def:
            text, ch = self._transform_create_trigger(stmt.trigger_def, text, filename, stmt.line_number)
            changes.extend(ch)

        # Apply generic rules (functions, syntax, hints) to all statements
        text, generic_changes = self._apply_generic_rules(text, filename, stmt.line_number)
        changes.extend(generic_changes)

        return text, changes

    # ------------------------------------------------------------------
    # DDL Transformations
    # ------------------------------------------------------------------
    def _transform_create_table(self, table: TableDef, text: str, filename: str, line: int
                                 ) -> Tuple[str, List[ChangeRecord]]:
        changes = []

        if self.options.convert_datatypes:
            for rule in self.rules.get_rules_by_category("DATATYPE"):
                if not rule.auto_fix:
                    continue
                new_text = re.sub(rule.old_pattern, rule.new_pattern, text, flags=re.IGNORECASE)
                if new_text != text:
                    changes.append(ChangeRecord(
                        rule_id=rule.rule_id, category="DATATYPE",
                        severity=rule.severity, description=rule.description,
                        description_ja=rule.description_ja,
                        old_text=text, new_text=new_text,
                        line_number=line, file_name=filename,
                    ))
                    text = new_text

        # Remove Oracle-specific storage clauses
        for pattern, desc, desc_ja in [
            (r"\s*STORAGE\s*\([^)]*\)", "STORAGE clause removed", "STORAGE句削除"),
            (r"\s*TABLESPACE\s+\w+", "TABLESPACE removed", "TABLESPACE削除"),
            (r"\s*PCTFREE\s+\d+", "PCTFREE removed", "PCTFREE削除"),
            (r"\s*INITRANS\s+\d+", "INITRANS removed", "INITRANS削除"),
            (r"\s*LOGGING\b", "LOGGING removed", "LOGGING削除"),
            (r"\s*NOLOGGING\b", "NOLOGGING removed", "NOLOGGING削除"),
            (r"\s*ENABLE\b", "ENABLE removed", "ENABLE削除"),
            (r"\s*USING\s+INDEX\s*(?:TABLESPACE\s+\w+)?", "USING INDEX clause removed", "USING INDEX句削除"),
        ]:
            new_text = re.sub(pattern, "", text, flags=re.IGNORECASE)
            if new_text != text:
                changes.append(ChangeRecord(
                    rule_id="SX_STORAGE", category="SYNTAX", severity="AUTO",
                    description=desc, description_ja=desc_ja,
                    old_text="", new_text="", line_number=line, file_name=filename,
                ))
                text = new_text

        # GLOBAL TEMPORARY → TEMPORARY (PostgreSQL uses TEMPORARY or TEMP)
        if table.is_temporary:
            new_text = re.sub(r"GLOBAL\s+TEMPORARY", "TEMPORARY", text, flags=re.IGNORECASE)
            if new_text != text:
                changes.append(ChangeRecord(
                    rule_id="SX_TEMP", category="SYNTAX", severity="AUTO",
                    description="GLOBAL TEMPORARY → TEMPORARY",
                    description_ja="GLOBAL TEMPORARY → TEMPORARY",
                    old_text="", new_text="", line_number=line, file_name=filename,
                ))
                text = new_text
            # ON COMMIT PRESERVE/DELETE ROWS stays the same in PG

        return text, changes

    def _transform_create_index(self, idx: IndexDef, text: str, filename: str, line: int
                                 ) -> Tuple[str, List[ChangeRecord]]:
        changes = []

        # Remove BITMAP keyword
        if idx.is_bitmap:
            new_text = re.sub(r"\bBITMAP\s+", "", text, flags=re.IGNORECASE)
            if new_text != text:
                changes.append(ChangeRecord(
                    rule_id="OBJ_004", category="OBJECT", severity="AUTO",
                    description="BITMAP INDEX → standard INDEX (consider GIN/GiST)",
                    description_ja="BITMAP INDEX → 標準INDEX（GIN/GiST検討）",
                    old_text="", new_text="", line_number=line, file_name=filename,
                ))
                text = new_text

        # Remove TABLESPACE
        new_text = re.sub(r"\s*TABLESPACE\s+\w+", "", text, flags=re.IGNORECASE)
        if new_text != text:
            changes.append(ChangeRecord(
                rule_id="SX_009", category="SYNTAX", severity="AUTO",
                description="TABLESPACE removed from index", description_ja="INDEXからTABLESPACE削除",
                old_text="", new_text="", line_number=line, file_name=filename,
            ))
            text = new_text

        # Remove storage params
        for pat in [r"\s*STORAGE\s*\([^)]*\)", r"\s*PCTFREE\s+\d+",
                     r"\s*INITRANS\s+\d+", r"\s*LOGGING\b", r"\s*NOLOGGING\b",
                     r"\s*COMPUTE\s+STATISTICS\b"]:
            text = re.sub(pat, "", text, flags=re.IGNORECASE)

        return text, changes

    def _transform_create_sequence(self, seq: SequenceDef, text: str, filename: str, line: int
                                    ) -> Tuple[str, List[ChangeRecord]]:
        changes = []

        if not self.options.convert_sequences:
            return text, changes

        # NOCACHE → remove
        new_text = re.sub(r"\s*NOCACHE\b", "", text, flags=re.IGNORECASE)
        if new_text != text:
            changes.append(ChangeRecord(
                rule_id="SEQ_002", category="SEQUENCE", severity="AUTO",
                description="NOCACHE removed", description_ja="NOCACHE削除",
                old_text="", new_text="", line_number=line, file_name=filename,
            ))
            text = new_text

        # NOORDER → remove
        new_text = re.sub(r"\s*NOORDER\b", "", text, flags=re.IGNORECASE)
        if new_text != text:
            changes.append(ChangeRecord(
                rule_id="SEQ_003", category="SEQUENCE", severity="AUTO",
                description="NOORDER removed", description_ja="NOORDER削除",
                old_text="", new_text="", line_number=line, file_name=filename,
            ))
            text = new_text

        # NOCYCLE → NO CYCLE
        new_text = re.sub(r"\bNOCYCLE\b", "NO CYCLE", text, flags=re.IGNORECASE)
        if new_text != text:
            changes.append(ChangeRecord(
                rule_id="SEQ_004", category="SEQUENCE", severity="AUTO",
                description="NOCYCLE → NO CYCLE", description_ja="NOCYCLE → NO CYCLE",
                old_text="", new_text="", line_number=line, file_name=filename,
            ))
            text = new_text

        # NOMINVALUE, NOMAXVALUE → remove
        for pat, rid in [(r"\s*NOMINVALUE\b", "SEQ_MIN"), (r"\s*NOMAXVALUE\b", "SEQ_MAX")]:
            text = re.sub(pat, "", text, flags=re.IGNORECASE)

        return text, changes

    def _transform_create_view(self, view: ViewDef, text: str, filename: str, line: int
                                ) -> Tuple[str, List[ChangeRecord]]:
        changes = []

        # Remove FORCE keyword
        if view.is_force:
            new_text = re.sub(r"\bFORCE\s+", "", text, flags=re.IGNORECASE)
            if new_text != text:
                changes.append(ChangeRecord(
                    rule_id="OBJ_005", category="OBJECT", severity="AUTO",
                    description="FORCE keyword removed from VIEW",
                    description_ja="VIEWからFORCEキーワード削除",
                    old_text="", new_text="", line_number=line, file_name=filename,
                ))
                text = new_text

        # For materialized views, handle BUILD and REFRESH clauses
        if view.is_materialized:
            for pat, desc in [
                (r"\s*BUILD\s+(?:IMMEDIATE|DEFERRED)\b", "BUILD clause removed"),
                (r"\s*REFRESH\s+(?:FAST|COMPLETE|FORCE)\s+ON\s+(?:COMMIT|DEMAND)\b",
                 "REFRESH clause removed (use REFRESH MATERIALIZED VIEW manually)"),
            ]:
                new_text = re.sub(pat, "", text, flags=re.IGNORECASE)
                if new_text != text:
                    changes.append(ChangeRecord(
                        rule_id="OBJ_006", category="OBJECT", severity="REVIEW",
                        description=desc, description_ja=desc,
                        old_text="", new_text="", line_number=line, file_name=filename,
                    ))
                    text = new_text

        return text, changes

    def _transform_create_synonym(self, syn: SynonymDef, text: str, filename: str, line: int
                                   ) -> Tuple[str, List[ChangeRecord]]:
        changes = []
        if not self.options.convert_synonyms:
            return text, changes

        # Convert synonym to view
        new_text = f"-- Converted from Oracle SYNONYM\nCREATE OR REPLACE VIEW {syn.name} AS SELECT * FROM {syn.target};"
        changes.append(ChangeRecord(
            rule_id="SYN_001", category="SYNONYM", severity="REVIEW",
            description=f"SYNONYM {syn.name} → VIEW (alternative: SET search_path)",
            description_ja=f"SYNONYM {syn.name} → VIEW（代替: SET search_path）",
            old_text=text, new_text=new_text, line_number=line, file_name=filename,
        ))
        return new_text, changes

    def _transform_create_procedure(self, proc: ProcedureDef, text: str, filename: str, line: int
                                     ) -> Tuple[str, List[ChangeRecord]]:
        changes = []
        if not self.options.convert_plsql:
            return text, changes

        # Apply PL/SQL rules
        for rule in self.rules.get_rules_by_category("PLSQL"):
            if not rule.auto_fix:
                # Still record as REVIEW/MANUAL
                if re.search(rule.old_pattern, text, re.IGNORECASE | re.DOTALL):
                    changes.append(ChangeRecord(
                        rule_id=rule.rule_id, category="PLSQL",
                        severity=getattr(rule, 'severity_override', rule.severity),
                        description=rule.description, description_ja=rule.description_ja,
                        old_text="", new_text="", line_number=line, file_name=filename,
                    ))
                continue

            new_text = re.sub(rule.old_pattern, rule.new_pattern, text,
                              flags=re.IGNORECASE | re.DOTALL)
            if new_text != text:
                changes.append(ChangeRecord(
                    rule_id=rule.rule_id, category="PLSQL",
                    severity=rule.severity, description=rule.description,
                    description_ja=rule.description_ja,
                    old_text="", new_text="", line_number=line, file_name=filename,
                ))
                text = new_text

        # Apply datatype rules within PL/SQL body
        if self.options.convert_datatypes:
            for rule in self.rules.get_rules_by_category("DATATYPE"):
                if not rule.auto_fix:
                    continue
                new_text = re.sub(rule.old_pattern, rule.new_pattern, text, flags=re.IGNORECASE)
                if new_text != text:
                    changes.append(ChangeRecord(
                        rule_id=rule.rule_id, category="DATATYPE",
                        severity=rule.severity, description=rule.description,
                        description_ja=rule.description_ja,
                        old_text="", new_text="", line_number=line, file_name=filename,
                    ))
                    text = new_text

        return text, changes

    def _transform_create_package(self, pkg: PackageDef, text: str, filename: str, line: int
                                   ) -> Tuple[str, List[ChangeRecord]]:
        changes = []
        if not self.options.convert_packages:
            return text, changes

        # Packages require manual conversion - add comment
        header = (
            f"-- [MANUAL] Oracle PACKAGE {pkg.name} needs manual conversion to PostgreSQL.\n"
            f"-- Recommended approach:\n"
            f"--   1. Create schema: CREATE SCHEMA {pkg.name.lower()};\n"
            f"--   2. Move each procedure/function into the schema as standalone functions.\n"
            f"--   3. Package variables → session variables or configuration table.\n\n"
        )
        changes.append(ChangeRecord(
            rule_id="PL_013", category="PLSQL", severity="MANUAL",
            description=f"PACKAGE {pkg.name} → Schema + individual functions",
            description_ja=f"PACKAGE {pkg.name} → スキーマ + 個別関数",
            old_text="", new_text="", line_number=line, file_name=filename,
        ))

        # Still apply basic transformations within the package body
        if self.options.convert_plsql:
            text, plsql_changes = self._apply_plsql_rules(text, filename, line)
            changes.extend(plsql_changes)

        return header + text, changes

    def _transform_create_trigger(self, trig: TriggerDef, text: str, filename: str, line: int
                                   ) -> Tuple[str, List[ChangeRecord]]:
        changes = []
        if not self.options.convert_triggers:
            return text, changes

        # PostgreSQL requires a separate trigger function
        func_name = f"{trig.name}_fn"
        events_str = " OR ".join(trig.events)
        timing = trig.timing
        row_clause = "FOR EACH ROW" if trig.for_each_row else "FOR EACH STATEMENT"

        # Transform the trigger body
        body = trig.body if trig.body else "BEGIN\n  -- TODO: migrate trigger body\n  RETURN NEW;\nEND;"
        body = re.sub(r":NEW\.", "NEW.", body, flags=re.IGNORECASE)
        body = re.sub(r":OLD\.", "OLD.", body, flags=re.IGNORECASE)

        # Apply PL/SQL transformations to body
        if self.options.convert_plsql:
            body, body_changes = self._apply_plsql_rules(body, filename, line)
            changes.extend(body_changes)
        if self.options.convert_datatypes:
            for rule in self.rules.get_rules_by_category("DATATYPE"):
                if rule.auto_fix:
                    body = re.sub(rule.old_pattern, rule.new_pattern, body, flags=re.IGNORECASE)

        # Ensure body ends with RETURN
        if trig.for_each_row and "RETURN" not in body.upper():
            body = body.rstrip().rstrip(";").rstrip()
            if body.upper().endswith("END"):
                body = body[:-3].rstrip() + "\n  RETURN NEW;\nEND;"
            else:
                body += "\n  RETURN NEW;"

        new_text = (
            f"-- Converted from Oracle TRIGGER {trig.name}\n"
            f"CREATE OR REPLACE FUNCTION {func_name}()\n"
            f"RETURNS TRIGGER AS $$\n"
            f"{body}\n"
            f"$$ LANGUAGE plpgsql;\n\n"
            f"CREATE TRIGGER {trig.name}\n"
            f"  {timing} {events_str}\n"
            f"  ON {trig.table_name}\n"
            f"  {row_clause}\n"
            f"  EXECUTE FUNCTION {func_name}();"
        )

        changes.append(ChangeRecord(
            rule_id="TR_001", category="TRIGGER", severity="REVIEW",
            description=f"Trigger {trig.name} → function {func_name} + CREATE TRIGGER",
            description_ja=f"トリガー {trig.name} → 関数 {func_name} + CREATE TRIGGER",
            old_text=text, new_text=new_text, line_number=line, file_name=filename,
        ))

        return new_text, changes

    # ------------------------------------------------------------------
    # Generic Rule Application
    # ------------------------------------------------------------------
    def _apply_generic_rules(self, text: str, filename: str, line: int
                              ) -> Tuple[str, List[ChangeRecord]]:
        """Apply function, syntax, and hint rules to any statement text."""
        changes = []

        # Apply function rules
        for rule in self.rules.get_rules_by_category("FUNCTION"):
            if not rule.auto_fix:
                if re.search(rule.old_pattern, text, re.IGNORECASE):
                    sev = getattr(rule, 'severity_override', rule.severity)
                    changes.append(ChangeRecord(
                        rule_id=rule.rule_id, category="FUNCTION", severity=sev,
                        description=rule.description, description_ja=rule.description_ja,
                        old_text="", new_text="", line_number=line, file_name=filename,
                    ))
                continue

            new_text = re.sub(rule.old_pattern, rule.new_pattern, text, flags=re.IGNORECASE)
            if new_text != text:
                changes.append(ChangeRecord(
                    rule_id=rule.rule_id, category="FUNCTION", severity=rule.severity,
                    description=rule.description, description_ja=rule.description_ja,
                    old_text="", new_text="", line_number=line, file_name=filename,
                ))
                text = new_text

        # Apply syntax rules
        for category in ["SYNTAX"]:
            for rule in self.rules.get_rules_by_category(category):
                if not rule.auto_fix:
                    if re.search(rule.old_pattern, text, re.IGNORECASE | re.DOTALL):
                        sev = getattr(rule, 'severity_override', rule.severity)
                        changes.append(ChangeRecord(
                            rule_id=rule.rule_id, category=category, severity=sev,
                            description=rule.description, description_ja=rule.description_ja,
                            old_text="", new_text="", line_number=line, file_name=filename,
                        ))
                    continue

                new_text = re.sub(rule.old_pattern, rule.new_pattern, text,
                                  flags=re.IGNORECASE | re.DOTALL)
                if new_text != text:
                    changes.append(ChangeRecord(
                        rule_id=rule.rule_id, category=category, severity=rule.severity,
                        description=rule.description, description_ja=rule.description_ja,
                        old_text="", new_text="", line_number=line, file_name=filename,
                    ))
                    text = new_text

        # Remove FROM DUAL
        new_text = re.sub(r"\s+FROM\s+DUAL\b", "", text, flags=re.IGNORECASE)
        if new_text != text:
            changes.append(ChangeRecord(
                rule_id="SX_001", category="SYNTAX", severity="AUTO",
                description="FROM DUAL removed", description_ja="FROM DUAL削除",
                old_text="", new_text="", line_number=line, file_name=filename,
            ))
            text = new_text

        # Oracle hints
        new_text = re.sub(r"/\*\+[^*]*\*/", "/* Oracle hint removed */", text)
        if new_text != text:
            changes.append(ChangeRecord(
                rule_id="HINT_001", category="SYNTAX", severity="AUTO",
                description="Oracle optimizer hint removed",
                description_ja="Oracleオプティマイザヒント削除",
                old_text="", new_text="", line_number=line, file_name=filename,
            ))
            text = new_text

        return text, changes

    def _apply_plsql_rules(self, text: str, filename: str, line: int
                            ) -> Tuple[str, List[ChangeRecord]]:
        """Apply PL/SQL specific rules."""
        changes = []
        for rule in self.rules.get_rules_by_category("PLSQL"):
            if not rule.auto_fix:
                continue
            new_text = re.sub(rule.old_pattern, rule.new_pattern, text,
                              flags=re.IGNORECASE | re.DOTALL)
            if new_text != text:
                changes.append(ChangeRecord(
                    rule_id=rule.rule_id, category="PLSQL", severity=rule.severity,
                    description=rule.description, description_ja=rule.description_ja,
                    old_text="", new_text="", line_number=line, file_name=filename,
                ))
                text = new_text
        return text, changes
