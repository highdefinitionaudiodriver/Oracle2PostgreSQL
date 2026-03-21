"""
Oracle2PostgreSQL - Test Suite
Tests parsing, transformation, and generation.
"""

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))

from src.oracle_parser import OracleParser, StatementType
from src.postgres_transformer import PostgresTransformer, TransformOptions
from src.postgres_generator import PostgresCodeGenerator
from src.report_generator import ReportGenerator
from src.migration_rules import MigrationRules


class TestOracleParser(unittest.TestCase):
    """Tests for Oracle SQL parser."""

    def setUp(self):
        self.parser = OracleParser()

    def test_parse_create_table(self):
        sql = """CREATE TABLE employees (
            employee_id NUMBER(10) NOT NULL,
            first_name  VARCHAR2(50),
            salary      NUMBER(12,2),
            hire_date   DATE DEFAULT SYSDATE,
            is_active   NUMBER(1) DEFAULT 1,
            CONSTRAINT pk_emp PRIMARY KEY (employee_id)
        ) TABLESPACE users;"""

        schema = self.parser.parse_string(sql)
        self.assertEqual(len(schema.tables), 1)

        table = schema.tables[0]
        self.assertEqual(table.name, "employees")
        self.assertEqual(len(table.columns), 5)
        self.assertEqual(table.columns[0].name, "employee_id")
        self.assertFalse(table.columns[0].nullable)
        self.assertEqual(len(table.constraints), 1)
        self.assertEqual(table.constraints[0].constraint_type, "PRIMARY KEY")

    def test_parse_global_temporary_table(self):
        sql = "CREATE GLOBAL TEMPORARY TABLE temp_data (id NUMBER(10), val VARCHAR2(100)) ON COMMIT PRESERVE ROWS;"
        schema = self.parser.parse_string(sql)
        self.assertEqual(len(schema.tables), 1)
        self.assertTrue(schema.tables[0].is_temporary)

    def test_parse_create_sequence(self):
        sql = "CREATE SEQUENCE seq_test START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE NOORDER;"
        schema = self.parser.parse_string(sql)
        self.assertEqual(len(schema.sequences), 1)
        seq = schema.sequences[0]
        self.assertEqual(seq.name, "seq_test")
        self.assertEqual(seq.start_value, 1)
        self.assertEqual(seq.increment_by, 1)

    def test_parse_create_index(self):
        sql = "CREATE BITMAP INDEX idx_test ON employees(is_active) TABLESPACE idx_ts;"
        schema = self.parser.parse_string(sql)
        self.assertEqual(len(schema.indexes), 1)
        idx = schema.indexes[0]
        self.assertTrue(idx.is_bitmap)
        self.assertEqual(idx.table_name, "employees")

    def test_parse_create_view(self):
        sql = "CREATE OR REPLACE FORCE VIEW v_test AS SELECT * FROM employees WHERE is_active = 1;"
        schema = self.parser.parse_string(sql)
        self.assertEqual(len(schema.views), 1)
        self.assertTrue(schema.views[0].is_force)
        self.assertEqual(schema.views[0].name, "v_test")

    def test_parse_create_synonym(self):
        sql = "CREATE PUBLIC SYNONYM emp FOR hr.employees;"
        schema = self.parser.parse_string(sql)
        self.assertEqual(len(schema.synonyms), 1)
        self.assertTrue(schema.synonyms[0].is_public)
        self.assertEqual(schema.synonyms[0].name, "emp")

    def test_parse_procedure(self):
        sql = """CREATE OR REPLACE PROCEDURE sp_test(
            p_id IN NUMBER,
            p_name OUT VARCHAR2
        )
        IS
        BEGIN
            SELECT first_name INTO p_name FROM employees WHERE employee_id = p_id;
        END sp_test;"""

        schema = self.parser.parse_string(sql)
        self.assertEqual(len(schema.procedures), 1)
        proc = schema.procedures[0]
        self.assertEqual(proc.name, "sp_test")
        self.assertEqual(len(proc.parameters), 2)
        self.assertFalse(proc.is_function)

    def test_parse_function(self):
        sql = """CREATE OR REPLACE FUNCTION fn_test(p_id IN NUMBER)
        RETURN NUMBER
        IS
            v_count NUMBER;
        BEGIN
            SELECT COUNT(*) INTO v_count FROM employees WHERE department_id = p_id;
            RETURN v_count;
        END fn_test;"""

        schema = self.parser.parse_string(sql)
        self.assertEqual(len(schema.procedures), 1)
        func = schema.procedures[0]
        self.assertTrue(func.is_function)
        self.assertEqual(func.return_type, "NUMBER")

    def test_parse_trigger(self):
        sql = """CREATE OR REPLACE TRIGGER trg_test
            BEFORE INSERT ON employees
            FOR EACH ROW
        BEGIN
            :NEW.hire_date := NVL(:NEW.hire_date, SYSDATE);
        END trg_test;"""

        schema = self.parser.parse_string(sql)
        self.assertEqual(len(schema.triggers), 1)
        trig = schema.triggers[0]
        self.assertEqual(trig.name, "trg_test")
        self.assertEqual(trig.timing, "BEFORE")
        self.assertIn("INSERT", trig.events)
        self.assertTrue(trig.for_each_row)

    def test_detect_oracle_features(self):
        sql = """SELECT /*+ INDEX(e) */ NVL(salary, 0), SYSDATE, DECODE(status, 1, 'Y', 'N')
        FROM employees e
        WHERE e.department_id = d.department_id(+)
        START WITH manager_id IS NULL
        CONNECT BY PRIOR employee_id = manager_id;"""

        schema = self.parser.parse_string(sql)
        self.assertTrue(schema.has_oracle_hints)
        self.assertTrue(schema.has_connect_by)
        self.assertTrue(schema.has_outer_join_plus)
        self.assertIn("SYSDATE", schema.oracle_functions_used)
        self.assertIn("NVL", schema.oracle_functions_used)
        self.assertIn("DECODE", schema.oracle_functions_used)

    def test_statement_type_detection(self):
        cases = [
            ("CREATE TABLE t (id NUMBER)", StatementType.CREATE_TABLE),
            ("CREATE SEQUENCE s START WITH 1", StatementType.CREATE_SEQUENCE),
            ("CREATE UNIQUE INDEX i ON t(c)", StatementType.CREATE_INDEX),
            ("CREATE OR REPLACE VIEW v AS SELECT 1", StatementType.CREATE_VIEW),
            ("SELECT * FROM t", StatementType.SELECT),
            ("INSERT INTO t VALUES (1)", StatementType.INSERT),
            ("UPDATE t SET c = 1", StatementType.UPDATE),
            ("DELETE FROM t", StatementType.DELETE),
            ("DROP TABLE t", StatementType.DROP),
            ("ALTER TABLE t ADD c NUMBER", StatementType.ALTER_TABLE),
            ("GRANT SELECT ON t TO u", StatementType.GRANT),
        ]
        for sql, expected_type in cases:
            schema = self.parser.parse_string(sql)
            self.assertEqual(schema.statements[0].type, expected_type,
                             f"Failed for: {sql}")


class TestMigrationRules(unittest.TestCase):
    """Tests for migration rules engine."""

    def setUp(self):
        self.rules = MigrationRules()

    def test_rule_count(self):
        self.assertGreater(len(self.rules.rules), 50)

    def test_categories_exist(self):
        categories = self.rules.get_all_categories()
        for cat in ["DATATYPE", "FUNCTION", "SYNTAX", "PLSQL", "SEQUENCE"]:
            self.assertIn(cat, categories)

    def test_get_rules_by_category(self):
        dt_rules = self.rules.get_rules_by_category("DATATYPE")
        self.assertGreater(len(dt_rules), 10)
        for r in dt_rules:
            self.assertEqual(r.category, "DATATYPE")

    def test_get_rule_by_id(self):
        rule = self.rules.get_rule_by_id("DT_001")
        self.assertIsNotNone(rule)
        self.assertEqual(rule.category, "DATATYPE")

    def test_auto_fixable_rules(self):
        auto_rules = self.rules.get_auto_fixable_rules()
        self.assertGreater(len(auto_rules), 0)
        for r in auto_rules:
            self.assertTrue(r.auto_fix)


class TestPostgresTransformer(unittest.TestCase):
    """Tests for Oracle→PostgreSQL transformer."""

    def setUp(self):
        self.parser = OracleParser()
        self.transformer = PostgresTransformer()

    def _transform(self, sql):
        schema = self.parser.parse_string(sql)
        return self.transformer.transform(schema)

    def test_datatype_varchar2(self):
        result = self._transform("CREATE TABLE t (name VARCHAR2(100));")
        self.assertIn("VARCHAR(", result.transformed_text)
        self.assertNotIn("VARCHAR2", result.transformed_text)

    def test_datatype_number(self):
        result = self._transform("CREATE TABLE t (id NUMBER(10), val NUMBER(12,2));")
        self.assertIn("NUMERIC(10)", result.transformed_text)
        self.assertIn("NUMERIC(12,2)", result.transformed_text)

    def test_datatype_clob(self):
        result = self._transform("CREATE TABLE t (content CLOB);")
        self.assertIn("TEXT", result.transformed_text)

    def test_datatype_blob(self):
        result = self._transform("CREATE TABLE t (data BLOB);")
        self.assertIn("BYTEA", result.transformed_text)

    def test_datatype_date(self):
        result = self._transform("CREATE TABLE t (created DATE);")
        self.assertIn("TIMESTAMP", result.transformed_text)

    def test_function_sysdate(self):
        result = self._transform("SELECT SYSDATE FROM DUAL;")
        self.assertIn("CURRENT_TIMESTAMP", result.transformed_text)
        self.assertNotIn("FROM DUAL", result.transformed_text)

    def test_function_nvl(self):
        result = self._transform("SELECT NVL(salary, 0) FROM employees;")
        self.assertIn("COALESCE(", result.transformed_text)

    def test_function_substr(self):
        result = self._transform("SELECT SUBSTR(name, 1, 5) FROM employees;")
        self.assertIn("SUBSTRING(", result.transformed_text)

    def test_function_sys_guid(self):
        result = self._transform("SELECT SYS_GUID() FROM DUAL;")
        self.assertIn("gen_random_uuid()", result.transformed_text)

    def test_syntax_from_dual_removed(self):
        result = self._transform("SELECT 1 FROM DUAL;")
        self.assertNotIn("FROM DUAL", result.transformed_text)

    def test_syntax_minus_to_except(self):
        result = self._transform(
            "SELECT id FROM t1 MINUS SELECT id FROM t2;")
        self.assertIn("EXCEPT", result.transformed_text)

    def test_sequence_nextval(self):
        result = self._transform("SELECT seq_test.NEXTVAL FROM DUAL;")
        self.assertIn("nextval('seq_test')", result.transformed_text)

    def test_storage_clause_removed(self):
        result = self._transform(
            "CREATE TABLE t (id NUMBER(10)) TABLESPACE users PCTFREE 10 LOGGING;")
        self.assertNotIn("TABLESPACE", result.transformed_text)
        self.assertNotIn("PCTFREE", result.transformed_text)
        self.assertNotIn("LOGGING", result.transformed_text)

    def test_hint_removed(self):
        result = self._transform("SELECT /*+ INDEX(e idx_name) */ * FROM employees e;")
        self.assertNotIn("/*+", result.transformed_text)
        self.assertIn("Oracle hint removed", result.transformed_text)

    def test_global_temporary(self):
        result = self._transform(
            "CREATE GLOBAL TEMPORARY TABLE t (id NUMBER) ON COMMIT PRESERVE ROWS;")
        self.assertIn("TEMPORARY", result.transformed_text)
        self.assertNotIn("GLOBAL TEMPORARY", result.transformed_text)

    def test_synonym_to_view(self):
        result = self._transform("CREATE PUBLIC SYNONYM emp FOR hr.employees;")
        self.assertIn("CREATE OR REPLACE VIEW", result.transformed_text)

    def test_change_counts(self):
        result = self._transform("""
            SELECT NVL(salary, 0), SYSDATE, SUBSTR(name, 1, 3)
            FROM employees;
        """)
        self.assertGreater(result.auto_converted, 0)

    def test_bitmap_index_removed(self):
        result = self._transform("CREATE BITMAP INDEX idx ON t(col) TABLESPACE ts;")
        self.assertNotIn("BITMAP", result.transformed_text)
        self.assertNotIn("TABLESPACE", result.transformed_text)

    def test_sequence_nocache_removed(self):
        result = self._transform("CREATE SEQUENCE seq_test NOCACHE NOCYCLE NOORDER;")
        self.assertNotIn("NOCACHE", result.transformed_text)
        self.assertNotIn("NOORDER", result.transformed_text)
        self.assertIn("NO CYCLE", result.transformed_text)

    def test_force_view_keyword_removed(self):
        result = self._transform("CREATE OR REPLACE FORCE VIEW v AS SELECT 1;")
        self.assertNotIn("FORCE", result.transformed_text)
        self.assertIn("VIEW", result.transformed_text)


class TestPostgresGenerator(unittest.TestCase):
    """Tests for PostgreSQL code generator."""

    def test_generate_output_file(self):
        from src.postgres_transformer import TransformResult
        with tempfile.TemporaryDirectory() as tmpdir:
            result = TransformResult(
                filename="test.sql",
                original_text="SELECT SYSDATE FROM DUAL;",
                transformed_text="SELECT CURRENT_TIMESTAMP;",
                auto_converted=2,
                needs_review=0,
                manual_only=0,
            )
            gen = PostgresCodeGenerator(tmpdir)
            out_path = gen.generate(result)

            self.assertTrue(os.path.exists(out_path))
            self.assertTrue(out_path.endswith("_pg.sql"))

            with open(out_path, "r", encoding="utf-8") as f:
                content = f.read()
            self.assertIn("Oracle", content)
            self.assertIn("PostgreSQL", content)
            self.assertIn("SELECT CURRENT_TIMESTAMP", content)


class TestReportGenerator(unittest.TestCase):
    """Tests for report generation."""

    def test_generate_reports(self):
        from src.postgres_transformer import TransformResult, ChangeRecord
        with tempfile.TemporaryDirectory() as tmpdir:
            result = TransformResult(
                filename="test.sql",
                original_text="original",
                transformed_text="transformed",
                auto_converted=3,
                needs_review=1,
                manual_only=0,
                changes=[
                    ChangeRecord(
                        rule_id="DT_001", category="DATATYPE", severity="AUTO",
                        description="NUMBER → NUMERIC", description_ja="NUMBER → NUMERIC",
                        old_text="", new_text="", line_number=1, file_name="test.sql",
                    ),
                    ChangeRecord(
                        rule_id="FN_001", category="FUNCTION", severity="REVIEW",
                        description="SYSDATE → CURRENT_TIMESTAMP",
                        description_ja="SYSDATE → CURRENT_TIMESTAMP",
                        old_text="", new_text="", line_number=5, file_name="test.sql",
                    ),
                ],
            )

            gen = ReportGenerator(tmpdir)
            paths = gen.generate([result])

            self.assertTrue(os.path.exists(paths["html"]))
            self.assertTrue(os.path.exists(paths["csv"]))

            with open(paths["html"], "r", encoding="utf-8") as f:
                html = f.read()
            self.assertIn("Migration Report", html)
            self.assertIn("DATATYPE", html)

            with open(paths["csv"], "r", encoding="utf-8-sig") as f:
                csv_content = f.read()
            self.assertIn("DT_001", csv_content)


class TestEndToEnd(unittest.TestCase):
    """End-to-end integration test."""

    def test_full_pipeline_with_samples(self):
        samples_dir = os.path.join(os.path.dirname(__file__), "samples")
        if not os.path.isdir(samples_dir):
            self.skipTest("samples directory not found")

        with tempfile.TemporaryDirectory() as tmpdir:
            parser = OracleParser()
            transformer = PostgresTransformer()
            generator = PostgresCodeGenerator(tmpdir)
            results = []

            for f in sorted(os.listdir(samples_dir)):
                if f.endswith(".sql"):
                    filepath = os.path.join(samples_dir, f)
                    schema = parser.parse_file(filepath)
                    result = transformer.transform(schema)
                    generator.generate(result)
                    results.append(result)

            self.assertGreater(len(results), 0)
            total_changes = sum(r.total_changes for r in results)
            self.assertGreater(total_changes, 0)

            # Generate report
            report_gen = ReportGenerator(tmpdir)
            report_paths = report_gen.generate(results)
            self.assertTrue(os.path.exists(report_paths["html"]))

            # Print summary
            print(f"\n{'='*60}")
            print(f"End-to-End Test Results")
            print(f"{'='*60}")
            for r in results:
                fname = os.path.basename(r.filename)
                print(f"  {fname}: Auto={r.auto_converted}, "
                      f"Review={r.needs_review}, Manual={r.manual_only}")
            print(f"{'='*60}")
            print(f"  Total files: {len(results)}")
            print(f"  Total changes: {total_changes}")
            print(f"  Auto: {sum(r.auto_converted for r in results)}")
            print(f"  Review: {sum(r.needs_review for r in results)}")
            print(f"  Manual: {sum(r.manual_only for r in results)}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
