-- Sample Oracle DDL: Table definitions
-- Tests: data type conversion, storage clause removal, constraints

CREATE TABLE employees (
    employee_id    NUMBER(10)       NOT NULL,
    first_name     VARCHAR2(50),
    last_name      VARCHAR2(100)    NOT NULL,
    email          VARCHAR2(255)    UNIQUE,
    phone          VARCHAR2(20),
    hire_date      DATE             DEFAULT SYSDATE,
    salary         NUMBER(12,2),
    commission_pct NUMBER(4,2),
    department_id  NUMBER(10),
    manager_id     NUMBER(10),
    is_active      NUMBER(1)        DEFAULT 1,
    photo          BLOB,
    resume         CLOB,
    notes          NCLOB,
    raw_data       RAW(2000),
    xml_config     XMLTYPE,
    CONSTRAINT pk_employees PRIMARY KEY (employee_id),
    CONSTRAINT fk_emp_dept FOREIGN KEY (department_id)
        REFERENCES departments(department_id),
    CONSTRAINT chk_salary CHECK (salary > 0)
)
TABLESPACE users
STORAGE (INITIAL 64K NEXT 64K PCTINCREASE 0)
PCTFREE 10
INITRANS 2
LOGGING;

CREATE TABLE departments (
    department_id   NUMBER(10)    NOT NULL,
    department_name VARCHAR2(100) NOT NULL,
    location_id     NUMBER(10),
    created_at      DATE          DEFAULT SYSDATE,
    CONSTRAINT pk_departments PRIMARY KEY (department_id)
)
TABLESPACE users
LOGGING;

-- Global temporary table
CREATE GLOBAL TEMPORARY TABLE temp_report_data (
    report_id    NUMBER(10),
    report_date  DATE,
    amount       NUMBER(15,2),
    description  VARCHAR2(500)
) ON COMMIT PRESERVE ROWS;

-- Table with LONG RAW and BFILE
CREATE TABLE documents (
    doc_id       NUMBER(10) NOT NULL,
    doc_name     VARCHAR2(200),
    doc_content  LONG RAW,
    doc_ref      BFILE,
    binary_val   BINARY_FLOAT,
    double_val   BINARY_DOUBLE,
    CONSTRAINT pk_documents PRIMARY KEY (doc_id)
);

-- COMMENT ON
COMMENT ON TABLE employees IS 'Employee master table';
COMMENT ON COLUMN employees.employee_id IS 'Unique employee identifier';
