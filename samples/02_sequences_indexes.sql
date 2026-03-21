-- Sample Oracle DDL: Sequences and Indexes
-- Tests: sequence conversion, bitmap index, storage removal

CREATE SEQUENCE seq_employee_id
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 999999999
    NOCACHE
    NOCYCLE
    NOORDER;

CREATE SEQUENCE seq_department_id
    START WITH 100
    INCREMENT BY 10
    CACHE 20
    CYCLE;

-- Unique index
CREATE UNIQUE INDEX idx_emp_email ON employees(email)
    TABLESPACE idx_ts
    STORAGE (INITIAL 64K)
    PCTFREE 10
    COMPUTE STATISTICS;

-- Bitmap index (Oracle-specific)
CREATE BITMAP INDEX idx_emp_active ON employees(is_active)
    TABLESPACE idx_ts
    NOLOGGING;

-- Composite index
CREATE INDEX idx_emp_name ON employees(last_name, first_name);

-- Function-based index
CREATE INDEX idx_emp_upper_email ON employees(UPPER(email));
