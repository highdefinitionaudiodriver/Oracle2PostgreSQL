-- Sample Oracle PL/SQL: Package
-- Tests: Package → Schema conversion (MANUAL)

CREATE OR REPLACE PACKAGE pkg_employee_mgmt AS
    -- Constants
    c_max_salary CONSTANT NUMBER := 999999.99;

    -- Types
    TYPE t_emp_record IS RECORD (
        emp_id    NUMBER,
        emp_name  VARCHAR2(200),
        salary    NUMBER(12,2)
    );

    TYPE t_emp_table IS TABLE OF t_emp_record;

    -- Procedures
    PROCEDURE hire_employee(
        p_first_name  IN VARCHAR2,
        p_last_name   IN VARCHAR2,
        p_email       IN VARCHAR2,
        p_dept_id     IN NUMBER,
        p_salary      IN NUMBER DEFAULT 50000
    );

    PROCEDURE terminate_employee(
        p_employee_id IN NUMBER
    );

    -- Functions
    FUNCTION get_employee_count(
        p_department_id IN NUMBER
    ) RETURN NUMBER;

    FUNCTION get_top_earners(
        p_department_id IN NUMBER,
        p_limit         IN NUMBER DEFAULT 10
    ) RETURN t_emp_table PIPELINED;

END pkg_employee_mgmt;
/

CREATE OR REPLACE PACKAGE BODY pkg_employee_mgmt AS

    PROCEDURE hire_employee(
        p_first_name  IN VARCHAR2,
        p_last_name   IN VARCHAR2,
        p_email       IN VARCHAR2,
        p_dept_id     IN NUMBER,
        p_salary      IN NUMBER DEFAULT 50000
    )
    IS
        v_emp_id NUMBER;
    BEGIN
        SELECT seq_employee_id.NEXTVAL INTO v_emp_id FROM DUAL;

        INSERT INTO employees (
            employee_id, first_name, last_name, email,
            hire_date, salary, department_id, is_active
        ) VALUES (
            v_emp_id, p_first_name, p_last_name, p_email,
            SYSDATE, p_salary, p_dept_id, 1
        );

        DBMS_OUTPUT.PUT_LINE('Hired: ' || p_first_name || ' ' || p_last_name
                             || ' (ID: ' || v_emp_id || ')');
        COMMIT;
    EXCEPTION
        WHEN DUP_VAL_ON_INDEX THEN
            RAISE_APPLICATION_ERROR(-20010, 'Duplicate email: ' || p_email);
    END hire_employee;

    PROCEDURE terminate_employee(
        p_employee_id IN NUMBER
    )
    IS
    BEGIN
        UPDATE employees
        SET is_active = 0
        WHERE employee_id = p_employee_id;

        IF SQL%NOTFOUND THEN
            RAISE_APPLICATION_ERROR(-20011, 'Employee not found: ' || p_employee_id);
        END IF;

        COMMIT;
    END terminate_employee;

    FUNCTION get_employee_count(
        p_department_id IN NUMBER
    ) RETURN NUMBER
    IS
        v_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM employees
        WHERE department_id = p_department_id
          AND is_active = 1;
        RETURN v_count;
    END get_employee_count;

    FUNCTION get_top_earners(
        p_department_id IN NUMBER,
        p_limit         IN NUMBER DEFAULT 10
    ) RETURN t_emp_table PIPELINED
    IS
    BEGIN
        FOR rec IN (
            SELECT employee_id, first_name || ' ' || last_name AS emp_name, salary
            FROM employees
            WHERE department_id = p_department_id
              AND is_active = 1
            ORDER BY salary DESC
            FETCH FIRST p_limit ROWS ONLY
        ) LOOP
            PIPE ROW(t_emp_record(rec.employee_id, rec.emp_name, rec.salary));
        END LOOP;
        RETURN;
    END get_top_earners;

END pkg_employee_mgmt;
/
