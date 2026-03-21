-- Sample Oracle PL/SQL: Procedures and Functions
-- Tests: PL/SQL→PL/pgSQL conversion, Oracle function mapping

CREATE OR REPLACE PROCEDURE sp_update_salary(
    p_employee_id  IN  NUMBER,
    p_new_salary   IN  NUMBER,
    p_result       OUT VARCHAR2
)
IS
    v_old_salary   NUMBER(12,2);
    v_emp_name     VARCHAR2(200);
    v_update_count NUMBER;
BEGIN
    -- Get current salary
    SELECT salary, first_name || ' ' || last_name
    INTO v_old_salary, v_emp_name
    FROM employees
    WHERE employee_id = p_employee_id;

    -- Update salary
    UPDATE employees
    SET salary = p_new_salary
    WHERE employee_id = p_employee_id;

    v_update_count := SQL%ROWCOUNT;

    IF SQL%FOUND THEN
        DBMS_OUTPUT.PUT_LINE('Updated salary for ' || v_emp_name);
        p_result := 'SUCCESS';
    ELSE
        p_result := 'NOT_FOUND';
    END IF;

    COMMIT;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        p_result := 'NOT_FOUND';
        DBMS_OUTPUT.PUT_LINE('Employee not found: ' || p_employee_id);
    WHEN OTHERS THEN
        ROLLBACK;
        p_result := 'ERROR: ' || SQLERRM;
        RAISE_APPLICATION_ERROR(-20001, 'Failed to update salary: ' || SQLERRM);
END sp_update_salary;
/

CREATE OR REPLACE FUNCTION fn_get_department_count(
    p_department_id IN NUMBER
) RETURN NUMBER
IS
    v_count NUMBER;
BEGIN
    SELECT COUNT(*)
    INTO v_count
    FROM employees
    WHERE department_id = p_department_id
      AND is_active = 1;

    RETURN v_count;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN 0;
END fn_get_department_count;
/

-- Procedure using cursors and Oracle-specific functions
CREATE OR REPLACE PROCEDURE sp_generate_report
IS
    CURSOR c_departments IS
        SELECT department_id, department_name
        FROM departments
        ORDER BY department_name;

    v_today     DATE := SYSDATE;
    v_guid      RAW(16);
    v_month_diff NUMBER;
BEGIN
    v_guid := SYS_GUID();

    FOR rec IN c_departments LOOP
        v_month_diff := MONTHS_BETWEEN(SYSDATE, ADD_MONTHS(v_today, -12));

        INSERT INTO temp_report_data (report_id, report_date, amount, description)
        SELECT seq_employee_id.NEXTVAL,
               TRUNC(SYSDATE),
               NVL(SUM(e.salary), 0),
               'Department: ' || rec.department_name
        FROM employees e
        WHERE e.department_id = rec.department_id
          AND SUBSTR(e.last_name, 1, 1) != 'X'
          AND LENGTHB(e.email) < 255
          AND e.hire_date >= ADD_MONTHS(SYSDATE, -24);

        DBMS_OUTPUT.PUT_LINE('Processed: ' || rec.department_name);
    END LOOP;

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE_APPLICATION_ERROR(-20002, 'Report generation failed: ' || SQLERRM);
END sp_generate_report;
/
