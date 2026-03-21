-- Sample Oracle DML: Various query patterns
-- Tests: Oracle-specific syntax, functions, hints, joins

-- Oracle hints
SELECT /*+ INDEX(e idx_emp_name) PARALLEL(e, 4) */
       e.employee_id,
       e.first_name || ' ' || e.last_name AS full_name,
       DECODE(e.is_active, 1, 'Active', 0, 'Inactive', 'Unknown') AS status,
       NVL2(e.commission_pct, e.salary + e.commission_pct, e.salary) AS total_comp,
       NVL(e.phone, 'N/A') AS phone,
       TO_CHAR(e.hire_date, 'YYYY-MM-DD') AS hire_date_str,
       MONTHS_BETWEEN(SYSDATE, e.hire_date) AS months_employed,
       ADD_MONTHS(e.hire_date, 12) AS first_anniversary,
       TRUNC(e.hire_date) AS hire_date_only
FROM employees e
WHERE ROWNUM <= 100
  AND e.hire_date >= TO_DATE('2020-01-01', 'YYYY-MM-DD')
  AND REGEXP_LIKE(e.email, '^[A-Za-z]')
  AND e.department_id IN (
      SELECT department_id FROM departments WHERE department_name LIKE '%Engineering%'
  );

-- Oracle (+) outer join syntax
SELECT e.employee_id, e.last_name, d.department_name
FROM employees e, departments d
WHERE e.department_id = d.department_id(+);

-- CONNECT BY hierarchical query
SELECT employee_id,
       first_name || ' ' || last_name AS name,
       manager_id,
       LEVEL AS depth,
       SYS_CONNECT_BY_PATH(last_name, '/') AS path
FROM employees
START WITH manager_id IS NULL
CONNECT BY PRIOR employee_id = manager_id
ORDER SIBLINGS BY last_name;

-- FROM DUAL
SELECT SYS_GUID() AS new_id FROM DUAL;
SELECT SYSDATE AS current_time FROM DUAL;
SELECT seq_employee_id.NEXTVAL FROM DUAL;

-- MINUS (Oracle) → EXCEPT (PostgreSQL)
SELECT department_id FROM departments
MINUS
SELECT DISTINCT department_id FROM employees;

-- LISTAGG
SELECT department_id,
       LISTAGG(last_name, ', ') WITHIN GROUP (ORDER BY last_name) AS employees
FROM employees
GROUP BY department_id;

-- MERGE statement
MERGE INTO employees tgt
USING (SELECT 999 AS emp_id, 'New' AS fname, 'Employee' AS lname FROM DUAL) src
ON (tgt.employee_id = src.emp_id)
WHEN MATCHED THEN
    UPDATE SET tgt.first_name = src.fname
WHEN NOT MATCHED THEN
    INSERT (employee_id, first_name, last_name, hire_date)
    VALUES (src.emp_id, src.fname, src.lname, SYSDATE);

-- Substitution variables
SELECT * FROM employees WHERE department_id = &dept_id;
