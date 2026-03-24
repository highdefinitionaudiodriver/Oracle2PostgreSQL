-- Sample Oracle DDL: Views and Synonyms
-- Tests: FORCE view, materialized view, synonym conversion

CREATE OR REPLACE FORCE VIEW v_active_employees AS
SELECT e.employee_id,
       e.first_name || ' ' || e.last_name AS full_name,
       e.email,
       e.hire_date,
       d.department_name,
       NVL(e.salary, 0) AS salary
FROM employees e
JOIN departments d ON e.department_id = d.department_id
WHERE e.is_active = 1;

-- Materialized view with refresh
CREATE MATERIALIZED VIEW mv_dept_summary
    BUILD IMMEDIATE
    REFRESH COMPLETE ON DEMAND
AS
SELECT d.department_id,
       d.department_name,
       COUNT(e.employee_id) AS emp_count,
       NVL(SUM(e.salary), 0) AS total_salary,
       NVL(AVG(e.salary), 0) AS avg_salary
FROM departments d
LEFT JOIN employees e ON d.department_id = e.department_id
GROUP BY d.department_id, d.department_name;

-- Synonyms
CREATE PUBLIC SYNONYM emp FOR hr.employees;
CREATE SYNONYM dept FOR hr.departments;
