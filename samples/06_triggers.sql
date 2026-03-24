-- Sample Oracle DDL: Triggers
-- Tests: Trigger → function + trigger conversion

CREATE OR REPLACE TRIGGER trg_emp_audit
    BEFORE INSERT OR UPDATE OR DELETE ON employees
    FOR EACH ROW
DECLARE
    v_action VARCHAR2(10);
BEGIN
    IF INSERTING THEN
        v_action := 'INSERT';
        :NEW.hire_date := NVL(:NEW.hire_date, SYSDATE);
    ELSIF UPDATING THEN
        v_action := 'UPDATE';
    ELSIF DELETING THEN
        v_action := 'DELETE';
    END IF;

    INSERT INTO audit_log (
        audit_id, table_name, action, record_id,
        old_value, new_value, changed_by, changed_at
    ) VALUES (
        seq_audit_id.NEXTVAL,
        'EMPLOYEES',
        v_action,
        NVL(:NEW.employee_id, :OLD.employee_id),
        :OLD.salary,
        :NEW.salary,
        USER,
        SYSDATE
    );
END trg_emp_audit;
/

-- Simple auto-increment trigger
CREATE OR REPLACE TRIGGER trg_emp_id
    BEFORE INSERT ON employees
    FOR EACH ROW
BEGIN
    IF :NEW.employee_id IS NULL THEN
        SELECT seq_employee_id.NEXTVAL INTO :NEW.employee_id FROM DUAL;
    END IF;
END trg_emp_id;
/
