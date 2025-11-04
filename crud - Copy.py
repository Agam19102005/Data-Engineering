# CRUD functions for employee, department, and roles
# app/crud.py

from app.db import get_conn
from psycopg2.extras import RealDictCursor

# Helper to run a query
def _run(query, params=None, fetch=False, fetchone=False):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params or ())
            if fetchone:
                result = cur.fetchone()
            elif fetch:
                result = cur.fetchall()
            else:
                conn.commit()
                result = None
            return result
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()  # close instead of put_conn()

# ---------------------------
# Department CRUD
# ---------------------------
def create_department(name, description=None):
    q = "INSERT INTO department (name, description) VALUES (%s, %s) RETURNING *;"
    return _run(q, (name, description), fetchone=True)

def get_departments():
    return _run("SELECT * FROM department ORDER BY id;", fetch=True)

def get_department_by_id(dept_id):
    return _run("SELECT * FROM department WHERE id=%s;", (dept_id,), fetchone=True)

def update_department(dept_id, name=None, description=None):
    q = "UPDATE department SET name=COALESCE(%s, name), description=COALESCE(%s, description) WHERE id=%s RETURNING *;"
    return _run(q, (name, description, dept_id), fetchone=True)

def delete_department(dept_id):
    _run("DELETE FROM department WHERE id=%s;", (dept_id,))

# ---------------------------
# Role CRUD
# ---------------------------
def create_role(title, salary_grade=None):
    q = "INSERT INTO role (title, salary_grade) VALUES (%s, %s) RETURNING *;"
    return _run(q, (title, salary_grade), fetchone=True)

def get_roles():
    return _run("SELECT * FROM role ORDER BY id;", fetch=True)

def get_role_by_id(role_id):
    return _run("SELECT * FROM role WHERE id=%s;", (role_id,), fetchone=True)

def update_role(role_id, title=None, salary_grade=None):
    q = "UPDATE role SET title=COALESCE(%s, title), salary_grade=COALESCE(%s, salary_grade) WHERE id=%s RETURNING *;"
    return _run(q, (title, salary_grade, role_id), fetchone=True)

def delete_role(role_id):
    _run("DELETE FROM role WHERE id=%s;", (role_id,))

# ---------------------------
# Employee CRUD
# ---------------------------
def create_employee(first_name, last_name, email, phone=None, dept_id=None, role_id=None, hire_date=None):
    q = """INSERT INTO employee (first_name, last_name, email, phone, dept_id, role_id, hire_date)
           VALUES (%s,%s,%s,%s,%s,%s, COALESCE(%s, CURRENT_DATE)) RETURNING *;"""
    return _run(q, (first_name, last_name, email, phone, dept_id, role_id, hire_date), fetchone=True)

def get_employee(employee_id):
    q = """
    SELECT e.*, d.name AS department, r.title AS role
    FROM employee e
    LEFT JOIN department d ON e.dept_id = d.id
    LEFT JOIN role r ON e.role_id = r.id
    WHERE e.id = %s;
    """
    return _run(q, (employee_id,), fetchone=True)

def list_employees(limit=100, offset=0):
    q = "SELECT * FROM employee ORDER BY id LIMIT %s OFFSET %s;"
    return _run(q, (limit, offset), fetch=True)

def update_employee(emp_id, **fields):
    allowed = {"first_name", "last_name", "email", "phone", "dept_id", "role_id", "hire_date"}
    set_parts, params = [], []
    for k, v in fields.items():
        if k in allowed:
            set_parts.append(f"{k} = %s")
            params.append(v)
    if not set_parts:
        return None
    params.append(emp_id)
    q = f"UPDATE employee SET {', '.join(set_parts)} WHERE id = %s RETURNING *;"
    return _run(q, params, fetchone=True)

def delete_employee(emp_id):
    _run("DELETE FROM employee WHERE id = %s;", (emp_id,))
