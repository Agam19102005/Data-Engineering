# app/api.py
from flask import Flask, request, jsonify
from app import crud

app = Flask(__name__)

# ---------------------------
# Department Routes
# ---------------------------
@app.route("/departments", methods=["POST"])
def create_department():
    data = request.get_json()
    dept = crud.create_department(data["name"], data.get("description"))
    return jsonify(dept), 201

@app.route("/departments", methods=["GET"])
def list_departments():
    return jsonify(crud.get_departments())

@app.route("/departments/<int:dept_id>", methods=["GET"])
def get_department(dept_id):
    dept = crud.get_department_by_id(dept_id)
    if dept:
        return jsonify(dept)
    return jsonify({"error": "Department not found"}), 404

@app.route("/departments/<int:dept_id>", methods=["PUT"])
def update_department(dept_id):
    data = request.get_json()
    updated = crud.update_department(dept_id, data.get("name"), data.get("description"))
    if updated:
        return jsonify(updated)
    return jsonify({"error": "Department not found"}), 404

@app.route("/departments/<int:dept_id>", methods=["DELETE"])
def delete_department(dept_id):
    crud.delete_department(dept_id)
    return jsonify({"message": "Department deleted"}), 200


# ---------------------------
# Role Routes
# ---------------------------
@app.route("/roles", methods=["POST"])
def create_role():
    data = request.get_json()
    role = crud.create_role(data["title"], data.get("salary_grade"))
    return jsonify(role), 201

@app.route("/roles", methods=["GET"])
def list_roles():
    return jsonify(crud.get_roles())

@app.route("/roles/<int:role_id>", methods=["GET"])
def get_role(role_id):
    role = crud.get_role_by_id(role_id)
    if role:
        return jsonify(role)
    return jsonify({"error": "Role not found"}), 404

@app.route("/roles/<int:role_id>", methods=["PUT"])
def update_role(role_id):
    data = request.get_json()
    updated = crud.update_role(role_id, data.get("title"), data.get("salary_grade"))
    if updated:
        return jsonify(updated)
    return jsonify({"error": "Role not found"}), 404

@app.route("/roles/<int:role_id>", methods=["DELETE"])
def delete_role(role_id):
    crud.delete_role(role_id)
    return jsonify({"message": "Role deleted"}), 200


# ---------------------------
# Employee Routes
# ---------------------------
@app.route("/employees", methods=["POST"])
def create_employee():
    data = request.get_json()
    emp = crud.create_employee(
        data["first_name"], data["last_name"], data["email"],
        data.get("phone"), data.get("dept_id"),
        data.get("role_id"), data.get("hire_date")
    )
    return jsonify(emp), 201

@app.route("/employees", methods=["GET"])
def list_employees():
    return jsonify(crud.list_employees())

@app.route("/employees/<int:emp_id>", methods=["GET"])
def get_employee(emp_id):
    emp = crud.get_employee(emp_id)
    if emp:
        return jsonify(emp)
    return jsonify({"error": "Employee not found"}), 404

@app.route("/employees/<int:emp_id>", methods=["PUT"])
def update_employee(emp_id):
    data = request.get_json()
    updated = crud.update_employee(emp_id, **data)
    if updated:
        return jsonify(updated)
    return jsonify({"error": "Employee not found"}), 404

@app.route("/employees/<int:emp_id>", methods=["DELETE"])
def delete_employee(emp_id):
    crud.delete_employee(emp_id)
    return jsonify({"message": "Employee deleted"}), 200


# ---------------------------
# Run server
# ---------------------------
if __name__ == "__main__":
    app.run(debug=True)
