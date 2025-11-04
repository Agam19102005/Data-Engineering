# Command-line interface for managing employees
# app/cli.py
import argparse
from app import crud

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["create-dept","list-depts","create-emp","list-emps","get-emp","delete-emp"])
    parser.add_argument("--first", help="first name")
    parser.add_argument("--last", help="last name")
    parser.add_argument("--email", help="email")
    parser.add_argument("--dept", type=int)
    args = parser.parse_args()

    if args.action == "create-dept":
        name = input("Department name: ")
        desc = input("Description: ")
        print(crud.create_department(name, desc))
    elif args.action == "list-depts":
        print(crud.get_departments())
    elif args.action == "create-emp":
        print(crud.create_employee(args.first, args.last, args.email, dept_id=args.dept))
    elif args.action == "list-emps":
        print(crud.list_employees())
    elif args.action == "get-emp":
        eid = int(input("Employee id: "))
        print(crud.get_employee(eid))
    elif args.action == "delete-emp":
        eid = int(input("Employee id: "))
        crud.delete_employee(eid)
        print("Deleted.")

if __name__ == "__main__":
    main()
