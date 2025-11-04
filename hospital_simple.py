# hospital_simple.py
from pymongo import MongoClient
from bson.objectid import ObjectId  # For using _id to delete
import sys

# --- 1. CONNECT TO MONGODB ---
try:
    # Connect to your local MongoDB server
    client = MongoClient("mongodb://localhost:27017/")
    
    # Create or select the database
    db = client['hospitalDB']  
    
    # Create or select the collections
    doctors_col = db['doctors']
    patients_col = db['patients']
    
    print("✅ Connected to local MongoDB successfully!")
except Exception as e:
    print(f"❌ Error connecting to MongoDB: {e}")
    sys.exit() # Exit the script if connection fails

# --- 2. DOCTOR FUNCTIONS (CREATE & READ) ---
def add_doctor():
    print("\n--- Add New Doctor ---")
    name = input("Enter name: ")
    specialty = input("Enter specialty: ")
    
    # Create a dictionary for the new doctor
    doctor_doc = {"name": name, "specialty": specialty}
    
    # Insert the dictionary into the 'doctors' collection
    doctors_col.insert_one(doctor_doc)
    print(f"✅ Doctor '{name}' added.")

def view_all_doctors():
    print("\n--- All Doctors ---")
    
    # Find all documents in the 'doctors' collection
    doctors = doctors_col.find()
    
    count = 0
    for doc in doctors:
        count += 1
        print(f"ID: {doc['_id']}, Name: Dr. {doc['name']}, Specialty: {doc['specialty']}")
    
    if count == 0:
        print("No doctors found.")

# --- 3. PATIENT FUNCTIONS (CREATE, READ, DELETE) ---
def add_patient():
    print("\n--- Add New Patient ---")
    name = input("Enter name: ")
    try:
        age = int(input("Enter age: "))
        
        # Create the patient document
        patient_doc = {"name": name, "age": age}
        
        # Insert it into the 'patients' collection
        patients_col.insert_one(patient_doc)
        print(f"✅ Patient '{name}' added.")
    except ValueError:
        print("❌ Invalid age. Must be a number.")

def view_all_patients():
    print("\n--- All Patients ---")
    patients = patients_col.find()
    count = 0
    for pat in patients:
        count += 1
        print(f"ID: {pat['_id']}, Name: {pat['name']}, Age: {pat['age']}")
    if count == 0:
        print("No patients found.")

def delete_patient():
    print("\n--- Delete Patient ---")
    # First, show all patients so the user can copy an ID
    view_all_patients()
    patient_id_str = input("Enter patient ID to delete: ")
    
    try:
        # Convert the string ID to a MongoDB ObjectId
        object_id_to_delete = ObjectId(patient_id_str)
        
        # Delete the document that matches the _id
        result = patients_col.delete_one({"_id": object_id_to_delete})
        
        if result.deleted_count > 0:
            print("✅ Patient deleted successfully.")
        else:
            print("❌ Patient not found.")
    except Exception as e:
        print(f"❌ Invalid ID format or error: {e}")

# --- 4. MAIN MENU AND APPLICATION LOOP ---
def main():
    while True:
        print("\n===== Simple Hospital System =====")
        print("1. Add Doctor")
        print("2. View All Doctors")
        print("3. Add Patient")
        print("4. View All Patients")
        print("5. Delete Patient")
        print("0. Exit")
        print("================================")
        
        choice = input("Enter your choice: ")
        
        if choice == '1':
            add_doctor()
        elif choice == '2':
            view_all_doctors()
        elif choice == '3':
            add_patient()
        elif choice == '4':
            view_all_patients()
        elif choice == '5':
            delete_patient()
        elif choice == '0':
            print("👋 Goodbye!")
            client.close() # Close the database connection
            break
        else:
            print("❌ Invalid choice. Please try again.")

# Run the main function when the script is executed
if __name__ == "__main__":
    main()