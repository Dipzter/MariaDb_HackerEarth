import tkinter as tk
from tkinter import messagebox, simpledialog
from database import get_connection, create_role, grant_privilege_to_role, grant_role_to_user

class RoleManagerGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("MariaDB Role Manager")
        
        # Create buttons
        tk.Button(root, text="Create Role", command=self.create_role, width=20).pack(pady=5)
        tk.Button(root, text="Grant Privilege to Role", command=self.grant_privilege, width=20).pack(pady=5)
        tk.Button(root, text="Assign Role to User", command=self.grant_role, width=20).pack(pady=5)
        tk.Button(root, text="Test Connection", command=self.test_connection, width=20).pack(pady=5)
        
    def get_input(self, title, prompt):
        """Helper function to get user input using a dialog box."""
        return simpledialog.askstring(title, prompt, parent=self.root)
        
    def create_role(self):
        role_name = self.get_input("Create Role", "Enter name for new role:")
        if role_name:
            create_role(role_name)
            messagebox.showinfo("Success", f"Role '{role_name}' created!")
        
    def grant_privilege(self):
        role_name = self.get_input("Grant Privilege", "Enter role name:")
        privilege = self.get_input("Grant Privilege", "Enter privilege (e.g., SELECT, INSERT):")
        table_name = self.get_input("Grant Privilege", "Enter table name:")
        if role_name and privilege and table_name:
            grant_privilege_to_role(role_name, privilege, table_name)
            messagebox.showinfo("Success", f"Granted {privilege} on {table_name} to {role_name}!")
        
    def grant_role(self):
        role_name = self.get_input("Assign Role", "Enter role name:")
        user_name = self.get_input("Assign Role", "Enter username:")
        if role_name and user_name:
            grant_role_to_user(role_name, user_name)
            messagebox.showinfo("Success", f"Role '{role_name}' assigned to user '{user_name}'!")
            
    def test_connection(self):
        try:
            conn = get_connection()
            conn.close()
            messagebox.showinfo("Success", "✅ Connection test successful!")
        except Exception as e:
            messagebox.showerror("Error", f"❌ Connection failed: {e}")

if __name__ == "__main__":
    root = tk.Tk()
    app = RoleManagerGUI(root)
    root.mainloop()