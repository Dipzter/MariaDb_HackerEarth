import tkinter as tk
from tkinter import messagebox
from database_D import create_role, grant_privileges, grant_role_to_user

root = tk.Tk()
root.title("MariaDB Role Manager")

role_entry = tk.Entry(root)
privilege_role_entry = tk.Entry(root)
privileges = ['SELECT', 'INSERT', 'UPDATE', 'DELETE']
privilege_var = tk.StringVar(root)
privilege_dropdown = tk.OptionMenu(root, privilege_var, *privileges)
table_entry = tk.Entry(root)
grant_role_entry = tk.Entry(root)
user_entry = tk.Entry(root)

def create_role_gui():
    role_name = role_entry.get()
    if role_name:
        create_role(role_name)
        messagebox.showinfo("Success", f"Role '{role_name}' created.")
    else:
        messagebox.showwarning("Input Error", "Please try again.")
        
        
def grant_privileges_gui():
    role_name = privilege_role_entry.get()
    privileges = privilege_var.get()
    table_name = table_entry.get()
    if role_name and privileges and table_name:
        grant_privileges(role_name, privileges, table_name)
        messagebox.showinfo("Success", f"Granted {privileges} on {table_name} to '{role_name}'.")
    else:
        messagebox.showwarning("Input Error", "Please fill all fields.")
        
def grant_role_to_user_gui():
    role_name = role_entry.get()
    user_name = user_entry.get()
    if role_name and user_name:
        grant_role_to_user(role_name, user_name)
        messagebox.showinfo("Success", f"Granted role '{role_name}' to user '{user_name}'.")
    else:
        messagebox.showwarning("Input Error", "Please fill all fields.")
    
    
    
tk.Label(root, text="Create Role Name:", font = ('Arial', 12, 'bold')).grid(row=0, column = 0, sticky = "w", padx = 5, pady = 5)

tk.Label(root, text="Role Name:").grid(row=1, column=0, sticky="w", padx=5)
role_entry.grid(row=1, column=1, padx=5, pady=2)

tk.Button(root, text= "Create Role", command=create_role_gui).grid(row=1, column=2, padx=5, pady=2)



tk.Label(root, text="Grant Privileges to Role:", font = ('Arial', 12, 'bold')).grid(row=2, column = 0, sticky = "w", padx = 5, pady = 10)

tk.Label(root, text="Role Name:").grid(row=3, column=0, sticky="w", padx=5)
privilege_role_entry.grid(row=3, column=1, padx=5, pady=2)

tk.Label(root, text="Privileges:").grid(row=4, column=0, sticky="w", padx=5)

privilege_var.set(privileges[0])  # default value
privilege_dropdown.grid(row=4, column=1, padx=5, pady=2)

tk.Label(root, text="Table Name:").grid(row=5, column=0, sticky="w", padx=5)
table_entry.grid(row=5, column=1, padx=5, pady=2)

tk.Button(root, text="Grant Privileges", command=grant_privileges_gui).grid(row=5, column=2, padx=5, pady=2)




tk.Label(root, text="Grant Role to User:", font = ('Arial', 12, 'bold')).grid(row=6, column = 0, sticky = "w", padx = 5, pady = 10)

tk.Label(root, text="Role Name:").grid(row=7, column=0, sticky="w", padx=5)
grant_role_entry.grid(row=7, column=1, padx=5, pady=2)

tk.Label(root, text="User Name:").grid(row=8, column=0, sticky="w", padx=5)
user_entry.grid(row=8, column=1, padx=5, pady=2)

tk.Button(root, text="Grant Role to User", command=grant_role_to_user_gui).grid(row=8, column=2, padx=5, pady=2)
    
root.mainloop()
