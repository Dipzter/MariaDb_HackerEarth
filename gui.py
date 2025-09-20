import tkinter as tk
from tkinter import messagebox
from database import create_role, grant_privileges, grant_role_to_user

root = tk.Tk()
root.title("MariaDB Role Manager")

