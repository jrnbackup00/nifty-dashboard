import sqlite3

DB_NAME = "auth.db"


# -----------------------------------
# Ensure database + tables exist
# -----------------------------------
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            email TEXT PRIMARY KEY,
            role TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS permissions (
            email TEXT,
            page TEXT,
            PRIMARY KEY (email, page)
        )
    """)

    conn.commit()
    conn.close()


# -----------------------------------
# Helper: always ensure tables exist
# -----------------------------------
def get_connection():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            email TEXT PRIMARY KEY,
            role TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS permissions (
            email TEXT,
            page TEXT,
            PRIMARY KEY (email, page)
        )
    """)

    conn.commit()

    return conn


# -----------------------------------
# User Management
# -----------------------------------
def add_user(email, role="viewer"):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "INSERT OR IGNORE INTO users (email, role) VALUES (?, ?)",
        (email, role)
    )

    conn.commit()
    conn.close()


def get_user(email):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT email, role FROM users WHERE email = ?",
        (email,)
    )

    row = cursor.fetchone()
    conn.close()

    if row:
        return {"email": row[0], "role": row[1]}

    return None


def get_all_users():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT email, role FROM users")
    rows = cursor.fetchall()

    conn.close()

    return [{"email": r[0], "role": r[1]} for r in rows]


def update_user_role(email, role):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "UPDATE users SET role = ? WHERE email = ?",
        (role, email)
    )

    conn.commit()
    conn.close()


def delete_user(email):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "DELETE FROM users WHERE email = ?",
        (email,)
    )

    conn.commit()
    conn.close()


def count_admins():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT COUNT(*) FROM users WHERE role = 'admin'"
    )

    count = cursor.fetchone()[0]

    conn.close()
    return count


# -----------------------------------
# Permissions
# -----------------------------------
def add_permission(email, page):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "INSERT OR IGNORE INTO permissions (email, page) VALUES (?, ?)",
        (email, page)
    )

    conn.commit()
    conn.close()


def has_permission(email, page):

    user = get_user(email)

    if user and user["role"] == "admin":
        return True

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT 1 FROM permissions WHERE email=? AND page=?",
        (email, page)
    )

    result = cursor.fetchone()

    conn.close()

    return result is not None