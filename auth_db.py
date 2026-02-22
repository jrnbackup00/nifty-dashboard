import sqlite3

DB_NAME = "auth.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            email TEXT PRIMARY KEY,
            role TEXT NOT NULL
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS permissions (
            email TEXT,
            page TEXT,
            PRIMARY KEY (email, page)
        )
    """)

    conn.commit()
    conn.close()


def add_user(email, role="user"):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO users (email, role) VALUES (?, ?)", (email, role))
    conn.commit()
    conn.close()


def add_permission(email, page):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO permissions (email, page) VALUES (?, ?)", (email, page))
    conn.commit()
    conn.close()


def get_user(email):
    conn = sqlite3.connect("auth.db")
    cursor = conn.cursor()

    cursor.execute("SELECT email, role FROM users WHERE email = ?", (email,))
    row = cursor.fetchone()
    conn.close()

    if row:
        return {
            "email": row[0],
            "role": row[1]
        }

    return None


def has_permission(email, page):
    role = get_user(email)

    if role == "admin":
        return True

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT 1 FROM permissions WHERE email=? AND page=?", (email, page))
    result = c.fetchone()
    conn.close()

    return result is not None

def get_all_users():
    conn = sqlite3.connect("auth.db")
    cursor = conn.cursor()
    cursor.execute("SELECT email, role FROM users")
    users = cursor.fetchall()
    conn.close()

    return [{"email": row[0], "role": row[1]} for row in users]

def add_user(email, role):
    conn = sqlite3.connect("auth.db")
    cursor = conn.cursor()

    cursor.execute(
        "INSERT OR IGNORE INTO users (email, role) VALUES (?, ?)",
        (email, role)
    )

    conn.commit()
    conn.close()

def update_user_role(email, role):
    conn = sqlite3.connect("auth.db")
    cursor = conn.cursor()

    cursor.execute(
        "UPDATE users SET role = ? WHERE email = ?",
        (role, email)
    )

    conn.commit()
    conn.close()

def delete_user(email):
    conn = sqlite3.connect("auth.db")
    cursor = conn.cursor()

    cursor.execute(
        "DELETE FROM users WHERE email = ?",
        (email,)
    )

    conn.commit()
    conn.close()

def count_admins():
    conn = sqlite3.connect("auth.db")
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM users WHERE role = 'admin'")
    count = cursor.fetchone()[0]

    conn.close()
    return count