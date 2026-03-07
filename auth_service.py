from sqlalchemy.orm import Session
from database import SessionLocal
from models import User, Permission


def get_all_users():
    db: Session = SessionLocal()
    users = db.query(User).all()
    db.close()

    return [{"email": u.email, "role": u.role} for u in users]


def add_user(email, role):
    db: Session = SessionLocal()

    user = db.query(User).filter(User.email == email).first()

    if not user:
        db.add(User(email=email, role=role))
        db.commit()

    db.close()


def update_user_role(email, role):
    db: Session = SessionLocal()

    user = db.query(User).filter(User.email == email).first()

    if user:
        user.role = role
        db.commit()

    db.close()


def delete_user(email):
    db: Session = SessionLocal()

    user = db.query(User).filter(User.email == email).first()

    if user:
        db.delete(user)
        db.commit()

    db.close()


def count_admins():
    db: Session = SessionLocal()

    count = db.query(User).filter(User.role == "admin").count()

    db.close()

    return count