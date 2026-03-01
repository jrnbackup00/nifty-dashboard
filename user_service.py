from sqlalchemy.orm import Session
from models import User
from database import SessionLocal


def get_user_by_email(email: str):
    db: Session = SessionLocal()
    try:
        return db.query(User).filter(User.email == email).first()
    finally:
        db.close()


def create_user(email: str, role="viewer", plan_type="free"):
    db: Session = SessionLocal()
    try:
        user = User(
            email=email,
            role=role,
            plan_type=plan_type
        )
        db.add(user)
        db.commit()
        db.refresh(user)
        return user
    finally:
        db.close()