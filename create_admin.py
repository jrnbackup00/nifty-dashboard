from user_service import create_user

email = "jrnbackup00@gmail.com"

user = create_user(
    email=email,
    role="admin",
    plan_type="institutional"
)

print("Admin created:", user.email)