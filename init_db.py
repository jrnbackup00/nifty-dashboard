from database import engine
from models import Base, SymbolGroupMap

def init_db():
    Base.metadata.create_all(bind=engine)