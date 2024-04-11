import os
from contextlib import contextmanager
from sqlalchemy import create_engine,Table 
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from sqlalchemy import inspect
from sqlalchemy.orm import declarative_base
# Load environment variables from the .env file
load_dotenv()

# Get the DATABASE_URL from the environment variables
schema='genpact'

db_url = f'postgresql://postgres:postgres123@d1.c1mggvnkwauf.us-east-1.rds.amazonaws.com:5432/demo'

# Set the default schema name
default_schema = 'genpact'

# Create the engine with the default schema
engine = create_engine(db_url,connect_args={'options': f'-c search_path={default_schema}'})
#engine = create_engine(db_url)
Base = declarative_base()
inspector = inspect(engine)
schemas = inspector.get_schema_names()

class Agent(Base):
    __table__ = Table('agent', Base.metadata, schema=schema,autoload_with=engine)

class Customer(Base):
    __table__ = Table('customer', Base.metadata, schema=schema,autoload_with=engine)

class Product(Base):
    __table__ = Table('products', Base.metadata, schema=schema,autoload_with=engine)

class AgentSchedule(Base):
    __table__ = Table('agent_schedule', Base.metadata, schema=schema,autoload_with=engine)
    
class Appointment(Base):
    __table__ = Table('appointment', Base.metadata, schema=schema,autoload_with=engine)

# Print the list of schemas
#print(schemas)
LocalSession = sessionmaker(bind=engine)


@contextmanager
def get_session(): 
    session = LocalSession()
    try:
        yield session
    finally:
        session.close()


def session(func):
    def wrapper(*args, **kwargs):
        with get_session() as session:
            return func(session, *args, **kwargs)
    return wrapper