from fastapi import FastAPI, HTTPException, Depends, Header, Query, status
from sqlalchemy import Table, create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.orm import declarative_base
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import  Literal, Optional
from datetime import datetime, timedelta
import boto3

#--------- Constants -------------
schema='genpact'
SQLALCHEMY_DATABASE_URL = "postgresql://postgres:postgres123@d1.c1mggvnkwauf.us-east-1.rds.amazonaws.com:5432/demo"
success_message = "Request processed successfully "

#----------- DB schema & connection -------------
# SQLAlchemy setup
def setup_db():
    engine = create_engine(SQLALCHEMY_DATABASE_URL,pool_pre_ping=True,pool_recycle=3600)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal, engine

Base = declarative_base()
SessionLocal, engine  = setup_db()

# Dependency to get the database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Define SQLAlchemy models
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

# ---------- Utilities --------------------
def row2dict(row):
    return {column.name: getattr(row, column.name) for column in row.__table__.columns}

def generate_time_slots(start_time, end_time, duration = 30):
    # Convert start and end time strings to datetime objects
    start = datetime.strptime(str(start_time), '%H:%M:%S')
    end = datetime.strptime(str(end_time), '%H:%M:%S')

    # Initialize list to store time slots
    time_slots = []

    # Generate time slots in (duration)30-minute intervals
    current_time = start
    while current_time < end:
        next_time = current_time + timedelta(minutes=duration)
        time_slots.append((current_time.strftime('%H:%M'), next_time.strftime('%H:%M')))
        current_time = next_time

    return time_slots

def make_contact_visible(date, start_time):
    if date != datetime.now().date().strftime('%Y-%m-%d'):
        return False
    
    # Assuming appointment.from_time is in the format HH:MM
    appointment_from_time = datetime.strptime(str(start_time), '%H:%M:%S')

    # Calculate 30 minutes before and after the appointment start time
    thirty_minutes_before = appointment_from_time - timedelta(minutes=30)
    thirty_minutes_after = appointment_from_time + timedelta(minutes=30)

    # Get the current time
    current_time = datetime.now().time()

    # Check if the current time is within the range
    if thirty_minutes_before.time() <= current_time <= thirty_minutes_after.time():
        return True
    else:
        return False
    
#------------ Pydantics Models -----------------

class ProductSchema(BaseModel):
    name: str
    category: str

class AgentSchema(BaseModel):
    first_name: str
    last_name: str
    date_of_joining: datetime

class AgentScheduleSchema(BaseModel):
    agent_id: int
    call_name: str
    date: str
    shift_from: str
    shift_to: str
    alloted_product_id: int
    slot_duration_mins: int

class FeedbackSchema(BaseModel):
    appointment_id: int
    rating: int

class CustomerSchema(BaseModel):    
    username: str   
    created_at : Optional[datetime] = str(datetime.now()) # type: ignore
    email_id: str
    mobile_no: str

class AppointmentSchema(BaseModel):
    customer_id: int
    schedule_id: int
    from_time: str
    to_time: str
    apt_details :Optional[str] = ""

class TriggerCallSchema(BaseModel):
    appointment_id: int

class ResponseModel(BaseModel):
    message: str
    payload : Optional[dict] = {}

#---------- API endpoints -------------
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Replace with specific allowed origins
    allow_methods=["*"],  # Replace with specific HTTP methods
    allow_headers=["*"],  # Replace with specific headers
)


### ------------- Customer Endpoints -----------
# Get Agents List for a selected product
@app.get("/customer/{product_id}/agents-list/",  response_model=ResponseModel, tags=["customer"])
async def get_schedules_overview(product_id: int, user_id: int = Header(), db: Session = Depends(get_db)):

    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User ID not provided")
    
    if not db.query(Customer).filter(Customer.id == user_id).first():
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid User ID")
    
    if not db.query(Product).filter(Product.id == product_id).first():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")
    
    agents = db.query(AgentSchedule).filter(AgentSchedule.alloted_product_id == product_id, AgentSchedule.date >= datetime.now().strftime("%Y-%m-%d")).all()
    agents_dict = []

    # agents = [row2dict(agent) for agent in agents]
    for agent in agents:
        agent = row2dict(agent)
        agent.pop("alloted_product_id")
        agent.pop("slot_duration_mins")
        agents_dict.append(agent)

    return ResponseModel(message=success_message, payload={"agents":agents_dict })



@app.get(path="/slots/{product_id}/{date}",  response_model=ResponseModel, tags=["customer","agent"])
async def get_agent_schedules(product_id: int, date: str, type:Literal["customer","agent"]=Header(), agent_id:int = Query(default=None), db: Session = Depends(get_db)):

    if not db.query(Product).filter(Product.id == product_id).first():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")
    
    if type=="agent" :
        if not agent_id :
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Agent id not provided")
        elif not db.query(Agent).filter(Agent.id == agent_id).first():
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Agent not found")

    if not agent_id:
        schedules = db.query(AgentSchedule).filter(AgentSchedule.alloted_product_id == product_id, AgentSchedule.date == date).all()
    else:
        schedules = db.query(AgentSchedule).filter(AgentSchedule.alloted_product_id == product_id,  AgentSchedule.date == date, AgentSchedule.agent_id == agent_id).all()

    response = []
    for schedule in schedules:        
        time_slots = generate_time_slots(schedule.shift_from, schedule.shift_to, schedule.slot_duration_mins)        
       
        for start_time,end_time in time_slots:
            record = {}
            record["id"] = schedule.id
            record["agent_name"] = schedule.call_name
            record["date"] = schedule.date
            record["start"] = start_time
            record["end"] = end_time

            # Get Booked slots from Appointment DB     
            appointment   =  db.query(Appointment).filter(Appointment.schedule_id == schedule.id, Appointment.from_time == start_time).first()
            if appointment:
                record ['is_booked'] = True
                if type == "agent":
                    customer = db.query(Customer).filter(Customer.id == appointment.customer_id).first()
                    if  customer:
                        record["customer_name"]  = customer.username                
                        record["details"] = appointment.apt_details
                        record["call_status"] = appointment.call_status
                        if appointment.call_status == "pending" and make_contact_visible(schedule.date, appointment.from_time) : 
                            record["contact"] = customer.mobile_no
                        if appointment.call_status == "completed":
                            record["call_rating"] = appointment.call_rating                            
                        record["agent_comments"] = appointment.agent_comments
            else:
                record['is_booked'] = False

            if type != "agent" or (type=="agent" and  record['is_booked']):
                response.append(record)       
                
    return ResponseModel(message=success_message, payload={"slots":response})


@app.get("/products/", response_model=ResponseModel, tags=["customer"])
async def get_products(db: Session = Depends(get_db)):
    return ResponseModel(message=success_message, payload={"products":[row2dict(product) for product in db.query(Product).all()]})

@app.get("/products/{product_id}/agents/", response_model=ResponseModel,tags=["customer"])
async def get_agents_for_product(product_id: int, db: Session = Depends(get_db)):
    return ResponseModel(message=success_message, payload={"products":[row2dict(product) for product in db.query(Agent).join(Agent.schedules).filter(AgentSchedule.alloted_product_id == product_id).all()]})


@app.post(path="/customer/create", response_model=ResponseModel, tags=["customer"])
async def create_customer(customer: CustomerSchema, db: Session = Depends(get_db)):
    new_customer = Customer(**customer.dict())
    db.add(new_customer)
    db.commit()
    db.refresh(new_customer)
    return ResponseModel(message=success_message, payload={"customer_id":new_customer.id})


@app.post("/customer/book_appointment/", response_model=ResponseModel, tags=["customer"])
async def book_appointment(appointment: AppointmentSchema, db: Session = Depends(get_db)):

    if not db.query(Customer).filter(Customer.id == appointment.customer_id).first():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Please register contact details before scheduling an appointment")

    if not db.query(AgentSchedule).filter(AgentSchedule.id == appointment.schedule_id).first():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Invalid Appointment slot")
    
    appointment = Appointment(**appointment.dict())
    db.add(appointment)
    db.commit()
    db.refresh(appointment)
    return ResponseModel(message=success_message, payload={"appointment_id":appointment.id})


# Give feedback
@app.put("/appointments/{appointment_id}/feedback/", response_model=ResponseModel, tags=["customer"])
async def record_feedback(feedback:  FeedbackSchema, db: Session = Depends(get_db)):

    appointment = db.query(Appointment).filter(Appointment.id == feedback.appointment_id).first()
    if not appointment:
        raise HTTPException(status_code=404, detail="Appointment not found")
    appointment.call_rating = feedback.rating
    db.commit()
    db.refresh(appointment)

    return ResponseModel(message=success_message)

@app.get("/agents/list", response_model=ResponseModel, tags=["agent"])
async def get_agents(db: Session = Depends(get_db)):
    return ResponseModel(message=success_message, payload={"agents":[row2dict(agent) for agent in db.query(Agent).all()]})

@app.post("/agent/add-schedule/", response_model=ResponseModel, tags=["agent"])
async def add_schedule(schedule: AgentScheduleSchema, db: Session = Depends(get_db)):

    if not db.query(Product).filter(Product.id == schedule.alloted_product_id).first():
        raise HTTPException(status_code=404, detail="Product not found")

    schedule = AgentSchedule(**schedule.dict())
    db.add(schedule)
    db.commit()
    db.refresh(schedule)
    return ResponseModel(message=success_message, payload={"schedule_id":schedule.id})

@app.post("/agent/send-reminder/{appointment_id}", response_model=ResponseModel, tags=["agent"])
async def send_reminder(appointment_id: int, db: Session = Depends(get_db)):

    appointment = db.query(Appointment).filter(Appointment.id == appointment_id).first()
    if not appointment:
        raise HTTPException(status_code=404, detail="Appointment not found")

    if appointment.call_status != "pending":
        return ResponseModel(message="Appointment already completed")

    customer = db.query(Customer).filter(Customer.id == appointment.customer_id).first()
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")
    
    # Initialize AWS SDK clients
    ses_client = boto3.client('ses')
    # Send reminders via email using Amazon SES
    email_subject = "Genpact Demo: Appointment Reminder"
    email_body = f"Hi {customer.username},\n\nThis is a reminder for your appointment scheduled at {appointment.from_time} to {appointment.to_time}.\n\nRegards,\nYour Genpact Appointment Booking System"

    # Replace with customer's email address
    to_email = customer.email_id

    # Send email reminder
    ses_client.send_email(
        Source="noreply@demo.com",  # Replace with your verified SES sender email
        Destination={'ToAddresses': [to_email]},
        Message={
            'Subject': {'Data': email_subject},
            'Body': {'Text': {'Data': email_body}}
        })
    
    return ResponseModel(message=success_message)

@app.post("/agent/trigger-call/", response_model=ResponseModel, tags=["agent"])
async def trigger_call(appointment_id: int, db: Session = Depends(get_db)):
    
    appointment = db.query(Appointment).filter(Appointment.id == appointment_id).first()
    
    if not appointment:
        raise HTTPException(status_code=404, detail="Appointment not found")
    
    customer = db.query(Customer).filter(Customer.id == appointment.customer_id).first()
    if not customer:
        raise HTTPException(status_code=404, detail="Appointment booked for invalid customer")
    
    # Trigger calls via AWS Connector
    connector_client = boto3.client('connect')

    # Placeholder logic to trigger calls
    response = connector_client.start_outbound_voice_contact(
        DestinationPhoneNumber=customer.mobile_no,
        ContactFlowId='CONTACT_FLOW_ID',
        InstanceId='CONNECT_INSTANCE_ID'
    )
    print(response)

    # Update the appointment status
    appointment.status = "completed"
    db.commit()
    db.refresh(appointment)

    # Implement the logic to trigger the call to the end-user    
    return {"message": "Call triggered successfully"}

#---------- Run the server -------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
