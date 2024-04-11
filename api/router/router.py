from fastapi import FastAPI, HTTPException, Depends, Header, Query, status, APIRouter
from sqlalchemy import Table, create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.orm import declarative_base
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import  Literal, Optional
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
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



# class AgentSchema(BaseModel):
#     first_name: str
#     last_name: str
#     date_of_joining: datetime

class AgentSchema(BaseModel):
    full_name: str
    date_of_joining: datetime
    working_on_sat: bool
    working_on_sun: bool
    leave_from: Optional[datetime] = None
    leave_to: Optional[datetime] = None
    slot_time: int
    buffer_time: int

class FeedbackSchema(BaseModel):
    appointment_id: int
    rating: int

class CustomerSchema(BaseModel):    
    username: str   
    created_at : Optional[datetime] = str(datetime.now()) # type: ignore
    email_id: str
    mobile_no: str
    product_id : int

class ProductSchema(BaseModel):
    name: str
    created_at : Optional[datetime] = str(datetime.now()) # type: ignore
    category : str

class AppointmentSchema(BaseModel):
    customer_id: int
    call_status: str = None
    call_rating:int =None
    agent_id:int
    created_at: Optional[datetime]  = None
    is_booked: bool =None
    appointment_desc: str
    appointment_at: Optional[datetime]


class TriggerCallSchema(BaseModel):
    appointment_id: int

class ResponseModel(BaseModel):
    message: str
    payload : Optional[dict] = {}

#---------- API endpoints -------------
app = APIRouter()

# #----------------- Product Endpoints------------
# @app.get("/products/", response_model=ResponseModel, tags=["products"])
# async def get_products(db: Session = Depends(get_db)):
#     return ResponseModel(message=success_message, payload={"products":[row2dict(product) for product in db.query(Product).all()]})

# ### ------------- Customer Endpoints -----------
# # Get Agents List for a selected product
# @app.get("/customer/{product_id}/agents-list/",  response_model=ResponseModel, tags=["customer"])
# async def get_schedules_overview(product_id: int, user_id: int = Header(), db: Session = Depends(get_db)):

#     if not user_id:
#         raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User ID not provided")
    
#     if not db.query(Customer).filter(Customer.id == user_id).first():
#         raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid User ID")
    
#     if not db.query(Product).filter(Product.id == product_id).first():
#         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")
    
#     agents = db.query(AgentSchedule).filter(AgentSchedule.alloted_product_id == product_id, AgentSchedule.date >= datetime.now().strftime("%Y-%m-%d")).all()
#     agents_dict = []

#     # agents = [row2dict(agent) for agent in agents]
#     for agent in agents:
#         agent = row2dict(agent)
#         agent.pop("alloted_product_id")
#         agent.pop("slot_duration_mins")
#         agents_dict.append(agent)

#     return ResponseModel(message=success_message, payload={"agents":agents_dict })


# @app.get("/products/{product_id}/agents/", response_model=ResponseModel,tags=["customer"])
# async def get_agents_for_product(product_id: int, db: Session = Depends(get_db)):
#     return ResponseModel(message=success_message, payload={"products":[row2dict(product) for product in db.query(Agent).join(Agent.schedules).filter(AgentSchedule.alloted_product_id == product_id).all()]})
def send_email(email, user_id, product_id):
    # Initialize AWS SES client
    ses_client = boto3.client('ses', region_name='your_aws_region')

    # Generate appointment link
    appointment_link = book_appointment(user_id, product_id)["localhost_link"]

    # Define email parameters
    sender_email = 'your_sender_email@example.com'
    subject = 'Welcome to Our Service'
    body_text = f'Thank you for signing up! Click the link below to book your appointment:\n\n{appointment_link}'
    body_html = f'<html><body><h1>Thank you for signing up!</h1><p>Click the link below to book your appointment:</p><p><a href="{appointment_link}">Book Appointment</a></p></body></html>'

    # Send email
    response = ses_client.send_email(
        Destination={'ToAddresses': [email]},
        Message={
            'Body': {
                'Html': {'Charset': 'UTF-8', 'Data': body_html},
                'Text': {'Charset': 'UTF-8', 'Data': body_text}
            },
            'Subject': {'Charset': 'UTF-8', 'Data': subject}
        },
        Source=sender_email
    )

    print("Email sent to:", email)

def book_appointment(customer_id: int, product_id: int):
    localhost_link = f"http://localhost:3000/customer/bookAppointment?customer_id={customer_id}&product_id={product_id}"
    return {"localhost_link": localhost_link}

@app.post(path="/customer/create", response_model=ResponseModel, tags=["customer"])
async def create_customer(customer: CustomerSchema, db: Session = Depends(get_db)):
    new_customer = Customer(**customer.dict())
    db.add(new_customer)
    db.commit()
    db.refresh(new_customer)
    # send_email(email, user_id, product_id)
    return ResponseModel(message=success_message, payload={"customer_id":new_customer.id})

@app.post(path="/product/create", response_model=ResponseModel, tags=["product"])
async def create_product(product: ProductSchema, db: Session = Depends(get_db)):
    try:
        new_product = Product(**product.dict())
        db.add(new_product)
        db.commit()
        db.refresh(new_product)
        return ResponseModel(message=success_message, payload={"product_id":new_product.id})
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    

@app.post(path="/agent/create", response_model=ResponseModel, tags=["agent"])
async def create_agent(agent: AgentSchema, db: Session = Depends(get_db)):
    try:
        new_agent = Agent(**agent.dict())
        db.add(new_agent)
        db.commit()
        db.refresh(new_agent)
        return ResponseModel(message=success_message, payload={"agent_id":new_agent.id})
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    
@app.post(path="/appointment/create", response_model=ResponseModel, tags=["appointment"])
async def create_appointment(appointment: AppointmentSchema, db: Session = Depends(get_db)):
    try:
        new_appointment = Appointment(**appointment.dict())
        db.add(new_appointment)
        db.commit()
        db.refresh(new_appointment)
        return ResponseModel(message=success_message, payload={"appointment_id":new_appointment.id})
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    


# @app.post("/customer/book_appointment/", response_model=ResponseModel, tags=["customer"])
# async def book_appointment(appointment: AppointmentSchema, db: Session = Depends(get_db)):

#     if not db.query(Customer).filter(Customer.id == appointment.customer_id).first():
#         raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Please register contact details before scheduling an appointment")

#     if not db.query(AgentSchedule).filter(AgentSchedule.id == appointment.schedule_id).first():
#         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Invalid Appointment slot")
    
#     appointment = Appointment(**appointment.dict())
#     db.add(appointment)
#     db.commit()
#     db.refresh(appointment)
#     return ResponseModel(message=success_message, payload={"appointment_id":appointment.id})


# # # Give feedback
# # @app.put("/appointments/{appointment_id}/feedback/", response_model=ResponseModel, tags=["customer"])
# # async def record_feedback(feedback:  FeedbackSchema, db: Session = Depends(get_db)):

# #     appointment = db.query(Appointment).filter(Appointment.id == feedback.appointment_id).first()
# #     if not appointment:
# #         raise HTTPException(status_code=404, detail="Appointment not found")
# #     appointment.call_rating = feedback.rating
# #     db.commit()
# #     db.refresh(appointment)

# #     return ResponseModel(message=success_message)
# # API to get data from a specific table
# @app.get("/customer/", response_model=ResponseModel, tags=["customer"])
# async def get_all_customers(db: Session = Depends(get_db)):

#     return ResponseModel(message=success_message, payload={"customers":[row2dict(product) for product in db.query(Customer).all()]})


# #-----------------------------------------------agent-------------------------------------------------------------------#
@app.get("/agents/list", response_model=ResponseModel, tags=["agent"])
async def get_agents(db: Session = Depends(get_db)):
    return ResponseModel(message=success_message, payload={"agents":[row2dict(agent) for agent in db.query(Agent).all()]})

# @app.post("/agent/add-schedule/", response_model=ResponseModel, tags=["agent"])
# async def add_schedule(schedule: AgentScheduleSchema, db: Session = Depends(get_db)):

#     if not db.query(Product).filter(Product.id == schedule.alloted_product_id).first():
#         raise HTTPException(status_code=404, detail="Product not found")

#     schedule = AgentSchedule(**schedule.dict())
#     db.add(schedule)
#     db.commit()
#     db.refresh(schedule)
#     return ResponseModel(message=success_message, payload={"schedule_id":schedule.id})

def format_db_response(result):
    result_dict = []
    for item in result:
        item_dict = item.__dict__
        # Remove the attribute holding the reference to the database session
        item_dict.pop('_sa_instance_state', None)
        result_dict.append(item_dict)
    return result_dict
import random
@app.get(path="/slots/{product_id}/{date}",  tags=["customer","agent"])
async def get_agent_schedules(product_id: int, date: str, type:Literal["customer","agent"]=Header(), agent_id:int = Query(default=None), db: Session = Depends(get_db)):
    
    Agents = db.query(Agent).filter(Agent.product_id == product_id).all()
    if not Agents:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")
    print(Agents)
    # agent_ids = db.query(Agent.id).filter(Agent.product_id == product_id).all()
    Agents = format_db_response(Agents)
    agents=[]
    for i in Agents:
        agents.append(i["id"])
    avail_id  = random.choice(agents)
    return avail_id
    # if not db.query(Product).filter(Product.id == product_id).first():
    #     raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")
    
    # if type=="agent" :
    #     if not agent_id :
    #         raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Agent id not provided")
    #     elif not db.query(Agent).filter(Agent.id == agent_id).first():
    #         raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Agent not found")

    # if not agent_id:
    #     schedules = db.query(AgentSchedule).filter(AgentSchedule.alloted_product_id == product_id, AgentSchedule.date == date).all()
    # else:
    #     schedules = db.query(AgentSchedule).filter(AgentSchedule.alloted_product_id == product_id,  AgentSchedule.date == date, AgentSchedule.agent_id == agent_id).all()

    # response = []
    # for schedule in schedules:        
    #     time_slots = generate_time_slots(schedule.shift_from, schedule.shift_to, schedule.slot_duration_mins)        
       
    #     for start_time,end_time in time_slots:
    #         record = {}
    #         record["id"] = schedule.id
    #         record["agent_name"] = schedule.call_name
    #         record["date"] = schedule.date
    #         record["start"] = start_time
    #         record["end"] = end_time

    #         # Get Booked slots from Appointment DB     
    #         appointment   =  db.query(Appointment).filter(Appointment.schedule_id == schedule.id, Appointment.from_time == start_time).first()
    #         if appointment:
    #             record ['is_booked'] = True
    #             if type == "agent":
    #                 customer = db.query(Customer).filter(Customer.id == appointment.customer_id).first()
    #                 if  customer:
    #                     record["customer_name"]  = customer.username                
    #                     record["details"] = appointment.apt_details
    #                     record["call_status"] = appointment.call_status
    #                     if appointment.call_status == "pending" and make_contact_visible(schedule.date, appointment.from_time) : 
    #                         record["contact"] = customer.mobile_no
    #                     if appointment.call_status == "completed":
    #                         record["call_rating"] = appointment.call_rating                            
    #                     record["agent_comments"] = appointment.agent_comments
    #         else:
    #             record['is_booked'] = False

    #         if type != "agent" or (type=="agent" and  record['is_booked']):
    #             response.append(record)       
                
    # return ResponseModel(message=success_message, payload={"slots":response})



# @app.post("/agent/send-reminder/{appointment_id}", response_model=ResponseModel, tags=["agent"])
# async def send_reminder(appointment_id: int, db: Session = Depends(get_db)):

#     appointment = db.query(Appointment).filter(Appointment.id == appointment_id).first()
#     if not appointment:
#         raise HTTPException(status_code=404, detail="Appointment not found")

#     if appointment.call_status != "pending":
#         return ResponseModel(message="Appointment already completed")

#     customer = db.query(Customer).filter(Customer.id == appointment.customer_id).first()
#     if not customer:
#         raise HTTPException(status_code=404, detail="Customer not found")
    
#     # Initialize AWS SDK clients
#     ses_client = boto3.client('ses')
#     # Send reminders via email using Amazon SES
#     email_subject = "Genpact Demo: Appointment Reminder"
#     email_body = f"Hi {customer.username},\n\nThis is a reminder for your appointment scheduled at {appointment.from_time} to {appointment.to_time}.\n\nRegards,\nYour Genpact Appointment Booking System"

#     # Replace with customer's email address
#     to_email = customer.email_id

#     # Send email reminder
#     ses_client.send_email(
#         Source="noreply@demo.com",  # Replace with your verified SES sender email
#         Destination={'ToAddresses': [to_email]},
#         Message={
#             'Subject': {'Data': email_subject},
#             'Body': {'Text': {'Data': email_body}}
#         })
    
#     return ResponseModel(message=success_message)

# @app.post("/agent/trigger-call/", response_model=ResponseModel, tags=["agent"])
# async def trigger_call(appointment_id: int, db: Session = Depends(get_db)):
    
#     appointment = db.query(Appointment).filter(Appointment.id == appointment_id).first()
    
#     if not appointment:
#         raise HTTPException(status_code=404, detail="Appointment not found")
    
#     customer = db.query(Customer).filter(Customer.id == appointment.customer_id).first()
#     if not customer:
#         raise HTTPException(status_code=404, detail="Appointment booked for invalid customer")
    
#     # Trigger calls via AWS Connector
#     connector_client = boto3.client('connect')

#     # Placeholder logic to trigger calls
#     response = connector_client.start_outbound_voice_contact(
#         DestinationPhoneNumber=customer.mobile_no,
#         ContactFlowId='CONTACT_FLOW_ID',
#         InstanceId='CONNECT_INSTANCE_ID'
#     )
#     print(response)

#     # Update the appointment status
#     appointment.status = "completed"
#     db.commit()
#     db.refresh(appointment)

#     # Implement the logic to trigger the call to the end-user    
#     return {"message": "Call triggered successfully"}

# #------------test db ------------------#
# @app.get("/tables/", response_model=ResponseModel)
# async def get_tables(db: Session = Depends(get_db)):
#     tables = Base.metadata.tables.keys()
#     return ResponseModel(message="List of tables", payload={"tables": list(tables)})


@app.post("/cancel_appointment/",tags=['appointment'])
async def cancel_appointment_route(customer_id: int,db: Session = Depends(get_db)):
    # Create session

    
    try:
        # Execute SQL query to delete appointment
        query = text("DELETE FROM genpact.appointment WHERE customer_id = :customer_id")
        db.execute(query, {"customer_id": customer_id})
        
        # Commit transaction
        db.commit()
        
        # Return success message
        return {"message": "Appointment canceled successfully."}
    except Exception as e:
        # Rollback transaction in case of error
        db.rollback()
        
        # Raise HTTPException with error message
        raise HTTPException(status_code=500, detail=f"Error canceling appointment: {str(e)}")
    finally:
        # Close session
        db.close()


@app.get("/userDetail/{customer_id}",tags=['customer'])
async def get_user_detail(customer_id: int,db: Session = Depends(get_db)):
    # Create session

    
    try:
        # Execute SQL query to fetch customer details
        query = text("SELECT * FROM genpact.customer WHERE id = :customer_id")
        result = db.execute(query, {"customer_id": customer_id})
        
        # Fetch the row
        user_detail = result.fetchone()
        
        if user_detail is None:
            # If no user found with the provided ID, raise HTTPException
            raise HTTPException(status_code=404, detail="User not found")
        
        # Get column names
        columns = result.keys()
        
        # Create dictionary from row
        user_detail_dict = {col: value for col, value in zip(columns, user_detail)}
        
        return user_detail_dict
    except Exception as e:
        # Rollback transaction in case of error
        db.rollback()
        
        # Raise HTTPException with error message
        raise HTTPException(status_code=500, detail=f"Error retrieving user detail: {str(e)}")
    finally:
        # Close session
        db.close()

@app.get("/appointments/{customer_id}",tags=['appointment'])
def get_appointments(customer_id: int,db: Session = Depends(get_db)):
    # Create session
    try:
        appointments = db.query(Appointment).filter(Appointment.customer_id == customer_id).all()
        # schedules = db.query(AgentSchedule).filter(AgentSchedule.agent_id == 4).first()
        if not appointments:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")
        print(appointments)
        # agent_ids = db.query(Agent.id).filter(Agent.product_id == product_id).all()
        appointments = format_db_response(appointments)

        new_data=[]
        print(appointments,"\n\n")
        for i in appointments:
            data = i 
            agent_id = i['agent_id']
            start_time = i["scheduled_at"]
            print(start_time,agent_id)
            schedules = db.query(AgentSchedule).filter(AgentSchedule.agent_id == 4).first()
            query = text("select * from genpact.agent_schedule where agent_id= :agent_id ")
            if not schedules:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")
            item_dict = schedules.__dict__
            # Remove the attribute holding the reference to the database session
            item_dict.pop('_sa_instance_state', None)
            
            print(item_dict)
            result =  data|item_dict
            new_data.append(result)

        return new_data
    
    except Exception as e:
        # Rollback transaction in case of error
        db.rollback()
        
        # Raise HTTPException with error message
        raise HTTPException(status_code=200, detail=f"No record found - {e}")
    finally:
        # Close session
        db.close()

@app.get("/appointments/{agent_id}",tags=['appointment'])
def get_booked_agent_schedule(agent_id:int,db: Session = Depends(get_db)):
    # Define database connection URL
    
    # Build the SQL query to select agent schedules for the given agent ID with status "booked"
    query = text("""
        SELECT * FROM genpact.agent_schedule 
        WHERE agent_id = :agent_id 
        AND status = 'booked'
    """)

    # Execute the query
    result = db.execute(query, {"agent_id": agent_id})
    
    # Fetch all the rows
    booked_schedules = result.fetchall()
    
    # Close the connection
    db.close()
    
    return booked_schedules