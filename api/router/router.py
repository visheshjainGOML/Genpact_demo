import random
from fastapi import FastAPI, HTTPException, Depends, Header, Query, status, APIRouter
# from grpc import StatusCode
from sqlalchemy import Table, create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.orm import declarative_base
from datetime import datetime
import uuid
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Literal, Optional
from sqlalchemy import text
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os   
from datetime import datetime, time
import pytz
from icalendar import Calendar, Event
from icalendar import Event as CalEvent
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication


load_dotenv()

# --------- Constants -------------
schema = 'genpact'
SQLALCHEMY_DATABASE_URL = os.getenv('POSTGRES_URL')
success_message = "Request processed successfully "

# ----------- DB schema & connection -------------
# SQLAlchemy setup

AWS_ACCESS_KEY=os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY=os.getenv('AWS_SECRET_KEY')
AWS_REGION=os.getenv('AWS_REGION')


client = boto3.client(
    'ses',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

def send_email(sender, recipient, subject, body, start_time=None, end_time=None, date=None):
    # Check if date, start time, and end time are provided
    if start_time and end_time and date:
        ics_content = create_calendar_invite(sender, recipient, subject, body, start_time, end_time, date)
        send_email_with_attachment(sender, recipient, subject, body, ics_content)
    else:
        send_plain_email(sender, recipient, subject, body)

def create_calendar_invite(sender, recipient, subject, body, start_time, end_time, date):
    cal = Calendar()
    event = CalEvent()
    event.add('summary', subject)
    event.add('dtstart', datetime.combine(date, start_time))
    event.add('dtend', datetime.combine(date, end_time))
    event.add('description', body)
    cal.add_component(event)
    return cal.to_ical()
def send_email_with_attachment(sender, recipient, subject, body, attachment_content):
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = recipient

    text = MIMEText(body)
    msg.attach(text)

    attachment = MIMEApplication(attachment_content, "octet-stream")
    attachment.add_header('Content-Disposition', 'attachment', filename="invite.ics")
    msg.attach(attachment)

    try:
        response = client.send_raw_email(
            Source=sender,
            Destinations=[recipient],
            RawMessage={'Data': msg.as_string()}
        )
        print("Email with calendar invite sent! Message ID:", response['MessageId'])
    except ClientError as e:
        print("Error sending email: ", e.response['Error']['Message'])

def send_plain_email(sender, recipient, subject, body):
    try:
        response = client.send_email(
            Destination={
                'ToAddresses': [recipient],
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': 'UTF-8',
                        'Data': body,
                    },
                },
                'Subject': {
                    'Charset': 'UTF-8',
                    'Data': subject,
                },
            },
            Source=sender,
        )
        print("Plain email sent! Message ID:", response['MessageId'])
    except ClientError as e:
        print("Error sending email: ", e.response['Error']['Message'])


def setup_db():
    engine = create_engine(SQLALCHEMY_DATABASE_URL,
                           pool_pre_ping=True, pool_recycle=3600)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal, engine


Base = declarative_base()
SessionLocal, engine = setup_db()

# Dependency to get the database session


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Define SQLAlchemy models
class Agent(Base):
    __table__ = Table('agent', Base.metadata,
                      schema=schema, autoload_with=engine)

class Customer(Base):
    __table__ = Table('customer', Base.metadata,
                      schema=schema, autoload_with=engine)

class Product(Base):
    __table__ = Table('products', Base.metadata,
                      schema=schema, autoload_with=engine)

class AgentSchedule(Base):
    __table__ = Table('agent_schedule', Base.metadata,
                      schema=schema, autoload_with=engine)

class Appointment(Base):
    __table__ = Table('appointment', Base.metadata,
                      schema=schema, autoload_with=engine)
    
class Event(Base):
    __table__ = Table('event', Base.metadata,
                      schema=schema, autoload_with=engine)

class Template(Base):
    __table__ = Table('templates', Base.metadata,
                      schema=schema, autoload_with=engine)


# ---------- Utilities --------------------

def convert_timezone(input_time, input_date, output_timezone):
    # input_time = HH:MM:SS
    # input_date = DD/MM/YYYY
    # output_timezone = +/-HH:MM (UTC)
    
    # Convert input time and date to datetime object in IST
    time_format = "%H:%M:%S"
    date_format = "%Y-%m-%d"

    input_datetime_str = f"{input_date} {input_time}"
    input_datetime = datetime.strptime(input_datetime_str, f"{date_format} {time_format}")
    
    # Step 1: Convert to UTC time
    utc_offset_sign = '+'
    utc_offset_hours = 5  # IST is UTC+5
    utc_offset_minutes = 30
    
    if utc_offset_sign == '-':
        utc_time = input_datetime + timedelta(hours=utc_offset_hours, minutes=utc_offset_minutes)
    elif utc_offset_sign == '+':
        utc_time = input_datetime - timedelta(hours=utc_offset_hours, minutes=utc_offset_minutes)

    # Step 2: Convert to output time zone
    output_offset_sign = output_timezone[0]
    output_offset_hours = int(output_timezone[1:3])  # Corrected indexing
    output_offset_minutes = int(output_timezone[4:])
    
    if output_offset_sign == '-':
        output_time = utc_time - timedelta(hours=output_offset_hours, minutes=output_offset_minutes)
    elif output_offset_sign == '+':
        output_time = utc_time + timedelta(hours=output_offset_hours, minutes=output_offset_minutes)

    # Format output time and date
    output_time_format = output_time.strftime(time_format)
    output_date_format = output_time.strftime(date_format)
    output_utc_offset = output_time.strftime("%z")
    
    # output_time_str = f"{output_time_format} {output_date_format} {output_utc_offset}"
    
    return output_time_format, output_date_format


async def row2dict(row):
    return {column.name: getattr(row, column.name) for column in row.__table__.columns}

async def generate_time_slots(start_time, end_time, duration=30):
    # Convert start and end time strings to datetime objects
    start = datetime.strptime(str(start_time), '%H:%M:%S')
    end = datetime.strptime(str(end_time), '%H:%M:%S')

    # Initialize list to store time slots
    time_slots = []

    # Generate time slots in (duration)30-minute intervals
    current_time = start
    while current_time < end:
        next_time = current_time + timedelta(minutes=duration)
        time_slots.append((current_time.strftime(
            '%H:%M'), next_time.strftime('%H:%M')))
        current_time = next_time

    return time_slots



async def make_contact_visible(date, start_time):
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
    

#

def format_db_response(result):
    try:
        result_dict = []
        for item in result:
            item_dict = item.__dict__
            # Remove the attribute holding the reference to the database session
            item_dict.pop('_sa_instance_state', None)
            result_dict.append(item_dict)
        return result_dict
    
    except Exception as e:
        # raise e
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
# ------------ Pydantics Models -----------------

class AgentSchema(BaseModel):
    full_name: str
    date_of_joining: datetime
    leave_from: Optional[datetime] = None
    leave_to: Optional[datetime] = None
    slot_time: int
    buffer_time: int
    product_id: int
    agent_email: str
    shift_from: time
    shift_to: time
    weekly_off: list[str]
    password: str = 'agent'



class LoginSchema(BaseModel):
    username: str
    password: str


class LoginSchema(BaseModel):
    username: str
    password: str


class FeedbackSchema(BaseModel):
    appointment_id: int
    rating: int


class CustomerSchema(BaseModel):
    username: str
    created_at: Optional[datetime] = str(datetime.now())  # type: ignore
    email_id: str
    mobile_no: str
    product_id: int
    pre_screening: dict = None
    case_id: str
    email_body: str
    email_subject: str
    email_author: str

class TemplateSchema(BaseModel):
    template_name: str
    template_type: str
    content: str
 


class ProductSchema(BaseModel):
    name: str
    created_at: Optional[datetime] = str(datetime.now())  # type: ignore
    category: str


class AppointmentSchema(BaseModel):
    customer_id: int
    call_status: str = None
    call_rating: int = None
    agent_id: int
    created_at: Optional[datetime] = None
    is_booked: bool = None
    appointment_description: str
    # scheduled_at: Optional[datetime]
    date: str
    start_time: str
    end_time: str
    customer_timezone: str

class EventSchema(BaseModel):
    status: str
    event_name: str
    timestamp: str
    event_details: Optional[dict] = {}
    case_id: str

class OriginalAppointmentSchema(BaseModel):
    customer_id: int
    call_status: str = None
    call_rating: int = None
    agent_id: int
    created_at: Optional[datetime] = None
    is_booked: bool = None
    scheduled_at: Optional[datetime]


class TriggerCallSchema(BaseModel):
    appointment_id: int


class ResponseModel(BaseModel):
    message: str
    payload: Optional[dict] = {}


class UpdateAppointment(BaseModel):
    date: str
    start_time: str
    end_time: str
    reason:str
# ---------- API endpoints -------------
app = APIRouter()

# ----------------- Product Endpoints------------

@app.get("/products/", response_model=ResponseModel, tags=["products"])
async def get_products(db: Session = Depends(get_db)):
    return ResponseModel(message=success_message, payload={"products": [await row2dict(product) for product in db.query(Product).all()]})

# ### ------------- Email Endpoints -----------
# @app.get("/email/customer/bookAppointment", response_model=ResponseModel, tags=["Email"])
# async def book_appointment(customer_id: int, product_id: int):
#     try:
#         localhost_link = f"http://localhost:3000/customer/bookAppointment?customer_id={customer_id}&product_id={product_id}"
#         return {"localhost_link": localhost_link}
#     except Exception as e:
#         return HTTPException(status_code=500,details=f"Error sending email {e}")

# ### ------------- Create  Endpoints -----------
@app.post(path="/customer/create", response_model=ResponseModel, tags=["customer"],status_code=201)
async def create_customer(customer: CustomerSchema, db: Session = Depends(get_db)):
    try:
        case_id = str(uuid.uuid4())
        case_id = case_id[:8]
        customer = customer.__dict__
        print(customer)
        customer['case_id'] = case_id
        new_customer = Customer(**customer)

        # text = """abcdef"""

        text = """WARNING - This email originated outside of Genpact.
                  Do not reply, click on links or open attachments unless you recognize the sender
                  and know the content is safe. If you believe the content of this email may be
                  unsafe, please forward it as an attachment to thislooksphishy@genpact.com or use
                  the 'This Looks Phishy' Outlook button."""

        if text in new_customer.email_body:
            # Remove the warning message
            new_customer.email_body = new_customer.email_body.replace(text, "").strip()

        print(new_customer.email_body)


        db.add(new_customer)
        db.commit()
        db.refresh(new_customer)
        print(new_customer.id)
        customer_id = new_customer.id
        send_email("Someshwar.Garud@genpact.com", new_customer.email_id, f"Schedule Your Appointment with Us - Case ID: {case_id}", f"""
Case ID: {new_customer.case_id} 
Thank you for connecting with us! We are excited to discuss how we can assist you further and explore potential solutions together.
                   
To ensure we can provide you with personalized attention, please use the following link to schedule an appointment at your convenience:
https://main.d2el3bzkhp7t3w.amplifyapp.com/customer/bookAppointment?customer_id={customer_id}&product_id={new_customer.product_id}&case_id={case_id}
 
We look forward to meeting you and are here to assist you every step of the way.

Warm regards

Genpact Team """)
        event1_data = {
            'status': 'New Email Received',
            'event_name': 'A new email has been received',
            'event_details': {"email":f"""
From: Someshwar.Garud@genpact.com
To: {new_customer.email_id}

Subject: {new_customer.email_subject}

{new_customer.email_body}
""",
"details": f"New Email has been received from {new_customer.email_id} at {str(datetime.now())}"
            },
            'timestamp': str(datetime.now()),
            'case_id': case_id
        }

        event2_data = {
            'status': 'Unique Case ID created',
            'event_name': 'A unique Case ID has been created',
            'event_details': {
                "email":"",
                "details":f"A new unique Case ID has been created"
            },
            'timestamp': str(datetime.now()),
            'case_id': case_id
        }
        event1 = Event(**event1_data)
        event2 = Event(**event2_data)
        db.add(event1)
        db.add(event2)
        db.commit()
        db.refresh(new_customer)

        # await send_email(email, user_id, product_id)
        return ResponseModel(message=success_message, payload={"customer_id": new_customer.id, "case_id": case_id})
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@app.post(path="/product/create", response_model=ResponseModel, tags=["product"], status_code=201)
async def create_product(product: ProductSchema, db: Session = Depends(get_db)):
    try:
        new_product = Product(**product.dict())
        db.add(new_product)
        db.commit()
        db.refresh(new_product)
        return ResponseModel(message=success_message, payload={"product_id": new_product.id})
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@app.post(path="/agent/create", response_model=ResponseModel, tags=["agent"],status_code=201)
async def create_agent(agent: AgentSchema, db: Session = Depends(get_db)):
    try:
        new_agent = Agent(**agent.dict())
        db.add(new_agent)
        db.commit()
        db.refresh(new_agent)
        return ResponseModel(message=success_message, payload={"agent_id": new_agent.id})
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@app.post(path="/appointment/create", response_model=ResponseModel, tags=["appointment"])
async def create_appointment(appointment: AppointmentSchema, db: Session = Depends(get_db)):
    try:
        existing_appointment = appointment.dict()

        #checking if the user have a pending appointment
        query = text("""SELECT COUNT(*) AS num_columns FROM genpact.agent_schedule WHERE status = 'booked' AND customer_id = :customer_id""")
        data = db.execute(query, {"customer_id": existing_appointment['customer_id']})
        result = data.fetchone()  

        db.commit()
        if result[0] > 0:
            return ResponseModel(message="You already have an exsiting appointment you cannot book one more until that is closed")

        #modifying string into proper datatype
        new_appointment = OriginalAppointmentSchema(
            customer_id=(existing_appointment['customer_id']),
            agent_id=existing_appointment['agent_id'],
            created_at=existing_appointment['created_at'],
            scheduled_at=datetime.strptime(
                existing_appointment['date'] + ' ' + existing_appointment['start_time'], '%d-%m-%y %H:%M')
        )

        date_obj = datetime.strptime(
            existing_appointment['date'], '%d-%m-%y').date()
        # Convert start time string to time object
        start_time_obj = time.fromisoformat(existing_appointment['start_time'])
        end_time_obj = time.fromisoformat(existing_appointment['end_time'])
        print(start_time_obj, type(start_time_obj))
        new_appointment = Appointment(**new_appointment.dict())
        db.add(new_appointment)
        db.commit()
        db.refresh(new_appointment)
        # Update agent_schedule status to "booked" for the corresponding appointment
        query = text("""
           INSERT INTO genpact.agent_schedule (status, customer_id, agent_id, start_time,end_time,date,appointment_id,customer_timezone,appointment_description) VALUES ('booked', :customer_id, :agent_id, :start_time,:end_time,:date,:appointment_id,:customer_timezone,:appointment_description);""")
        db.execute(
            query,
            {
                "customer_id": appointment.customer_id,
                "agent_id": appointment.agent_id,
                "start_time": start_time_obj,
                "end_time": end_time_obj,
                "date": date_obj,
                "appointment_id": new_appointment.id,
                "customer_timezone":existing_appointment['customer_timezone'],
                "appointment_description":existing_appointment['appointment_description'],

            }
        )
        query = db.query(Agent).filter(Agent.id ==appointment.agent_id )
        agent_data = query.first()
        agent_email = agent_data.agent_email
        product_id = agent_data.product_id
        query = db.query(Customer).filter(Customer.id == existing_appointment['customer_id'] )
        customer_data = query.first()
        Customer_email= customer_data.email_id
        case_id = customer_data.case_id

        send_email("Someshwar.Garud@genpact.com", Customer_email, f"Confirmation of Your Scheduled Appointment - Case ID: {case_id}",f"""
Case ID: {case_id}
We are pleased to confirm that your appointment has been successfully scheduled. Thank you for choosing our services!
To view the details of your appointment, please click the following link: https://main.d2el3bzkhp7t3w.amplifyapp.com/customer/bookedAppointment?customer_id={existing_appointment['customer_id']}&product_id={product_id}
Should you need to reschedule or cancel your appointment, please use the links below at your convenience:
Reschedule Your Appointment - https://main.d2el3bzkhp7t3w.amplifyapp.com/customer/bookedAppointment?customer_id={existing_appointment['customer_id']}&product_id={product_id}&case_id={case_id}
Cancel Your Appointment - https://main.d2el3bzkhp7t3w.amplifyapp.com/customer/bookedAppointment?customer_id={existing_appointment['customer_id']}&product_id={product_id}&case_id={case_id}
If you have any specific requests or questions prior to our meeting, do not hesitate to contact us directly through this email.
We look forward to our conversation and are here to assist you with any questions you may have prior to our meeting.
Warm regards,
Genpact Team 
""",start_time_obj,end_time_obj,date_obj)

        send_email("Someshwar.Garud@genpact.com", agent_email, f"New Appointment Booked - Case ID: {case_id}", f""" 
Case ID: {case_id}
We are pleased to inform you that a new appointment has been booked. Please log in to your agent portal to view the details and prepare for the upcoming meeting.
Quick Reminder:
Check the Appointment Date and Time: Ensure your schedule is updated.
                   
Review Customer Details: Familiarize yourself with the customer's requirements and previous interactions to provide a tailored experience.
Access your portal here: https://main.d2el3bzkhp7t3w.amplifyapp.com/  
Thank you for your dedication and hard work. Let's continue providing exceptional service to our clients!
Best Regards,
                   
Genpact Team
""",start_time_obj,end_time_obj,date_obj)
        event1_details = {
            "event_name": "Appointment notification is sent",
            "event_details": {
                "email": f"""
From: Someshwar.Garud@genpact.com
To: {Customer_email}

Subject: Confirmation of Your Scheduled Appointment - Case ID: {case_id}

Case ID: {case_id}
We are pleased to confirm that your appointment has been successfully scheduled. Thank you for choosing our services!

To view the details of your appointment, please click the following link: https://main.d2el3bzkhp7t3w.amplifyapp.com/customer/bookedAppointment?customer_id={existing_appointment['customer_id']}&product_id={product_id}
Should you need to reschedule or cancel your appointment, please use the links below at your convenience:

Reschedule Your Appointment - https://main.d2el3bzkhp7t3w.amplifyapp.com/customer/bookedAppointment?customer_id={existing_appointment['customer_id']}&product_id={product_id}&case_id={case_id}
Cancel Your Appointment - https://main.d2el3bzkhp7t3w.amplifyapp.com/customer/bookedAppointment?customer_id={existing_appointment['customer_id']}&product_id={product_id}&case_id={case_id}

If you have any specific requests or questions prior to our meeting, do not hesitate to contact us directly through this email.
We look forward to our conversation and are here to assist you with any questions you may have prior to our meeting.

Warm regards,
Genpact Team
                """,
                "details": f"Appointment notification successfully sent to {Customer_email} at {str(datetime.now())}",
                   "start_time":existing_appointment['start_time'],
                "end_time":existing_appointment['end_time'],
                'date':existing_appointment['date']
            },
            "timestamp": str(datetime.now()),
            "case_id": case_id,
            "status": "Appointment Notification Sent"
        }

        event2_details = {
            "event_name": "Customer Response is awaiting",
            "event_details": {
                "email": "",
                "details": f"Awaiting customer response for Case ID: {case_id} at {str(datetime.now())}",
                 "start_time":existing_appointment['start_time'],
                "end_time":existing_appointment['end_time'],
                'date':existing_appointment['date']
            },
            "timestamp": str(datetime.now()),
            "case_id": case_id,
            "status": "Awaiting Customer Response"
        }

        event3_details = {
            "event_name": "The appointment confirmation is received",
            "event_details": {
                "email": "",
                "details": f"The appointment confirmation has been received for Case ID: {case_id} at {str(datetime.now())}",
                 "start_time":existing_appointment['start_time'],
                "end_time":existing_appointment['end_time'],
                'date':existing_appointment['date']
            },
            "timestamp": str(datetime.now()),
            "case_id": case_id,
            "status": "Appointment Confirmation Received"
        }

        event4_details = {
            "event_name": "The appointment is ready for interview",
            "event_details": {
                "email": "",
                "details": f"The appointment with Case ID: {case_id} is ready for interview at {str(datetime.now())}",
                 "start_time":existing_appointment['start_time'],
                "end_time":existing_appointment['end_time'],
                'date':existing_appointment['date']
            },
            "timestamp": str(datetime.now()),
            "case_id": case_id,
            "status": "Ready For Interview"
        }

        event1 = Event(**event1_details)
        event2 = Event(**event2_details)
        event3 = Event(**event3_details)
        event4 = Event(**event4_details)

        db.add(event1)
        db.add(event2)
        db.add(event3)
        db.add(event4)
        db.commit()
        return ResponseModel(message=success_message, payload={"appointment_id": new_appointment.id, "case_id":case_id})
    except Exception as e:
        # raise e
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


# #-----------------------------------------------agent-------------------------------------------------------------------#


@app.get(path="/slots/{product_id}/{date}", response_model=ResponseModel, tags=["customer", "agent"])
async def get_agent_schedules(product_id: int, date: str, slot_time:str, db: Session = Depends(get_db)): # type:Literal["customer", "agent"] = Header(), agent_id: int = Query(default=None), db: Session = Depends(get_db)):
    try:
        Agents = db.query(Agent).filter(Agent.product_id == product_id).all()
        if not Agents:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")
        print(Agents)
        date_obj = datetime.strptime(date, '%d-%m-%y').date()
        time_obj = time.fromisoformat(slot_time)

        # agent_ids = db.query(Agent.id).filter(Agent.product_id == product_id).all()
        Agents = format_db_response(Agents)
        print(Agents,"Agents")
        available_agents = []
        for i in Agents:
            try:
                data = db.query(AgentSchedule).filter(AgentSchedule.start_time == time_obj,AgentSchedule.date == date_obj,AgentSchedule.agent_id==i["id"]).all()
                # data = format_db_response(Agents)
                print(data,"data")
            except Exception as e:
                print(e)
            if data==[]:
                available_agents.append(i["id"])
        print(available_agents,"available_agents")
        if len(available_agents)==0:
            return ResponseModel(message="No available agent found on the particular time")
        avail_id = random.choice(available_agents)
        return ResponseModel(message="Available agent on the particular time",payload={"agent_id":avail_id})
    except Exception as e:
        # raise e
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


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
#     ses_client.await send_email(
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


@app.post("/cancel_appointment/{appointment_id}", response_model=ResponseModel,tags=['appointment'])
async def cancel_appointment_route(appointment_id: int,reason:str, db: Session = Depends(get_db)):
    # Create session
    try:
        # Execute SQL query to delete appointment
        try:
            query = db.query(Appointment).filter(Appointment.id == appointment_id)
            data = query.first()
            cust_id= data.customer_id
            agent_id= data.agent_id
            query = db.query(Customer).filter(Customer.id == cust_id)
            customer_data = query.first()
            Customer_email= customer_data.email_id
            case_id = customer_data.case_id
            query = db.query(Agent).filter(Agent.id == agent_id)
            agent_email = query.first()
            agent_email = agent_email.agent_email
            send_email("Someshwar.Garud@genpact.com", Customer_email, f"Confirmation of Your Appointment Cancellation - Case ID: {case_id}", f"""
Case ID: {case_id}
We have received your request and successfully cancelled your scheduled appointment. We are sorry to see you go, but understand that circumstances can change.

If you wish to reschedule at a later time or if there is anything else we can assist you with, please do not hesitate to reach out.

Thank you for your interest in our services. We hope to have the opportunity to assist you in the future.

Best regards,

Genpact Team
""")
            send_email("Someshwar.Garud@genpact.com", agent_email, f"appointment Cancelled - Case ID: {case_id}", f"""
Case ID: {case_id}
Hello, your scheduled appointment has been cancelled""")
        except:
            return ResponseModel(message="No appointment found")
        query1 = text("""
    UPDATE 
        genpact.agent_schedule
    SET 
        status = 'cancelled',
        appointment_id = NULL,
        reason =:reason
    WHERE 
        agent_schedule.appointment_id = :appointment_id;
""")
        query2 = text("""DELETE FROM genpact.appointment
WHERE genpact.appointment.id = :appointment_id;""")
        

        db.execute(query1, {"appointment_id": appointment_id,"reason":reason})
        db.execute(query2, {"appointment_id": appointment_id})

        event_details = {
            "event_name": "Appointment has been cancelled",
            "event_details": {
                "email": f"""
From: Someshwar.Garud@genpact.com
To: {Customer_email}

Subject: Confirmation of Your Appointment Cancellation - Case ID: {case_id}

We have received your request and successfully cancelled your scheduled appointment. We are sorry to see you go, but understand that circumstances can change.

If you wish to reschedule at a later time or if there is anything else we can assist you with, please do not hesitate to reach out.

Thank you for your interest in our services. We hope to have the opportunity to assist you in the future.

Best regards,

Genpact Team
""",
                "details": f"Appointment for Case ID {case_id} cancelled due to {reason}"
            },
            "timestamp": str(datetime.now()),
            "case_id": case_id,
            "status": "Appointment Cancelled"
        }
        
        new_event = Event(**event_details)
        db.add(new_event)
        

        # Commit transaction
        db.commit()

        # Return success message
        return ResponseModel(message="Appointment canceled successfully.",payload={"case_id":case_id})
    except Exception as e:
        # Rollback transaction in case of error
        db.rollback()

        # Raise HTTPException with error message
        raise HTTPException(
            status_code=500, detail=f"Error canceling appointment: {str(e)}")
    finally:
        # Close session
        db.close()


@app.post("/update_appointment/{appointment_id}", tags=['appointment'])
async def cancel_appointment_route(appointment_id: int, data: UpdateAppointment, db: Session = Depends(get_db)):
    # Create session
    try:
        # Execute SQL query to delete appointment
        data = data.dict()
        query = text("""UPDATE genpact.agent_schedule SET start_time = :start_time, end_time =:end_time, date = :date, reason=:reason WHERE agent_schedule.appointment_id = :appointment_id""")
        start_time_obj = time.fromisoformat(data['start_time'])
        end_time_obj = time.fromisoformat(data['end_time'])
        date_obj = datetime.strptime(data['date'], '%d-%m-%y').date()

        db.execute(query, {"date": date_obj, "start_time": start_time_obj,
                   "end_time": end_time_obj, "appointment_id": appointment_id,"reason":data['reason']})

        # Commit transaction
        db.commit()
        query = text(
            """UPDATE genpact.appointment SET scheduled_at = :scheduled_at WHERE appointment.id = :appointment_id """)

        scheduled_at = datetime.strptime(
            data['date'] + ' ' + data['start_time'], '%d-%m-%y %H:%M')
        db.execute(query, {"appointment_id": appointment_id,
                   "scheduled_at": scheduled_at})
        
        query = db.query(Appointment).filter(Appointment.id == appointment_id)
        data = query.first()
        cust_id= data.customer_id
        agent_id= data.agent_id
        query = db.query(Customer).filter(Customer.id == cust_id)
        Customer_email = query.first()
        case_id = Customer_email.case_id
        Customer_email= Customer_email.email_id
        query = db.query(Agent).filter(Agent.id == agent_id)
        agent_email = query.first()
        agent_email = agent_email.agent_email
        print("111111111111111111111111111111111111")
         # Commit transaction
        event_details = {
            "event_name": "Appointment has been rescheduled",
            "event_details": {
                "email": f"""
From: Someshwar.Garud@genpact.com
To: {Customer_email}

Subject: Confirmation of Your Rescheduled Appointment - Case ID: {case_id}

Case ID: {case_id}
We have successfully updated your appointment details as requested. Thank you for continuing to choose us for your needs!

Please review the updated appointment information to ensure everything is correct. If you need further adjustments or have specific requirements for our meeting, feel free to reach out to us directly through this email.

Best Regards,

Genpact Team
""",
                "details": f"Appointment Rescheduled for Case ID {case_id}",
                # "start_time":data['start_time'],
                # "end_time":data['end_time'],
                # 'date':data['date']
            },
            "timestamp": str(datetime.now()),
            "case_id": case_id,
            "status": "Appointment Rescheduled"
        }
        
        new_event = Event(**event_details)
        db.add(new_event)
        db.commit()
        print("111111111111111111111111111111111111")

        send_email("Someshwar.Garud@genpact.com", Customer_email, f"Confirmation of Your Rescheduled Appointment - Case ID: {case_id}", f"""
Case ID: {case_id}
We have successfully updated your appointment details as requested. Thank you for continuing to choose us for your needs!

Please review the updated appointment information to ensure everything is correct. If you need further adjustments or have specific requirements for our meeting, feel free to reach out to us directly through this email.

Best Regards,

Genpact Team
                   """,start_time_obj,end_time_obj,date_obj)
        send_email("Someshwar.Garud@genpact.com", agent_email, f"Appointment Rescheduled - Case ID: {case_id}", f"""
                   Case ID: {case_id}
                   The booked appointment has been rescheduled""",start_time_obj,end_time_obj,date_obj)

       
        # Return success message
        return {"message": "Appointment updated successfully."}
    except Exception as e:
        # Rollback transaction in case of error
        db.rollback()

        # Raise HTTPException with error message
        raise HTTPException(
            status_code=500, detail=f"Error canceling appointment: {str(e)}")
    finally:
        # Close session
        db.close()


@app.get("/userDetail/{customer_id}", tags=['customer'])
async def get_user_detail(customer_id: int, db: Session = Depends(get_db)):
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
        user_detail_dict = {col: value for col,
                            value in zip(columns, user_detail)}

        return user_detail_dict
    except Exception as e:
        # Rollback transaction in case of error
        db.rollback()

        # Raise HTTPException with error message
        raise HTTPException(
            status_code=500, detail=f"Error retrieving user detail: {str(e)}")
    finally:
        # Close session
        db.close()


@app.get("/appointments/{customer_id}", tags=['appointment'])
def get_appointments(customer_id: int, db: Session = Depends(get_db)):
    try:
        # Fetch agent schedules with related appointment details
        appointments = db.query(AgentSchedule, Appointment).\
            join(Appointment, AgentSchedule.appointment_id == Appointment.id).\
            filter(AgentSchedule.customer_id == customer_id).all()
        
        case_id = db.query(Customer).filter(Customer.id == customer_id)
        case_id = case_id.first()

        case_id = case_id.case_id
        

        if not appointments:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, 
                detail="Appointments not found for the customer"
            )

        formatted_appointments = []
        for agent_schedule, appointment in appointments:
            appointment_dict = appointment.__dict__
            agent_schedule_dict = agent_schedule.__dict__
            
            # Convert SQLAlchemy objects to dictionaries and merge them
            appointment_info = appointment_dict | agent_schedule_dict
            
            # Fetch agent info
            agent_info = db.query(Agent).filter(Agent.id == agent_schedule.agent_id).first()
            
            if not agent_info:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND, 
                    detail="Agent not found"
                )
            
            agent_info_dict = agent_info.__dict__

            # Convert timezone
            timezone = appointment_info['customer_timezone']
            start_time = appointment_info['start_time']
            end_time = appointment_info['end_time']
            date = appointment_info['date']
            start_time, date = convert_timezone(start_time, date, timezone)
            end_time, _ = convert_timezone(end_time, date, timezone)

            appointment_info['start_time'] = start_time
            appointment_info['end_time'] = end_time
            appointment_info['date'] = date

            # Merge agent info with appointment info
            result = appointment_info | agent_info_dict
            result['case_id'] = case_id
            formatted_appointments.append(result)

        # Sort appointments by date
        formatted_appointments_sorted = sorted(formatted_appointments, key=lambda x: x['date'], reverse=True)
        
        return formatted_appointments_sorted
        


    except Exception as e:
        # Rollback transaction in case of error
        db.rollback()

        # Raise HTTPException with error message
        raise HTTPException(status_code=500, detail=f"Error fetching appointments: {e}")
    
    finally:
        # Close session
        db.close()



@app.get("/appointments/list/{agent_id}", tags=['appointment'])
def get_agent_appointments(agent_id: int, db: Session = Depends(get_db)):
    # Connect to the PostgreSQL database

    # Execute raw SQL query
    query = text("""
        SELECT
    appointments.*,
    schedule.reason,
    customer.username,
    customer.email_id,
    customer.mobile_no,
    customer.case_id,
    schedule.start_time,
    schedule.end_time,
    schedule.date,
    schedule.appointment_description                 
                 
FROM
    genpact.appointment AS appointments
JOIN
    genpact.customer ON appointments.customer_id = customer.id
JOIN
    genpact.agent_schedule AS schedule ON appointments.id = schedule.appointment_id
WHERE
    appointments.agent_id = :agent_id 
ORDER BY
    schedule.date DESC;
    """)

    # Execute the query
    result = db.execute(query, {"agent_id": agent_id})
    columns = result.keys()

    # Convert each row into a dictionary with column names as keys
    appointments_with_schedule = [
        {col: val for col, val in zip(columns, row)} for row in result.fetchall()]

    print(appointments_with_schedule, type(appointments_with_schedule))
    # Close the connection
    db.close()
    # result = filter_json_by_time(appointments_with_schedule)
    # return result
    
    return appointments_with_schedule


@app.post("/agent/add-comments/{appointment_id}", response_model=ResponseModel, tags=["agent"])
async def add_comments(appointment_id: int, comments: str, db: Session = Depends(get_db)):
    try:
        # Check if appointment exists
        appointment = db.query(Appointment).filter(
            Appointment.id == appointment_id).first()
        if not appointment:
            raise HTTPException(
                status_code=404, detail="Appointment not found")

        # Update the agent_comments field
        appointment.call_rating = comments
        db.commit()

        return ResponseModel(message="Comments added successfully")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/customer/call/{appointment_id}/{rating}", response_model=ResponseModel, tags=["customer"])
async def add_comments(appointment_id: int, rating: int, db: Session = Depends(get_db)):
    try:
        # Check if appointment exists
        appointment = db.query(Appointment).filter(
            Appointment.id == appointment_id).first()
        if not appointment:
            raise HTTPException(
                status_code=404, detail="Appointment not found")

        # Update the agent_comments field
        appointment.call_rating = rating
        db.commit()

        return ResponseModel(message="Rating added successfully")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.put("/customer/update/{id}", tags=["customer"])
def update_customer(id: int, customer_data: CustomerSchema, db: Session = Depends(get_db)):
    try:
        query = text("""
            UPDATE genpact.customer
            SET username = :username, email_id = :email_id, mobile_no = :mobile_no, product_id = :product_id
            WHERE id = :id;
        """)

        db.execute(
            query,
            {
                "id": id,
                "username": customer_data.username,
                "email_id": customer_data.email_id,
                "mobile_no": customer_data.mobile_no,
                "product_id": customer_data.product_id,
            }
        )
        db.commit()
        return {"message": "Customer details updated successfully"}
    except Exception as e:
        # Rollback transaction in case of error
        db.rollback()
        # Raise HTTPException with error message
        raise HTTPException(
            status_code=500, detail=f"Failed to update customer: {str(e)}")


def filter_json_by_time(json_data):
    # Get current time
    current_time = datetime.now()

    # Calculate filter end time (current time + 30 minutes)
    filter_end_time = current_time + timedelta(minutes=30)

    # Extract only the time part of filter end time
    filter_end_time = filter_end_time.time()
    print(filter_end_time)
    # Filter records based on start time
    filtered_records = []
    for record in json_data:
        start_time = datetime.strptime(record['start_time'], '%H:%M:%S').time()
        if current_time.time() <= start_time <= filter_end_time:
            filtered_records.append(record)

    return filtered_records


@app.get("/appointments/description/{appointment_id}", tags=['appointment'])
def get_appointments(appointment_id: int, db: Session = Depends(get_db)):
    # Create session
    try:
        appointments = db.query(AgentSchedule).filter(
            AgentSchedule.appointment_id == appointment_id).all()
        # schedules = db.query(AgentSchedule).filter(AgentSchedule.agent_id == 4).first()
        if not appointments:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")
        print(appointments)
        # agent_ids = db.query(Agent.id).filter(Agent.product_id == product_id).all()
        appointments = format_db_response(appointments)

        return {"appointment_details": appointments}
    except Exception as e:
        # Rollback transaction in case of error
        db.rollback()

        # Raise HTTPException with error message
        raise HTTPException(status_code=200, detail=f"No record found - {e}")
    finally:
        # Close session
        db.close()

@app.get('/agents',tags=['agent'])
async def get_all_agent_details(db: Session = Depends(get_db)):
    try:
        agents_details = db.query(Agent).all()
        agents_details = format_db_response(agents_details)

        return {"agents_details": agents_details}
    except Exception as e:
        # Rollback transaction in case of error
        db.rollback()

        # Raise HTTPException with error message
        raise HTTPException(status_code=200, detail=f"No record found - {e}")
    finally:
        # Close session
        db.close()

@app.put('/agents/update',tags=["agent"])
async def update_all_agent_detials(updated_agent_details:dict,db: Session = Depends(get_db)):
    try:
        data = updated_agent_details["agents_details"]
        for value in data:
            id = value["id"]
            print(value)
            try:
                db.query(Agent).filter(Agent.id == id).update(value)
                data=db.query(Agent).filter(Agent.id == id).all()
                print(format_db_response(data),"\n\n")
                db.commit()
            except Exception as e:
                return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")
        return ResponseModel(message="All the row updated successfully")
    except Exception as e:
        # Rollback transaction in case of error
        db.rollback()

        # Raise HTTPException with error message
        raise HTTPException(status_code=200, detail=f"No record found - {e}")
    finally:
        # Close session
        db.close()

# @app.post("/list/cancelled_appointments", tags=['appointment'])
# def get_cancelled_appointments(db: Session = Depends(get_db)):
#     try:
#         query = db.query(AgentSchedule).filter(
#             AgentSchedule.status == "cancelled").all()
#         # schedules = db.query(AgentSchedule).filter(AgentSchedule.agent_id == 4).first()
#         if not query:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND, detail="query not found")
#         print(query)
#         # agent_ids = db.query(Agent.id).filter(Agent.product_id == product_id).all()
#         cancelled_appointment_data = format_db_response(query)

#         return cancelled_appointment_data

#     except Exception as e:
#         db.rollback()
#         raise HTTPException(status_code=500, detail=f"Error fetching cancelled appointments: {str(e)}")
#     finally:
#         db.close()  

from sqlalchemy import text
from sqlalchemy.orm import joinedload
@app.post("/list/cancelled_appointments/{agent_id}", tags=['appointment'])
def get_cancelled_appointments(agent_id:int, db: Session = Depends(get_db)):
    try:
        query = db.query(AgentSchedule, Customer).join(Customer, AgentSchedule.customer_id == Customer.id).filter(AgentSchedule.status == "cancelled").filter(AgentSchedule.agent_id == agent_id)
 # Optional: to load Customer objects along with AgentSchedule objects

        results = query.all()
        print(len(results))
        result = []
        for agent_schedule, customer in results:
            entry = {
                "agent_schedule": agent_schedule.id,
                "start_time": agent_schedule.start_time,
                "end_time": agent_schedule.end_time,
                "status": agent_schedule.status,
                "username": customer.username,
                "mobile_no": customer.mobile_no,
                "email_id": customer.email_id,
                "agent_id":agent_schedule.agent_id,
                "status": agent_schedule.status,
                "date": agent_schedule.date,
                "reason":agent_schedule.reason,
                "appointment_description":agent_schedule.appointment_description
            }

            result.append(entry)
        print(len(result))
        result_sorted = sorted(result, key=lambda x: x['date'], reverse=True)
        return result_sorted
       

    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching cancelled appointments: {str(e)}"
        )
    finally:
        # Close the database connection
        db.close()

@app.get("/get_email_data/{id_type}/{id}")
def get_email_data(id_type:str,id:int,db: Session = Depends(get_db)):
    try:
        if id_type=="agent":
            query = db.query(Agent.agent_email).filter(Agent.id == id)
            result = query.first()
            print(result,type(result))
            print(result.id)
            return result
        
        elif id_type=="customer":
            query = db.query(Customer).filter(Customer.id == id)
            result = query.first()
            return result
    except Exception as e:
        return e
class LeaveSchema(BaseModel):
    leave_type:str
    leave_from:str
    leave_to:str
class AgentLeave(Base):
    __table__ = Table('agent_leave', Base.metadata,
                      schema=schema, autoload_with=engine)


@app.post('/agents/create-leave/{agent_id}', tags=["agent"])
async def create_leave(agent_id:int, leave_data: LeaveSchema, db: Session = Depends(get_db)):
    try:
        leave_data = leave_data.__dict__
        print(leave_data)
        date_format = "%d-%m-%y"
        leave_data["agent_id"]=agent_id
        leave_data["leave_from"] = datetime.strptime( leave_data["leave_from"], date_format)
        leave_data["leave_to"]=datetime.strptime( leave_data["leave_to"], date_format)
        new_customer = AgentLeave(
            agent_id = agent_id,
            leave_from = leave_data["leave_from"],
            leave_to = leave_data["leave_to"],
            leave_type = leave_data["leave_type"]
        )
        db.add(new_customer)
        db.commit()
        db.refresh(new_customer)

        return ResponseModel(message="Agent details updated successfully")
    except Exception as e:
        # Rollback transaction in case of error
        db.rollback()

        # Raise HTTPException with error message
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error updating agent: {e}")
    finally:
        # Close session
        db.close()
# @app.post('/agents/create-leave/{agent_id}', tags=["agent"])
# async def create_leave(agent_id:int, leave_data: LeaveSchema, db: Session = Depends(get_db)):
#     try:
#         leave_data = leave_data.__dict__
#         print(leave_data)
#         date_format = "%y-%m-%d"
#         leave_data["agent_id"]=agent_id
#         # leave_data["leave_from"] = datetime.strptime( leave_data["leave_from"], date_format)
#         # leave_data["leave_to"]=datetime.strptime( leave_data["leave_to"], date_format)
#         new_customer = AgentLeave(
#             agent_id = agent_id,
#             leave_from = datetime.strptime(leave_data["leave_from"], date_format).date(),
#             leave_to = datetime.strptime( leave_data["leave_to"], date_format).date()
#         )
#         db.add()
#         db.commit()
#         db.refresh(new_customer)

#         return ResponseModel(message="Agent details updated successfully")
#     except Exception as e:
#         # Rollback transaction in case of error
#         db.rollback()

#         # Raise HTTPException with error message
#         raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error updating agent: {e}")
#     finally:
#         # Close session
#         db.close()

class AgentShiftSchema(BaseModel):
    agent_id:int
    shift_date_from:str
    shift_date_to:str
class AgentShift(Base):
    __table__ = Table('agent_shifts', Base.metadata,
                      schema=schema, autoload_with=engine)


@app.post('/agents/create-shift/{agent_id}', tags=["agent"])
async def create_shift(agent_id:int, leave_data: AgentShiftSchema, db: Session = Depends(get_db)):
    try:
        leave_data = leave_data.__dict__
        date_format = "%y-%m-%d"
        leave_data["agent_id"]=agent_id
        leave_data["leave_from"] = datetime.strptime( leave_data["shift_date_from"], date_format)
        leave_data["leave_to"]=datetime.strptime( leave_data["shift_date_to"], date_format)
        new_customer = AgentShift(**leave_data)
        db.add(new_customer)
        db.commit()
        db.refresh(new_customer)

        return ResponseModel(message="Agent details updated successfully")
    except Exception as e:
        # Rollback transaction in case of error
        db.rollback()

        # Raise HTTPException with error message
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error updating agent: {e}")
    finally:
        # Close session
        db.close()

@app.post('/agents/login', tags=["agent"])
async def agent_login(agent_info:dict, db: Session = Depends(get_db)):
    try:
        results = db.query(Agent).filter(Agent.agent_email == agent_info['username']).filter(Agent.password == agent_info['password']).all()
        results = format_db_response(results)
        role = results[0]['role']
        id = results[0]['id']
        name = results[0]['full_name']
        # db.commit()
        # db.refresh(new_customer)

        return ResponseModel(message="Login successful.",payload={"role":role, "userDetails":{"id":id, "name":name}})
    except Exception as e:
        # Rollback transaction in case of error
        db.rollback()

        # Raise HTTPException with error message
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error logging in: {e}")
    finally:
        # Close session
        db.close()


@app.post('/get_logs', tags=["events"])
async def get_event_logs(case_id:str, db: Session = Depends(get_db)):
    try:
        results = db.query(Event).filter(Event.case_id == case_id)
        results = format_db_response(results)
        print(results)
            
        # db.commit()
        # db.refresh(new_customer)

        return ResponseModel(message="Logs successful.",payload={"data":results})
    except Exception as e:
        # Rollback transaction in case of error
        db.rollback()

        # Raise HTTPException with error message
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error getting logs: {e}")
    finally:
        # Close session
        db.close()

@app.post('/send_reminder', tags=["events"])
async def send_reminder(case_id: str, db: Session = Depends(get_db)):
    try:
        # Fetch customer details
        customer = db.query(Customer).filter(Customer.case_id == case_id).first()
        
        if not customer:
            return ResponseModel(message="No customer found.", payload={"data": []})
        
        email_id = customer.email_id
        customer_name = customer.username  # Assuming you have a 'username' field in the Customer model
        
        # Fetch appointment details using customer_id
        appointment_query = db.query(Appointment).filter(Appointment.customer_id == customer.id).first()
        
        if not appointment_query:
            return ResponseModel(message="No appointment found.", payload={"data": []})
        
        appointment_date = appointment_query.scheduled_at.strftime('%Y-%m-%d')
        
        # Fetch slot details using appointment_id
        slot_query = db.query(AgentSchedule).filter(AgentSchedule.appointment_id == appointment_query.id).first()
        
        if not slot_query:
            return ResponseModel(message="No slot found.", payload={"data": []})
        
        start_time = slot_query.start_time.strftime('%H:%M')
        end_time = slot_query.end_time.strftime('%H:%M')
        
        send_email("Someshwar.Garud@genpact.com", {email_id}, f"Appointment Reminder - Case ID: {case_id}", f"""
Hi {customer_name},

Your appointment is scheduled for {appointment_date} from {start_time} to {end_time}. Please make sure to attend your appointment on time.

Best Regards,
Genpact Team
                   """)
        
        return ResponseModel(message="Email sent successfully.")
    
    except Exception as e:
        # Rollback transaction in case of error
        db.rollback()

        # Raise HTTPException with error message
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error sending reminder: {e}")
    
    finally:
        # Close session
        db.close()

@app.post('/appointment/completed', tags=["appoinment"])
async def mark_appointment_as_completed(case_id: str, db: Session = Depends(get_db)):
    try:
        event1_details = {
            "event_name": "Appointment has been completed",
            "event_details": {
                "email": "",
                "details": f"Appointment with Case ID: {case_id} has been completed at {str(datetime.now())}"
            },
            "timestamp": str(datetime.now()),
            "case_id": case_id,
            "status": "Appointment Completed"
        }

        event2_details = {
            "event_name": "Case has been successfully completed",
            "event_details": {
                "email": "",
                "details": f"The case with Case ID: {case_id} has been successfully completed at {str(datetime.now())}"
            },
            "timestamp": str(datetime.now()),
            "case_id": case_id,
            "status": "Case Successfully Completed"
        }

        event3_details = {
            "event_name": "Case has been successfully closed",
            "event_details": {
                "email": "",
                "details": f"The case with Case ID: {case_id} has been successfully closed at {str(datetime.now())}"
            },
            "timestamp": str(datetime.now()),
            "case_id": case_id,
            "status": "Case Closed"
        }

        event1 = Event(**event1_details)
        event2 = Event(**event2_details)
        event3 = Event(**event3_details)

        db.add(event1)
        db.add(event2)
        db.add(event3)
        db.commit()

        return ResponseModel(message="Appointment marked completed.")
    
    except Exception as e:
        # Rollback transaction in case of error
        db.rollback()
        
        # Log the exception details
        print(f"Error marking appointment as completed: {e}")
        
        # Raise HTTPException with error message
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error marking appointment as completed: {e}")
    
    finally:
        # Close session
        db.close()


@app.post('/agents/update/leave-shift/{agent_id}', tags=["agent"])
async def create_shift(agent_id:int, source_data: dict, db: Session = Depends(get_db)):
    
    try:
        date_format = "%d-%m-%y"
       
        for value in source_data["leaveDetails"]:
            leave_data = {}
            print(value)
            leave_data["agent_id"]=agent_id
            leave_data["leave_type"]=value["leave_type"]
            leave_data["leave_from"] = datetime.strptime( value["from_date"], date_format)
            leave_data["leave_to"]=datetime.strptime( value["to_date"], date_format)
            new_customer = AgentLeave(**leave_data)
            db.add(new_customer)
            db.commit()
            db.refresh(new_customer)

        shift_data= {}
        shift_data["agent_id"]=agent_id
        shift_data["shift_date_from"] = datetime.strptime( source_data["shift_start_date"], date_format)
        shift_data["shift_date_to"]=datetime.strptime( source_data["shift_to_date"], date_format)
        new_customer = AgentShift(**shift_data)
        db.add(new_customer)
        db.commit()
        db.refresh(new_customer)
        return ResponseModel(message="successfully updated")
    except Exception as e:
        # Rollback transaction in case of error
        db.rollback()

        # Raise HTTPException with error message
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error updating agent: {e}")
    finally:
        # Close session
        db.close()

class Question(Base):
    __table__ = Table('questions', Base.metadata,
                      schema=schema, autoload_with=engine)
    
@app.get("/questions/")
async def get_questions(db: Session = Depends(get_db)):
    try:
        questions = db.query(Question.question).all()

        # Extract the 'question' attribute from each row
        question_list = [row.question for row in questions]
        return question_list
    except Exception as e:
        print("Eror fecthing the records")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    
class QuestionAnswer(Base):
    __table__ = Table('questions_answers', Base.metadata,
                      schema=schema, autoload_with=engine)
    
class QuestionAnswerSchema(BaseModel):
    case_id :str
    question_answer_pair: dict

@app.post('/question_answer/create',status_code=201)
def question_answer_create(data:QuestionAnswerSchema,db: Session = Depends(get_db)):
    try:
        new_product = QuestionAnswer(**data.dict())
        db.add(new_product)
        db.commit()
        db.refresh(new_product)
        return ResponseModel(message=success_message, payload={"question_answer_id": new_product.id})
    except Exception as e:
        return HTTPException(status_code=400,detail=f"unable to create new record {e}")
    

@app.post("/templates/update", response_model=ResponseModel, tags=["templated"], status_code=201)
def create_or_update_template(templates: TemplateSchema):
    db = get_db()  # Assuming get_db() returns a new db session
    try:
        db_template = db.query(Template).filter_by(type=templates.template_type, name=templates.template_name).first()

        if db_template:
            db_template.content = templates.content
        else:
            new_template = Template(**templates.dict())
            db.add(new_template)
        
        db.commit()
        return ResponseModel(message="Template created or updated successfully")
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        db.close()


from fastapi import HTTPException, status
from sqlalchemy import update

@app.post(path="/appointment/change_agent", response_model=ResponseModel, tags=["appointment"])
async def change_appointment_agent(appointment_id: int, new_agent_id: int, reason: str, db: Session = Depends(get_db)):
    try:
        # Fetch the appointment details
        appointment_record = db.query(Appointment).filter(Appointment.id == appointment_id).first()
        
        if not appointment_record:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Appointment not found")

        old_agent_id = appointment_record.agent_id
        
        # Fetch customer_timezone from agent_schedule
        agent_schedule_record = db.query(AgentSchedule).filter(AgentSchedule.appointment_id == appointment_id).first()
        print(agent_schedule_record)
        
        if not agent_schedule_record:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent schedule record not found")
        
        customer_timezone = agent_schedule_record.customer_timezone
        appointment_description = agent_schedule_record.appointment_description
        
        # Update agent_schedule record
        query = text("""
            UPDATE genpact.agent_schedule 
            SET status = 'agent_changed', appointment_id = null, reason = :reason
            WHERE appointment_id = :appointment_id
        """)
        db.execute(query, {"reason": reason, "appointment_id": appointment_id})
        
        # Delete the old appointment record
        db.delete(appointment_record)
        
        # Create a new appointment record
        new_appointment = Appointment(
            customer_id=appointment_record.customer_id,
            agent_id=new_agent_id,
            created_at=datetime.now(),
            scheduled_at=appointment_record.scheduled_at,
            is_booked=True
        )
        db.add(new_appointment)
        db.commit()
        db.refresh(new_appointment)

        # Add new record to agent_schedule table for the new agent
        new_schedule_record = AgentSchedule(
            agent_id=new_agent_id,
            status='booked',
            customer_id=appointment_record.customer_id,
            start_time=appointment_record.scheduled_at.time(),
            end_time=(appointment_record.scheduled_at + timedelta(hours=1)).time(),
            date=appointment_record.scheduled_at.date(),
            appointment_id=new_appointment.id,
            customer_timezone=customer_timezone,  # Using customer_timezone from agent_schedule
            appointment_description=appointment_description
        )
        db.add(new_schedule_record)
        db.commit()

        # Fetching agent and customer details for emails
        agent_data = db.query(Agent).filter(Agent.id == new_agent_id).first()
        agent_email = agent_data.agent_email
        product_id = agent_data.product_id

        customer_data = db.query(Customer).filter(Customer.id == appointment_record.customer_id).first()
        Customer_email = customer_data.email_id
        case_id = customer_data.case_id

        # Sending emails
        send_email("Someshwar.Garud@genpact.com", Customer_email, f"Appointment Agent Changed - Case ID: {case_id}",
                   f""" 
Case ID: {case_id}
Your appointment agent has been changed. The new agent is now responsible for your case. 
For further details, please click the following link: 
https://main.d2el3bzkhp7t3w.amplifyapp.com/customer/appointmentDetails?appointment_id={new_appointment.id}
""")

        send_email("Someshwar.Garud@genpact.com", agent_email, f"Appointment Agent Changed - Case ID: {case_id}",
                   f""" 
Case ID: {case_id}
You have been assigned as the new agent for the appointment. 
For further details, please click the following link: 
https://main.d2el3bzkhp7t3w.amplifyapp.com/agent/appointmentDetails?appointment_id={new_appointment.id}
""")
        start_time_str = new_appointment.scheduled_at.strftime('%H:%M')
        end_time_str = (new_appointment.scheduled_at + timedelta(hours=1)).strftime('%H:%M')
        events = [
            {
                "event_name": "Appointment Agent Changed",
                "event_details": {
                    "email": "",
                    "details": f"Agent changed from {old_agent_id} to {new_agent_id} for Case ID: {case_id} at {str(datetime.now())}",
                    "start_time": start_time_str,
                    "end_time": end_time_str,
                    'date': str(appointment_record.scheduled_at.date())
                },
                "timestamp": str(datetime.now()),
                "case_id": case_id,
                "status": "Agent Changed"
            },
            {
                "event_name": "Appointment Details Updated",
                "event_details": {
                    "email": "",
                    "details": f"Appointment details updated for Case ID: {case_id} at {str(datetime.now())}",
                    "start_time": start_time_str,
                    "end_time": end_time_str,
                    'date': str(new_appointment.scheduled_at.date())
                },
                "timestamp": str(datetime.now()),
                "case_id": case_id,
                "status": "Appointment Updated"
            }
        ]

        for event_data in events:
            # Convert time objects to strings for JSON serialization
            event_data["event_details"]["start_time"] = event_data["event_details"]["start_time"]
            event_data["event_details"]["end_time"] = event_data["event_details"]["end_time"]

            event = Event(**event_data)
            db.add(event)

        db.commit()
        
        return ResponseModel(message="Agent changed successfully", payload={"new_appointment_id": new_appointment.id})
    
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))