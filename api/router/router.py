import json
import random
import asyncio
from apscheduler.schedulers.background import BackgroundScheduler
import time
from fastapi import FastAPI, HTTPException, Depends, Header, Query, status, APIRouter
# from grpc import StatusCode
from sqlalchemy import Table, create_engine, desc
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.orm import declarative_base
from datetime import datetime
import re
import uuid
from urllib import response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Literal, Optional
from sqlalchemy import text
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import boto3
import csv
import io
import random
import tempfile
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
from starlette.responses import FileResponse, StreamingResponse
from fastapi.responses import FileResponse
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.scrypt import Scrypt
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes as asym_hashes
from cryptography.hazmat.primitives.asymmetric import utils
from cryptography.hazmat.primitives import constant_time
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import (
   decode_dss_signature,
   encode_dss_signature,
)
from cryptography.hazmat.primitives.serialization import (
   load_pem_private_key,
   load_pem_public_key,
)
from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
from cryptography.hazmat.primitives import padding as sym_padding
from base64 import b64encode, b64decode
from os import urandom

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


def generate_key():
   # Generate a random secret key
   return urandom(32)

def encrypt_data(data, key):
   # Encrypt the data
   iv = urandom(16)  # AES block size in CBC mode
   cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
   encryptor = cipher.encryptor()
   padder = sym_padding.PKCS7(128).padder()
   padded_data = padder.update(data.encode()) + padder.finalize()
   encrypted = encryptor.update(padded_data) + encryptor.finalize()
   return b64encode(iv + encrypted).decode()

def decrypt_data(encrypted_data, key):
   # Decrypt the data
   encrypted_data = b64decode(encrypted_data)
   iv = encrypted_data[:16]
   cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
   decryptor = cipher.decryptor()
   unpadder = sym_padding.PKCS7(128).unpadder()
   decrypted_padded = decryptor.update(encrypted_data[16:]) + decryptor.finalize()
   decrypted = unpadder.update(decrypted_padded) + unpadder.finalize()
   return decrypted.decode()
# Example usage

secret_key = generate_key()


def send_sms(phone_number, message):
    client_sms = boto3.client(
        'pinpoint-sms-voice-v2',
        region_name="us-east-1",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    # Initialize the boto3 client for Pinpoint

    # Your Pool ID from the Pinpoint SMS account
    pool_id = 'pool-a6a3c38142714bea86510ded50156a3d'
    

    response = client_sms.send_text_message(
        DestinationPhoneNumber=phone_number,
        OriginationIdentity=pool_id,
        MessageBody=message,
        MessageType="TRANSACTIONAL"
    )
    return response['MessageId']

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

def check_agent_availability(agent_id, appointment_data, db: Session = Depends(get_db)):
    try:
        # Retrieve agent's shift timings
        print("*******************", agent_id)
        print("^^^^^^^^^^^^", appointment_data)
        agent = db.query(Agent).filter(Agent.id == agent_id).first()
        print("*******************", agent)
        agent_shift_from = agent.shift_from
        print("#####################AGENT_SHIFT FROM", agent_shift_from)
        agent_shift_to = agent.shift_to
        print("#####################AGENT_SHIFT to", agent_shift_to)

        # Check if appointment slot falls within agent's shift timings
        appointment_start_time = datetime.strptime(appointment_data['start_time'], '%H:%M').time()
        print("#####################appointment_start_time", appointment_start_time)
        appointment_end_time = datetime.strptime(appointment_data['end_time'], '%H:%M').time()
        print("#####################appointment_end_time", appointment_end_time)
        

        if not (agent_shift_from <= appointment_start_time <= agent_shift_to and
                agent_shift_from <= appointment_end_time <= agent_shift_to):
            return False

        # Retrieve agent's leave dates
        agent_leave_dates = db.query(AgentLeave).filter(AgentLeave.agent_id == agent_id).all()
        agent_leave_from = agent.leave_from
        print("#####################agent_leave_from", agent_leave_from)
        agent_leave_to = agent.leave_to
        print("#####################agent_leave_to", agent_leave_to)

        # Check if appointment slot falls within agent's leave dates
        appointment_date = datetime.strptime(appointment_data['date'], '%d-%m-%y').date()
        print("#####################appointment_date", appointment_date)

        for leave in agent_leave_dates:
            if leave.leave_from <= appointment_date <= leave.leave_to:
                return False

        if agent_leave_from <= appointment_date <= agent_leave_to:
            return False
        
        agent_leave_dates = db.query(Agent).filter(Agent.id == agent_id).all()
        agent_leave_from = agent.leave_from
        print("#####################agent_leave_from", agent_leave_from)
        agent_leave_to = agent.leave_to
        print("#####################agent_leave_to", agent_leave_to)

        # Check if appointment slot falls within agent's leave dates
        appointment_date = datetime.strptime(appointment_data['date'], '%d-%m-%y').date()

        for leave in agent_leave_dates:
            if leave.leave_from <= appointment_date <= leave.leave_to:
                return False

        if agent_leave_from <= appointment_date <= agent_leave_to:
            return False

        return True

    except Exception as e:
        # Handle any exceptions here
        return False
    

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
    
class QuestionAnswer(Base):
    __table__ = Table('questions_answers', Base.metadata,
                      schema=schema, autoload_with=engine)
    
class Question(Base):
    __table__ = Table('questions', Base.metadata,
                      schema=schema, autoload_with=engine)
    
class Frequency(Base):
    __table__ = Table('frequency', Base.metadata,
                      schema=schema, autoload_with=engine)


# ---------- Utilities --------------------

def watch_events(case_id):
    session = sessionmaker(bind=engine)()

    # Query for new events using ORM
    new_event = session.query(Event).filter(Event.case_id == case_id and Event.event_status == "Appointment Notification Sent").first()
    print("Threading")
    if new_event:
        print(case_id)  # Assuming there's a 'case_id' column in the Event model
        # Call the send_automatic_reminders function asynchronously (assuming it's asynchronous)
        print("#########")
        print(case_id)
        asyncio.run(send_automatic_reminders(case_id, session))

        session.close()

def get_ist_time():
    # Get UTC time
    utc_now = datetime.utcnow()
    
    # Define UTC timezone
    utc_timezone = pytz.timezone('UTC')
    
    # Localize UTC time
    utc_now = utc_timezone.localize(utc_now)
    
    # Convert to IST timezone
    ist_timezone = pytz.timezone('Asia/Kolkata')
    ist_now = utc_now.astimezone(ist_timezone)
    ist_now_time = ist_now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    
    return ist_now_time

def convert_from_ist_timezone(input_time, input_date, output_timezone):
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
    print("INSIDE CONVERT FROM IST: ", output_timezone)
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
    
    output_time_str = f"{output_time_format} {output_date_format}"
    
    return output_time_str

def convert_to_ist_timezone(input_time, input_date, input_timezone):
    # input_time = HH:MM:SS
    # input_date = DD/MM/YYYY
    # input_timezone = +/-HH:MM (UTC)
    
    # Convert input time and date to datetime object in input timezone
    time_format = "%H:%M"
    date_format = "%d-%m-%y"

    input_datetime_str = f"{input_date} {input_time}"
    input_datetime = datetime.strptime(input_datetime_str, f"{date_format} {time_format}")
    
    # Step 1: Convert input time to UTC
    input_offset_sign = input_timezone[0]
    input_offset_hours = int(input_timezone[1:3])
    input_offset_minutes = int(input_timezone[4:])

    if input_offset_sign == '-':
        input_time = input_datetime + timedelta(hours=input_offset_hours, minutes=input_offset_minutes)
    elif input_offset_sign == '+':
        input_time = input_datetime - timedelta(hours=input_offset_hours, minutes=input_offset_minutes)
    
    # Step 2: Convert UTC time to IST
    ist_offset_hours = 5
    ist_offset_minutes = 30
    ist_time = input_time + timedelta(hours=ist_offset_hours, minutes=ist_offset_minutes)
    
    # Format output time and date
    output_time_format = ist_time.strftime(time_format)
    output_date_format = ist_time.strftime(date_format)
    output_ist_offset = "+05:30"  # IST offset

    output_time_str = f"{output_time_format} {output_date_format}"
    
    return output_time_str


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
    role: str
    agent_activity: str = 'active'

class FrequencySchema(BaseModel):
    email_count: str
    email_interval: Optional[dict] = {}

class FrequencySchema_update(BaseModel):
    reschedule_count: int

class LoginSchema(BaseModel):
    username: str
    password: str

class FeedbackSchema(BaseModel):
    appointment_id: int
    rating: int


class CustomerSchema(BaseModel):
    username: str
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
    
class QuestionAnswerSchema(BaseModel):
    case_id :str
    question_answer_pair: dict

class AppointmentSchema(BaseModel):
    customer_id: int
    call_status: str = None
    call_rating: int = None
    is_booked: bool = None
    appointment_description: str
    # scheduled_at: Optional[datetime]
    date: str
    start_time: str
    end_time: str
    customer_timezone: str

class EventSchema(BaseModel):
    event_status: str
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

class TemplateSchema(BaseModel):
    template_name: str
    template_type: str
    content: str

class AgentInactiveInput(BaseModel):
    agent_id: int
    reason: str

class AgentAppointmentGenerator(BaseModel):
    username: str
    mobile_no: str
    email_id: str

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
#         localhost_link = f"https://localhost:3000/customer/bookAppointment?customer_id={customer_id}&product_id={product_id}"
#         return {"localhost_link": localhost_link}
#     except Exception as e:
#         return HTTPException(status_code=500,details=f"Error sending email {e}")

# ### ------------- Create  Endpoints -----------
@app.post(path="/customer/create", response_model=ResponseModel, tags=["customer"],status_code=201)
async def create_customer(customer: CustomerSchema, db: Session = Depends(get_db)):
    try:
        event_data = {}
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

            address_regex = r"Address\s*:\s*(.*?)(?=\s*State:|$)"
            state_regex = r"State\s*:\s*(.*)"

            address_match = re.search(address_regex, new_customer.email_body, re.IGNORECASE)
            state_match = re.search(state_regex, new_customer.email_body, re.IGNORECASE)

            address = address_match.group(1).strip() if address_match else None

            # If state is found, remove it and any characters that come after it
            if state_match:
                address = address.replace(state_match.group(0), '').strip()

            state = state_match.group(1).strip() if state_match else None
            new_customer.address = address
            new_customer.state = state
            time = get_ist_time()
            print("$%$%$%$%$%$%$%$%$%$%$%$%$%$%$", time)
            new_customer.created_at=get_ist_time()

            print("Address:", address)
            print("State:", state)
            db.add(new_customer)
            db.commit()
        except:
            event_data = {
            'event_status': 'Case Creation Failed',
            'event_name': 'There was error creating a Case',
            'event_details': {
                "email":"",
                "details":f"Case Creation Failed"
            },
            'timestamp': str(get_ist_time()),
            'case_id': case_id
        }
        event1 = Event(**event_data)
        db.add(event1)
        db.commit()

        db.refresh(new_customer)
        customer_id = new_customer.id
        email_author = str(new_customer.email_author).lower()
        encrypted_case_id = encrypt_data(case_id, secret_key)

        template_data = db.query(Template).filter(Template.template_name == "Scheduling", Template.template_type == "Email").order_by(desc(Template.id)).first()
        content = template_data.content
        try:
            send_email("prashant.kambi@genpact.com", new_customer.email_id, f"Schedule Your Appointment with Us - Case ID: {case_id}", f"""
Hi {new_customer.username}
{content}
                   
To ensure we can provide you with personalized attention, please use the following link to schedule an appointment at your convenience:
https://d2dwd3ks06zig3.cloudfront.net/customer/bookAppointment?customer_id={customer_id}&product_id={new_customer.product_id}&case_id={encrypted_case_id}
 
We look forward to meeting you and are here to assist you every step of the way.

Warm regards

Genpact Team """)
            
            send_email("prashant.kambi@genpact.com", email_author, f"New Case Creation Acknowledgement - Case ID: {case_id}", f""" 
Hi, a new case has been created for the following details:
Name: {new_customer.username}
Email ID: {new_customer.email_id}
Mobile: {new_customer.mobile_no}
                   
Warm regards""")
        except:
           event_data = {
            'event_status': 'Appointment Initiation Failed',
            'event_name': 'The email fetched from mail looks incorrect',
            'event_details': {
                "email":"",
                "details":f"Appointment Initiation Failed"
            },
            'timestamp': str(get_ist_time()),
            'case_id': case_id
        }
        try:
            print(new_customer.mobile_no)
            send_sms(str(new_customer.mobile_no),f"""Appointment scheduled - Case ID: {case_id}.""")
        except:
            pass
 
        
        event1 = Event(**event_data)
        db.add(event1)
        db.commit()
    
        event1_data = {
            'event_status': 'New Email Received',
            'event_name': 'A new email has been received',
            'event_details': {"email":f"""
From: prashant.kambi@genpact.com
To: {new_customer.email_id}

Subject: {new_customer.email_subject}

{new_customer.email_body}
""",
"details": f"New Email has been received from {new_customer.email_id} at {str(datetime.now())}"
            },
            'timestamp': str(get_ist_time()),
            'case_id': case_id
        }

        event2_data = {
            'event_status': 'Case Created',
            'event_name': 'A new Case has been created',
            'event_details': {
                "email":"",
                "details":f"A new Case has been created"
            },
            'timestamp': str(get_ist_time()),
            'case_id': case_id
        }

        event3_data = {
            "event_name": "Appointment notification is sent",
            "event_details": {
                "email": f"""
From: prashant.kambi@genpact.com
To: {new_customer.email_id}

Subject: Schedule Your Appointment with Us - Case ID: {case_id}

Case ID: {case_id}
Case ID: {new_customer.case_id} 
Thank you for connecting with us! We are excited to discuss how we can assist you further and explore potential solutions together.
                   
To ensure we can provide you with personalized attention, please use the following link to schedule an appointment at your convenience:
https://d2dwd3ks06zig3.cloudfront.net/customer/bookAppointment?customer_id={customer_id}&product_id={new_customer.product_id}&case_id={encrypted_case_id}
 
We look forward to meeting you and are here to assist you every step of the way.

Warm regards

Genpact Team
                """,
                "details": f"Appointment notification successfully sent to {new_customer.email_id} at {str(datetime.now())}"
            },
            "timestamp": str(get_ist_time()),
            "case_id": case_id,
            "event_status": "Appointment Notification Sent"
        }

        event4_data = {
            "event_name": "Customer Response is awaiting",
            "event_details": {
                "email": "",
                "details": f"Awaiting customer response for Case ID: {case_id} at {str(datetime.now())}"
            },
            "timestamp": str(get_ist_time()),
            "case_id": case_id,
            "event_status": "Awaiting Customer Response"
        }
        event1 = Event(**event1_data)
        event2 = Event(**event2_data)
        event3 = Event(**event3_data)
        event4 = Event(**event4_data)
        db.add(event1)
        db.add(event2)
        db.add(event3)
        db.add(event4)
        db.commit()
        db.refresh(new_customer)
        start_time = datetime.now()
        scheduler = BackgroundScheduler()
        scheduler.add_job(watch_events,  "date", run_date=start_time, args=[case_id])
        scheduler.start()

        start_time = datetime.now()


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
        print("INSIDE CREATE APPOINTMENT: ")
        existing_appointment = appointment.dict()
        existing_appointment['start_time'] = convert_to_ist_timezone(existing_appointment['start_time'], existing_appointment['date'], existing_appointment['customer_timezone'])
        print("start-time", existing_appointment['start_time'])
        existing_appointment['end_time'] = convert_to_ist_timezone(existing_appointment['end_time'], existing_appointment['date'], existing_appointment['customer_timezone'])
        existing_appointment['start_time'] = existing_appointment['start_time'].split(' ')
        existing_appointment['date'] = existing_appointment['start_time'][1]
        existing_appointment['start_time'] = existing_appointment['start_time'][0]
        existing_appointment['end_time'] = existing_appointment['end_time'].split(' ')[0]
        print("EXISTING APPOINTMENT:*****************", existing_appointment)

        # Check if the user has a pending appointment
        query = text("""SELECT COUNT(*) AS num_columns FROM genpact.agent_schedule WHERE status = 'booked' AND customer_id = :customer_id""")
        data = db.execute(query, {"customer_id": existing_appointment['customer_id']})
        result = data.fetchone()

        db.commit()
        if result[0] > 0:
            return ResponseModel(message="You already have an existing appointment. You cannot book one more until that is closed")

        # Format date to "YYYY-MM-DD"
        formatted_date = datetime.strptime(existing_appointment['date'], '%d-%m-%y').strftime('%Y-%m-%d')

        # Find available agents using round-robin method
        query = text("""SELECT agent_id FROM genpact.agent_schedule ags JOIN genpact.agent ag ON ag.id = ags.agent_id WHERE ags.status = 'booked' AND ag.role = 'agent' AND ag.agent_activity = 'active' AND ags.date = :date AND ags.start_time <= :end_time AND ags.end_time >= :start_time""")
        data = db.execute(query, {
            "date": formatted_date,
            "start_time": existing_appointment['start_time'],
            "end_time": existing_appointment['end_time']
        })
        booked_agents = set(row[0] for row in data.fetchall())

        # Get all active agents
        query = db.query(Agent.id).filter(Agent.role == 'agent', Agent.agent_activity == 'active').all()
        all_agents = set(row[0] for row in query)

        # Filter available agents by removing booked agents
        available_agents = list(all_agents - booked_agents)

        if not available_agents:
            return ResponseModel(message="No agents available for the selected slot. Please choose another time.")

        final_available_agents = []

        # Check shift and leave dates for each available agent
        for agent_id in available_agents:
            agent_data = db.query(Agent).filter(Agent.id == agent_id).first()

            agent_shift_from = agent_data.shift_from
            agent_shift_to = agent_data.shift_to
            appointment_start_time = time.fromisoformat(existing_appointment['start_time'])
            appointment_end_time = time.fromisoformat(existing_appointment['end_time'])

            # Check if the appointment is within agent's shift
            if not (agent_shift_from <= appointment_start_time <= agent_shift_to and agent_shift_from <= appointment_end_time <= agent_shift_to):
                continue

            # Check if the agent has any leave on the appointment date
            agent_leave_dates = db.query(AgentLeave).filter(AgentLeave.agent_id == agent_id).all()
            appointment_date = datetime.strptime(existing_appointment['date'], '%d-%m-%y').date()

            for leave_info in agent_leave_dates:
                leave_from = leave_info.leave_from.date()
                leave_to = leave_info.leave_to.date()

                if leave_from <= appointment_date <= leave_to:
                    break  # Agent has leave on this date, skip to next agent
            else:
                final_available_agents.append(agent_id)

        if not final_available_agents:
            return ResponseModel(message="No agents available for the selected slot. Please choose another time.")

        # Implement round-robin to select an agent (simple round-robin, you can modify for fairness)
        selected_agent_id = final_available_agents[0]
        print("INSIDE CREATE APPOINTMENT: ")
        # Modify string into proper datatype
        new_appointment = OriginalAppointmentSchema(
            customer_id=existing_appointment['customer_id'],
            agent_id=selected_agent_id,
            created_at=datetime.now(),
            scheduled_at=datetime.strptime(
                existing_appointment['date'] + ' ' + existing_appointment['start_time'], '%d-%m-%y %H:%M')
        )


        date_obj = datetime.strptime(existing_appointment['date'], '%d-%m-%y').date()
        start_time_obj = time.fromisoformat(existing_appointment['start_time'])
        end_time_obj = time.fromisoformat(existing_appointment['end_time'])

        new_appointment = Appointment(**new_appointment.dict())
        db.add(new_appointment)
        db.commit()
        db.refresh(new_appointment)
        print("INSIDE CREATE APPOINTMENT: ")

        # Update agent_schedule status to "booked" for the corresponding appointment
        query = text("""
           INSERT INTO genpact.agent_schedule (status, customer_id, agent_id, start_time,end_time,date,appointment_id,customer_timezone,appointment_description) VALUES ('booked', :customer_id, :agent_id, :start_time,:end_time,:date,:appointment_id,:customer_timezone,:appointment_description);""")
        db.execute(
            query,
            {
                "customer_id": appointment.customer_id,
                "agent_id": selected_agent_id,
                "start_time": start_time_obj,
                "end_time": end_time_obj,
                "date": date_obj,
                "appointment_id": new_appointment.id,
                "customer_timezone": existing_appointment['customer_timezone'],
                "appointment_description": existing_appointment['appointment_description'],
            }
        )
        query = db.query(Agent).filter(Agent.id == selected_agent_id)
        agent_data = query.first()
        agent_email = agent_data.agent_email
        product_id = agent_data.product_id
        query = db.query(Customer).filter(Customer.id == existing_appointment['customer_id'] )
        customer_data = query.first()
        Customer_email= customer_data.email_id
        print("INSIDE CREATE APPOINTMENT: ")
        case_id = customer_data.case_id
        email_author = str(customer_data.email_author).lower()
        encrypted_case_id =  encrypt_data(case_id, secret_key)
        print("INSIDE CREATE APPOINTMENT: ", encrypted_case_id)
        template_data = db.query(Template).filter(Template.template_name == "Confirmation", Template.template_type == "Email").order_by(desc(Template.id)).first()
        content = template_data.content

        send_email("prashant.kambi@genpact.com", Customer_email, f"Confirmation of Your Scheduled Appointment - Case ID: {case_id}",f"""
Hi {customer_data.username}
{content}
To view the details of your appointment, please click the following link: https://d2dwd3ks06zig3.cloudfront.net/customer/bookedAppointment?customer_id={existing_appointment['customer_id']}&product_id=1&case_id={encrypted_case_id}
Should you need to reschedule or cancel your appointment, please use the links below at your convenience:
Reschedule Your Appointment - https://d2dwd3ks06zig3.cloudfront.net/customer/bookedAppointment?customer_id={existing_appointment['customer_id']}&product_id=1&case_id={encrypted_case_id}
Cancel Your Appointment - https://d2dwd3ks06zig3.cloudfront.net/customer/bookedAppointment?customer_id={existing_appointment['customer_id']}&product_id=1&case_id={encrypted_case_id}
If you have any specific requests or questions prior to our meeting, do not hesitate to contact us directly through this email.
We look forward to our conversation and are here to assist you with any questions you may have prior to our meeting.
Warm regards,
Genpact Team 
""",start_time_obj,end_time_obj,date_obj)
        try:
            send_sms(str(customer_data.mobile_no), f"Confirmation of Your Scheduled Appointment - Case ID: {case_id}.")
        except:
            pass

        send_email("prashant.kambi@genpact.com", agent_email, f"New Appointment Booked - Case ID: {case_id}", f""" 
Hi {agent_data.full_name}
We are pleased to inform you that a new appointment has been booked. Please log in to your agent portal to view the details and prepare for the upcoming meeting.
Quick Reminder:
Check the Appointment Date and Time: Ensure your schedule is updated.
                   
Review Customer Details: Familiarize yourself with the customer's requirements and previous interactions to provide a tailored experience.
Access your portal here: https://d2dwd3ks06zig3.cloudfront.net/  
Thank you for your dedication and hard work. Let's continue providing exceptional service to our clients!
Best Regards,
                   
Genpact Team
""",start_time_obj,end_time_obj,date_obj)
        
        send_email("prashant.kambi@genpact.com", email_author, f"Appointment creation acknowledgment - Case ID: {case_id}", f"""
Hi, a new appointmnet has been created for the following details:
Customer Name: {customer_data.username}
Customer Email ID: {customer_data.email_id}
Customer Mobile: {customer_data.mobile_no}
Agent Name: {agent_data.full_name}
Agent Email: {agent_data.agent_email}
                   
Warm regards""")


        event1_details = {
            "event_name": "The appointment confirmation is received",
            "event_details": {
                "email": "",
                "details": f"The appointment confirmation has been received for Case ID: {case_id} at {str(datetime.now())}",
                 "start_time":existing_appointment['start_time'],
                "end_time":existing_appointment['end_time'],
                'date':existing_appointment['date']
            },
            "timestamp": str(get_ist_time()),
            "case_id": case_id,
            "event_status": "Appointment Confirmation Received"
        }

        event2_details = {
            "event_name": "The case is ready for interview",
            "event_details": {
                "email": "",
                "details": f"The appointment with Case ID: {case_id} is ready for interview at {str(datetime.now())}",
                 "start_time":existing_appointment['start_time'],
                "end_time":existing_appointment['end_time'],
                'date':existing_appointment['date']
            },
            "timestamp": str(get_ist_time()),
            "case_id": case_id,
            "event_status": "Ready For Interview"
        }

        event1 = Event(**event1_details)
        event2 = Event(**event2_details)

        db.add(event1)
        db.add(event2)
        db.commit()
        return ResponseModel(message=success_message, payload={"appointment_id": new_appointment.id, "case_id":case_id})
    except Exception as e:
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
            agent_data = query.first()
            agent_email = agent_data.agent_email
            email_author = str(customer_data.email_author).lower()
            template_data = db.query(Template).filter(Template.template_name == "Cancellation", Template.template_type == "Email").order_by(desc(Template.id)).first()
            content = template_data.content

            send_email("prashant.kambi@genpact.com", Customer_email, f"Confirmation of Your Appointment Cancellation - Case ID: {case_id}", f"""
Hi {customer_data.username}
{content}
""")
            try:
                send_sms(str(customer_data.mobile_no),f"Confirmation of Your Appointment Cancellation - Case ID: {case_id}.")
            except:
                pass
            
            send_email("prashant.kambi@genpact.com", agent_email, f"appointment Cancelled - Case ID: {case_id}", f"""
Case ID: {agent_data.full_name}
Hello, your scheduled appointment has been cancelled""")
            
            send_email("prashant.kambi@genpact.com", email_author, f"Appointment cancellation acknowledgement - Case ID: {case_id}", f"""
Hi, the appointment has been cancelled for the following details:
Customer Name: {customer_data.username}
Customer Email ID: {customer_data.email_id}
Customer Mobile: {customer_data.mobile_no}
Agent Name: {agent_data.full_name}
Agent Email: {agent_data.agent_email}
                   
Warm regards""")
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
From: prashant.kambi@genpact.com
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
            "timestamp": str(get_ist_time()),
            "case_id": case_id,
            "event_status": "Appointment Cancelled"
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
        apt_data = db.query(AgentSchedule).filter(AgentSchedule.appointment_id == appointment_id).first()
        customer_timezone = apt_data.customer_timezone
        print("CUSTOMER TIMEZONE IN UPDATE APPOINTMENT API: ", customer_timezone)
        data = data.dict()
        data['start_time'] = convert_to_ist_timezone(data['start_time'], data['date'], customer_timezone)
        print("start-time", data['start_time'])
        data['end_time'] = convert_to_ist_timezone(data['end_time'], data['date'], customer_timezone)
        data['start_time'] = data['start_time'].split(' ')
        data['date'] = data['start_time'][1]
        data['start_time'] = data['start_time'][0]
        data['end_time'] = data['end_time'].split(' ')[0]
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
        customer_data = query.first()
        customer_id = customer_data.id
        case_id = customer_data.case_id
        encrypted_case_id = encrypt_data(case_id, secret_key)
        Customer_email= customer_data.email_id
        query = db.query(Agent).filter(Agent.id == agent_id)
        agent_data = query.first()
        agent_email = agent_data.agent_email
        print("111111111111111111111111111111111111")
         # Commit transaction
        event1_details = {
            "event_name": "Appointment has been rescheduled",
            "event_details": {
                "email": f"""
From: prashant.kambi@genpact.com
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
            "timestamp": str(get_ist_time()),
            "case_id": case_id,
            "event_status": "Appointment Rescheduled"
        }
        event2_details = {
            "event_name": "The case is ready for interview",
            "event_details": {
                "email": "",
                "details": f"The appointment with Case ID: {case_id} is ready for interview at {str(datetime.now())}",
            },
            "timestamp": str(get_ist_time()),
            "case_id": case_id,
            "event_status": "Ready For Interview"
        }
        
        new_event1 = Event(**event1_details)
        new_event2 = Event(**event2_details)
        db.add(new_event1)
        db.add(new_event2)
        db.commit()
        print("111111111111111111111111111111111111")
        template_data = db.query(Template).filter(Template.template_name == "Rescheduling", Template.template_type == "Email").order_by(desc(Template.id)).first()
        content = template_data.content

        send_email("prashant.kambi@genpact.com", Customer_email, f"Confirmation of Your Rescheduled Appointment - Case ID: {case_id}", f"""
Hi {customer_data.username}
Please click the following link: https://d2dwd3ks06zig3.cloudfront.net/customer/bookedAppointment?customer_id={customer_id}&product_id=1&case_id={encrypted_case_id}
{content}
                   """,start_time_obj,end_time_obj,date_obj)
        
        try:
            send_sms(str(customer_data.mobile_no),f"Confirmation of Your Rescheduled Appointment - Case ID: {case_id}")
        except: 
            pass
        
        send_email("prashant.kambi@genpact.com", agent_email, f"Appointment Rescheduled - Case ID: {case_id}", f"""
                   Hi {agent_data.full_name}
                   The booked appointment has been rescheduled""",start_time_obj,end_time_obj,date_obj)
        
        send_email("prashant.kambi@genpact.com", customer_data.email_author, f"Appointment updation acknowledgement - Case ID: {case_id}", f"""
Hi, the appointment has been rescheduled for the following details:

Customer Name: {customer_data.username}
Customer Email ID: {customer_data.email_id}
Customer Mobile: {customer_data.mobile_no}
Agent Name: {agent_data.full_name}
Agent Email: {agent_data.agent_email}
New slot time: {start_time_obj, end_time_obj}
New slot date: {date_obj}
                   
Warm regards""")

       
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
            start_time = convert_from_ist_timezone(start_time, date, timezone)
            print("START_TIME:", start_time)
            end_time = convert_from_ist_timezone(end_time, date, timezone)
            print("END_TIME:", end_time)
            start_time = start_time.split(' ')
            date = start_time[1]
            start_time = start_time[0]
            end_time = end_time.split(' ')[0]

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
    schedule.status,
    customer.username,
    customer.email_id,
    customer.mobile_no,
    customer.address,
    customer.state,
    customer.case_id,
    schedule.start_time,
    schedule.end_time,
    schedule.date,
    schedule.customer_timezone,
    schedule.appointment_description,
    latest_event.event_status,
    latest_event.timestamp AS "last_updated_date",
    customer.created_at
FROM
    genpact.appointment AS appointments
JOIN
    genpact.customer ON appointments.customer_id = customer.id
JOIN
    genpact.agent_schedule AS schedule ON appointments.id = schedule.appointment_id
JOIN 
    (
        SELECT
            e.case_id,
            e.event_status,
            e.timestamp,
            e.created_at
        FROM
            genpact.event AS e
        JOIN
            (
                SELECT
                    case_id,
                    MAX(timestamp) AS latest_timestamp
                FROM
                    genpact.event
                GROUP BY
                    case_id
            ) AS latest ON e.case_id = latest.case_id AND e.timestamp = latest.latest_timestamp
    ) AS latest_event ON customer.case_id = latest_event.case_id
WHERE
    appointments.agent_id = :agent_id
    AND schedule.status = 'booked'
ORDER BY
    schedule.date DESC;
    """)

    # Execute the query
    result = db.execute(query, {"agent_id": agent_id})
    appointments_with_schedule = []

    columns = result.keys()
    rows = result.fetchall()
    for row in rows:
        appointment = {col: val for col, val in zip(columns, row)}
        print("APPOINTMENT:", appointment)
        appointment['start_time'] = convert_from_ist_timezone(appointment['start_time'], appointment['date'], appointment['customer_timezone'])
        appointment['end_time'] = convert_from_ist_timezone(appointment['end_time'], appointment['date'],  appointment['customer_timezone'])
        appointment['start_time'] = appointment['start_time'].split(' ')[0]
        appointment['end_time'] = appointment['end_time'].split(' ')[0]
        appointments_with_schedule.append(appointment)

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
        results = db.query(Event).filter(Event.case_id == case_id).order_by(Event.timestamp)
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
async def send_reminder(case_id: str, reason: str, db: Session = Depends(get_db)):
    try:
        # Fetch customer details
        customer = db.query(Customer).filter(Customer.case_id == case_id).first()
        customer_id = customer.id
        product_id = customer.product_id
        
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
        start_time = slot_query.start_time
        date = slot_query.date
        customer_timezone = slot_query.customer_timezone
        start_time = convert_from_ist_timezone(start_time, date, customer_timezone)
        end_time = slot_query.end_time
        end_time = convert_from_ist_timezone(end_time, date, customer_timezone)
        print("START_TIME in send reminder: ", start_time)
        print("END_TIME in send reminder: ", end_time)
        start_time = start_time.split(' ')
        date = start_time[1]
        start_time = start_time[0]
        end_time = end_time.split(' ')[0]
        encrypted_case_id = encrypt_data(case_id, secret_key)

        # Call count_event_status function to get the count
        count_data = await count_event_status(case_id, "Reminder Sent", db)
        reminder_count = count_data[f"Count for Reminder Sent"] + 1
        print("REMINDER COUNT:::::::::::::::::::::::", reminder_count)
        template_data = db.query(Template).filter(Template.template_name == "Reminder", Template.template_type == "Email").order_by(desc(Template.id)).first()
        content = template_data.content
        
        send_email("prashant.kambi@genpact.com", ", ".join({email_id}), f"Appointment Reminder - Case ID: {case_id}", f"""
Hi {customer_name},
{content}
Your appointment is scheduled for {appointment_date} from {start_time} to {end_time}. Please make sure to attend your appointment on time.
Please use the below link for scheduling the appointment:
https://d2dwd3ks06zig3.cloudfront.net/customer/bookAppointment?customer_id={customer_id}&product_id={product_id}&case_id={encrypted_case_id}

Best Regards,
Genpact Team
                   """)
        try:
            send_sms(str(customer.mobile_no), f"""Appointment Reminder - Case ID: {case_id}.""")
        except:
            pass

        event_data = {
            'event_status': 'Reminder Sent',
            'event_name': f'Reminder Number : {reminder_count} has been sent',
            'event_details': {
                "email":"",
                "details":f"Reminder Number : {reminder_count}",
                "reminder_reason":reason
            },
            'timestamp': str(get_ist_time()),
            'case_id': case_id
        }
        
        event1 = Event(**event_data)
        db.add(event1)
        db.commit()
        
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
async def mark_appointment_as_completed(case_id: str, status_expected: str, reason: str, db: Session = Depends(get_db)):
    try:
        if status_expected=="Cancelled":
            customer_data = db.query(Customer).filter(Customer.case_id == case_id).first()
            id = customer_data.id
            # Update agent_schedule_data
            agent_schedule_data = db.query(AgentSchedule).filter(AgentSchedule.customer_id == id, AgentSchedule.appointment_id.is_not(None)).first()
            agent_schedule_data.status = "Cancelled"
            agent_schedule_data.reason = reason
            agent_schedule_data.appointment_id = None

            # Commit the agent_schedule_data changes
            db.commit()

            # Delete appointment record
            db.query(Appointment).filter(Appointment.customer_id == id).delete()

            # Commit the changes
            db.commit()

            event1_details = {
                "event_name": "Case has been successfully closed",
                "event_details": {
                    "email": "",
                    "details": f"""
The case with Case ID: {case_id} has been successfully closed at {str(datetime.now())}.
Reason: {reason}"""
                },
                "timestamp": str(get_ist_time()),
                "case_id": case_id,
                "event_status": status_expected
            }

            event1 = Event(**event1_details)

            db.add(event1)
            db.commit()
        
        elif status_expected=="Submitted":
            customer_data = db.query(Customer).filter(Customer.case_id == case_id).first()
            id = customer_data.id
            # Update agent_schedule_data
            agent_schedule_data = db.query(AgentSchedule).filter(AgentSchedule.customer_id == id, AgentSchedule.appointment_id.is_not(None)).first()
            agent_schedule_data.status = "Submitted"
            agent_schedule_data.reason = reason
            agent_schedule_data.appointment_id = None

            # Commit the agent_schedule_data changes
            db.commit()

            # Delete appointment record
            db.query(Appointment).filter(Appointment.customer_id == id).delete()

            # Commit the changes
            db.commit()

            event1_details = {
                "event_name": "Case has been successfully submitted",
                "event_details": {
                    "email": "",
                    "details": f"""
The case with Case ID: {case_id} has been successfully submitted at {str(datetime.now())}.
Reason: {reason}"""
                },
                "timestamp": str(get_ist_time()),
                "case_id": case_id,
                "event_status": status_expected
            }

            event1 = Event(**event1_details)

            db.add(event1)
            db.commit()

        else:
            customer_data = db.query(Customer).filter(Customer.case_id == case_id).first()
            id = customer_data.id

            # Update agent_schedule_data
            agent_schedule_data = db.query(AgentSchedule).filter(AgentSchedule.customer_id == id, AgentSchedule.appointment_id.is_not(None)).first()
            agent_schedule_data.status = "Awaiting Customer Response"
            agent_schedule_data.reason = reason
            agent_schedule_data.appointment_id = None

            # Commit the agent_schedule_data changes
            db.commit()

            # Delete appointment record
            db.query(Appointment).filter(Appointment.customer_id == id).delete()

            # Commit the changes
            db.commit()
            event1_details = {
                "event_name": "Case has been marked as Awaiting Customer Response",
                "event_details": {
                    "email": "",
                    "details": f"""
The case with Case ID: {case_id} has been marked as Awaiting Customer Response at {str(datetime.now())}.
Reason: {reason}"""
                },
                "timestamp": str(get_ist_time()),
                "case_id": case_id,
                "event_status": status_expected
            }

            event1 = Event(**event1_details)

            db.add(event1)

            db.commit()

        return ResponseModel(message="Appointment marked completed")
    
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
        date_format = "%d-%m-%Y"
       
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

        db.execute(
            update(Agent)
            .where(Agent.id == agent_id)
            .values(shift_from=source_data['shift_from'], shift_to=source_data['shift_to'])
        )
        db.commit()
        return ResponseModel(message="successfully updated")
    except Exception as e:
        # Rollback transaction in case of error
        db.rollback()

        # Raise HTTPException with error message
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error updating agent: {e}")
    finally:
        # Close session
        db.close()
    
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
def create_or_update_template(templates: TemplateSchema, db: Session = Depends(get_db)):
    try:
        db_template = db.query(Template).filter_by(template_name=templates.template_name, template_type=templates.template_type).first()
 
        if db_template:
            db_template.content = templates.content
        else:
            new_template = Template(**templates.dict())
            db.add(new_template)
 
        db.commit()
        return ResponseModel(message="Updated successfully")
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
        customer_id = appointment_record.customer_id
        
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
        encrypted_case_id = encrypt_data(case_id, secret_key)

        # Sending emails
        send_email("prashant.kambi@genpact.com", Customer_email, f"Appointment Agent Changed - Case ID: {case_id}",
                   f""" 
Hi {customer_data.username}
Your appointment agent has been changed. The new agent is now responsible for your case. 
For further details, please click the following link: 
https://d2dwd3ks06zig3.cloudfront.net/customer/bookedAppointment?customer_id={customer_id}&product_id=1&case_id={encrypted_case_id}
""")
        try:
            send_sms(str(customer_data.mobile_no),f"Appointment Agent Changed - Case ID: {case_id}")
        except:
            pass

        send_email("prashant.kambi@genpact.com", agent_email, f"Appointment Agent Changed - Case ID: {case_id}",
                   f""" 
Hi {agent_data.full_name}
You have been assigned as the new agent for the appointment. 
For further details, please click the following link: 
https://d2dwd3ks06zig3.cloudfront.net/agent/appointmentDetails?appointment_id={new_appointment.id}
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
                "timestamp": str(get_ist_time()),
                "case_id": case_id,
                "event_status": "Agent Changed"
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
                "timestamp": str(get_ist_time()),
                "case_id": case_id,
                "event_status": "Appointment Updated"
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


@app.get("/admin/appointments/list", tags=['appointment'])
def get_all_agent_appointments(db: Session = Depends(get_db)):
    # Connect to the PostgreSQL database

    # Execute raw SQL query
    query = text("""
        SELECT
    main_query.*
FROM
    (
        SELECT
            cust.case_id AS "case_id",
            schedule.date AS "appointment_date",
            schedule.start_time AS "start_time",
            schedule.customer_timezone AS "customer_timezone",
            schedule.end_time AS "end_time",
            cust.username AS "username",
            cust.email_id AS "email_id",
            cust.mobile_no AS "mobile_no",
            cust.address AS "address",
            cust.state AS "state",
            schedule.appointment_description AS "appointment_description",
            schedule.agent_id AS "agent_id",
            schedule.appointment_id AS "id",
            schedule.reason AS "comments",
            cust.product_id AS "products",
            agent.full_name AS "agent_name",
            event.event_status AS "event_status",
            event.timestamp AS "last_updated_date",
            cust.created_at AS "created_date",
            ROW_NUMBER() OVER (PARTITION BY cust.case_id ORDER BY event.timestamp DESC,event.id desc) AS row_num
        FROM
            genpact.customer AS cust
       LEFT JOIN
            genpact.agent_schedule AS schedule ON cust.id = schedule.customer_id
       LEFT JOIN
            genpact.event AS event ON cust.case_id = event.case_id
       LEFT JOIN
            genpact.agent AS agent ON schedule.agent_id = agent.id
    ) AS main_query
WHERE
    main_query.row_num = 1 and main_query."case_id" is not NULL
ORDER BY
    main_query."last_updated_date";
    """)
    print("Appointment Query")
    # Execute the query
    result = db.execute(query)
    appointments_with_schedule = []

    columns = result.keys()
    rows = result.fetchall()
    for row in rows:
        appointment = {col: val for col, val in zip(columns, row)}
        print("APPOINTMENT:", appointment)
        if appointment['start_time'] and appointment['end_time'] and appointment['appointment_date'] and appointment['customer_timezone']:
            appointment['start_time'] = convert_from_ist_timezone(appointment['start_time'], appointment['appointment_date'], appointment['customer_timezone'])
            appointment['end_time'] = convert_from_ist_timezone(appointment['end_time'], appointment['appointment_date'],  appointment['customer_timezone'])
            appointment['start_time'] = appointment['start_time'].split(' ')[0]
            appointment['end_time'] = appointment['end_time'].split(' ')[0]
            appointments_with_schedule.append(appointment)

    print(appointments_with_schedule, type(appointments_with_schedule))
    # Close the connection
    db.close()
    # result = filter_json_by_time(appointments_with_schedule)
    # return result

    return appointments_with_schedule


@app.post("/reminderfrequency/update", response_model=ResponseModel, tags=["frequency"], status_code=201)
def create_or_update_frequency(frequency: FrequencySchema, db: Session = Depends(get_db)):
    try:

        if str(len(frequency.email_interval)) != frequency.email_count:
            print(len(frequency.email_interval),frequency.email_count)
            raise HTTPException(status_code=400, detail="Number of email intervals does not match the email count")

        db_frequency = db.query(Frequency).first()
        if db_frequency:
            # If the record exists, update it
            db_frequency.email_count = frequency.email_count
            db_frequency.email_interval = frequency.email_interval
        else:
            # If the record doesn't exist, create a new one
            new_frequency = Frequency(email_count=frequency.email_count, email_interval=frequency.email_interval)
            db.add(new_frequency)

        db.commit()

        return ResponseModel(message="Frequency Updated successfully")
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        db.close()

@app.post("/appointments/report", tags=['report'])
def export_appointments_csv(db: Session = Depends(get_db)):
    try:
        # Fetch only the specified columns
        appointments = db.query(
            Appointment.id,
            Appointment.customer_id,
            Appointment.agent_id,
            Appointment.created_at,
            Appointment.scheduled_at
        ).all()

        # Create a temporary file to store the CSV
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as temp_file:
            csv_writer = csv.writer(temp_file)
            
            # Write the headers
            headers = ["id", "customer_id", "agent_id", "created_at", "scheduled_at"]
            csv_writer.writerow(headers)
            
            # Write the data rows
            for appointment in appointments:
                csv_writer.writerow(appointment)
            
            temp_file.flush()

        # Return the temporary file as a response
        return FileResponse(temp_file.name, filename="appointments.csv")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.post("/agents/report", tags=['report'])
def export_agents_csv(db: Session = Depends(get_db)):
    try:
        # Fetch only the specified columns
        result = db.query(
            AgentSchedule.agent_id,
            AgentSchedule.date,
            AgentSchedule.start_time,
            AgentSchedule.end_time,
            AgentSchedule.status,
            AgentSchedule.customer_id,
            AgentSchedule.appointment_id,
            AgentSchedule.reason,
            AgentSchedule.customer_timezone,
            AgentSchedule.appointment_description
        ).all()

        appointments_with_schedule = []

        # Manually specify the column names
        headers = [
            "agent_id",
            "date",
            "start_time",
            "end_time",
            "status",
            "customer_id",
            "appointment_id",
            "reason",
            "customer_timezone",
            "appointment_description"
        ]
        for row in result:
            appointment = {col: val for col, val in zip(headers, row)}  # Use headers instead of result.keys()
            appointment['start_time'] = convert_from_ist_timezone(appointment['start_time'], appointment['date'], appointment['customer_timezone'])
            appointment['end_time'] = convert_from_ist_timezone(appointment['end_time'], appointment['date'],  appointment['customer_timezone'])
            appointment['start_time'] = appointment['start_time'].split(' ')[0]
            appointment['end_time'] = appointment['end_time'].split(' ')[0]
            appointments_with_schedule.append(appointment)

        # Create a temporary file to store the CSV
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as temp_file:
            csv_writer = csv.writer(temp_file)
            
            # Write the headers
            csv_writer.writerow(headers)
            
            # Write the data rows
            for agent in appointments_with_schedule:
                # Create a list of values for each row using the specified order of columns
                row_data = [
                    agent["agent_id"],
                    agent["date"],
                    agent["start_time"],
                    agent["end_time"],
                    agent["status"],
                    agent["customer_id"],
                    agent["appointment_id"],
                    agent["reason"],
                    agent["customer_timezone"],
                    agent["appointment_description"]
                ]
                csv_writer.writerow(row_data)
            
            temp_file.flush()

        # Return the temporary file as a response
        return FileResponse(temp_file.name, filename="agents.csv")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.post("/events/report", tags=['report'])
def export_events_csv(db: Session = Depends(get_db)):
    try:
        # Fetch only the specified columns
        events = db.query(
            Event.event_status,
            Event.event_name,
            Event.timestamp,
            Event.event_details
        ).all()

        # Create a temporary file to store the CSV
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as temp_file:
            csv_writer = csv.writer(temp_file)
            
            # Write the headers
            headers = [
                "event_status",
                "event_name",
                "timestamp",
                "event_details"
            ]
            csv_writer.writerow(headers)
            
            # Write the data rows
            for event in events:
                csv_writer.writerow(event)
            
            temp_file.flush()

        # Return the temporary file as a response
        return FileResponse(temp_file.name, filename="events.csv")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    
@app.post("/agent_inactive", tags=['agent'])
def agent_inactive(input_data: AgentInactiveInput, db: Session = Depends(get_db)):
        # Retrieve the agent with the provided agent_id
    try:

        new_agent = AgentInactiveInput(**input_data.dict())

        print(new_agent)
        # Retrieve the agent with the provided agent_id
        # agent = db.query(Agent).filter(Agent.id == input_data.agent_id).first()
        agent = db.query(Agent).filter(Agent.id == new_agent.agent_id).first()

        if agent:

            agent.agent_activity = "inactive"
            agent.reason = input_data.reason



            db.commit()
            return {"message": f"Agent with ID {input_data.agent_id} has been marked as inactive."}
        else:
            raise HTTPException(status_code=404, detail=f"Agent with ID {input_data.agent_id} not found.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/attachments", tags=["customer"])
async def get_email_details(case_id: str, db: Session = Depends(get_db)):
    try:
        customer_record = db.query(Customer).filter(Customer.case_id == case_id).first()
        if not customer_record:
            raise HTTPException(status_code=404, detail="Customer record not found")
        from_email = customer_record.email_author
        subject = customer_record.email_subject
        body = customer_record.email_body
        print(from_email)
        print(subject)
        print(body)

        email = f"""
From: {from_email}

Subject: {subject}

{body}
"""
        return email
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))



# @app.post("/send_reminders/{case_id}", tags=["reminder"])
# async def send_reminders(case_id: str,db: Session = Depends(get_db)):
#     try:
#         # Get the timestamp from the event table where status is 'Appointment Notification Sent'
#         event = db.query(Event).filter(Event.status == 'Appointment Notification Sent', Event.case_id == case_id).first()
#         if event:
#             timestamp = event.timestamp
#             # Fetch email count and email interval from frequency table
#             frequency = db.query(Frequency).first()
#             if frequency:
#                 email_count = frequency.email_count
#                 email_interval = frequency.email_interval
#                 # Iterate through the email intervals and send reminders
#
#                 for interval_str in email_interval.values():
#                     # Extract numeric part of interval
#                     interval_hours = int(interval_str[:-3])
#                     print("Interval hours:", interval_hours)
#                     reminder_time = timestamp + timedelta(hours=interval_hours)
#                     print("Reminder date:", reminder_time.date())
#                     print("Reminder time:", reminder_time.time())
#                     timestamp = reminder_time
#                     # Send reminder email
#
#                     # email_subject = "Genpact Demo: Appointment Reminder"
#                     # email_body = f"Hi {customer.username},\n\nThis is a reminder for your appointment scheduled at {appointment.from_time} to {appointment.to_time}.\n\nRegards,\nYour Genpact Appointment Booking System"
#                     #
#                     # # Replace with customer's email address
#                     # to_email = customer.email_id
#                     #
#                     # # Send email reminder
#                     # ses_client.send_email(
#                     #     Source="noreply@demo.com",  # Replace with your verified SES sender email
#                     #     Destination={'ToAddresses': [to_email]},
#                     #     Message={
#                     #         'Subject': {'Data': email_subject},
#                     #         'Body': {'Text': {'Data': email_body}}
#                     #     })
#
#
#
#
#         else:
#             raise HTTPException(status_code=404, detail="No event found with status 'Appointment Notification Sent'")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error sending reminders: {e}")

    # return {"message": "Reminders sent successfully"}



@app.post("/reschedulefrequency/update", response_model=ResponseModel, tags=["frequency"], status_code=201)
def create_or_update_frequency(frequency: FrequencySchema_update, db: Session = Depends(get_db)):
    try:


        db_frequency = db.query(Frequency).first()
        if db_frequency:
            # If the record exists, update it
            db_frequency.reschedule_count = frequency.reschedule_count
        else:
            # If the record doesn't exist, create a new one
            new_frequency = Frequency(reschedule_count=frequency.reschedule_count)
            db.add(new_frequency)

        db.commit()

        return ResponseModel(message="Frequency Updated successfully")
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        db.close()

@app.post("/questions/Update", status_code=201)
async def update_questions(Questions: list[str], db: Session = Depends(get_db)):
    try:
        if not Questions:
            return HTTPException(status_code=400, detail="Questions list is empty")
        

        existed_questions = db.query(Question).all()
        for q in existed_questions:
            db.delete(q)
        print(Questions)
        for question_text in Questions:
            print(question_text)

            db_question = Question(question=question_text)

            print(db_question)
            db.add(db_question)
           
        db.commit()
        return {"message": "Questions saved successfully"}

    except Exception as e:
        db.rollback()
        return HTTPException(status_code=400, detail=str(e))
    
@app.post("/count/event_status", tags=["events"])
async def count_event_status(caseid: str, event_status:str, db: Session = Depends(get_db)):
    try:
        count = db.query(Event).filter(Event.case_id == caseid, Event.event_status == event_status).count()
        return {f"Count for {event_status}": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/count/check_reschedule_count", tags=["appointment"])
async def count_reschedule_status(case_id: str, db: Session = Depends(get_db)):
    try:
        case_id = decrypt_data(case_id, secret_key)
        print(case_id)
        frequency_entry = db.query(Frequency).first()

        count = db.query(Event).filter(Event.case_id == case_id, Event.event_status == "Appointment Rescheduled").count()

        if count > int(frequency_entry.reschedule_count):
            return {"You have exceed maximum number of rescheduling count, please register a new case"}


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/count/check_reminder_count", tags=["appointment"])
async def count_reminder_status(case_id: str, db: Session = Depends(get_db)):
    try:
        case_id = decrypt_data(case_id, secret_key)
        print(case_id)
        frequency_entry = db.query(Frequency).first()

        count = db.query(Event).filter(Event.case_id == case_id, Event.event_status == "Reminder Sent").count()

        if count > int(frequency_entry.email_count):
            return {"You have exceed maximum number of reminder count, please register a new case"}


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@app.post("/send_automatic_reminders/{case_id}", tags=["reminder"])
async def send_automatic_reminders(case_id: str, db: Session = Depends(get_db)):
    try:
        # Get the timestamp from the event table where status is 'Appointment Notification Sent'
        encrypted_case_id = encrypt_data(case_id, secret_key)
        event = db.query(Event).filter(Event.event_status == 'Appointment Notification Sent', Event.case_id == case_id).first()

        customer = db.query(Customer).filter(Customer.case_id == case_id).first()
        customer_id = customer.id
        product_id = customer.product_id

        # appointment_query = db.query(Appointment).filter(Appointment.customer_id == customer.id).first()

        # slot_query = db.query(AgentSchedule).filter(AgentSchedule.appointment_id == appointment_query.id).first()

        reminder_count = 0
        if event:

                timestamp = event.timestamp
                # Fetch email count and email interval from frequency table
                frequency = db.query(Frequency).first()
                if frequency:
                    email_count = frequency.email_count
                    email_interval = frequency.email_interval
                    # Iterate through the email intervals and send reminders

                    for interval_str in email_interval.values():
                        # Extract numeric part of interval
                        interval_hours = int(interval_str[:-3])
                        print("Interval hours:", interval_hours)
                        reminder_time = timestamp + timedelta(hours=interval_hours)
                        print("Reminder date:", reminder_time.date())
                        print("Reminder time:", reminder_time.time())
                        print(datetime.now())

                        time_difference = (reminder_time - datetime.now()).total_seconds()
                        reminder_count += 1

                        while time_difference > 0:
                            current_time = datetime.now()
                            time_difference = (reminder_time - current_time).total_seconds()





                        email_id = customer.email_id
                        customer_name = customer.username

                        # start_time = slot_query.start_time.strftime('%H:%M')
                        # end_time = slot_query.end_time.strftime('%H:%M')

                        # appointment_date = appointment_query.scheduled_at.strftime('%Y-%m-%d')

                        print("email_id:", email_id)

                        print("customer_name:", customer_name)

                        # print("start_time:", start_time)
                        #
                        # print("end_time:", end_time)

                        # print("appointment_date:", appointment_date)


                        # send_email("prashant.kambi@genpact.com", ", ".join({email_id}),
                        #            f"Appointment Reminder - Case ID: {case_id}", f"""
                        # Hi {customer_name},
                        #
                        # Your appointment is scheduled for {appointment_date} from {start_time} to {end_time}. Please make sure to attend your appointment on time.
                        #
                        # Best Regards,
                        # Genpact Team
                        #                    """)
                        template_data = db.query(Template).filter(Template.template_name == "Reminder", Template.template_type == "Email").order_by(desc(Template.id)).first()
                        content = template_data.content
                        confirm_appt = db.query(Event).filter(Event.event_status == 'Appointment Confirmation Received',
                                                       Event.case_id == case_id).first()
                        print(confirm_appt)
                        if not confirm_appt:
                            send_email("prashant.kambi@genpact.com", ", ".join({email_id}),
                                       f"Appointment Reminder - Case ID: {case_id}", f"""
Hi {customer_name},

{content}

Please use the below link for scheduling the appointment:
https://d2dwd3ks06zig3.cloudfront.net/customer/bookAppointment?customer_id={customer_id}&product_id={product_id}&case_id={encrypted_case_id}
Best Regards,
Genpact Team
""")

                            event_data = {
                                'event_status': 'Reminder Sent',
                                'event_name': f'Reminder Number : {reminder_count} has been sent',
                                'event_details': {
                                    "email": "",
                                    "details": f"Reminder Number : {reminder_count}"
                                },
                                'timestamp': str(get_ist_time()),
                                'case_id': case_id
                            }

                            event1 = Event(**event_data)
                            db.add(event1)
                            db.commit()

        else:
            raise HTTPException(status_code=404, detail="No event found with status 'Appointment Notification Sent'")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error sending reminders: {e}")

    return {"message": "Reminders sent successfully"}

# combined agent report
@app.post("/combined_agent/report", tags=['report'])
def export_combined_agent_csv(db: Session = Depends(get_db)):
    try:
        # Fetch the agents data using the provided SQL query
        agents_data = text("""
                        SELECT DISTINCT
                            ag.id AS "agent_id",
                            ag.full_name AS "agent_name",
                            ag.date_of_joining AS "joining_date",
                            al.leave_from AS "leave_from",
                            al.leave_to AS "leave_to",
                            ag.slot_time AS "slot_time",
                            ag.buffer_time AS "buffer_time",
                            ag.agent_email AS "email_id",
                            al.leave_type AS "leave_type",
                            ag.weekly_off AS "weekly_off",
                            ag.shift_from AS "shift_from",
                            ag.shift_to AS "shift_to"
                        FROM
                            genpact.agent AS ag
                        LEFT JOIN
                            genpact.agent_schedule AS agsch ON ag.id = agsch.agent_id
                        LEFT JOIN
                            genpact.agent_shifts AS sh ON ag.id = sh.agent_id
                        LEFT JOIN
                            genpact.agent_leave AS al ON ag.id = al.agent_id
       """)
        result = db.execute(agents_data)
        # Create a temporary file to store the CSV

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as temp_file:
            csv_writer = csv.writer(temp_file)

            # Write the headers
            headers = [
                "agent_id", "agent_name", "joining_date", "leave_from", "leave_to",
                "slot_time", "buffer_time", "email_id", "leave_type", "weekly_off",
                "shift_from", "shift_to"
            ]
            csv_writer.writerow(headers)

            # Write the data rows
            for agent_data in result:
                csv_writer.writerow(agent_data)

            temp_file.flush()

        # Return the temporary file as a response
        return FileResponse(temp_file.name, filename="combine_agents_data.csv")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@app.post(path="/agent/customer/create", response_model=ResponseModel, tags=["customer"],status_code=201)
async def create_customer(customer: AgentAppointmentGenerator, db: Session = Depends(get_db)):
    try:
        customer_data = customer.dict()
        new_customer = Customer(**customer_data)
        name = new_customer.username
        mobile_num = new_customer.mobile_no
        email_id = new_customer.email_id
        query = db.query(Customer).filter(Customer.username == name).filter(Customer.mobile_no == mobile_num).filter(Customer.email_id == email_id).first()
        case_id = query.case_id
        customer_id = query.id
        product_id = query.product_id
        encrypted_case_id = encrypt_data(case_id, secret_key)
        event_data = {
            'event_status': 'Agent Triggered Generate Appointment Scheduling Link',
            'event_name': 'An appointment scheduling link has been triggered by the agent',
            'event_details': {
                "email":"",
                "details":f"Agent Triggered Generate Appointment Scheduling Link to {email_id} at {str(datetime.now())}"
            },
            'timestamp': str(get_ist_time()),
            'case_id': case_id
        }
        event1 = Event(**event_data)
        db.add(event1)
        db.commit()
        
        return ResponseModel(message="Appointment link generated succesfully", payload={"link": f"https://d2dwd3ks06zig3.cloudfront.net/customer/bookAppointment?customer_id={customer_id}&product_id=1&case_id={encrypted_case_id}"})
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@app.post(path="/submit/question/", response_model=ResponseModel, tags=["customer"], status_code=201)
def check_values_in_dictionary(unique_case_id:str, db: Session = Depends(get_db)):
    try:
        query_result = db.query(QuestionAnswer).filter(
            QuestionAnswer.case_id == unique_case_id
        ).first()

        if query_result:
            question_answer_pair = query_result.question_answer_pair
            for value in question_answer_pair.values():
                if value is not None:
                    return {"result": True, "message": "True"}

            return {"result": False, "message": "False"}

        else:
            return {"result": False, "message": "False"}

    except Exception as e:
        print(f"An error occurred: {e}")
        return {"result": False, "message": "An error occurred"}  # Include the 'message' field
