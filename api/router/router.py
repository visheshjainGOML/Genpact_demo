import random
from fastapi import FastAPI, HTTPException, Depends, Header, Query, status, APIRouter
# from grpc import StatusCode
from sqlalchemy import Table, create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.orm import declarative_base
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Literal, Optional
from sqlalchemy import text
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import boto3
from dotenv import load_dotenv
import os   
from datetime import datetime, time
import pytz

load_dotenv()

# --------- Constants -------------
schema = 'genpact'
SQLALCHEMY_DATABASE_URL = 'postgresql://postgres:postgres123@d1.c1mggvnkwauf.us-east-1.rds.amazonaws.com:5432/demo'#os.getenv('postgres_url')
success_message = "Request processed successfully "

# ----------- DB schema & connection -------------
# SQLAlchemy setup


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


# ---------- Utilities --------------------


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

async def send_email(email, user_id, product_id):
    try:
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
    except Exception as e:
        return HTTPException(status_code=500,details=f"Error sending email {e}")

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
    agent_email: int
    shift_from: time
    shit_to: time
    weekly_off: list[str]


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


class OriginalAppointmentSchema(BaseModel):
    customer_id: int
    call_status: str = None
    call_rating: int = None
    agent_id: int
    created_at: Optional[datetime] = None
    is_booked: bool = None
    appointment_description: str
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
        new_customer = Customer(**customer.dict())
        db.add(new_customer)
        db.commit()
        db.refresh(new_customer)
        # await send_email(email, user_id, product_id)
        return ResponseModel(message=success_message, payload={"customer_id": new_customer.id})
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
            appointment_description=existing_appointment['appointment_description'],
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
           INSERT INTO genpact.agent_schedule (status, customer_id, agent_id, start_time,end_time,date,appointment_id) VALUES ('booked', :customer_id, :agent_id, :start_time,:end_time,:date,:appointment_id);""")
        db.execute(
            query,
            {
                "customer_id": appointment.customer_id,
                "agent_id": appointment.agent_id,
                "start_time": start_time_obj,
                "end_time": end_time_obj,
                "date": date_obj,
                "appointment_id": new_appointment.id
            }
        )
        db.commit()
        return ResponseModel(message=success_message, payload={"appointment_id": new_appointment.id})
    except Exception as e:
        # raise e
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


# #-----------------------------------------------agent-------------------------------------------------------------------#


@app.get(path="/slots/{product_id}/{date}", response_model=ResponseModel, tags=["customer", "agent"])
async def get_agent_schedules(product_id: int, date: str, reschedule_time:str, db: Session = Depends(get_db)): # type: Literal["customer", "agent"] = Header(), agent_id: int = Query(default=None), db: Session = Depends(get_db)):
    try:
        Agents = db.query(Agent).filter(Agent.product_id == product_id).all()
        if not Agents:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")
        print(Agents)
        date_obj = datetime.strptime(date, '%d-%m-%y').date()
        time_obj = time.fromisoformat(reschedule_time)

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

        # Commit transaction
        db.commit()

        # Return success message
        return ResponseModel(message="Appointment canceled successfully.")
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

        # Commit transaction
        db.commit()
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
    # Create session
    try:
        appointments = db.query(Appointment).filter(
            Appointment.customer_id == customer_id).all()
        # schedules = db.query(AgentSchedule).filter(AgentSchedule.agent_id == 4).first()
        if not appointments:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")
        print(appointments)
        # agent_ids = db.query(Agent.id).filter(Agent.product_id == product_id).all()
        appointments =  format_db_response(appointments)

        new_data = []
        print(appointments, "\n\n")
        for i in appointments:
            print(i)
            data = i
            appointment_id = i['id']
            agent_id = i['agent_id']
            agent_info = db.query(Agent).filter(Agent.id == agent_id).first()
            schedules = db.query(AgentSchedule).filter(
                AgentSchedule.appointment_id == appointment_id).first()
            if not schedules:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")
            # Convert the object to a dictionary
            item_dict = schedules.__dict__
            agent_info_dict = agent_info.__dict__
            # Remove the attribute holding the reference to the database session
            # item_dict.pop('_sa_instance_state', None)

            print(item_dict)
            result = data | item_dict | agent_info_dict
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


@app.get("/appointments/list/{agent_id}", tags=['appointment'])
def get_agent_appointments(agent_id: int, db: Session = Depends(get_db)):
    # Connect to the PostgreSQL database

    # Execute raw SQL query
    query = text("""
        SELECT
    appointments.*,
    customer.username,
    customer.email_id,
    customer.mobile_no,
    schedule.start_time,
    schedule.end_time,
    schedule.date
FROM
    genpact.appointment AS appointments
JOIN
    genpact.customer ON appointments.customer_id = customer.id
JOIN
    genpact.agent_schedule AS schedule ON appointments.id = schedule.appointment_id
WHERE
    appointments.agent_id = :agent_id
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
        appointments = db.query(Appointment).filter(
            Appointment.id == appointment_id).all()
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
            try:
                update_query = db.query(Agent).filter(Agent.id == id).update(value)
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
        query = db.query(AgentSchedule, Customer,Appointment).\
    join(Customer, AgentSchedule.customer_id == Customer.id).\
    filter(AgentSchedule.status == "cancelled") # Optional: to load Customer objects along with AgentSchedule objects

        results = query.all()
        print(results)
        result = []
        for agent_schedule, customer,appointment in results:
            entry = {
                "id": agent_schedule.id,
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
                "appointment_description":appointment.appointment_description
            }
            if agent_id==agent_schedule.agent_id:
                result.append(entry)
        return result
       

    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching cancelled appointments: {str(e)}"
        )
    finally:
        # Close the database connection
        db.close()
