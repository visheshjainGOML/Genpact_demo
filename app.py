from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import json
import uvicorn
from datetime import datetime, timedelta

app = FastAPI()
origins = [
   "http://localhost:3000", "*"
]
app.add_middleware(
   CORSMiddleware,
   allow_origins=origins,
   allow_credentials=True,
   allow_methods=["*"],
   allow_headers=["*"],
)

def get_db_connection():
    return psycopg2.connect(
        host="d1.c1mggvnkwauf.us-east-1.rds.amazonaws.com",
        dbname="postgres",  
        user="postgres",
        password="postgres123",
        cursor_factory=RealDictCursor
    )
class AgentDetail(BaseModel):
    agent_id: int
    agent_name: str

from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import json
import uvicorn
from datetime import datetime, timedelta

app = FastAPI()

class Customer(BaseModel):
    customer_id: int
    customer_name: str
    customer_mobile_no: str
    customer_email_id: str
    product_type: str

class BookingRequest(BaseModel):
    agent_id: int
    date: str
    start: str
    end: str
    customer: Customer

class TimeSlot(BaseModel):
    start: str
    end: str
    flagBooked: bool
    customer: Optional[Customer] = None

def get_db_connection():
    return psycopg2.connect(
        host="d1.c1mggvnkwauf.us-east-1.rds.amazonaws.com",
        dbname="postgres",  
        user="postgres",
        password="postgres123",
        cursor_factory=RealDictCursor
    )

def db_connection():
    try:
        conn = get_db_connection()
        yield conn
    finally:
        conn.close()

def generate_default_calendar():
    calendar = []
    start_time = datetime.strptime("09:00", "%H:%M")
    for _ in range(48):  # for 24 hours, assuming 30-minute slots
        end_time = start_time + timedelta(minutes=30)
        calendar.append({
            "start": start_time.strftime("%H:%M"),
            "end": end_time.strftime("%H:%M"),
            "flagBooked": False,
            "customer": None
        })
        start_time = end_time
    return calendar
@app.put("/book-slot/")
async def book_slot(booking_request: BookingRequest, conn: psycopg2.extensions.connection = Depends(db_connection)):
    cur = conn.cursor()
    try:
        # Try to fetch any existing calendar for the given agent and date
        cur.execute("""
            SELECT calendar FROM booking_system.agent_booking
            WHERE agent_id = %s AND date = %s
            """, (booking_request.agent_id, booking_request.date))
        result = cur.fetchone()

        # If there's no existing booking for this agent and date, use a default, empty calendar
        if not result:
            calendar = generate_default_calendar()
        else:
            # If there is an existing record, use the calendar from the database
            # Assuming psycopg2's RealDictCursor or similar is used, no need for json.loads()
            calendar = result['calendar']

        # Identify if the requested slot is already in the calendar and booked
        slot_found = False
        print(f"Requested slot: {booking_request.start} to {booking_request.end}")
        for slot in calendar:
            print(f"Available slot: {slot['start']} to {slot['end']}")

       
            if slot['start'] == booking_request.start and slot['end'] == booking_request.end:
                if slot['flagBooked']:
                    # The slot was found, and it is already booked
                    raise HTTPException(status_code=400, detail="Slot already booked.")
                slot['flagBooked'] = True
                slot['customer'] = booking_request.customer.dict()
                slot_found = True
                break

        if not slot_found:
            # This condition should not typically happen if your calendar generation covers all slots
            # This means the requested slot isn't valid (doesn't exist in your generated calendar)
            raise HTTPException(status_code=404, detail="Requested slot is invalid or does not exist.")

        # Prepare the calendar for database insertion/update
        calendar_json_string = json.dumps(calendar)

        if not result:
            # If no existing booking, insert a new one
            cur.execute("""
                INSERT INTO booking_system.agent_booking (agent_id, agent_name, product_type, date, calendar)
                VALUES (%s, %s, %s, %s, %s)
                """, (
                    booking_request.agent_id,
                    'Default Agent Name',  # Placeholder, adjust as necessary
                    booking_request.customer.product_type,
                    booking_request.date,
                    calendar_json_string
                ))
        else:
            # Update the existing booking
            cur.execute("""
                UPDATE booking_system.agent_booking
                SET calendar = %s
                WHERE agent_id = %s AND date = %s
                """, (calendar_json_string, booking_request.agent_id, booking_request.date))

        conn.commit()
        return {"message": "Booking confirmed."}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()


@app.get("/slots/{agent_id}/{date}/", response_model=List[TimeSlot])
async def get_slots(agent_id: int, date: str, conn: psycopg2.extensions.connection = Depends(db_connection)):
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT calendar FROM booking_system.agent_booking
            WHERE agent_id = %s AND date = %s
            """, (agent_id, date))
        result = cur.fetchone()

        # If no booking information is found for the agent and date, return the default calendar
        if not result or not result['calendar']:
            calendar = generate_default_calendar()
        else:
            # If booking information is found, check if calendar needs parsing
            calendar = result['calendar']
            if isinstance(calendar, str):
                # Parse the string to a Python object if necessary
                calendar = json.loads(calendar)

        slots = []
        for slot in calendar:
            # Directly use slot data to populate TimeSlot models
            slots.append(TimeSlot(
                start=slot['start'], 
                end=slot['end'], 
                flagBooked=slot.get('flagBooked', False),
                customer=slot.get('customer')
            ))

        return slots
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()




@app.get("/agents/", response_model=List[AgentDetail])
async def get_agents(conn: psycopg2.extensions.connection = Depends(db_connection)):
    cur = conn.cursor()
    try:
        # Assuming your table name is 'agent_booking' and it contains 'agent_id' and 'agent_name'
        cur.execute("SELECT DISTINCT agent_id, agent_name FROM booking_system.agent_booking")
        agents = cur.fetchall()
        
        # If no agents found, you might want to return an empty list or handle differently
        if not agents:
            return []

        # Transform the query results into a list of AgentDetail models
        agent_details = [AgentDetail(agent_id=agent['agent_id'], agent_name=agent['agent_name']) for agent in agents]
        return agent_details
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()


def run_server():
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
 
if __name__ == "__main__":
    
    run_server()
