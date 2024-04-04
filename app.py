from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import json
import uvicorn
from datetime import datetime, timedelta

app = FastAPI()

def get_db_connection():
    return psycopg2.connect(
        host="d1.c1mggvnkwauf.us-east-1.rds.amazonaws.com",
        dbname="postgres",  
        user="postgres",
        password="postgres123",
        cursor_factory=RealDictCursor
    )

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
    end: str  # New field for the end time
    customer: Customer


class TimeSlot(BaseModel):
    start: str
    end: str
    flagBooked: bool
    customer: Optional[Customer] = None

def db_connection():
    try:
        conn = get_db_connection()
        yield conn
    finally:
        conn.close()




def generate_default_calendar():
    start_time = datetime.strptime("09:00", "%H:%M")
    default_calendar = {}
    for _ in range(48):  # Creating 48 time slots, each 30 minutes long
        end_time = start_time + timedelta(minutes=30)
        slot_key = f"{start_time.strftime('%H:%M')}"
        default_calendar[slot_key] = {
            "start": start_time.strftime("%H:%M"),
            "end": end_time.strftime("%H:%M"),
            "flagBooked": False,
            "customer": None
        }
        start_time = end_time
    return default_calendar



@app.put("/book-slot/")
async def book_slot(booking_request: BookingRequest, conn: psycopg2.extensions.connection = Depends(db_connection)):
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT calendar FROM booking_system.agent_booking
            WHERE agent_id = %s AND date = %s
            """, (booking_request.agent_id, booking_request.date))
        result = cur.fetchone()

        # Check if a calendar exists for the agent on the given date, if not, generate a default calendar
        calendar = generate_default_calendar() if not result else result['calendar']

        slot_key = f"{booking_request.start}-{booking_request.end}"

        # If slot already booked or overlaps, raise an exception
        if slot_key in calendar and calendar[slot_key]['flagBooked']:
            raise HTTPException(status_code=400, detail="Slot already booked.")

        # Update or create the calendar entry
        calendar[slot_key] = {
            'flagBooked': True,
            'customer': booking_request.customer.dict(),
            'start': booking_request.start,  # Use the start time from the request
            'end': booking_request.end  # Use the end time from the request
        }

        # If result exists, update; otherwise, insert a new row
        if result:
            cur.execute("""
                UPDATE booking_system.agent_booking
                SET calendar = %s
                WHERE agent_id = %s AND date = %s
                """, (json.dumps(calendar), booking_request.agent_id, booking_request.date))
        else:
            cur.execute("""
                INSERT INTO booking_system.agent_booking (agent_id, agent_name, product_type, date, calendar)
                VALUES (%s, %s, %s, %s, %s)
                """, (
                    booking_request.agent_id,
                    "Default Agent Name",  # This should be dynamically determined or adjusted
                    booking_request.customer.product_type,
                    booking_request.date,
                    json.dumps(calendar)
                ))

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
        if not result:
            raise HTTPException(status_code=404, detail="Booking information not found.")

        calendar = result['calendar']
        slots = []
        for start_time, details in calendar.items():
            slots.append(TimeSlot(start=start_time, end=details['end'], flagBooked=details['flagBooked']))

        return slots
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()

def run_server():
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
 
if __name__ == "__main__":
    run_server()
