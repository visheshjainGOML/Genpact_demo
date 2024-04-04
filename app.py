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
        cur.execute("""
            SELECT calendar FROM booking_system.agent_booking
            WHERE agent_id = %s AND date = %s
            """, (booking_request.agent_id, booking_request.date))
        result = cur.fetchone()

        # Directly use the calendar if it's already a Python list; no need for json.loads()
        if result and result['calendar']:
            calendar = result['calendar']
        else:
            calendar = generate_default_calendar()
        
        # Find the slot to book or update
        slot_found = False
        for slot in calendar:
            if slot['start'] == booking_request.start and slot['end'] == booking_request.end:
                if slot['flagBooked']:
                    raise HTTPException(status_code=400, detail="Slot already booked.")
                slot['flagBooked'] = True
                slot['customer'] = booking_request.customer.dict()  # Convert customer info directly
                slot_found = True
                break
        
        if not slot_found:
            raise HTTPException(status_code=404, detail="Slot does not exist.")
        # Before updating or inserting the calendar into the database, serialize it:
        calendar_json_string = json.dumps(calendar)

        # Then, use the serialized string in your SQL command


        
        # Update or insert the booking
        if result:
            cur.execute("""
                UPDATE booking_system.agent_booking
                SET calendar = %s
                WHERE agent_id = %s AND date = %s
                """, (calendar_json_string, booking_request.agent_id, booking_request.date))
        else:
            cur.execute("""
                INSERT INTO booking_system.agent_booking (agent_id, agent_name, product_type, date, calendar)
                VALUES (%s, %s, %s, %s, %s)
                """, (
                    booking_request.agent_id,
                    "Default Agent Name",  # Placeholder, adjust as needed
                    booking_request.customer.product_type,
                    booking_request.date,
                    calendar_json_string
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
        if not result or not result['calendar']:
            raise HTTPException(status_code=404, detail="Booking information not found.")

        # Directly use the calendar if it's already a Python object (list)
        # No need to use json.loads() if psycopg2 with RealDictCursor already decodes it
        calendar = result['calendar']

        slots = []
        for slot in calendar:
            slot_data = TimeSlot(
                start=slot['start'], 
                end=slot['end'], 
                flagBooked=slot['flagBooked'],
                customer=slot.get('customer')
            )
            slots.append(slot_data)

        return slots
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()




def run_server():
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
 
if __name__ == "__main__":
    run_server()
