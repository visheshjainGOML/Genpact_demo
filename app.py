from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import json
import uvicorn

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

@app.put("/book-slot/")
async def book_slot(booking_request: BookingRequest, conn: psycopg2.extensions.connection = Depends(db_connection)):
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT calendar FROM booking_system.agent_booking
            WHERE agent_id = %s AND date = %s
            """, (booking_request.agent_id, booking_request.date))
        result = cur.fetchone()

        calendar = {}
        if result:
            calendar = result['calendar']
      
        slot_key = f"{booking_request.start}"

        # If slot already booked, we raise an exception
        if slot_key in calendar and calendar[slot_key]['flagBooked']:
            raise HTTPException(status_code=400, detail="Slot already booked.")

        # Update or create the calendar entry
        calendar[slot_key] = {
            'flagBooked': True,
            'customer': booking_request.customer.dict(),
            # Assuming an 'end' value is necessary, adjust as needed
            'end': "end_time_here"
        }

        if result:
            # Update existing record
            cur.execute("""
                UPDATE booking_system.agent_booking
                SET calendar = %s
                WHERE agent_id = %s AND date = %s
                """, (json.dumps(calendar), booking_request.agent_id, booking_request.date))
        else:
            # Insert new record
            cur.execute("""
                INSERT INTO booking_system.agent_booking (agent_id, date, calendar)
                VALUES (%s, %s, %s)
                """, (booking_request.agent_id, booking_request.date, json.dumps({slot_key: calendar[slot_key]})))

        conn.commit()
        return {"message": "Booking confirmed."}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()

def run_server():
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
 
if __name__ == "__main__":
    run_server()
