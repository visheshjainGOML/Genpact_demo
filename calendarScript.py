from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel, validator
from typing import List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import json
import os
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

    @validator('start')
    def validate_time_format(cls, value):
        if not value: raise ValueError("Start time is required")
     
        return value

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

@app.post("/book-slot/")
async def book_slot(booking_request: BookingRequest, conn: psycopg2.extensions.connection = Depends(db_connection)):
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT calendar FROM booking_system.agent_booking
            WHERE agent_id = %s AND date = %s
            """, (booking_request.agent_id, booking_request.date))
        result = cur.fetchone()
        if not result:
            raise HTTPException(status_code=404, detail="Booking information not found.")

      
        calendar = result['calendar']
        slot_key = f"{booking_request.start}"

  
        if calendar[slot_key]['flagBooked']:
            raise HTTPException(status_code=400, detail="Slot already booked.")

        calendar[slot_key]['flagBooked'] = True
        calendar[slot_key]['customer'] = booking_request.customer.dict()

        cur.execute("""
            UPDATE booking_system.agent_booking
            SET calendar = %s
            WHERE agent_id = %s AND date = %s
            """, (json.dumps(calendar), booking_request.agent_id, booking_request.date))
        conn.commit()
        return {"message": "Booking successful."}
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
            slots.append(TimeSlot(start=start_time, end=details['end'], flagBooked=details['flagBooked'], customer=details.get('customer')))

        return slots
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()

def run_server():
 
    uvicorn.run("calendarScript:app", host="0.0.0.0", port=8000, reload=True)
 
 
if __name__ == "__main__":
    run_server()