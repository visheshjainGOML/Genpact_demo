from flask import Flask, request, jsonify, abort
from pydantic import BaseModel, ValidationError, validator
from typing import List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import json
import os

app = Flask(__name__)

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
        # You might want to add more validation logic here
        return value

class TimeSlot(BaseModel):
    start: str
    end: str
    flagBooked: bool
    customer: Optional[Customer] = None

@app.route("/book-slot/", methods=["POST"])
def book_slot():
    try:
        booking_request = BookingRequest(**request.json)
    except ValidationError as e:
        abort(400, str(e))
    with get_db_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT calendar FROM booking_system.agent_booking
                WHERE agent_id = %s AND date = %s
                """, (booking_request.agent_id, booking_request.date))
            result = cur.fetchone()
            if not result:
                abort(404, "Booking information not found.")
            calendar = result['calendar']
            slot_key = f"{booking_request.start}"
            if calendar[slot_key]['flagBooked']:
                abort(400, "Slot already booked.")
            calendar[slot_key]['flagBooked'] = True
            calendar[slot_key]['customer'] = booking_request.customer.dict()
            cur.execute("""
                UPDATE booking_system.agent_booking
                SET calendar = %s
                WHERE agent_id = %s AND date = %s
                """, (json.dumps(calendar), booking_request.agent_id, booking_request.date))
            conn.commit()
            return jsonify({"message": "Booking successful."})
        except Exception as e:
            conn.rollback()
            abort(500, str(e))
        finally:
            cur.close()

@app.route("/slots/<int:agent_id>/<date>/", methods=["GET"])
def get_slots(agent_id, date):
    with get_db_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT calendar FROM booking_system.agent_booking
                WHERE agent_id = %s AND date = %s
                """, (agent_id, date))
            result = cur.fetchone()
            if not result:
                abort(404, "Booking information not found.")
            calendar = result['calendar']
            slots = [TimeSlot(start=start_time, end=details['end'], flagBooked=details['flagBooked'], customer=details.get('customer')).dict() for start_time, details in calendar.items()]
            return jsonify(slots)
        except Exception as e:
            abort(500, str(e))
        finally:
            cur.close()

if __name__ == "__main__":
    app.run( port=8000, debug=True)
