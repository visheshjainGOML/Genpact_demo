import psycopg2

def connect_to_db():
    try:
        conn = psycopg2.connect(
            host="d1.c1mggvnkwauf.us-east-1.rds.amazonaws.com",
            dbname="postgres",  
            user="postgres",
            password="postgres123"
        )
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

def create_schema_and_tables():
    commands = (
        """
        CREATE SCHEMA IF NOT EXISTS booking_system;
        """,
        """
        CREATE TABLE IF NOT EXISTS booking_system.customer (
            customer_id SERIAL PRIMARY KEY,
            customer_name VARCHAR(255) NOT NULL,
            customer_mobile_no VARCHAR(20),
            customer_email_id VARCHAR(255) NOT NULL,
            product_type VARCHAR(100)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS booking_system.agent_booking (
            agent_id SERIAL PRIMARY KEY,
            agent_name VARCHAR(255) NOT NULL,
            product_type VARCHAR(255) NOT NULL,
            date DATE NOT NULL,
            calendar JSONB NOT NULL
        );
        """
    )
    
    conn = None
    try:
        conn = connect_to_db()
        cur = conn.cursor()
        for command in commands:
            cur.execute(command)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    create_schema_and_tables()
