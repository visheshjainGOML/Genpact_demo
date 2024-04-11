from fastapi import FastAPI
import uvicorn
import os
import sys
from router import router
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from log import setup_logger

logger=setup_logger()
# Create FastAPI app instance
app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "OPTIONS", "POST", "PUT", "DELETE", "PATCH"],
    allow_headers=["*"],
)


class loggerMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        # Log the request
        print("logger request:", request.method, request.url)
        logger.info(f"Request received: {request.method} {request.url}")

        # Call the next middleware or the request handler
        response = await call_next(request)

        # Log the response code
        print("logger response code:", response.status_code)
        logger.info(f"Response code: {response.status_code}")

        return response

# Add logger middleware to the app
app.add_middleware(loggerMiddleware)

# Include routers (assuming these are imported correctly)
app.include_router(router.app)




# Define server running function
def run_server():
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)

# Entry point
if __name__ == "__main__":
    run_server()
