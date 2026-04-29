from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from connection import send_to_event_hub, generate_uber_ride_confirmation

app = FastAPI(
    title="Urban Mobility Reservation Portal",
    description="Smart transit reservation event producer for real-time mobility analytics"
)

templates = Jinja2Templates(directory="templates")


@app.get("/")
def mobility_home(request: Request):
    return templates.TemplateResponse(
        "home.html",
        {
            "request": request,
            "page_title": "Urban Mobility Reservation Portal",
            "main_heading": "Welcome to the Urban Mobility Reservation Portal",
            "sub_heading": "Schedule seamless city transit with our intelligent mobility platform.",
            "cta_text": "Schedule Transit"
        }
    )


@app.get("/book")
def schedule_transit(request: Request):
    mobility_trip = generate_uber_ride_confirmation()
    event_status = send_to_event_hub(mobility_trip)

    return templates.TemplateResponse(
        "confirmation.html",
        {
            "request": request,
            "page_title": "Transit Request Confirmed",
            "main_heading": "Trip Scheduled Successfully",
            "sub_heading": "Your urban mobility request has been successfully processed. Thank you for choosing our smart transit platform.",
            "event_status": event_status
        }
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        reload=True
    )