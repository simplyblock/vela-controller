from fastapi import FastAPI
import logging
import signal
from threading import Thread, Event

# import your routers from other modules
from .backup import router as backup_router
from .ressources import router as ressources_router
from .backupmonitor import monitor

# Setup logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Global stop_event for thread termination
stop_event = Event()
POLL_INTERVAL = 5  # Poll interval for background worker, adjust as needed

# Background worker function
def background_worker():
    while not stop_event.is_set():
        try:
            monitor.run_once()  # Your monitoring logic here
        except Exception:
            logger.exception("Monitor failed")
        stop_event.wait(POLL_INTERVAL)

# Start the background worker in a separate thread
def start_background_worker():
    worker_thread = Thread(target=background_worker)
    worker_thread.daemon = True  # Automatically terminate on app exit
    worker_thread.start()

# Signal handler to gracefully shut down the background worker
def _handle_sig(signum, frame):
    stop_event.set()

# Register signal handlers
signal.signal(signal.SIGINT, _handle_sig)
signal.signal(signal.SIGTERM, _handle_sig)

app = FastAPI()

# Include routers for different modules
app.include_router(backup_router)
app.include_router(ressources_router)

# Start background worker when app starts
start_background_worker()

