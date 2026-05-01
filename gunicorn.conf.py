"""
gunicorn.conf.py  — Production Gunicorn configuration
======================================================
Usage:
    gunicorn -c gunicorn.conf.py "app:app"

Or via Procfile:
    web: gunicorn -c gunicorn.conf.py "app:app"
"""

import multiprocessing
import os

# Binding 
bind    = f"0.0.0.0:{os.getenv('PORT', '8000')}"
backlog = 2048          # max queued connections waiting for a worker

# Workers 
# Rule of thumb: (2 × CPU cores) + 1
# gthread handles bursts better than sync for I/O-heavy Flask apps
worker_class = "gthread"
workers      = int(os.getenv("WEB_CONCURRENCY", multiprocessing.cpu_count() * 2 + 1))
threads      = int(os.getenv("GUNICORN_THREADS", 2))   # threads per worker

# Timeouts 
timeout      = 60       # kill worker if no response in 60 s
graceful_timeout = 30   # wait 30 s for in-flight requests on SIGTERM
keepalive    = 5        # keep-alive for persistent HTTP connections (seconds)

# Logging 
accesslog    = "-"      # stdout  → captured by Docker / systemd / Heroku
errorlog     = "-"      # stderr
loglevel     = os.getenv("LOG_LEVEL", "info")
access_log_format = (
    '{"time":"%(t)s","remote":"%({X-Forwarded-For}i)s",'
    '"method":"%(m)s","path":"%(U)s","status":%(s)s,"bytes":%(b)s,"ms":%(M)s}'
)

# Process naming
proc_name = "jkuat-scheduler"

# Preloading
# Loads app code once in the master process before forking workers.
# Saves RAM via copy-on-write; also catches import errors at startup.
preload_app = True

# Server hooks
def on_starting(server):
    server.log.info("Gunicorn master starting — %s workers × %s threads", workers, threads)


def worker_exit(server, worker):
    server.log.info("Worker exited (pid %s)", worker.pid)