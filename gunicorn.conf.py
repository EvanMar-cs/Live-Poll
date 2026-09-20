import os

# Flask-SocketIO runs in threading mode, so websockets need a threaded worker.
# The default sync worker is held by a single open websocket, so every other
# request hangs behind it.
worker_class = "gthread"

# Each open websocket occupies one thread, so this caps concurrent connections
# per worker.
threads = int(os.getenv("GUNICORN_THREADS", "100"))

# Keep at one worker unless REDIS_URL is set (gunicorn reads WEB_CONCURRENCY or
# -w for the worker count). Without Redis each worker has its own copy of the
# poll state, so votes never reach the clients on the other workers.
