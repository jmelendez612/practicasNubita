# utils/logger.py
from datetime import datetime
from utils.config import LOG_PATH

def log_event(msg, path=LOG_PATH):
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linea = f"{fecha} - {msg}\n"
    with open(path, "a") as f:
        f.write(linea)
