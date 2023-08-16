import uvicorn
from apiServer import app
import logging

if __name__ == "__main__":
    logging.getLogger("uvicorn.error").setLevel(logging.INFO)
    uvicorn.run(app, host="0.0.0.0", port=8000)
