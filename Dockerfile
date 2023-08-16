FROM python:3.11.1-alpine

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5000
COPY . .
CMD ["python", "main.py"]