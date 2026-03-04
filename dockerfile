FROM python:3.11-slim

# OCR + PDF/image tooling
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr \
    poppler-utils \
    fonts-dejavu \
    libgl1 \
    libglib2.0-0 \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1

# Web service (adjust module if yours differs)
CMD ["sh", "-c", "gunicorn api.server:app --bind 0.0.0.0:${PORT} --workers 2 --threads 4"]
