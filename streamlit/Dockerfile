FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends build-essential curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py /app/app.py
COPY .streamlit /app/.streamlit

EXPOSE 8501
ENV PORT=8501
CMD ["streamlit","run","app.py",
     "--server.address","0.0.0.0",
     "--server.port","8501",
     "--server.headless","true",
     "--server.enableCORS","false",
     "--server.enableXsrfProtection","false"]
