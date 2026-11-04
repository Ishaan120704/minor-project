FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir streamlit==1.41.1 psycopg2-binary==2.9.10 pandas==2.2.3 plotly==5.24.1 streamlit-autorefresh==1.0.1
COPY app.py .
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501"]
