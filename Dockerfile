FROM python:3.9
EXPOSE 5000

COPY ./ /app
WORKDIR /app

RUN pip install uvicorn
RUN pip install -r requirements.txt

CMD ["uvicorn", "bb_gateway.asgi:app", "--host", "0.0.0.0", "--port", "5000"]
