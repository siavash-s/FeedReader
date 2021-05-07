FROM python:3.9-buster

WORKDIR /src

COPY requirement.txt requirement.txt

RUN pip install -r requirement.txt

COPY . .

RUN chmod +x wait-for-it.sh

CMD ["python", "main.py"]
