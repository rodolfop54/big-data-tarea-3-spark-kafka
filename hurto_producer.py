import time
import json
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

departamentos = [
    "ATLÁNTICO",
    "ANTIOQUIA",
    "CUNDINAMARCA",
    "CESAR",
    "VALLE"
]

municipios = [
    "Barranquilla",
    "Sabanagrande",
    "Medellín",
    "Bogotá D.C.",
    "Valledupar",
    "Cali"
]

tipos_hurto = [
    "HURTO ABIGEATO",
    "HURTO PIRATERÍA TERRESTRE",
    "HURTO ENTIDADES FINANCIERAS"
]

while True:
    evento = {
        "departamento": random.choice(departamentos),
        "municipio": random.choice(municipios),
        "tipo_hurto": random.choice(tipos_hurto),
        "timestamp": int(time.time())
    }

    producer.send("hurto_eventos", value=evento)

    print(f"Enviado: {evento}")

    time.sleep(2)