import os
import json
import tweepy
import boto3
from textblob import TextBlob

SECRET_NAME = "tesis/twitter/api_keys"
REGION_NAME = "us-east-1"
DATA_TABLE_NAME = "TesisTwitterData"
STATE_TABLE_NAME = "TesisTwitterState"

# Lista de usuarios de X a monitorear
TARGET_USERNAMES = [
    "Reforma",
    "El_Universal_Mx",
    "Milenio",
    "Excelsior",
    "AristeguiOnline",
    "ElFinanciero_Mx",
    "ProcesoMX",
    "ElEconomistaMX",
    "AnimalPolitico",
]

# --- Inicialización de clientes de AWS  ---
session = boto3.session.Session()
secrets_client = session.client(service_name="secretsmanager", region_name=REGION_NAME)
dynamodb = boto3.resource("dynamodb")
# Tablas de dynamodb para los datos y para el estado
data_table = dynamodb.Table(DATA_TABLE_NAME)
state_table = dynamodb.Table(STATE_TABLE_NAME)


def get_secret():
    """Obtiene el secreto API X desde AWS Secrets Manager"""
    try:
        get_secret_value_response = secrets_client.get_secret_value(
            SecretId=SECRET_NAME
        )
        secret = json.loads(get_secret_value_response["SecretString"])
        return secret["X_BEARER_TOKEN"]
    except Exception as e:
        print(f"Error al obtener el secreto: {e}")
        raise e


BEARER_TOKEN = get_secret()
api_client = tweepy.Client(BEARER_TOKEN)


def lambda_handler(event, context):
    """
    Punto de entrada de la función Lambda. Incluye lógica de estado para evitar duplicados.
    """
    print("Función con estado iniciada.")

    for username in TARGET_USERNAMES:
        print(f"--- Procesando usuario: {username} ---")
        last_seen_id = None

        # 1. LEER ESTADO: Obtener el último tweet_id visto para este usuario
        try:
            response = state_table.get_item(Key={"username": username})
            if "Item" in response:
                last_seen_id = response["Item"]["last_seen_tweet_id"]
                print(f"Último ID visto para {username}: {last_seen_id}")
        except Exception as e:
            print(
                f"No se pudo leer el estado para {username} (quizás es la primera vez). Error: {e}"
            )

        # 2. CONSULTAR CON CONTEXTO: Usar since_id si lo tenemos
        try:
            # max_results puede ser más alto, hasta 100 por llamada por restriccion de cuenta developer X
            response = api_client.get_users_tweets(
                id=api_client.get_user(username=username).data.id,
                max_results=100,
                tweet_fields=["created_at", "public_metrics"],
                since_id=last_seen_id,
            )

            if not response.data:
                print(f"No hay tuits nuevos para {username} desde el último chequeo.")
                continue

            new_tweets = response.data
            print(f"Se encontraron {len(new_tweets)} tuits nuevos para {username}.")

            # El tuit más reciente en la respuesta de la API siempre es el primero
            new_last_seen_id = new_tweets[0].id

            # 3. GUARDAR DATOS: Guardar los nuevos tweets en la tabla de dynamnodb
            for tweet in new_tweets:
                sentiment = TextBlob(tweet.text).sentiment.polarity
                tweet_data = {
                    "tweet_id": str(tweet.id),
                    "created_at": tweet.created_at.isoformat(),
                    "username": username,
                    "text": tweet.text,
                    "retweet_count": tweet.public_metrics["retweet_count"],
                    "like_count": tweet.public_metrics["like_count"],
                    "sentiment_score": str(sentiment),
                }
                data_table.put_item(Item=tweet_data)

            # 4. ACTUALIZAR ESTADO: Guardar el ID del tuit más reciente para la próxima ejecucion
            print(
                f"Actualizando el último ID visto para {username} a: {new_last_seen_id}"
            )
            state_table.put_item(
                Item={"username": username, "last_seen_tweet_id": str(new_last_seen_id)}
            )

        except Exception as e:
            print(f"Error procesando la API de X para {username}: {e}")
            continue

    return {
        "statusCode": 200,
        "body": json.dumps("Proceso con estado completado exitosamente!"),
    }
