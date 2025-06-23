import os
import json
import tweepy
import boto3
from textblob import TextBlob

# --- CONFIGURACION AWS ---
SECRET_NAME = "tesis/twitter/api_keys"
REGION_NAME = "us-east-2"
DATA_TABLE_NAME = "TesisTwitterData"
STATE_TABLE_NAME = "TesisTwitterState"

# Listas de Medios y Candidatos
MEDIOS = [
    "Reforma",
    "El_Universal_Mx",
    "Milenio",
    "AristeguiOnline",
    "SinEmbargoMX",
    "Excélsior",
    "ElFinanciero_Mx",
    "Proceso",
    "El_Informador",
    "La_Opcion_MX",
    "AnimalPolitico",
    "ElEconomistaMX",
    "El_Pais_Mexico",
    "El_Informador_MX",
    "La_Jornada",
    "El_Sol_de_Mexico",
]
CANDIDATOS = {
    "AMLO": ["@lopezobrador_", '"Andrés Manuel"', '"López Obrador"', "AMLO"],
    "Anaya": ["@RicardoAnayaC", '"Ricardo Anaya"'],
    "Meade": ["@JoseAMeadeK", '"José Antonio Meade"'],
}

# Rango de fechas de la búsqueda histórica
START_TIME = "2018-03-30T00:00:00Z"
END_TIME = "2018-06-27T23:59:59Z"

# --- Inicialización de clientes de AWS ---
session = boto3.session.Session()
secrets_client = session.client(service_name="secretsmanager", region_name=REGION_NAME)
dynamodb = boto3.resource("dynamodb")
data_table = dynamodb.Table(DATA_TABLE_NAME)
state_table = dynamodb.Table(STATE_TABLE_NAME)


def get_secret():
    """Obtiene el secreto desde AWS Secrets Manager."""
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
api_client = tweepy.Client(BEARER_TOKEN, wait_on_rate_limit=True)


def lambda_handler(event, context):
    """
    Función Lambda que realiza una búsqueda histórica REANUDABLE.
    """
    print(f"Iniciando búsqueda de tuits entre {START_TIME} y {END_TIME}")

    for medio in MEDIOS:
        for candidato_nombre, terminos_busqueda in CANDIDATOS.items():

            # 1. CREAR ID ÚNICO PARA LA BÚSQUEDA Y CONSTRUIR LA CONSULTA
            search_id = f"{medio}_{candidato_nombre}"
            query_terminos = f"({' OR '.join(terminos_busqueda)})"
            query = f"from:{medio} {query_terminos} -is:retweet"

            print(f"--- Procesando búsqueda ID: {search_id} ---")
            print(f"Consulta: {query}")

            last_seen_id = None
            # 2. LEER ESTADO ANTERIOR PARA ESTA BÚSQUEDA ESPECÍFICA
            try:
                response = state_table.get_item(Key={"search_query_id": search_id})
                if "Item" in response:
                    last_seen_id = response["Item"]["last_seen_tweet_id"]
                    print(f"Búsqueda reanudada. Último ID visto: {last_seen_id}")
            except Exception as e:
                print(f"No se pudo leer el estado para {search_id}. Error: {e}")

            try:
                # 3. EJECUTAR BÚSQUEDA CON PAGINADOR Y since_id
                paginator = tweepy.Paginator(
                    api_client.search_all_tweets,
                    query=query,
                    start_time=START_TIME,
                    end_time=END_TIME,
                    since_id=last_seen_id,
                    tweet_fields=["created_at", "public_metrics"],
                    max_results=100,
                ).flatten(
                    limit=500
                )  # Límite de seguridad por ejecución de Lambda

                # Lista para guardar los tuits del lote antes de actualizar el estado
                nuevos_tweets_del_lote = list(paginator)

                if nuevos_tweets_del_lote:
                    print(f"Se encontraron {len(nuevos_tweets_del_lote)} tuits nuevos.")

                    # 4. GUARDAR LOS NUEVOS TUITS EN LA TABLA
                    for tweet in nuevos_tweets_del_lote:
                        sentiment = TextBlob(tweet.text).sentiment.polarity
                        tweet_data = {
                            "tweet_id": str(tweet.id),
                            "created_at": tweet.created_at.isoformat(),
                            "autor_medio": medio,
                            "candidato_mencionado": candidato_nombre,
                            "texto": tweet.text,
                            "retweet_count": tweet.public_metrics["retweet_count"],
                            "like_count": tweet.public_metrics["like_count"],
                            "sentiment_score": str(sentiment),
                        }
                        data_table.put_item(Item=tweet_data)

                    # 5. ACTUALIZAR ESTADO con el ID del tuit más reciente del LOTE ACTUAL
                    new_last_seen_id = nuevos_tweets_del_lote[0].id
                    print(
                        f"Actualizando estado para '{search_id}' al nuevo ID: {new_last_seen_id}"
                    )
                    state_table.put_item(
                        Item={
                            "search_query_id": search_id,
                            "last_seen_tweet_id": str(new_last_seen_id),
                        }
                    )
                else:
                    print(
                        "No se encontraron tuits nuevos para esta consulta en esta ejecución."
                    )

            except Exception as e:
                print(
                    f"Error fatal durante la paginación o guardado para {search_id}: {e}"
                )
                continue

    return {
        "statusCode": 200,
        "body": json.dumps("Proceso de búsqueda con estado completado."),
    }
