import os
import json
import tweepy
import boto3
import re
from datetime import datetime
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


SECRET_NAME = os.environ.get("SECRET_NAME", "tesis/twitter/api_keys")
REGION_NAME = os.environ.get("AWS_REGION", "us-east-2")
DATA_TABLE_NAME = "TesisTwitterData"
STATE_TABLE_NAME = "TesisTwitterState"

MEDIOS = [
    "Reforma",
    "El_Universal_Mx",
    "latinus_us",
    "Milenio",
    "Pajaropolitico",
    "AristeguiOnline",
    "SinEmbargoMX",
]
POLITICOS = {
    "Sheinbaum": ["claudia sheinbaum", "@claudiashein"],
    "Galvez": ["xóchitl gálvez", "@xochitlgalvez"],
    "Moreno": ["alito moreno", "alejandro moreno", "@alitomorenoc"],
}
LISTA_DE_BUSQUEDAS = [
    (medio, politico, terminos)
    for medio in MEDIOS
    for politico, terminos in POLITICOS.items()
]

# --- Inicialización de clientes ---
session = boto3.session.Session()
secrets_client = session.client(service_name="secretsmanager", region_name=REGION_NAME)
dynamodb = boto3.resource("dynamodb", region_name=REGION_NAME)
data_table = dynamodb.Table(DATA_TABLE_NAME)
state_table = dynamodb.Table(STATE_TABLE_NAME)
vader_analyzer = SentimentIntensityAnalyzer()


def get_secret():
    try:
        get_secret_value_response = secrets_client.get_secret_value(
            SecretId=SECRET_NAME
        )
        secret = json.loads(get_secret_value_response["SecretString"])
        return secret["X_BEARER_TOKEN"]
    except Exception as e:
        print(f"ERROR: No se pudo obtener el secreto. {e}")
        raise e


api_client = tweepy.Client(
    get_secret(), wait_on_rate_limit=False
)


def limpiar_texto(texto):
    texto = re.sub(r"http\S+|www\S+|https\S+", "", texto, flags=re.MULTILINE)
    texto = re.sub(r"\@\w+", "", texto)
    texto = re.sub(r"#\w+", "", texto)
    return texto.strip()


def lambda_handler(event, context):
    # ID para guardar puntero/cursor en la tabla de estado
    CURSOR_ID = "search_cursor"

    # 1. OBTENER LA TAREA ACTUAL
    cursor_posicion = 0
    try:
        response = state_table.get_item(Key={"id": CURSOR_ID})
        if "Item" in response:
            cursor_posicion = int(response["Item"]["last_search_index"])
    except Exception:
        pass

    # Si el cursor se pasa de la lista, reinicia a 0
    if cursor_posicion >= len(LISTA_DE_BUSQUEDAS):
        cursor_posicion = 0
        print("Se ha completado un ciclo de búsquedas. Reiniciando cursor.")

    # Obtenemos la tarea para esta ejecución
    medio, politico_nombre, terminos_busqueda = LISTA_DE_BUSQUEDAS[cursor_posicion]

    # 2. EJECUTAR BÚSQUEDA (un medio y un político a la vez)
    search_id = f"search-{medio}_{politico_nombre}"
    query = f"from:{medio} ({' OR '.join(terminos_busqueda)}) -is:retweet"
    print(f"Ejecutando tarea #{cursor_posicion}: {query}")

    last_seen_id = None
    try:
        response = state_table.get_item(Key={"id": search_id})
        if "Item" in response:
            last_seen_id = response["Item"]["last_seen_tweet_id"]
    except Exception:
        pass

    try:
        response = api_client.search_recent_tweets(
            query=query,
            since_id=last_seen_id,
            max_results=10,
            tweet_fields=["created_at", "public_metrics"],
        )
        if response.data:
            id_del_tuit_mas_reciente = response.data[0].id
            for tweet in response.data:
                texto_limpio = limpiar_texto(tweet.text.lower())
                sentiment_score = vader_analyzer.polarity_scores(texto_limpio)[
                    "compound"
                ]
                data_table.put_item(
                    Item={
                        "tweet_id": str(tweet.id),
                        "created_at": tweet.created_at.isoformat(),
                        "autor_medio": medio,
                        "politico_mencionado": politico_nombre,
                        "texto": tweet.text,
                        "retweet_count": tweet.public_metrics["retweet_count"],
                        "like_count": tweet.public_metrics["like_count"],
                        "sentiment_score": str(sentiment_score),
                    }
                )

            # Actualizar el 'last_seen_id' para cada busqueda
            state_table.put_item(
                Item={
                    "id": search_id,
                    "last_seen_tweet_id": str(id_del_tuit_mas_reciente),
                }
            )
            print(f"Se encontraron y guardaron {len(response.data)} tuits.")
        else:
            print("No se encontraron tuits nuevos para esta búsqueda.")

    except Exception as e:
        print(f"ERROR durante la búsqueda. Error: {e}")

    # 3. ACTUALIZAR EL CURSOR PARA LA PRÓXIMA EJECUCIÓN
    siguiente_posicion = cursor_posicion + 1
    state_table.put_item(
        Item={"id": CURSOR_ID, "last_search_index": siguiente_posicion}
    )
    print(f"Tarea completada. El próximo turno será la tarea #{siguiente_posicion}.")

    return {
        "statusCode": 200,
        "body": json.dumps(f"Tarea #{cursor_posicion} completada."),
    }
