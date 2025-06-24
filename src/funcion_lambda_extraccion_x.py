import os
import json
import tweepy
import boto3
from textblob import TextBlob


SECRET_NAME = os.environ.get("SECRET_NAME", "tesis/twitter/api_keys")
REGION_NAME = os.environ.get("AWS_REGION", "us-east-2")
DATA_TABLE_NAME = "TesisTwitterData"
STATE_TABLE_NAME = "TesisTwitterState"

# Define los actores políticos actuales a monitorear
MEDIOS = ['Reforma', 'El_Universal_Mx', 'Milenio', 'AristeguiOnline', 'SinEmbargoMX', 'Excelsior', 'ElFinanciero_Mx', 'ElEconomistaMX', 'Proceso', 'AnimalPolitico']
POLITICOS = {
    'Sheinbaum': ['@Claudiashein', '"Claudia Sheinbaum"'],
    'Galvez': ['@XochitlGalvez', '"Xóchitl Gálvez"'],
    'AMLO': ['@lopezobrador_', '"López Obrador"'],
    'Monreal': ['@RicardoMonrealA', '"Ricardo Monreal"'],
    'Ebrard': ['@m_ebrard', '"Marcelo Ebrard"'],
    'Cordero': ['@JoseCorderoMX', '"José Cordero"'],
    'Calleja': ['@CallejaMty', '"Calleja Monterrey"'],
    'Cortés': ['@MarkoCortes', '"Marko Cortés"'],
    'Castañeda': ['@CastanedaMiguel', '"Miguel Castañeda"'],
    'Zavala': ['@FelipeCalderon', '"Felipe Calderón"'],
    'Anaya': ['@RicardoAnayaC', '"Ricardo Anaya"'],
}

# --- Inicialización de clientes de AWS y Tweepy ---
session = boto3.session.Session()
secrets_client = session.client(service_name='secretsmanager', region_name=REGION_NAME)
dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
data_table = dynamodb.Table(DATA_TABLE_NAME)
state_table = dynamodb.Table(STATE_TABLE_NAME)


def get_secret():
    """Obtiene el Bearer Token desde AWS Secrets Manager."""
    try:
        get_secret_value_response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        secret = json.loads(get_secret_value_response['SecretString'])
        return secret['X_BEARER_TOKEN']
    except Exception as e:
        print(f"ERROR: No se pudo obtener el secreto desde Secrets Manager. {e}")
        raise e


api_client = tweepy.Client(get_secret(), wait_on_rate_limit=True)


def lambda_handler(event, context):
    """
    Función principal de Lambda.
    Busca tuits RECIENTES de medios sobre políticos y guarda el estado.
    """
    print("Iniciando recolección de tuits recientes...")

    for medio in MEDIOS:
        for politico_nombre, terminos_busqueda in POLITICOS.items():

            search_id = f"{medio}_{politico_nombre}"
            query_terminos = f"({' OR '.join(terminos_busqueda)})"
            query = f"from:{medio} {query_terminos} -is:retweet"

            print(f"--- Procesando búsqueda ID: {search_id} ---")

            last_seen_id = None
            try:
                response = state_table.get_item(Key={'search_query_id': search_id})
                if 'Item' in response:
                    last_seen_id = response['Item']['last_seen_tweet_id']
                    print(f"Búsqueda reanudada. Último ID visto: {last_seen_id}")
            except Exception as e:
                print(
                    f"Advertencia: No se pudo leer el estado para {search_id}. Se asumirá que es la primera vez. Error: {e}")

            try:
                paginator = tweepy.Paginator(
                    api_client.search_recent_tweets,
                    query=query,
                    since_id=last_seen_id,
                    tweet_fields=["created_at", "public_metrics"],
                    max_results=100
                ).flatten(limit=1000)

                nuevos_tweets_del_lote = list(paginator)

                if nuevos_tweets_del_lote:
                    print(f"Se encontraron {len(nuevos_tweets_del_lote)} tuits nuevos.")

                    for tweet in nuevos_tweets_del_lote:
                        sentiment = TextBlob(tweet.text).sentiment.polarity
                        tweet_data = {
                            'tweet_id': str(tweet.id),
                            'created_at': tweet.created_at.isoformat(),
                            'autor_medio': medio,
                            'politico_mencionado': politico_nombre,
                            'texto': tweet.text,
                            'retweet_count': tweet.public_metrics['retweet_count'],
                            'like_count': tweet.public_metrics['like_count'],
                            'sentiment_score': str(sentiment)
                        }
                        data_table.put_item(Item=tweet_data)

                    new_last_seen_id = nuevos_tweets_del_lote[0].id
                    print(f"Actualizando estado para '{search_id}' al nuevo ID: {new_last_seen_id}")
                    state_table.put_item(
                        Item={
                            'search_query_id': search_id,
                            'last_seen_tweet_id': str(new_last_seen_id)
                        }
                    )
                else:
                    print("No se encontraron tuits nuevos para esta consulta en esta ejecución.")

            except Exception as e:
                print(f"ERROR: Falla durante la búsqueda para {search_id}. Error: {e}")
                continue

    return {
        'statusCode': 200,
        'body': json.dumps('Proceso de recolección completado.')
    }