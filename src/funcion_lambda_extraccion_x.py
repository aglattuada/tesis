import os
import json
import tweepy
import boto3
import re
from datetime import datetime
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# --- CONFIGURACIÓN AWS ---
SECRET_NAME = os.environ.get("SECRET_NAME", "tesis/twitter/api_keys")
REGION_NAME = os.environ.get("AWS_REGION", "us-east-2")
DATA_TABLE_NAME = "TesisTwitterData"
STATE_TABLE_NAME = "TesisTwitterState"
TWEET_BUDGET_MENSUAL = 100

# --- LISTA DE POLITICOS Y MEDIOS A BUSCAR ---
MEDIOS = ['Reforma', 'El_Universal_Mx', 'latinus_us', 'Milenio', 'Pajaropolitico', 'AristeguiOnline', 'SinEmbargoMX']  # Aumentamos la lista
POLITICOS = {
    'Sheinbaum': ['claudia sheinbaum', '@claudiashein'],
    'Galvez': ['xóchitl gálvez', '@xochitlgalvez'],
    'Moreno': ['alito moreno', 'alejandro moreno', '@alitomorenoc'],
}

# --- INICIALIZACION DE CLIENTES Y TABLAS ---
session = boto3.session.Session();
secrets_client = session.client(service_name='secretsmanager', region_name=REGION_NAME);
dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME);
data_table = dynamodb.Table(DATA_TABLE_NAME);
state_table = dynamodb.Table(STATE_TABLE_NAME);
vader_analyzer = SentimentIntensityAnalyzer()


def get_secret():
    try:
        get_secret_value_response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        secret = json.loads(get_secret_value_response['SecretString']);
        return secret['X_BEARER_TOKEN']
    except Exception as e:
        print(f"ERROR: No se pudo obtener el secreto. {e}");
        raise e


api_client = tweepy.Client(get_secret(), wait_on_rate_limit=True)


def limpiar_texto(texto):
    texto = re.sub(r'http\S+|www\S+|https\S+', '', texto, flags=re.MULTILINE);
    texto = re.sub(r'\@\w+', '', texto);
    texto = re.sub(r'#\w+', '', texto);
    return texto.strip()


def lambda_handler(event, context):
    # 1. VERIFICAR EL PRESUPUESTO MENSUAL
    current_month_id = f"count-{datetime.utcnow().strftime('%Y-%m')}"
    tweets_recolectados_este_mes = 0
    try:
        response = state_table.get_item(Key={'id': current_month_id})
        if 'Item' in response:
            tweets_recolectados_este_mes = int(response['Item']['tweets_collected'])
    except Exception:
        pass

    print(f"Presupuesto mensual: {tweets_recolectados_este_mes}/{TWEET_BUDGET_MENSUAL} tuits recolectados este mes.")
    if tweets_recolectados_este_mes >= TWEET_BUDGET_MENSUAL:
        print("Cuota mensual alcanzada. Saliendo de la ejecución.")
        return {'statusCode': 200, 'body': 'Cuota mensual alcanzada.'}

    presupuesto_restante_en_ejecucion = TWEET_BUDGET_MENSUAL - tweets_recolectados_este_mes
    tweets_guardados_en_esta_ejecucion = 0

    for medio in MEDIOS:
        for politico_nombre, terminos_busqueda in POLITICOS.items():
            if presupuesto_restante_en_ejecucion <= 0: break

            search_id = f"search-{medio}_{politico_nombre}"
            query = f"from:{medio} ({' OR '.join(terminos_busqueda)}) -is:retweet"
            print(f"--- Buscando: {query} ---")

            last_seen_id = None
            try:
                response = state_table.get_item(Key={'id': search_id})
                if 'Item' in response: last_seen_id = response['Item']['last_seen_tweet_id']
            except Exception:
                pass

            try:
                # máximo 10 tuits o lo que quede del presupuesto
                tweets_a_pedir = min(10, presupuesto_restante_en_ejecucion)
                response = api_client.search_recent_tweets(query=query, since_id=last_seen_id,
                                                           max_results=tweets_a_pedir,
                                                           tweet_fields=["created_at", "public_metrics"])

                if not response.data: continue

                nuevos_tweets = response.data
                id_del_tuit_mas_reciente_del_lote = nuevos_tweets[0].id

                for tweet in nuevos_tweets:
                    texto_limpio = limpiar_texto(tweet.text.lower())
                    sentiment_score = vader_analyzer.polarity_scores(texto_limpio)['compound']
                    data_table.put_item(Item={'tweet_id': str(tweet.id), 'created_at': tweet.created_at.isoformat(),
                                              'autor_medio': medio, 'politico_mencionado': politico_nombre,
                                              'texto': tweet.text,
                                              'retweet_count': tweet.public_metrics['retweet_count'],
                                              'like_count': tweet.public_metrics['like_count'],
                                              'sentiment_score': str(sentiment_score)})

                # 2. ACTUALIZAR ESTADOS
                # Actualizar el contador mensual
                tweets_guardados_en_esta_ejecucion += len(nuevos_tweets)
                state_table.update_item(
                    Key={'id': current_month_id},
                    UpdateExpression="ADD tweets_collected :val",
                    ExpressionAttributeValues={':val': len(nuevos_tweets)},
                    ReturnValues="UPDATED_NEW"
                )
                # Actualizar el last_seen_id para esta búsqueda
                state_table.put_item(
                    Item={'id': search_id, 'last_seen_tweet_id': str(id_del_tuit_mas_reciente_del_lote)})

                presupuesto_restante_en_ejecucion -= len(nuevos_tweets)
                print(
                    f"Guardados {len(nuevos_tweets)} tuits. Presupuesto restante en esta ejecución: {presupuesto_restante_en_ejecucion}")

            except Exception as e:
                print(f"ERROR durante la búsqueda para {search_id}. Error: {e}");
                continue
        if presupuesto_restante_en_ejecucion <= 0: break

    print(f"Proceso completado. Se guardaron {tweets_guardados_en_esta_ejecucion} tuits en esta ejecución.")
    return {'statusCode': 200, 'body': json.dumps(f'Guardados: {tweets_guardados_en_esta_ejecucion} tuits.')}