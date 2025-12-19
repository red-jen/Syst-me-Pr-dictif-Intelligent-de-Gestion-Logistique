"""
DAG Airflow: Monitoring et Validation du Pipeline de Streaming

Ce DAG ne démarre PAS les services (ils tournent déjà via Docker Compose),
mais il VÉRIFIE leur santé et VALIDE que les données circulent correctement.

Ordre des tâches:
1. Vérifier que FastAPI répond (génère des données)
2. Vérifier que le Bridge TCP est actif
3. Vérifier que Spark Streaming traite des données
4. Valider que PostgreSQL reçoit les prédictions
5. Valider que MongoDB reçoit les agrégations
6. Générer un rapport de santé
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import subprocess
import psycopg2
from pymongo import MongoClient
import json

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 11, 21),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# Configuration - services Docker internes
POSTGRES_HOST = "postgres"
POSTGRES_DB = "logistics"
POSTGRES_USER = "loguser"
POSTGRES_PASS = "Ren-ji24"
MONGO_URI = "mongodb://mongo:27017"
MONGO_DB = "logistics"
MONGO_COLLECTION = "aggregates"


def validate_postgres_data(**context):
    """Vérifie que PostgreSQL reçoit des prédictions récentes"""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASS
        )
        cur = conn.cursor()
        
        # Vérifier que la table existe
        cur.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = 'stream_predictions'
        """)
        if cur.fetchone()[0] == 0:
            raise Exception("Table 'stream_predictions' n'existe pas!")
        
        # Compter les prédictions récentes (dernières 5 minutes)
        cur.execute("""
            SELECT COUNT(*), MAX(event_ts::text)
            FROM stream_predictions
            WHERE event_ts::timestamp > NOW() - INTERVAL '5 minutes'
        """)
        count, latest = cur.fetchone()
        
        cur.close()
        conn.close()
        
        print(f"✓ PostgreSQL: {count} prédictions dans les 5 dernières minutes")
        print(f"✓ Dernière prédiction: {latest}")
        
        if count == 0:
            print("⚠ Attention: Aucune prédiction récente détectée")
        
        # Push to XCom for downstream tasks
        context['task_instance'].xcom_push(key='postgres_count', value=count)
        context['task_instance'].xcom_push(key='postgres_latest', value=str(latest))
        
        return True
    except Exception as e:
        print(f"✗ Erreur PostgreSQL: {e}")
        raise


def validate_mongodb_data(**context):
    """Vérifie que MongoDB reçoit des agrégations récentes"""
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        col = db[MONGO_COLLECTION]
        
        # Compter tous les documents
        total = col.count_documents({})
        
        # Compter les agrégations des dernières 10 minutes
        from datetime import datetime, timedelta
        cutoff = (datetime.utcnow() - timedelta(minutes=10)).isoformat()
        recent = col.count_documents({"window_start": {"$gte": cutoff}})
        
        # Récupérer un échantillon
        sample = col.find_one()
        
        print(f"✓ MongoDB: {total} documents au total")
        print(f"✓ MongoDB: {recent} agrégations dans les 10 dernières minutes")
        print(f"✓ Exemple: {sample}")
        
        # Push to XCom
        context['task_instance'].xcom_push(key='mongo_total', value=total)
        context['task_instance'].xcom_push(key='mongo_recent', value=recent)
        
        if total == 0:
            raise Exception("Aucune donnée dans MongoDB!")
        
        client.close()
        return True
    except Exception as e:
        print(f"✗ Erreur MongoDB: {e}")
        raise


def generate_health_report(**context):
    """Génère un rapport de santé du pipeline"""
    ti = context['task_instance']
    
    # Récupérer les métriques des tâches précédentes
    postgres_count = ti.xcom_pull(task_ids='validate_postgres', key='postgres_count') or 0
    postgres_latest = ti.xcom_pull(task_ids='validate_postgres', key='postgres_latest') or 'N/A'
    mongo_total = ti.xcom_pull(task_ids='validate_mongodb', key='mongo_total') or 0
    mongo_recent = ti.xcom_pull(task_ids='validate_mongodb', key='mongo_recent') or 0
    
    report = {
        "timestamp": datetime.utcnow().isoformat(),
        "status": "HEALTHY" if postgres_count > 0 and mongo_total > 0 else "DEGRADED",
        "postgres": {
            "recent_predictions": postgres_count,
            "latest_timestamp": postgres_latest
        },
        "mongodb": {
            "total_aggregates": mongo_total,
            "recent_aggregates": mongo_recent
        }
    }
    
    print("\n" + "="*60)
    print("RAPPORT DE SANTÉ DU PIPELINE")
    print("="*60)
    print(json.dumps(report, indent=2))
    print("="*60 + "\n")
    
    # Sauvegarder le rapport (pourrait être envoyé par email, Slack, etc.)
    ti.xcom_push(key='health_report', value=report)
    
    return report["status"]


with DAG(
    dag_id="streaming_pipeline_monitoring",
    default_args=default_args,
    schedule_interval="*/10 * * * *",  # Toutes les 10 minutes
    catchup=False,
    description="Surveillance et validation du pipeline de streaming en temps réel",
    tags=["streaming", "monitoring", "dataco"],
) as dag:

    # Tâche 1: Vérifier que FastAPI répond
    check_fastapi = BashOperator(
        task_id="check_fastapi_health",
        bash_command="""
        echo "Vérification de FastAPI..."
        response=$(curl -s -o /dev/null -w "%{http_code}" http://fastapi:8000)
        if [ "$response" = "200" ]; then
            echo "✓ FastAPI est actif (HTTP $response)"
            exit 0
        else
            echo "✗ FastAPI ne répond pas (HTTP $response)"
            exit 1
        fi
        """,
    )

    # Tâche 2: Vérifier que le Bridge TCP est actif
    check_bridge = BashOperator(
        task_id="check_bridge_tcp",
        bash_command="""
        echo "Vérification du Bridge TCP..."
        if nc -z -w 5 bridge 9999; then
            echo "✓ Bridge TCP actif sur port 9999"
            exit 0
        else
            echo "✗ Bridge TCP ne répond pas"
            exit 1
        fi
        """,
    )

    # Tâche 3: Vérifier que Spark Streaming traite des données (via PostgreSQL)
    # Note: On ne peut pas vérifier le conteneur Docker depuis Airflow
    # On vérifie plutôt que des données arrivent récemment dans PostgreSQL
    check_spark = BashOperator(
        task_id="check_spark_streaming",
        bash_command="""
        echo "✓ Vérification indirecte de Spark (via données PostgreSQL dans la prochaine tâche)"
        echo "  Si PostgreSQL reçoit des prédictions récentes, c'est que Spark fonctionne"
        exit 0
        """,
    )

    # Tâche 4: Valider PostgreSQL (Python)
    validate_postgres = PythonOperator(
        task_id="validate_postgres",
        python_callable=validate_postgres_data,
        provide_context=True,
    )

    # Tâche 5: Valider MongoDB (Python)
    validate_mongodb = PythonOperator(
        task_id="validate_mongodb",
        python_callable=validate_mongodb_data,
        provide_context=True,
    )

    # Tâche 6: Générer un rapport de santé
    health_report = PythonOperator(
        task_id="generate_health_report",
        python_callable=generate_health_report,
        provide_context=True,
    )

    # Définir l'ordre d'exécution des tâches
    check_fastapi >> check_bridge >> check_spark >> [validate_postgres, validate_mongodb] >> health_report
