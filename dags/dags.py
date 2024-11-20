from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import pandas as pd
import praw

POSTGRES_CONN_ID = 'reddit_connection'
API_CONN_ID = 'reddit_api'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Configuration de l'API Reddit (PRAW)
USER_AGENT = "skon by /u/Ok-Mode4110"
REDDIT_CLIENT_ID = "QSNZNHJdo-uGehQOaW1xeA"
REDDIT_CLIENT_SECRET = "0P1Z8qRxxlcWVdbYO7R1LITGkgu2kg"

# DAG Definition
with DAG(dag_id='reddit_etl_pipeline',
         default_args=default_args,
         schedule_interval='@daily',  # Run daily
         catchup=False) as dag:

    @task()
    def extract_reddit_data(subreddit_name: str, limit=100):
        """Extract Reddit data (posts) from a specified subreddit."""
        reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=USER_AGENT
        )

        subreddit = reddit.subreddit(subreddit_name)
        posts_data = []

        for submission in subreddit.hot(limit=limit):
            posts_data.append({
                'Title': submission.title,
                'Score': submission.score,
                'ID': submission.id,
                'Author': str(submission.author),
                'Num_Comments': submission.num_comments,
                'Upvote_Ratio': submission.upvote_ratio,
                'Created': submission.created_utc,
                'URL': submission.url
            })

        return pd.DataFrame(posts_data).to_dict(orient="records")

    @task()
    def transform_reddit_data(posts_data):
        """Transform the extracted Reddit data."""
        for post in posts_data:
            post['timestamp'] = pd.to_datetime(post['Created'], unit='s').isoformat()
        return posts_data

    @task()
    def load_reddit_data(transformed_data):
        """Load transformed data into PostgreSQL."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Créer la table si elle n'existe pas
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS reddit_posts (
            title TEXT,
            score INT,
            id TEXT PRIMARY KEY,
            author TEXT,
            num_comments INT,
            upvote_ratio FLOAT,
            created TIMESTAMP,
            url TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Insertion des données transformées dans la table PostgreSQL
        for post in transformed_data:
            cursor.execute("""
            INSERT INTO reddit_posts (title, score, id, author, num_comments, upvote_ratio, created, url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
            """, (
                post['Title'],
                post['Score'],
                post['ID'],
                post['Author'],
                post['Num_Comments'],
                post['Upvote_Ratio'],
                pd.to_datetime(post['Created'], unit='s'),
                post['URL'],
            ))

        # Commit des transactions et fermer la connexion
        conn.commit()
        cursor.close()

    # DAG Workflow - ETL Pipeline
    subreddit_name = 'YOLO'  # Changez ce nom de subreddit ici
    posts_data = extract_reddit_data(subreddit_name=subreddit_name)
    transformed_data = transform_reddit_data(posts_data)
    load_reddit_data(transformed_data)
