"""Producteur Kafka : rejoue Questions.csv (+ Tags.csv) dans le topic 'questions' pour simuler un flux."""
import os, json, time
import pandas as pd
from kafka import KafkaProducer

DATA = os.getenv("DATA_DIR", "/opt/project/data")
BOOT = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
MAX_ROWS = int(os.getenv("MAX_ROWS", "20000"))
DELAY = float(os.getenv("PRODUCER_DELAY", "0"))
Q, T = f"{DATA}/Questions.csv", f"{DATA}/Tags.csv"

assert os.path.exists(Q) and os.path.exists(T), f"Questions.csv / Tags.csv introuvables dans {DATA}"

# Echantillon reparti sur toute la periode (le fichier est trie par date)
size_mb = os.path.getsize(Q) / 1e6
frac = 1.0 if size_mb < 200 else min(1.0, MAX_ROWS / 1_264_000 * 1.1)
parts = []
for chunk in pd.read_csv(Q, encoding="latin-1", chunksize=50_000):
    parts.append(chunk if frac >= 1 else chunk.sample(frac=frac, random_state=42))
qs = pd.concat(parts).head(MAX_ROWS)
print(f"{len(qs)} questions selectionnees")

tags = pd.read_csv(T, encoding="latin-1").dropna()
tags = tags[tags["Id"].isin(set(qs["Id"]))]
tag_map = tags.groupby("Id")["Tag"].apply(lambda s: [str(x) for x in s]).to_dict()

producer = KafkaProducer(bootstrap_servers=BOOT, linger_ms=50,
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"))

for i, r in enumerate(qs.itertuples(index=False), 1):
    date = pd.to_datetime(r.CreationDate, errors="coerce")
    msg = {
        "id": int(r.Id),
        "owner_user_id": None if pd.isna(r.OwnerUserId) else str(int(r.OwnerUserId)),
        "creation_date": None if pd.isna(date) else date.strftime("%Y-%m-%d %H:%M:%S"),
        "score": None if pd.isna(r.Score) else int(r.Score),
        "title": "" if pd.isna(r.Title) else str(r.Title),
        "body": "" if pd.isna(r.Body) else str(r.Body)[:5000],
        "tags": tag_map.get(r.Id, []),
    }
    producer.send("questions", msg)
    if DELAY:
        time.sleep(DELAY)
    if i % 5000 == 0:
        print(f"{i} messages envoyes")
producer.flush()
print(f"PRODUCER OK : {len(qs)} messages envoyes dans le topic 'questions'")
