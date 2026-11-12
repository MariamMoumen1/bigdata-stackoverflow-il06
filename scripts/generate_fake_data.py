"""Plan B : genere de fausses donnees au format StackSample. Usage : python generate_fake_data.py 20000 [--force]"""
import os, sys, random
import pandas as pd

n = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 20000
out = os.getenv("DATA_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data"))
if os.path.exists(f"{out}/Questions.csv") and "--force" not in sys.argv:
    sys.exit("Questions.csv existe deja : rien fait (ajoutez --force pour ecraser).")

TOPICS = {
    "python": ["python", "pandas", "numpy", "def", "import", "list comprehension"],
    "java": ["java", "spring", "jvm", "maven", "public static void"],
    "javascript": ["javascript", "node", "promise", "dom", "react"],
    "c#": ["c#", "dotnet", "linq", "visual studio"],
    "sql": ["sql", "select", "join", "postgres", "index"],
    "php": ["php", "laravel", "composer", "mysql"],
    "html": ["html", "div", "form", "tag"],
    "css": ["css", "flexbox", "margin", "stylesheet"],
    "android": ["android", "activity", "gradle", "kotlin"],
    "git": ["git", "commit", "branch", "merge", "rebase"],
}
names = list(TOPICS)
random.seed(1)
qs, tg = [], []
for i in range(1, n + 1):
    chosen = random.sample(names, random.randint(1, 3))
    words = [w for t in chosen for w in random.sample(TOPICS[t], 2)]
    year, month = random.randint(2009, 2020), random.randint(1, 12)
    qs.append({
        "Id": i, "OwnerUserId": random.randint(1, 5000),
        "CreationDate": f"{year}-{month:02d}-{random.randint(1, 28):02d}T10:00:00Z",
        "ClosedDate": "", "Score": random.randint(-2, 60),
        "Title": "How to fix " + " ".join(words) + " error?",
        "Body": "<p>I have a problem with " + " and ".join(words) + ".</p><code>x = 1</code>",
    })
    tg += [{"Id": i, "Tag": t} for t in chosen]
os.makedirs(out, exist_ok=True)
pd.DataFrame(qs).to_csv(f"{out}/Questions.csv", index=False, encoding="latin-1")
pd.DataFrame(tg).to_csv(f"{out}/Tags.csv", index=False, encoding="latin-1")
print(f"{n} fausses questions generees dans {out}")
