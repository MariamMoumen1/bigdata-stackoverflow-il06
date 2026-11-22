"""Point d'entree GOLD pour Airflow et execution autonome.
Delegue au job Spark SQL de Mohammed Hedda (jobs/sql/gold_trends.py).
"""
import sys
import os

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from sql.gold_trends import run_gold_job

if __name__ == "__main__":
    run_gold_job()
