"""Package sql : requetes Spark SQL pour la zone Gold.
Responsable : Mohammed Hedda.
"""
from sql.sql_queries import (
    SQL_TAG_MONTHLY_VOLUME,
    SQL_TAG_RANKINGS,
    SQL_TAG_MONTHLY_GROWTH,
    compute_gold_tables,
)

__all__ = [
    "SQL_TAG_MONTHLY_VOLUME",
    "SQL_TAG_RANKINGS",
    "SQL_TAG_MONTHLY_GROWTH",
    "compute_gold_tables",
]
