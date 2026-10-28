"""Column expressions that several ``single_etl`` scripts spell the same way.

The per-source scripts translate their plain tables into the canonical event
structure of ``model/etl/event_etl.py``. Two expressions recur there with
identical behaviour: a literal ``map`` column built from a config mapping, and
the pass-through of the reverse-geocoded subdivision stamped into the CSVs at
download time. Both live here so the scripts cannot drift apart.
"""

from collections.abc import Mapping

from pyspark.sql import Column
from pyspark.sql import functions as F


def literal_map(mapping: Mapping[str, object]) -> Column:
    """Embed ``mapping`` as a literal map column (a lookup for ``map(col, …)``).

    Args:
        mapping: Key/value pairs to embed.

    Returns:
        A literal map column with every key and value as a literal.
    """
    return F.create_map(*[F.lit(item) for pair in mapping.items() for item in pair])


def state_province(geocode_state: Column) -> Column:
    """The stamped subdivision of the point, ``NULL`` when the stamp found none.

    Args:
        geocode_state: ``geocode_state`` column stamped into the CSVs at
            download time.

    Returns:
        The subdivision code (``US-TX`` → ``TX``); an empty stamp (no
        subdivision found) or a ``NULL`` (no coordinate) resolves to ``NULL``.
    """
    return F.when(geocode_state != "", geocode_state).otherwise(F.lit(None).cast("string"))
