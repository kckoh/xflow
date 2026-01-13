"""
SQL Converter Utility for DuckDB <-> Spark SQL compatibility.

Provides bidirectional conversion:
- Spark SQL -> DuckDB (for preview)
- DuckDB -> Spark SQL (for ETL execution)
"""

import re
from typing import List, Tuple

# DuckDB -> Spark SQL function mapping
DUCKDB_TO_SPARK = {
    "unnest": "explode",
    "list_zip": "arrays_zip",
    "list_filter": "filter",
    "list_transform": "transform",
    "list_aggregate": "aggregate",
    "regexp_matches": "regexp_extract",
    "list_value": "array",
    "list_concat": "concat",
    "list_contains": "array_contains",
    "list_distinct": "array_distinct",
    "list_sort": "array_sort",
    "list_reverse": "array_reverse",
    "list_slice": "slice",
    "string_split": "split",
    "strftime": "date_format",
    "strptime": "to_timestamp",
}

# Spark -> DuckDB function mapping (reverse)
SPARK_TO_DUCKDB = {
    "explode": "unnest",
    "arrays_zip": "list_zip",
    "filter": "list_filter",
    "transform": "list_transform",
    "aggregate": "list_aggregate",
    "regexp_extract": "regexp_matches",
    "array": "list_value",
    "array_contains": "list_contains",
    "array_distinct": "list_distinct",
    "array_sort": "list_sort",
    "array_reverse": "list_reverse",
    "slice": "list_slice",
    "split": "string_split",
    "date_format": "strftime",
    "to_timestamp": "strptime",
}

# DuckDB-only functions (for warnings)
DUCKDB_ONLY_FUNCTIONS = {
    "unnest": "Use explode() instead",
    "list_zip": "Use arrays_zip() instead",
    "list_filter": "Use filter() instead",
    "list_transform": "Use transform() instead",
    "list_aggregate": "Use aggregate() instead",
    "regexp_matches": "Use regexp_extract() instead",
}


def validate_spark_compatibility(sql: str) -> List[dict]:
    """
    Detect functions in DuckDB SQL that are not compatible with Spark.

    Args:
        sql: SQL string to validate

    Returns:
        List of warnings [{"function": "unnest", "message": "Use explode() in Spark"}]
    """
    warnings = []
    sql_lower = sql.lower()

    for duckdb_func, suggestion in DUCKDB_ONLY_FUNCTIONS.items():
        pattern = rf"\b{duckdb_func}\s*\("
        if re.search(pattern, sql_lower):
            spark_func = DUCKDB_TO_SPARK.get(duckdb_func, "N/A")
            warnings.append(
                {
                    "function": duckdb_func,
                    "spark_equivalent": spark_func,
                    "message": f"DuckDB function {duckdb_func}() is not supported in Spark. {suggestion}",
                }
            )

    return warnings


def convert_spark_to_duckdb(sql: str) -> Tuple[str, List[str]]:
    """
    Convert Spark SQL to DuckDB SQL for preview execution.

    Handles:
    1. Simple function replacements (explode -> unnest, arrays_zip -> list_zip)
    2. LATERAL VIEW explode(arrays_zip(...)) pattern -> multiple unnest()

    Args:
        sql: Spark SQL string

    Returns:
        (converted SQL for DuckDB, list of conversions made)
    """
    converted_sql = sql
    conversions = []

    # Pattern 1: LATERAL VIEW explode(arrays_zip(col1, col2, ...)) t AS zipped
    # Convert to DuckDB: SELECT unnest(col1) as col1, unnest(col2) as col2 ...
    lateral_view_pattern = r"""
        (?P<select>SELECT\s+)
        (?P<columns>.*?)
        (?P<from>\s+FROM\s+)
        (?P<table>\w+)
        \s+LATERAL\s+VIEW\s+explode\s*\(\s*arrays_zip\s*\(
        (?P<array_cols>[^)]+)
        \)\s*\)\s*
        (?:\w+\s+)?                    # optional table alias
        AS\s+(?P<alias>\w+)
        (?P<rest>.*)
    """

    match = re.search(
        lateral_view_pattern, converted_sql, re.IGNORECASE | re.VERBOSE | re.DOTALL
    )

    if match:
        # Extract components
        select_clause = match.group("select")
        columns = match.group("columns").strip()
        from_clause = match.group("from")
        table = match.group("table")
        array_cols_str = match.group("array_cols")
        alias = match.group("alias")
        rest = match.group("rest") or ""

        # Parse array columns
        array_cols = [col.strip() for col in array_cols_str.split(",")]

        # Parse SELECT columns and replace alias.column with unnest(column)
        new_select_parts = []
        select_items = _parse_select_columns(columns)

        for item in select_items:
            item_stripped = item.strip()
            # Check if it's alias.column pattern
            alias_col_match = re.match(
                rf"{alias}\.(\w+)(?:\s+as\s+(\w+))?", item_stripped, re.IGNORECASE
            )

            if alias_col_match:
                col_name = alias_col_match.group(1)
                output_alias = alias_col_match.group(2) or col_name
                new_select_parts.append(f"unnest({col_name}) as {output_alias}")
            elif item_stripped == f"{alias}.*":
                # alias.* - expand all array columns
                for col in array_cols:
                    new_select_parts.append(f"unnest({col}) as {col}")
            else:
                # Keep as-is (non-alias columns)
                new_select_parts.append(item_stripped)

        new_columns = ", ".join(new_select_parts)
        converted_sql = f"{select_clause}{new_columns}{from_clause}{table}{rest}"
        conversions.append("LATERAL VIEW explode(arrays_zip(...)) -> multiple unnest()")

    # Pattern 2: Subquery with explode(arrays_zip(...))
    # SELECT zipped.col1, zipped.col2 FROM (SELECT explode(arrays_zip(col1, col2)) as zipped FROM input)
    subquery_pattern = r"""
        (?P<outer_select>SELECT\s+)
        (?P<outer_columns>.*?)
        \s+FROM\s*\(\s*
        SELECT\s+explode\s*\(\s*arrays_zip\s*\(
        (?P<array_cols>[^)]+)
        \)\s*\)\s*as\s+(?P<alias>\w+)
        \s+FROM\s+(?P<table>\w+)
        \s*\)
        (?P<rest>.*)
    """

    subquery_match = re.search(
        subquery_pattern, converted_sql, re.IGNORECASE | re.VERBOSE | re.DOTALL
    )

    if subquery_match:
        outer_select = subquery_match.group("outer_select")
        outer_columns = subquery_match.group("outer_columns").strip()
        array_cols_str = subquery_match.group("array_cols")
        alias = subquery_match.group("alias")
        table = subquery_match.group("table")
        rest = subquery_match.group("rest") or ""

        array_cols = [col.strip() for col in array_cols_str.split(",")]

        # Parse outer SELECT columns and replace alias.column with unnest(column)
        new_select_parts = []
        select_items = _parse_select_columns(outer_columns)

        for item in select_items:
            item_stripped = item.strip()
            alias_col_match = re.match(
                rf"{alias}\.(\w+)(?:\s+as\s+(\w+))?", item_stripped, re.IGNORECASE
            )

            if alias_col_match:
                col_name = alias_col_match.group(1)
                output_alias = alias_col_match.group(2) or col_name
                new_select_parts.append(f"unnest({col_name}) as {output_alias}")
            elif item_stripped == f"{alias}.*":
                for col in array_cols:
                    new_select_parts.append(f"unnest({col}) as {col}")
            else:
                new_select_parts.append(item_stripped)

        new_columns = ", ".join(new_select_parts)
        converted_sql = f"{outer_select}{new_columns} FROM {table}{rest}"
        conversions.append("subquery explode(arrays_zip(...)) -> multiple unnest()")

    # Pattern 3: Simple function replacements (if no complex pattern matched)
    if not conversions:
        for spark_func, duckdb_func in SPARK_TO_DUCKDB.items():
            pattern = rf"\b{spark_func}\s*\("
            if re.search(pattern, converted_sql, re.IGNORECASE):
                converted_sql = re.sub(
                    pattern, f"{duckdb_func}(", converted_sql, flags=re.IGNORECASE
                )
                conversions.append(f"{spark_func} -> {duckdb_func}")

    return converted_sql, conversions


def _parse_select_columns(columns_str: str) -> List[str]:
    """
    Parse SELECT column list, handling nested parentheses.

    Args:
        columns_str: "col1, col2, func(a, b) as c"

    Returns:
        ["col1", "col2", "func(a, b) as c"]
    """
    result = []
    current = ""
    depth = 0

    for char in columns_str:
        if char == "(":
            depth += 1
            current += char
        elif char == ")":
            depth -= 1
            current += char
        elif char == "," and depth == 0:
            if current.strip():
                result.append(current.strip())
            current = ""
        else:
            current += char

    if current.strip():
        result.append(current.strip())

    return result


def is_spark_sql(sql: str) -> bool:
    """
    Detect if SQL is written in Spark SQL syntax.

    Returns True if SQL contains Spark-specific patterns:
    - LATERAL VIEW
    - arrays_zip()
    - explode()
    """
    sql_lower = sql.lower()

    spark_patterns = [
        r"\blateral\s+view\b",
        r"\barrays_zip\s*\(",
        r"\bexplode\s*\(",
        r"\binline\s*\(",
        r"\bposexplode\s*\(",
    ]

    for pattern in spark_patterns:
        if re.search(pattern, sql_lower):
            return True

    return False


def get_spark_sql_for_array_transform(
    source_alias: str, array_columns: List[str], output_columns: List[str] = None
) -> str:
    """
    Generate Spark SQL to transform array columns into flat rows.

    Args:
        source_alias: Source table alias
        array_columns: Array columns (e.g., ['time', 'temperature_2m_max'])
        output_columns: Output column names (None to use original names)

    Returns:
        Spark SQL string
    """
    if not output_columns:
        output_columns = array_columns

    zip_args = ", ".join(array_columns)
    select_cols = ", ".join([f"zipped.{col}" for col in array_columns])

    sql = f"""SELECT {select_cols}
FROM {source_alias}
LATERAL VIEW explode(arrays_zip({zip_args})) t AS zipped"""

    return sql
