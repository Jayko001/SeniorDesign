import os
import re
from typing import Dict, Any, Optional, List, Tuple

import pandas as pd
from psycopg2 import sql
import psycopg2
from supabase import create_client


_AGG_KEYWORDS = [
    ("average", "mean"),
    ("avg", "mean"),
    ("mean", "mean"),
    ("sum", "sum"),
    ("total", "sum"),
    ("count", "count"),
    ("number of", "count"),
    ("how many", "count"),
    ("minimum", "min"),
    ("min", "min"),
    ("maximum", "max"),
    ("max", "max"),
]

_SORT_TRIGGERS = [
    "order",
    "sorted",
    "sort",
    "ranking",
    "rank",
    "descending",
    "ascending",
    "highest",
    "lowest",
    "top",
    "bottom",
    "largest",
    "smallest",
    "most",
    "least",
]

_SORT_DESC_TOKENS = [
    "descending",
    "desc",
    "highest",
    "top",
    "largest",
    "greatest",
    "most",
]

_SORT_ASC_TOKENS = [
    "ascending",
    "asc",
    "lowest",
    "bottom",
    "smallest",
    "least",
]

_LABEL_HINTS = [
    "name",
    "employee",
    "title",
    "department",
    "dept",
    "category",
    "type",
]

_SQL_AGG_MAP = {
    "mean": "AVG",
    "sum": "SUM",
    "min": "MIN",
    "max": "MAX",
}


def generate_analytics(
    natural_language: str,
    source_type: str,
    schema: Optional[Dict[str, Any]],
    source_config: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    column_names, numeric_columns = _extract_columns(schema)
    agg, agg_explicit = _infer_aggregation(natural_language)
    sort_request = None
    if not agg_explicit:
        sort_request = _infer_sort_request(natural_language, column_names, numeric_columns)

    if source_type == "csv":
        file_path = source_config.get("file_path")
        if not file_path:
            return None
        df = _load_csv_dataframe(file_path)
        if sort_request:
            label_col = _infer_label_column(
                natural_language,
                column_names,
                numeric_columns,
                sort_request["value_col"],
            )
            analytics = _build_sorted_dataframe_chart(
                df,
                sort_request["value_col"],
                label_col,
                sort_request["direction"],
                natural_language,
            )
            if analytics:
                return analytics
        group_by = _infer_group_by(natural_language, column_names)
        value_col = _infer_value_column(natural_language, column_names, numeric_columns, group_by, agg)
        return _build_from_dataframe(df, agg, value_col, group_by, natural_language)

    if source_type == "postgres":
        table_name = source_config.get("table_name")
        if not table_name:
            return None
        if sort_request:
            label_col = _infer_label_column(
                natural_language,
                column_names,
                numeric_columns,
                sort_request["value_col"],
            )
            analytics = _build_sorted_postgres_chart(
                source_config,
                table_name,
                sort_request["value_col"],
                label_col,
                sort_request["direction"],
                natural_language,
            )
            if analytics:
                return analytics
        group_by = _infer_group_by(natural_language, column_names)
        value_col = _infer_value_column(natural_language, column_names, numeric_columns, group_by, agg)
        analytics = _build_from_postgres_query(
            source_config,
            table_name,
            agg,
            value_col,
            group_by,
            natural_language,
        )
        if analytics:
            return analytics
        df = _load_supabase_dataframe(source_config, table_name)
        if df is None:
            return None
        if not column_names:
            column_names = list(df.columns)
            numeric_columns = _infer_numeric_columns(df)
        if sort_request:
            label_col = _infer_label_column(
                natural_language,
                column_names,
                numeric_columns,
                sort_request["value_col"],
            )
            analytics = _build_sorted_dataframe_chart(
                df,
                sort_request["value_col"],
                label_col,
                sort_request["direction"],
                natural_language,
            )
            if analytics:
                return analytics
        group_by = _infer_group_by(natural_language, column_names)
        value_col = _infer_value_column(natural_language, column_names, numeric_columns, group_by, agg)
        return _build_from_dataframe(df, agg, value_col, group_by, natural_language)

    return None


def _extract_columns(schema: Optional[Dict[str, Any]]) -> Tuple[List[str], List[str]]:
    if not schema:
        return [], []
    columns = schema.get("columns") or []
    column_names = [col.get("name") for col in columns if col.get("name")]
    numeric_columns = [
        col.get("name")
        for col in columns
        if col.get("name") and _is_numeric_type(col.get("type"))
    ]
    return column_names, numeric_columns


def _normalize(text: str) -> str:
    return re.sub(r"[^a-z0-9_]+", " ", text.lower()).strip()


def _infer_aggregation(text: str) -> Tuple[str, bool]:
    lowered = text.lower()
    for keyword, agg in _AGG_KEYWORDS:
        if re.search(rf"\b{re.escape(keyword)}\b", lowered):
            return agg, True
    return "count", False


def _infer_sort_request(
    text: str,
    columns: List[str],
    numeric_columns: List[str],
) -> Optional[Dict[str, str]]:
    lowered = text.lower()
    if not any(token in lowered for token in _SORT_TRIGGERS):
        return None
    direction = "desc"
    if any(token in lowered for token in _SORT_ASC_TOKENS):
        direction = "asc"
    elif any(token in lowered for token in _SORT_DESC_TOKENS):
        direction = "desc"

    normalized_text = _normalize(text)
    value_col = None
    for col in numeric_columns:
        if f" {_normalize(col)} " in f" {normalized_text} ":
            value_col = col
            break
    if not value_col and numeric_columns:
        value_col = numeric_columns[0]
    if not value_col:
        return None
    return {"value_col": value_col, "direction": direction}


def _infer_label_column(
    text: str,
    columns: List[str],
    numeric_columns: List[str],
    value_col: Optional[str],
) -> Optional[str]:
    if not columns:
        return None
    normalized_text = _normalize(text)
    for hint in _LABEL_HINTS:
        if hint in normalized_text:
            for col in columns:
                if col != value_col and hint in _normalize(col):
                    return col
    for col in columns:
        if col != value_col and f" {_normalize(col)} " in f" {normalized_text} ":
            return col
    for col in columns:
        if col != value_col and col not in numeric_columns:
            return col
    for col in columns:
        if col != value_col:
            return col
    return None


def _infer_group_by(text: str, columns: List[str]) -> Optional[str]:
    if not columns:
        return None
    normalized_text = _normalize(text)
    match = re.search(r"\b(group by|by|per)\b(.*)", normalized_text)
    if not match:
        return None
    tail = match.group(2)
    for column in columns:
        normalized_col = _normalize(column)
        if f" {normalized_col} " in f" {tail} ":
            return column
    return None


def _infer_value_column(
    text: str,
    columns: List[str],
    numeric_columns: List[str],
    group_by: Optional[str],
    agg: str,
) -> Optional[str]:
    if agg == "count":
        return None
    normalized_text = _normalize(text)
    candidates = [
        col
        for col in numeric_columns
        if f" {_normalize(col)} " in f" {normalized_text} " and col != group_by
    ]
    if candidates:
        return candidates[0]
    if numeric_columns:
        for col in numeric_columns:
            if col != group_by:
                return col
    for col in columns:
        if col != group_by:
            return col
    return None


def _is_numeric_type(type_value: Optional[str]) -> bool:
    if not type_value:
        return False
    type_str = str(type_value).lower()
    return any(token in type_str for token in ["int", "float", "double", "numeric", "decimal", "real"])


def _infer_numeric_columns(df: pd.DataFrame) -> List[str]:
    numeric_cols = []
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            numeric_cols.append(col)
    return numeric_cols


def _load_csv_dataframe(file_path: str) -> pd.DataFrame:
    max_rows = os.getenv("ANALYTICS_MAX_ROWS")
    nrows = int(max_rows) if max_rows and max_rows.isdigit() else None
    return pd.read_csv(file_path, nrows=nrows)


def _load_supabase_dataframe(source_config: Dict[str, Any], table_name: str) -> Optional[pd.DataFrame]:
    supabase_url = source_config.get("supabase_url") or os.getenv("SUPABASE_URL")
    supabase_key = source_config.get("supabase_key") or os.getenv("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        return None
    client = create_client(supabase_url, supabase_key)
    response = client.table(table_name).select("*").limit(1000).execute()
    rows = response.data if response and response.data else []
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _build_sorted_postgres_chart(
    source_config: Dict[str, Any],
    table_name: str,
    value_col: Optional[str],
    label_col: Optional[str],
    direction: str,
    natural_language: str,
) -> Optional[Dict[str, Any]]:
    if not value_col or not label_col:
        return None
    conn_params = {
        "host": source_config.get("host") or os.getenv("POSTGRES_HOST"),
        "port": source_config.get("port") or os.getenv("POSTGRES_PORT", 5432),
        "database": source_config.get("database") or os.getenv("POSTGRES_DB"),
        "user": source_config.get("user") or os.getenv("POSTGRES_USER"),
        "password": source_config.get("password") or os.getenv("POSTGRES_PASSWORD"),
    }
    if not all([conn_params["host"], conn_params["database"], conn_params["user"]]):
        return None

    try:
        conn_params["port"] = int(conn_params["port"])
    except (TypeError, ValueError):
        pass

    sort_dir = "ASC" if direction == "asc" else "DESC"

    conn = psycopg2.connect(**conn_params)
    try:
        with conn.cursor() as cursor:
            query = sql.SQL(
                "SELECT {label_col}, {value_col} "
                "FROM {table} "
                "WHERE {value_col} IS NOT NULL "
                "ORDER BY {value_col} {direction} "
                "LIMIT 20"
            ).format(
                label_col=_sql_identifier(label_col),
                value_col=_sql_identifier(value_col),
                table=_sql_identifier(table_name),
                direction=sql.SQL(sort_dir),
            )
            cursor.execute(query)
            rows = cursor.fetchall()
            if not rows:
                return None
            x_values = [str(row[0]) for row in rows]
            y_values = [_coerce_value(row[1]) for row in rows]
            title = _build_title("sort", value_col, None, natural_language)
            return {
                "chart_type": "bar",
                "title": title,
                "x": x_values,
                "y": y_values,
                "x_label": label_col,
                "y_label": value_col,
                "aggregation": "sort",
                "value_column": value_col,
                "group_by": None,
            }
    finally:
        conn.close()


def _build_sorted_dataframe_chart(
    df: pd.DataFrame,
    value_col: Optional[str],
    label_col: Optional[str],
    direction: str,
    natural_language: str,
) -> Optional[Dict[str, Any]]:
    if df is None or df.empty or not value_col or value_col not in df.columns:
        return None
    if label_col and label_col not in df.columns:
        label_col = None

    data_df = df.copy()
    data_df = data_df.dropna(subset=[value_col])
    data_df = data_df.sort_values(by=value_col, ascending=(direction == "asc")).head(20)
    if data_df.empty:
        return None

    if label_col:
        x_values = data_df[label_col].astype(str).tolist()
        x_label = label_col
    else:
        x_values = [str(idx) for idx in data_df.index.tolist()]
        x_label = "item"

    y_values = [_coerce_value(val) for val in data_df[value_col].tolist()]
    title = _build_title("sort", value_col, None, natural_language)
    return {
        "chart_type": "bar",
        "title": title,
        "x": x_values,
        "y": y_values,
        "x_label": x_label,
        "y_label": value_col,
        "aggregation": "sort",
        "value_column": value_col,
        "group_by": None,
    }


def _build_from_postgres_query(
    source_config: Dict[str, Any],
    table_name: str,
    agg: str,
    value_col: Optional[str],
    group_by: Optional[str],
    natural_language: str,
) -> Optional[Dict[str, Any]]:
    conn_params = {
        "host": source_config.get("host") or os.getenv("POSTGRES_HOST"),
        "port": source_config.get("port") or os.getenv("POSTGRES_PORT", 5432),
        "database": source_config.get("database") or os.getenv("POSTGRES_DB"),
        "user": source_config.get("user") or os.getenv("POSTGRES_USER"),
        "password": source_config.get("password") or os.getenv("POSTGRES_PASSWORD"),
    }
    if not all([conn_params["host"], conn_params["database"], conn_params["user"]]):
        return None

    try:
        conn_params["port"] = int(conn_params["port"])
    except (TypeError, ValueError):
        pass

    if agg != "count" and not value_col:
        return None

    conn = psycopg2.connect(**conn_params)
    try:
        with conn.cursor() as cursor:
            table_ident = _sql_identifier(table_name)
            if group_by:
                group_ident = _sql_identifier(group_by)
                if agg == "count":
                    query = sql.SQL(
                        "SELECT {group_col}, COUNT(*) AS metric "
                        "FROM {table} "
                        "GROUP BY {group_col} "
                        "ORDER BY metric DESC "
                        "LIMIT 20"
                    ).format(group_col=group_ident, table=table_ident)
                else:
                    agg_func = _SQL_AGG_MAP.get(agg)
                    if not agg_func:
                        return None
                    value_ident = _sql_identifier(value_col)
                    query = sql.SQL(
                        "SELECT {group_col}, {agg_func}({value_col}) AS metric "
                        "FROM {table} "
                        "GROUP BY {group_col} "
                        "ORDER BY metric DESC "
                        "LIMIT 20"
                    ).format(
                        group_col=group_ident,
                        agg_func=sql.SQL(agg_func),
                        value_col=value_ident,
                        table=table_ident,
                    )
                cursor.execute(query)
                rows = cursor.fetchall()
                x_values = [str(row[0]) for row in rows]
                y_values = [_coerce_value(row[1]) for row in rows]
                title = _build_title(agg, value_col, group_by, natural_language)
                return {
                    "chart_type": "bar",
                    "title": title,
                    "x": x_values,
                    "y": y_values,
                    "x_label": group_by,
                    "y_label": _build_metric_label(agg, value_col),
                    "aggregation": agg,
                    "value_column": value_col,
                    "group_by": group_by,
                }
            if agg == "count":
                query = sql.SQL("SELECT COUNT(*) FROM {table}").format(table=table_ident)
                cursor.execute(query)
                metric = cursor.fetchone()[0]
                title = _build_title(agg, value_col, group_by, natural_language)
                metric_label = _build_metric_label(agg, value_col)
                return {
                    "chart_type": "bar",
                    "title": title,
                    "x": [metric_label],
                    "y": [_coerce_value(metric)],
                    "x_label": "",
                    "y_label": metric_label,
                    "aggregation": agg,
                    "value_column": value_col,
                    "group_by": group_by,
                }
            agg_func = _SQL_AGG_MAP.get(agg)
            if not agg_func:
                return None
            value_ident = _sql_identifier(value_col)
            query = sql.SQL("SELECT {agg_func}({value_col}) FROM {table}").format(
                agg_func=sql.SQL(agg_func),
                value_col=value_ident,
                table=table_ident,
            )
            cursor.execute(query)
            metric = cursor.fetchone()[0]
            title = _build_title(agg, value_col, group_by, natural_language)
            return {
                "chart_type": "bar",
                "title": title,
                "x": [_build_metric_label(agg, value_col)],
                "y": [_coerce_value(metric)],
                "x_label": "",
                "y_label": _build_metric_label(agg, value_col),
                "aggregation": agg,
                "value_column": value_col,
                "group_by": group_by,
            }
    finally:
        conn.close()


def _sql_identifier(name: str) -> sql.Identifier:
    parts = [part for part in name.split(".") if part]
    if len(parts) > 1:
        return sql.Identifier(*parts)
    return sql.Identifier(name)


def _build_from_dataframe(
    df: pd.DataFrame,
    agg: str,
    value_col: Optional[str],
    group_by: Optional[str],
    natural_language: str,
) -> Optional[Dict[str, Any]]:
    if df is None or df.empty:
        return None
    group_by = group_by if group_by in df.columns else None
    if agg != "count" and (not value_col or value_col not in df.columns):
        numeric_cols = _infer_numeric_columns(df)
        value_col = numeric_cols[0] if numeric_cols else None
        if not value_col and agg != "count":
            agg = "count"

    if group_by:
        if agg == "count":
            series = df[group_by].value_counts(dropna=True)
        else:
            series = df.groupby(group_by)[value_col].agg(agg).dropna()
        series = series.sort_values(ascending=False).head(20)
        title = _build_title(agg, value_col, group_by, natural_language)
        return {
            "chart_type": "bar",
            "title": title,
            "x": [str(idx) for idx in series.index.tolist()],
            "y": [_coerce_value(val) for val in series.tolist()],
            "x_label": group_by,
            "y_label": _build_metric_label(agg, value_col),
            "aggregation": agg,
            "value_column": value_col,
            "group_by": group_by,
        }

    if agg == "count":
        metric = len(df)
    else:
        metric = df[value_col].agg(agg)
    title = _build_title(agg, value_col, group_by, natural_language)
    metric_label = _build_metric_label(agg, value_col)
    return {
        "chart_type": "bar",
        "title": title,
        "x": [metric_label],
        "y": [_coerce_value(metric)],
        "x_label": "",
        "y_label": metric_label,
        "aggregation": agg,
        "value_column": value_col,
        "group_by": group_by,
    }


def _coerce_value(value: Any) -> Any:
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value


def _build_title(agg: str, value_col: Optional[str], group_by: Optional[str], natural_language: str) -> str:
    if natural_language:
        return natural_language.strip().capitalize()
    metric = _build_metric_label(agg, value_col)
    if group_by:
        return f"{metric} by {group_by}"
    return metric


def _build_metric_label(agg: str, value_col: Optional[str]) -> str:
    if agg == "count":
        return "count"
    if value_col:
        return f"{agg}({value_col})"
    return agg
