"""
Visualization Generator Service
Infers a chart spec from data + request and builds Python plotting code.
"""

import json
import os
from typing import Any, Dict, List, Optional
from openai import OpenAI
from openai import APIConnectionError, APIError, RateLimitError


def get_openai_client() -> OpenAI:
    """Get OpenAI client with API key validation"""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError(
            "OPENAI_API_KEY environment variable is not set. "
            "Please set it in your .env file (root directory) or export it as an environment variable."
        )
    if api_key.startswith("your_") or "example" in api_key.lower():
        raise ValueError("OPENAI_API_KEY appears to be a placeholder. Please set your actual API key.")
    return OpenAI(api_key=api_key)


def _normalize_data(data: Any) -> List[Dict[str, Any]]:
    if data is None:
        return []
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if isinstance(data, dict):
        return [data]
    return []


def _extract_single_numeric_metric(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if len(rows) != 1:
        return None

    row = rows[0]
    numeric_items = [
        (key, value)
        for key, value in row.items()
        if isinstance(value, (int, float)) and not isinstance(value, bool)
    ]
    if len(numeric_items) != 1:
        return None

    key, value = numeric_items[0]
    return {"field": key, "value": value}


def _wants_colorful_style(natural_language: str) -> bool:
    text = natural_language.lower()
    return any(term in text for term in ["colorful", "colourful", "vibrant", "bright", "multicolor", "multicolored"])


def infer_visualization_spec(natural_language: str, data: Any) -> Dict[str, Any]:
    """
    Use OpenAI to infer a chart spec from the user's request and sample data.
    Returns a dict with: chart_type, x, y, title.
    """
    rows = _normalize_data(data)
    sample = rows[:20] if rows else []
    columns = list(sample[0].keys()) if sample else []
    single_metric = _extract_single_numeric_metric(sample)
    style = "colorful" if _wants_colorful_style(natural_language) else "default"

    # Avoid calling the LLM for the common single-metric case. This path is
    # deterministic and produces the correct bar chart for outputs like
    # [{"total_revenue_usd": 123.45}].
    if single_metric:
        return {
            "chart_type": "bar",
            "x": "metric",
            "y": "value",
            "title": "Dashboard",
            "metric_field": single_metric["field"],
            "style": style,
        }

    prompt = f"""You are a data visualization assistant.
Given a user request and a small data sample, pick the best chart and fields.
Return ONLY a JSON object with keys:
  chart_type: one of ["bar","line","scatter","hist","pie"]
  x: column name for x-axis (or label field)
  y: column name for y-axis (or value field)
  title: short title for the chart

User request:
{natural_language}

Columns:
{columns}

Sample rows:
{json.dumps(sample, indent=2, default=str)}

If there is only one numeric column, use hist on that column.
If there is a single row with a single numeric value, use a bar chart with x="metric" and y="value".
Return JSON only.
"""

    try:
        client = get_openai_client()
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "Return compact JSON only, no prose."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            max_tokens=300
        )
        content = response.choices[0].message.content.strip()
        spec = json.loads(content)
        if not isinstance(spec, dict):
            raise ValueError("Invalid visualization spec")
        spec.setdefault("style", style)
        return spec
    except (APIConnectionError, APIError, RateLimitError, ValueError, json.JSONDecodeError):
        # Fallback heuristic
        if sample:
            # Find numeric columns
            numeric_cols = []
            for col in columns:
                for row in sample:
                    val = row.get(col)
                    if isinstance(val, (int, float)):
                        numeric_cols.append(col)
                        break
            if len(columns) >= 2 and numeric_cols:
                y_col = numeric_cols[0]
                x_col = next((c for c in columns if c != y_col), columns[0])
                return {"chart_type": "bar", "x": x_col, "y": y_col, "title": "Dashboard", "style": style}
            if numeric_cols:
                return {
                    "chart_type": "bar",
                    "x": "metric",
                    "y": "value",
                    "title": "Dashboard",
                    "metric_field": numeric_cols[0],
                    "style": style,
                }
        return {"chart_type": "bar", "x": "metric", "y": "value", "title": "Dashboard", "style": style}


def build_plot_code(spec: Dict[str, Any], data: Any) -> str:
    """Build Python code (matplotlib) to render a chart to /output/plot.png."""
    rows = _normalize_data(data)
    data_json = json.dumps(rows, default=str)

    chart_type = str(spec.get("chart_type", "bar")).lower()
    x_key = str(spec.get("x", "metric"))
    y_key = str(spec.get("y", "value"))
    title = str(spec.get("title", "Dashboard"))
    metric_field = str(spec.get("metric_field", ""))
    style = str(spec.get("style", "default")).lower()

    return f"""import json
import math
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

data = {data_json}

def to_num(v):
    try:
        if v is None:
            return None
        if isinstance(v, bool):
            return int(v)
        return float(v)
    except Exception:
        return None

chart_type = "{chart_type}"
x_key = "{x_key}"
y_key = "{y_key}"
title = "{title}"
metric_field = "{metric_field}"
style = "{style}"

# Normalize data
if isinstance(data, dict):
    data = [data]

xs = []
ys = []


def build_color_list(count):
    if count <= 0:
        return None
    if style != "colorful":
        return None
    cmap = plt.cm.get_cmap("tab20", count)
    return [cmap(i) for i in range(count)]

def extract_metric_row(rows):
    if metric_field and rows:
        value = to_num(rows[0].get(metric_field))
        if value is not None:
            return metric_field, value
    if len(rows) != 1:
        return None, None
    numeric_items = []
    for key, value in rows[0].items():
        numeric_value = to_num(value)
        if numeric_value is not None:
            numeric_items.append((key, numeric_value))
    if len(numeric_items) == 1:
        return numeric_items[0]
    return None, None

metric_label, metric_value = extract_metric_row(data)

if x_key == "metric" and y_key == "value" and metric_label is not None:
    xs = [metric_label]
    ys = [metric_value]
else:
    for row in data:
        if not isinstance(row, dict):
            continue
        x_val = row.get(x_key)
        y_val = row.get(y_key)
        if chart_type == "hist":
            val = to_num(row.get(x_key))
            if val is not None:
                ys.append(val)
        else:
            y_num = to_num(y_val)
            if y_num is None:
                continue
            xs.append(str(x_val) if x_val is not None else "")
            ys.append(y_num)

plt.figure(figsize=(8, 5))
colors = build_color_list(len(ys))
if chart_type == "line":
    plt.plot(xs, ys, color=colors[0] if colors else None)
elif chart_type == "scatter":
    plt.scatter(xs, ys, c=colors if colors else None)
elif chart_type == "hist":
    plt.hist(ys, bins=10, color=colors[0] if colors else None)
elif chart_type == "pie":
    plt.pie(ys, labels=xs, colors=colors)
else:
    plt.bar(xs, ys, color=colors)

plt.title(title)
plt.tight_layout()
plt.savefig("/output/plot.png")
print(json.dumps({{"image_path": "/output/plot.png"}}))
"""
