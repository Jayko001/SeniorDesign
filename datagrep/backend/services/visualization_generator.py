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


def infer_visualization_spec(natural_language: str, data: Any) -> Dict[str, Any]:
    """
    Use OpenAI to infer a chart spec from the user's request and sample data.
    Returns a dict with: chart_type, x, y, title.
    """
    rows = _normalize_data(data)
    sample = rows[:20] if rows else []
    columns = list(sample[0].keys()) if sample else []

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
{json.dumps(sample, indent=2)}

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
                return {"chart_type": "bar", "x": x_col, "y": y_col, "title": "Dashboard"}
            if numeric_cols:
                return {"chart_type": "hist", "x": numeric_cols[0], "y": numeric_cols[0], "title": "Distribution"}
        return {"chart_type": "bar", "x": "metric", "y": "value", "title": "Dashboard"}


def build_plot_code(spec: Dict[str, Any], data: Any) -> str:
    """Build Python code (matplotlib) to render a chart to /output/plot.png."""
    rows = _normalize_data(data)
    data_json = json.dumps(rows)

    chart_type = str(spec.get("chart_type", "bar")).lower()
    x_key = str(spec.get("x", "metric"))
    y_key = str(spec.get("y", "value"))
    title = str(spec.get("title", "Dashboard"))

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

# Normalize data
if isinstance(data, dict):
    data = [data]

xs = []
ys = []
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
if chart_type == "line":
    plt.plot(xs, ys)
elif chart_type == "scatter":
    plt.scatter(xs, ys)
elif chart_type == "hist":
    plt.hist(ys, bins=10)
elif chart_type == "pie":
    plt.pie(ys, labels=xs)
else:
    plt.bar(xs, ys)

plt.title(title)
plt.tight_layout()
plt.savefig("/output/plot.png")
print(json.dumps({{"image_path": "/output/plot.png"}}))
"""
