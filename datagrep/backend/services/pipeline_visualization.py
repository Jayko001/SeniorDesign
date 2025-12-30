from typing import List, Optional, Dict, Any
import plotly.graph_objects as go

_DEFAULT_STEPS = ["Read source", "Transform data", "Write output"]


def _truncate_step(step: str, max_len: int = 40) -> str:
    if len(step) <= max_len:
        return step
    return step[: max_len - 3].rstrip() + "..."


def _empty_figure(message: str) -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(
        text=message,
        x=0.5,
        y=0.5,
        xref="paper",
        yref="paper",
        showarrow=False,
        font=dict(color="#666", size=16),
    )
    fig.update_layout(
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        plot_bgcolor="white",
        margin=dict(l=20, r=20, t=40, b=20),
        height=360,
    )
    return fig


def build_analytics_figure(
    analytics: Optional[Dict[str, Any]],
    node_color: str = "#2D7FF9",
) -> go.Figure:
    if not analytics:
        return _empty_figure("No analytics available yet.")

    chart_type = analytics.get("chart_type", "bar")
    title = analytics.get("title") or "Analytics"
    x_values = analytics.get("x") or []
    y_values = analytics.get("y") or []

    if chart_type == "indicator":
        value = analytics.get("value") or 0
        fig = go.Figure(
            go.Indicator(
                mode="number",
                value=value,
                title={"text": title},
            )
        )
        fig.update_layout(
            margin=dict(l=20, r=20, t=60, b=20),
            height=360,
        )
        return fig

    if not x_values or not y_values:
        return _empty_figure("No chart data available.")

    fig = go.Figure(
        data=[
            go.Bar(
                x=x_values,
                y=y_values,
                marker_color=node_color,
            )
        ]
    )
    fig.update_layout(
        title=title,
        xaxis=dict(title=analytics.get("x_label") or ""),
        yaxis=dict(title=analytics.get("y_label") or ""),
        plot_bgcolor="white",
        margin=dict(l=20, r=20, t=60, b=40),
        height=360,
    )
    return fig


def build_pipeline_figure(
    steps: Optional[List[str]],
    title: str = "Pipeline",
    node_color: str = "#2D7FF9"
) -> go.Figure:
    cleaned_steps = [step.strip() for step in (steps or []) if step and step.strip()]
    if not cleaned_steps:
        cleaned_steps = list(_DEFAULT_STEPS)

    x_values = list(range(len(cleaned_steps)))
    y_values = [0] * len(cleaned_steps)

    fig = go.Figure()

    for idx in range(len(cleaned_steps) - 1):
        fig.add_trace(
            go.Scatter(
                x=[x_values[idx], x_values[idx + 1]],
                y=[y_values[idx], y_values[idx + 1]],
                mode="lines",
                line=dict(color="#B0B0B0", width=2),
                hoverinfo="skip",
                showlegend=False,
            )
        )

    fig.add_trace(
        go.Scatter(
            x=x_values,
            y=y_values,
            mode="markers+text",
            marker=dict(size=28, color=node_color, line=dict(color="#1B1F23", width=1)),
            text=[f"{idx + 1}. {_truncate_step(step)}" for idx, step in enumerate(cleaned_steps)],
            textposition="top center",
            hovertext=cleaned_steps,
            hoverinfo="text",
            showlegend=False,
        )
    )

    fig.update_layout(
        title=title or "Pipeline",
        xaxis=dict(
            showgrid=False,
            zeroline=False,
            showticklabels=False,
            range=[-0.5, max(len(cleaned_steps) - 0.5, 0.5)],
        ),
        yaxis=dict(
            showgrid=False,
            zeroline=False,
            showticklabels=False,
            range=[-1, 1],
        ),
        plot_bgcolor="white",
        margin=dict(l=20, r=20, t=60, b=20),
        height=360,
    )

    return fig
