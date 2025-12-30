from urllib.parse import parse_qs
from typing import Optional

from dash import Dash, dcc, html, Input, Output

from services.pipeline_store import get_pipeline
from services.pipeline_visualization import build_analytics_figure


_COLOR_OPTIONS = [
    {"label": "Gold", "value": "#D4A72C"},
    {"label": "Medium Turquoise", "value": "#48D1CC"},
    {"label": "Light Green", "value": "#90EE90"},
    {"label": "Royal Blue", "value": "#2D7FF9"},
]


def _get_pipeline_id(search: Optional[str]) -> Optional[str]:
    if not search:
        return None
    params = parse_qs(search.lstrip("?"))
    values = params.get("pipeline_id")
    return values[0] if values else None


def create_dash_app() -> Dash:
    app = Dash(
        __name__,
        requests_pathname_prefix="/dash/",
    )

    app.layout = html.Div(
        style={"maxWidth": "1100px", "margin": "0 auto", "padding": "24px"},
        children=[
            dcc.Location(id="url", refresh=False),
            html.H3("Analytics Dashboard"),
            html.P(
                "Choose a highlight color and view the generated results.",
                style={"color": "#556"},
            ),
            html.Div(
                style={"maxWidth": "320px", "marginBottom": "16px"},
                children=[
                    html.Label("Highlight color", style={"fontWeight": "600"}),
                    dcc.Dropdown(
                        id="color-dropdown",
                        options=_COLOR_OPTIONS,
                        value="#2D7FF9",
                        clearable=False,
                    ),
                ],
            ),
            html.Div(id="pipeline-message", style={"color": "#C33", "marginBottom": "12px"}),
            dcc.Graph(id="pipeline-graph"),
        ],
    )

    @app.callback(
        Output("pipeline-graph", "figure"),
        Output("pipeline-message", "children"),
        Input("url", "search"),
        Input("color-dropdown", "value"),
    )
    def render_pipeline(search: Optional[str], node_color: str):
        pipeline_id = _get_pipeline_id(search)
        pipeline = get_pipeline(pipeline_id) if pipeline_id else None
        analytics = pipeline.get("analytics") if pipeline else None
        figure = build_analytics_figure(analytics=analytics, node_color=node_color)
        message = "" if analytics else "No analytics available. Generate a pipeline with data to see results."
        return figure, message

    return app
