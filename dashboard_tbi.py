"""
SHT Monitoring Dashboard
"""
import dash_ag_grid as dag
from dash import Dash, html, dcc, Input, Output, State, callback_context

from sqlite_tbi import TBIDatabase
from tbi_data import TBIData
from tbi_register import TBIRegister


# ── Initialise ────────────────────────────────────────────────────────────────
db       = TBIDatabase()
register = TBIRegister(db)


# ── Helpers ───────────────────────────────────────────────────────────────────
def _build_overview_with_register(reg: dict):
    """Load fresh data and merge with register state. Returns sorted overview."""
    fresh = TBIData(db)
    overview = fresh.overview.copy()
    overview["Registered"] = overview["PatientID"].apply(
        lambda pid: reg.get(str(pid), {}).get("register_confirmed", "") if reg else ""
    )
    overview = overview.sort_values("gcs_latest", ascending=True, na_position="last")
    return overview, fresh.n_patients


# ── Styles ────────────────────────────────────────────────────────────────────
KPI_STYLE = {
    "background": "#f0f4f8",
    "borderRadius": "10px",
    "padding": "20px 40px",
    "textAlign": "center",
    "boxShadow": "0 2px 6px rgba(0,0,0,0.1)",
    "minWidth": "160px",
}
INCLUDED_KPI_STYLE = {**KPI_STYLE, "background": "#e5ffe5"}
EXCLUDED_KPI_STYLE = {**KPI_STYLE, "background": "#ffe5e5"}

# ── App ───────────────────────────────────────────────────────────────────────
app = Dash(__name__)

# ── Column definitions ────────────────────────────────────────────────────────
PATIENT_COLDEFS = [
    {
        "headerName": "Eintrittsdatum",
        "field": "AdmissionDate",  # ISO for filtering
        "valueFormatter": {"function": "params.data.AdmissionDate_display || ''"},  # Swiss for display
        "filter": "agDateColumnFilter",
        "floatingFilterComponentParams": {"suppressFilterButton": True},
        "filterParams": {
            "browserDatePicker": True,
            "defaultOption": "greaterThan",
            "filterOptions": ["greaterThan"],
        },
    },
    {"headerName": "Nachname",        "field": "LastName",       "filter": "agTextColumnFilter"},
    {"headerName": "Vorname",         "field": "FirstName",      "filter": "agTextColumnFilter"},
    {"headerName": "General Consent", "field": "ConsentGeneral", "filter": "agTextColumnFilter"},
    {"headerName": "IFI Consent",     "field": "ConsentIFI",     "filter": "agTextColumnFilter"},
    {
        "headerName": "Registriert",
        "field": "Registered",
        "filter": "agTextColumnFilter",
        "editable": True,
        "cellEditor": "agSelectCellEditor",
        "cellEditorParams": {"values": ["", "Ja", "Nein"]},
        "cellStyle": {"cursor": "pointer"},
        "pinned": "right",
    },
    {"headerName": "Intensivstation", "field": "ICU", "filter": "agTextColumnFilter"},
    {"headerName": "Bett ID",         "field": "BedID",          "filter": "agTextColumnFilter"},
    {"headerName": "Status",          "field": "Status",         "filter": "agTextColumnFilter"},
    {"headerName": "Fallnummer", "field": "SocialSecurity", "filter": "agTextColumnFilter"},
    {"headerName": "Patienten ID",    "field": "PatientID",      "filter": "agTextColumnFilter"},
    {
        "headerName": "Alter",
        "field": "Age",
        "filter": "agNumberColumnFilter",
        "filterParams": {
            "defaultOption": "equals",
            "filterOptions": ["equals", "notEqual", "lessThan", "lessThanOrEqual",
                              "greaterThan", "greaterThanOrEqual", "inRange"],
        },
    },
    {"headerName": "GCS (aktuell)",   "field": "gcs_display",    "filter": "agTextColumnFilter"},
]

DECIDED_COLDEFS = [
    {"headerName": "Patienten ID", "field": "PatientID", "filter": "agTextColumnFilter", "flex": 2},
    {"headerName": "Fallnummer", "field": "SocialSecurity", "filter": "agTextColumnFilter", "flex": 2},
    {"headerName": "Nachname",     "field": "LastName",       "filter": "agTextColumnFilter", "flex": 2},
    {"headerName": "Vorname",      "field": "FirstName",      "filter": "agTextColumnFilter", "flex": 2},
    {
        "headerName": "Registriert",
        "field": "Registered",
        "filter": "agTextColumnFilter",
        "editable": True,
        "cellEditor": "agSelectCellEditor",
        "cellEditorParams": {"values": ["", "Ja", "Nein"]},
        "cellStyle": {"cursor": "pointer"},
        "flex": 1,
    },
]

# ── Layout ────────────────────────────────────────────────────────────────────
def serve_layout():
    reg = register.load()
    overview, _ = _build_overview_with_register(reg)

    pending_data  = overview[overview["Registered"] == ""].to_dict("records")
    included_data = overview[overview["Registered"] == "Ja"].to_dict("records")
    excluded_data = overview[overview["Registered"] == "Nein"].to_dict("records")

    pending_count  = len(pending_data)
    included_count = len(included_data)
    excluded_count = len(excluded_data)

    return html.Div(
        style={"fontFamily": "Arial", "padding": "24px", "maxWidth": "1600px", "margin": "0 auto"},
        children=[

            dcc.Store(id="register-store", data=reg),

            dcc.Interval(
                id="data-refresh-interval",
                interval=15* 60 * 1000,
                n_intervals=0,
            ),

            html.H1(
                "SHT Monitoring Dashboard",
                style={"textAlign": "center", "marginBottom": "30px", "color": "#1a1a2e"},
            ),

            # ── KPIs ──────────────────────────────────────────────────────
            html.Div(
                style={"display": "flex", "gap": "20px", "justifyContent": "center", "marginBottom": "36px"},
                children=[
                    html.Div(
                        [html.H2(str(pending_count), id="kpi-pending", style={"margin": 0}),
                         html.P("Aktuelle Patienten", style={"margin": 0})],
                        style=KPI_STYLE,
                    ),
                    html.Div(
                        [html.H2(str(included_count), id="kpi-included",
                                 style={"margin": 0, "color": "#27ae60"}),
                         html.P("Eingeschlossen", style={"margin": 0})],
                        style=INCLUDED_KPI_STYLE,
                    ),
                    html.Div(
                        [html.H2(str(excluded_count), id="kpi-excluded",
                                 style={"margin": 0, "color": "#c0392b"}),
                         html.P("Nicht eingeschlossen", style={"margin": 0})],
                        style=EXCLUDED_KPI_STYLE,
                    ),
                ],
            ),

            # ── Table 1: Aktuelle Patienten ───────────────────────────
            html.H2(
                children=[
                    "Aktuelle Patienten ",
                    html.Span(
                        "(Diagnose: Schädel-Hirn-Trauma (T1), Consent = ja, Station = IFI)",
                        style={"fontSize": "0.6em", "color": "#666", "fontWeight": "normal"},
                    ),
                ],
                style={"borderBottom": "2px solid #ddd", "paddingBottom": "6px"},
            ),
            dag.AgGrid(
                id="pending-table",
                rowData=pending_data,
                columnDefs=PATIENT_COLDEFS,
                dangerously_allow_code=True,
                defaultColDef={
                    "sortable": True, "resizable": True, "floatingFilter": True, "filter": True,
                    "filterParams": {"defaultOption": "startsWith", "filterOptions": ["startsWith"]},
                },
                dashGridOptions={
                    "animateRows": True,
                    "rowSelection": "single",
                    "singleClickEdit": True,
                },
                style={
                    "height": "400px",
                    "overflowY": "auto",
                    "marginBottom": "30px",
                    "overflowX": "scroll",
                },
            ),

            # ── Table 2: Eingeschlossene Patienten ────────────────────
            html.H3(
                children=[
                    "Eingeschlossene Patienten ",
                    html.Span(
                        "(Registriert = Ja)",
                        style={"fontSize": "0.7em", "color": "#666", "fontWeight": "normal"},
                    ),
                ],
                style={"borderBottom": "2px solid #27ae60", "paddingBottom": "6px"},
            ),
            dag.AgGrid(
                id="included-table",
                rowData=included_data,
                columnDefs=DECIDED_COLDEFS,
                defaultColDef={"sortable": True, "resizable": True, "floatingFilter": True, "filter": True},
                dashGridOptions={"animateRows": True, "rowSelection": "single", "singleClickEdit": True},
                style={"height": "300px", "overflowY": "auto", "marginBottom": "30px"},
            ),

            # ── Table 3: Nicht eingeschlossene Patienten ──────────────
            html.H3(
                children=[
                    "Nicht eingeschlossene Patienten ",
                    html.Span(
                        "(Registriert = Nein)",
                        style={"fontSize": "0.7em", "color": "#666", "fontWeight": "normal"},
                    ),
                ],
                style={"borderBottom": "2px solid #c0392b", "paddingBottom": "6px"},
            ),
            dag.AgGrid(
                id="excluded-table",
                rowData=excluded_data,
                columnDefs=DECIDED_COLDEFS,
                defaultColDef={"sortable": True, "resizable": True, "floatingFilter": True, "filter": True},
                dashGridOptions={"animateRows": True, "rowSelection": "single", "singleClickEdit": True},
                style={"height": "300px", "overflowY": "auto", "marginBottom": "30px"},
            ),
        ],
    )

app.layout = serve_layout


# ── Callbacks ─────────────────────────────────────────────────────────────────
@app.callback(
    Output("pending-table",  "rowData"),
    Output("included-table", "rowData"),
    Output("excluded-table", "rowData"),
    Output("kpi-pending",    "children"),
    Output("kpi-included",   "children"),
    Output("kpi-excluded",   "children"),
    Input("data-refresh-interval", "n_intervals"),
    Input("register-store", "data"),
)
def refresh_tables(_, reg):
    """Refresh all three tables and KPIs from fresh DB data."""
    overview, _ = _build_overview_with_register(reg or {})

    pending_data  = overview[overview["Registered"] == ""].to_dict("records")
    included_data = overview[overview["Registered"] == "Ja"].to_dict("records")
    excluded_data = overview[overview["Registered"] == "Nein"].to_dict("records")

    return (
        pending_data,
        included_data,
        excluded_data,
        str(len(pending_data)),
        str(len(included_data)),
        str(len(excluded_data)),
    )


@app.callback(
    Output("register-store", "data"),
    Input("pending-table",  "cellValueChanged"),
    Input("included-table", "cellValueChanged"),
    Input("excluded-table", "cellValueChanged"),
    State("register-store", "data"),
    prevent_initial_call=True,
)
def save_registered(pending_changed, included_changed, excluded_changed, current_register):
    """Save register-state from any of the three tables."""
    ctx = callback_context
    if not ctx.triggered:
        return current_register

    triggered_id = ctx.triggered[0]["prop_id"].split(".")[0]

    cell_changed = {
        "pending-table":  pending_changed,
        "included-table": included_changed,
        "excluded-table": excluded_changed,
    }.get(triggered_id)

    if not cell_changed or cell_changed[0].get("colId") != "Registered":
        return current_register

    reg = dict(current_register) if current_register else {}
    pid = str(cell_changed[0]["data"].get("PatientID", ""))
    val = cell_changed[0]["data"].get("Registered", "")
    entry = reg.get(pid, {})
    entry["register_confirmed"] = val
    reg[pid] = entry
    register.save(reg)
    return reg


