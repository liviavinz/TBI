"""
SHT Monitoring Dashboard
"""
import dash_ag_grid as dag
from dash import Dash, html, dcc, Input, Output, State

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
REGISTERED_KPI_STYLE = {**KPI_STYLE, "background": "#e5ffe5"}


# ── App ───────────────────────────────────────────────────────────────────────
app = Dash(__name__)


# ── Column definitions ────────────────────────────────────────────────────────
PATIENT_COLDEFS = [
    {"headerName": "Patienten ID",    "field": "PatientID",      "filter": "agNumberColumnFilter"},
    {"headerName": "Fallnummer",      "field": "SocialSecurity", "filter": "agTextColumnFilter"},
    {"headerName": "Nachname",        "field": "LastName",       "filter": "agTextColumnFilter"},
    {"headerName": "Vorname",         "field": "FirstName",      "filter": "agTextColumnFilter"},
    {"headerName": "Alter",           "field": "Age",            "filter": "agNumberColumnFilter"},
    {"headerName": "Geschlecht",      "field": "Gender",         "filter": "agTextColumnFilter"},
    {"headerName": "Bett ID",         "field": "BedID",          "filter": "agNumberColumnFilter"},
    {"headerName": "Intensivstation", "field": "ICU",            "filter": "agTextColumnFilter"},
    {"headerName": "Status",          "field": "Status",         "filter": "agSetColumnFilter"},
    {"headerName": "GCS (aktuell)",   "field": "gcs_display",    "filter": "agTextColumnFilter"},
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
]

REGISTER_COLDEFS = [
    {"headerName": "Patienten ID", "field": "PatientID",      "filter": "agNumberColumnFilter"},
    {"headerName": "Fallnummer",   "field": "SocialSecurity", "filter": "agTextColumnFilter"},
    {"headerName": "Nachname",     "field": "LastName",       "filter": "agTextColumnFilter"},
    {"headerName": "Vorname",      "field": "FirstName",      "filter": "agTextColumnFilter"},
    {"headerName": "Alter",        "field": "Age",            "filter": "agNumberColumnFilter"},
    {"headerName": "Geschlecht",   "field": "Gender",         "filter": "agTextColumnFilter"},
]


# ── Layout ────────────────────────────────────────────────────────────────────
def serve_layout():
    reg = register.load()
    overview, n_patients = _build_overview_with_register(reg)

    table_data      = overview.to_dict("records")
    registered_data = overview[overview["Registered"] == "Ja"].to_dict("records")
    registered_count = sum(
        1 for d in reg.values() if d.get("register_confirmed") == "Ja"
    )

    return html.Div(
        style={"fontFamily": "Arial", "padding": "24px", "maxWidth": "1600px", "margin": "0 auto"},
        children=[

            dcc.Store(id="register-store", data=reg),

            dcc.Interval(
                id="data-refresh-interval",
                interval=30 * 1000,
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
                        [html.H2(str(n_patients), id="kpi-total", style={"margin": 0}),
                         html.P("Total SHT Patienten", style={"margin": 0})],
                        style=KPI_STYLE,
                    ),
                    html.Div(
                        [html.H2(str(registered_count), id="kpi-registered",
                                 style={"margin": 0, "color": "#27ae60"}),
                         html.P("Registriert", style={"margin": 0})],
                        style=REGISTERED_KPI_STYLE,
                    ),
                ],
            ),

            # ── Patient table ──────────────────────────────────────────────
            html.H2("Patientenübersicht",
                    style={"borderBottom": "2px solid #ddd", "paddingBottom": "6px"}),
            dag.AgGrid(
                id="patient-table",
                rowData=table_data,
                columnDefs=PATIENT_COLDEFS,
                defaultColDef={"sortable": True, "resizable": True, "floatingFilter": True, "filter": True},
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

            # ── Register table ─────────────────────────────────────────────
            html.H3("Patientenregister",
                    style={"borderBottom": "2px solid #ddd", "paddingBottom": "6px"}),
            dag.AgGrid(
                id="registered-table",
                rowData=registered_data,
                columnDefs=REGISTER_COLDEFS,
                defaultColDef={"sortable": True, "resizable": True, "floatingFilter": True, "filter": True},
                dashGridOptions={"animateRows": True},
                style={"height": "300px", "overflowY": "auto", "marginBottom": "30px"},
            ),
        ],
    )

app.layout = serve_layout


# ── Callbacks ─────────────────────────────────────────────────────────────────
@app.callback(
    Output("patient-table",    "rowData"),
    Output("registered-table", "rowData"),
    Output("kpi-total",        "children"),
    Input("data-refresh-interval", "n_intervals"),
    Input("register-store", "data"),
)
def refresh_tables(_, reg):
    """Refresh both tables and the total-patient KPI from fresh DB data."""
    overview, n_patients = _build_overview_with_register(reg or {})
    table_data      = overview.to_dict("records")
    registered_data = overview[overview["Registered"] == "Ja"].to_dict("records")
    return table_data, registered_data, str(n_patients)


@app.callback(
    Output("register-store", "data"),
    Input("patient-table", "cellValueChanged"),
    State("register-store", "data"),
    prevent_initial_call=True,
)
def save_registered(cell_changed, current_register):
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


@app.callback(
    Output("kpi-registered", "children"),
    Input("register-store", "data"),
)
def update_kpi_registered(reg):
    if not reg:
        return "0"
    count = sum(
        1 for d in reg.values() if d.get("register_confirmed") == "Ja"
    )
    return str(count)


if __name__ == "__main__":
    app.run(debug=True, use_reloader=False, host="0.0.0.0", port=8051)