"""
Main script for the TBI application.
1. Sync data from SQL Server to SQLite
2. Start the dashboard
"""
from sync_tbi import TBISync
from dashboard_tbi import app
import dashboard_tbi

if __name__ == "__main__":
    # 1. Sync data
    print("Starting sync...")
    sync = TBISync()
    sync.run()
    print("Sync complete!")

    # 2. Reload data after sync
    from sqlite_tbi import TBIDatabase
    from tbi_data import TBIData
    from tbi_register import TBIRegister
    db = TBIDatabase()
    dashboard_tbi.data     = TBIData(db)
    dashboard_tbi.register = TBIRegister(db)

    # 3. Start dashboard
    print("Starting dashboard on http://localhost:8052")
    app.run(debug=True, use_reloader=False, port=8052)