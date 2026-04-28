def reset_database(self, confirm: bool = False):
    """Delete the SQLite file. Set confirm=True to actually do it."""
    if not confirm:
        raise ValueError("reset_database requires confirm=True")
    if os.path.exists(self.DB_PATH):
        os.remove(self.DB_PATH)