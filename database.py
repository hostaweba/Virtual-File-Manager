# database.py
import sqlite3
from pathlib import Path

#  how the database queries smart views
SMART_PROTOCOLS = {
    "tags://": ["custom_tags"],
    "y_c_m_e://": ["year", "category", "month", "extension"],
    "c_y_m://": ["category", "year", "month"],
    "y_m://": ["year", "month"],
    "y_m_c://": ["year", "month", "category"],
    "y_c_m://": ["year", "category", "month"]
}

# ---------------- Database Engine ----------------
class vmanDB:
    def __init__(self, path: Path):
        self.path = path
        self.conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self._ensure_schema()

    def _ensure_schema(self):
        c = self.conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS virtual_fs (
                id INTEGER PRIMARY KEY, parent_path TEXT, name TEXT, is_folder INTEGER,
                real_path TEXT, size INTEGER, extension TEXT, modified TEXT,
                color_tag TEXT DEFAULT '', secondary_name TEXT DEFAULT '',
                is_hidden INTEGER DEFAULT 0, in_trash INTEGER DEFAULT 0,
                is_favorite INTEGER DEFAULT 0, sha256 TEXT DEFAULT '',
                category TEXT DEFAULT 'Others', year TEXT DEFAULT '', month TEXT DEFAULT '',
                custom_tags TEXT DEFAULT ''
            );
        """)
        # Removed unused 'owner' column to keep schema clean
        for col in [
            "color_tag TEXT DEFAULT ''", "secondary_name TEXT DEFAULT ''", 
            "is_hidden INTEGER DEFAULT 0", "in_trash INTEGER DEFAULT 0", 
            "is_favorite INTEGER DEFAULT 0", "sha256 TEXT DEFAULT ''",
            "category TEXT DEFAULT 'Others'", "year TEXT DEFAULT ''", "month TEXT DEFAULT ''",
            "custom_tags TEXT DEFAULT ''", "hash_verified INTEGER DEFAULT 0",
            "creation_date TEXT DEFAULT ''"
        ]:
            try: c.execute(f"ALTER TABLE virtual_fs ADD COLUMN {col};")
            except sqlite3.OperationalError: pass
            
        c.execute("UPDATE virtual_fs SET creation_date = modified WHERE creation_date = '' OR creation_date IS NULL;")
        
        c.execute("CREATE INDEX IF NOT EXISTS idx_vfs_parent ON virtual_fs(parent_path);")
        c.execute("CREATE INDEX IF NOT EXISTS idx_vfs_ycme ON virtual_fs(year, category, month, extension);")
        c.execute("CREATE INDEX IF NOT EXISTS idx_vfs_tags ON virtual_fs(custom_tags);")
        self.conn.commit()

    def get_stats(self, current_prefix=""):
        c = self.conn.cursor()
        where_clause = "is_folder=0 AND in_trash=0"
        params = []
        
        if current_prefix and current_prefix != "/":
            if current_prefix.startswith("tags://"):
                parts = [p for p in current_prefix.replace("tags://", "").split("/") if p]
                if len(parts) >= 1:
                    where_clause += " AND custom_tags LIKE ?"
                    params.append(f"%{parts[0]}%")
            elif current_prefix.startswith("y_m_f://"):
                parts = [p for p in current_prefix.replace("y_m_f://", "").split("/") if p]
                if len(parts) >= 1:
                    where_clause += " AND year=?"
                    params.append(parts[0])
                if len(parts) >= 2:
                    where_clause += " AND month=?"
                    params.append(parts[1])
            elif "://" in current_prefix:
                proto = current_prefix.split("://")[0] + "://"
                if proto in SMART_PROTOCOLS:
                    cols = SMART_PROTOCOLS[proto]
                    parts = [p for p in current_prefix.replace(proto, "").split("/") if p]
                    for i in range(min(len(parts), len(cols))):
                        where_clause += f" AND {cols[i]}=?"
                        params.append(parts[i])
            else:
                where_clause += " AND parent_path LIKE ?"
                params.append(f"{current_prefix}%")

        c.execute(f"SELECT COUNT(*), COALESCE(SUM(size), 0), MIN(modified), MAX(modified), COALESCE(AVG(size), 0) FROM virtual_fs WHERE {where_clause}", params)
        res = c.fetchone()
        f_cnt, total_sz, oldest, newest, avg_sz = (res[0] or 0, res[1] or 0, res[2] or "N/A", res[3] or "N/A", res[4] or 0)
        c.execute(f"SELECT COUNT(*) FROM virtual_fs WHERE is_folder=1 AND in_trash=0 AND parent_path LIKE ?", (f"{current_prefix}%",))
        d_cnt = c.fetchone()[0]
        c.execute(f"SELECT extension, COUNT(*), COALESCE(SUM(size),0) FROM virtual_fs WHERE {where_clause} GROUP BY extension", params)
        dist = c.fetchall()
        c.execute(f"SELECT name, size FROM virtual_fs WHERE {where_clause} ORDER BY size DESC LIMIT 10", params)
        top_files = c.fetchall()
        c.execute(f"SELECT year || '-' || month as dt, COUNT(*), SUM(size) FROM virtual_fs WHERE {where_clause} AND year != '' GROUP BY dt ORDER BY dt", params)
        time_series = c.fetchall()
        return {"files": f_cnt, "folders": d_cnt, "used_bytes": total_sz, "avg_bytes": avg_sz, "oldest": oldest, "newest": newest, "distribution": dist, "top_files": top_files, "time_series": time_series}

    def close(self):
        try: self.conn.close()
        except Exception: pass