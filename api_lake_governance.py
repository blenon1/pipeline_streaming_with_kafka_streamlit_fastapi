from fastapi import FastAPI, HTTPException
import mysql.connector
import os

app = FastAPI(title="Data Lake Governance API")

# Connexion MySQL
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="R@kuzan19735.", 
    database="data_warehouse"
)
cursor = db.cursor(dictionary=True)

DATA_LAKE_PATH = "./data_lake"

@app.get("/")
def root():
    return {"message": "API de gouvernance du Data Lake 📁"}

# ✅ Lister les transactions
@app.get("/transactions")
def get_transactions(limit: int = 20):
    cursor.execute("SELECT * FROM transactions ORDER BY received_at DESC LIMIT %s", (limit,))
    return cursor.fetchall()

# ✅ Vérifier la permission d’un user sur un chemin
@app.get("/permissions/check")
def check_permission(user_id: str, path: str):
    cursor.execute("""
        SELECT permission_level FROM permissions
        WHERE user_id = %s AND lake_path = %s
    """, (user_id, path))
    row = cursor.fetchone()
    if row:
        return {"access": True, "level": row["permission_level"]}
    return {"access": False}

# ✅ Ajouter ou modifier une permission
@app.post("/permissions/set")
def set_permission(user_id: str, path: str, level: str):
    if level not in ("read", "write", "admin"):
        raise HTTPException(status_code=400, detail="Invalid permission level")
    
    cursor.execute("""
        INSERT INTO permissions (user_id, lake_path, permission_level)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE permission_level = VALUES(permission_level)
    """, (user_id, path, level))
    db.commit()
    return {"status": "updated"}

# ✅ Lister les partitions d’un feed
@app.get("/lake/{feed}/partitions")
def list_partitions(feed: str):
    feed_path = os.path.join(DATA_LAKE_PATH, feed)
    if not os.path.isdir(feed_path):
        raise HTTPException(status_code=404, detail="Feed non trouvé")
    return {"partitions": sorted(os.listdir(feed_path))}
