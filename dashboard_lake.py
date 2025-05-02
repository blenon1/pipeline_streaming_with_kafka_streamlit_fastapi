import streamlit as st
import requests
import pandas as pd
import matplotlib.pyplot as plt


API_BASE = "http://127.0.0.1:8000"

st.set_page_config(page_title="Governance Dashboard", layout="wide")
st.title("📊 Data Lake Governance Dashboard")

# Section 0 – Statistiques globales
st.header("📊 Statistiques des transactions")

# Récupération des données
resp = requests.get(f"{API_BASE}/transactions", params={"limit": 500})
if resp.status_code == 200:
    txs = pd.DataFrame(resp.json())

    # ➤ Pie Chart par statut
    st.subheader("Répartition des statuts")
    status_counts = txs["status"].value_counts()
    fig1, ax1 = plt.subplots()
    ax1.pie(status_counts, labels=status_counts.index, autopct='%1.1f%%', startangle=90)
    ax1.axis('equal')
    st.pyplot(fig1)

    # ➤ Histogramme par date
    st.subheader("Volume de transactions par jour")
    txs["received_at"] = pd.to_datetime(txs["received_at"])
    txs["day"] = txs["received_at"].dt.date
    daily = txs.groupby("day").size()
    fig2, ax2 = plt.subplots()
    daily.plot(kind='bar', ax=ax2)
    ax2.set_ylabel("Nb transactions")
    ax2.set_xlabel("Date")
    st.pyplot(fig2)
else:
    st.error("❌ Impossible de charger les données.")


# Section 1 – Transactions
st.header("🧾 Transactions récentes")
limit = st.slider("Nombre de transactions à afficher", min_value=1, max_value=100, value=20)
resp = requests.get(f"{API_BASE}/transactions", params={"limit": limit})

if resp.status_code == 200:
    st.dataframe(resp.json())
else:
    st.error("❌ Impossible de récupérer les transactions")

# Section 2 – Vérification des permissions
st.header("🔐 Vérifier les permissions")
user_id = st.text_input("User ID")
path = st.text_input("Chemin du data lake (ex: transaction_log/2025-04-10)")
if st.button("Vérifier"):
    r = requests.get(f"{API_BASE}/permissions/check", params={"user_id": user_id, "path": path})
    if r.ok:
        result = r.json()
        if result["access"]:
            st.success(f"✅ Accès autorisé ({result['level']})")
        else:
            st.warning("⛔ Accès refusé")
    else:
        st.error("Erreur lors de la vérification.")

# Section 3 – Ajouter ou modifier une permission
st.header("➕ Ajouter/Modifier une permission")
with st.form("permission_form"):
    new_user = st.text_input("User ID", key="new_user")
    new_path = st.text_input("Chemin du dossier", key="new_path")
    level = st.selectbox("Niveau de permission", ["read", "write", "admin"])
    submitted = st.form_submit_button("Enregistrer")
    if submitted:
        post = requests.post(f"{API_BASE}/permissions/set", params={
            "user_id": new_user,
            "path": new_path,
            "level": level
        })
        if post.ok:
            st.success("✅ Permission enregistrée.")
        else:
            st.error(f"Erreur : {post.text}")
