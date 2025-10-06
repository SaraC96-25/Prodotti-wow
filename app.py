import base64
import io
import zipfile
from typing import Dict, Any, List, Optional

import pandas as pd
import requests
import streamlit as st
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# --- robust slugify import with fallback ---
try:
    from slugify import slugify  # from python-slugify
except Exception:
    import re, unicodedata
    def slugify(value):
        # fallback minimale (ASCII-only)
        value = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii")
        value = re.sub(r"[^a-zA-Z0-9-]+", "-", value.lower()).strip("-")
        return re.sub(r"-+", "-", value)

# -----------------------
# Config & Helpers
# -----------------------
st.set_page_config(page_title="Shopify CSV → Prodotti", page_icon="🛒", layout="wide")

SHOPIFY_STORE = st.secrets.get("SHOPIFY_STORE", "")
SHOPIFY_TOKEN = st.secrets.get("SHOPIFY_TOKEN", "")
API_VERSION   = st.secrets.get("API_VERSION", "2024-07")

if not (SHOPIFY_STORE and SHOPIFY_TOKEN):
    st.warning("Configura `.streamlit/secrets.toml` con SHOPIFY_STORE e SHOPIFY_TOKEN.")

API_BASE = f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}"

HEADERS = {
    "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    "Content-Type": "application/json",
    "Accept": "application/json",
}

class ShopifyError(Exception):
    pass

def api_post(path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{API_BASE}{path}"
    resp = requests.post(url, headers=HEADERS, json=payload, timeout=60)
    if resp.status_code >= 400:
        raise ShopifyError(f"POST {path} -> {resp.status_code}: {resp.text}")
    return resp.json()

def api_put(path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{API_BASE}{path}"
    resp = requests.put(url, headers=HEADERS, json=payload, timeout=60)
    if resp.status_code >= 400:
        raise ShopifyError(f"PUT {path} -> {resp.status_code}: {resp.text}")
    return resp.json()

# -----------------------
# UI – Sidebar
# -----------------------
st.sidebar.header("Impostazioni")
default_vendor = st.sidebar.text_input("Vendor predefinito", value="Brand")
default_product_type = st.sidebar.text_input("Product Type predefinito", value="Altro")
default_price = st.sidebar.number_input("Prezzo predefinito (mancando nel CSV)", min_value=0.0, value=0.0, step=0.01, help="Shopify richiede almeno una variante con prezzo. Se il CSV non lo contiene, userò questo.")
default_status = st.sidebar.selectbox("Status prodotto", options=["active", "draft"], index=0)
inventory_policy = st.sidebar.selectbox("Inventory policy", options=["deny", "continue"], index=0, help="Se esaurito: 'deny' blocca, 'continue' consente.")
inventory_qty_default = st.sidebar.number_input("Quantità inventario di default", min_value=0, value=0, step=1)

st.sidebar.caption("Le immagini sono abbinate per **SKU** o **Handle URL** (il filename contiene SKU o handle).")

# -----------------------
# UI – Main
# -----------------------
st.title("CSV → Shopify + Immagini .zip")
st.write("Carica il tuo CSV (in italiano come l’esempio) e un .zip di immagini. L’app creerà i prodotti su Shopify e allegherà le immagini corrispondenti.")

csv_file = st.file_uploader("Carica CSV", type=["csv"])
zip_file = st.file_uploader("Carica immagini (.zip)", type=["zip"])

st.markdown("**Colonne attese nel CSV:** `Titolo Prodotto`, `SKU`, `Descrizione`, `Collezioni`, `Tag`, `Titolo della pagina`, `Meta descrizione`, `Handle URL`.")
st.caption("Se mancano colonne, l'app usa fallback sensati (es. handle generato).")

if csv_file:
    # Tenta decodifiche comuni
    try:
        df = pd.read_csv(csv_file)
    except UnicodeDecodeError:
        csv_file.seek(0)
        df = pd.read_csv(csv_file, encoding="latin-1")
    st.subheader("Anteprima CSV")
    st.dataframe(df.head(20), use_container_width=True)
else:
    df = None

# -----------------------
# ZIP → mappa immagini
# -----------------------
def build_image_index_from_zip(zf: zipfile.ZipFile) -> Dict[str, bytes]:
    """
    Ritorna un dizionario { filename_lower: file_bytes }
    Include solo file immagini comuni.
    """
    supported_ext = (".jpg", ".jpeg", ".png", ".gif", ".webp")
    index = {}
    for name in zf.namelist():
        if name.lower().endswith(supported_ext) and not name.endswith("/"):
            with zf.open(name) as f:
                index[name.split("/")[-1].lower()] = f.read()
    return index

def find_images_for_product(index: Dict[str, bytes], keys: List[str]) -> List[Dict[str, str]]:
    """
    Cerca immagini per una lista di chiavi (SKU, handle).
    Qualsiasi filename che contenga una delle chiavi viene associato.
    Ritorna lista di dict {attachment, filename}.
    """
    found = []
    keys = [k.lower() for k in keys if k]
    for fname, content in index.items():
        if any(k in fname for k in keys):
            b64 = base64.b64encode(content).decode("utf-8")
            found.append({"attachment": b64, "filename": fname})
    return found

# -----------------------
# Creazione prodotto
# -----------------------
@retry(
    reraise=True,
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=20),
    retry=retry_if_exception_type(ShopifyError),
)
def create_product(payload: Dict[str, Any]) -> Dict[str, Any]:
    return api_post("/products.json", {"product": payload})

def update_product_metafields(product_id: int, seo_title: Optional[str], seo_desc: Optional[str]) -> None:
    update = {"product": {}}
    if seo_title:
        update["product"]["metafields_global_title_tag"] = seo_title[:70]
    if seo_desc:
        update["product"]["metafields_global_description_tag"] = seo_desc[:320]
    if update["product"]:
        api_put(f"/products/{product_id}.json", update)

# -----------------------
# Go!
# -----------------------
if st.button("Crea prodotti su Shopify", type="primary", disabled=(df is None)):
    if df is None:
        st.error("Carica prima un CSV.")
        st.stop()

    # Prepara indice immagini (se presente)
    image_index = {}
    if zip_file:
        try:
            with zipfile.ZipFile(zip_file) as zf:
                image_index = build_image_index_from_zip(zf)
            st.success(f"Immagini indicizzate: {len(image_index)} file.")
        except zipfile.BadZipFile:
            st.error("Il file ZIP non è valido.")
            st.stop()

    required_cols = ["Titolo Prodotto", "SKU", "Descrizione"]
    for col in required_cols:
        if col not in df.columns:
            st.warning(f"Colonna mancante nel CSV: **{col}**")

    logs = []
    progress = st.progress(0.0)
    total = len(df)

    for i, row in df.iterrows():
        title = str(row.get("Titolo Prodotto", "")).strip()
        sku = str(row.get("SKU", "")).strip()
        body_html = str(row.get("Descrizione", "")).strip()

        collections = str(row.get("Collezioni", "") or "").strip()
        tags = str(row.get("Tag", "") or "").strip()
        seo_title = str(row.get("Titolo della pagina", "") or "").strip()
        seo_desc  = str(row.get("Meta descrizione", "") or "").strip()
        handle    = str(row.get("Handle URL", "") or "").strip() or (slugify(title) if title else None)

        if not title:
            logs.append({"row": i, "title": title, "sku": sku, "status": "skipped", "reason": "Titolo mancante"})
            progress.progress((i + 1) / total)
            continue

        variant = {
            "sku": sku if sku else None,
            "price": str(default_price),
            "inventory_policy": inventory_policy,
            "inventory_management": "shopify",
            "inventory_quantity": int(inventory_qty_default),
            "requires_shipping": True,
            "taxable": True
        }

        tag_list = [t.strip() for t in tags.split(",") if t.strip()]
        tags_str = ", ".join(tag_list) if tag_list else None

        images_payload = []
        if image_index:
            keys = [k for k in [sku, handle] if k]
            images_payload = find_images_for_product(image_index, keys)

        product_payload = {
            "title": title,
            "body_html": body_html,
            "vendor": default_vendor,
            "product_type": default_product_type,
            "status": default_status,
            "tags": tags_str,
            "variants": [variant],
        }
        if handle:
            product_payload["handle"] = handle
        if images_payload:
            product_payload["images"] = images_payload

        try:
            res = create_product(product_payload)
            prod = res.get("product", {})
            product_id = prod.get("id")
            try:
                update_product_metafields(product_id, seo_title, seo_desc)
            except ShopifyError as e:
                st.info(f"SEO non aggiornato per {title}: {e}")

            logs.append({
                "row": i,
                "title": title,
                "sku": sku,
                "product_id": product_id,
                "handle": prod.get("handle"),
                "status": "created",
                "images_attached": len(images_payload),
            })
        except ShopifyError as e:
            logs.append({
                "row": i,
                "title": title,
                "sku": sku,
                "status": "error",
                "error": str(e)[:500],
            })

        progress.progress((i + 1) / total)

    log_df = pd.DataFrame(logs)
    st.subheader("Risultati")
    st.dataframe(log_df, use_container_width=True)

    buf = io.StringIO()
    log_df.to_csv(buf, index=False)
    st.download_button("Scarica log CSV", buf.getvalue(), file_name="shopify_upload_log.csv", mime="text/csv")

    st.success("Completato.")
