import base64
import io
import re
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
    import unicodedata
    def slugify(value):
        value = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii")
        value = re.sub(r"[^a-zA-Z0-9-]+", "-", str(value).lower()).strip("-")
        return re.sub(r"-+", "-", value)

# -----------------------
# Config & Helpers
# -----------------------
st.set_page_config(page_title="Shopify CSV → Prodotti", page_icon="🛒", layout="wide")

raw_store = st.secrets.get("SHOPIFY_STORE", "")
SHOPIFY_TOKEN = st.secrets.get("SHOPIFY_TOKEN", "")
API_VERSION   = st.secrets.get("API_VERSION", "2024-07")

def normalize_store_host(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"^https?://", "", s, flags=re.I)  # remove protocol
    s = s.replace("/admin", "")
    s = s.strip("/")
    return s

SHOPIFY_STORE = normalize_store_host(raw_store)

def valid_host(h: str) -> bool:
    return bool(h) and ("." in h or h.endswith("myshopify.com"))

if not valid_host(SHOPIFY_STORE):
    st.error("`SHOPIFY_STORE` non valido o mancante. Usa il formato **nome-store.myshopify.com** senza `https://` né `/admin`.")
    st.stop()
if not SHOPIFY_TOKEN:
    st.error("`SHOPIFY_TOKEN` mancante. Inserisci l'Admin API access token nelle *secrets*.")
    st.stop()

API_BASE = f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}"

# ---- Requests session with retry + bigger timeouts ----
from requests.adapters import HTTPAdapter, Retry
session = requests.Session()
retries = Retry(total=5, backoff_factor=0.8, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET","POST","PUT"])
session.mount("https://", HTTPAdapter(max_retries=retries))
DEFAULT_TIMEOUT = (10, 180)  # (connect, read) seconds

HEADERS = {
    "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    "Content-Type": "application/json",
    "Accept": "application/json",
}

class ShopifyError(Exception):
    pass

def api_get(path: str) -> Dict[str, Any]:
    url = f"{API_BASE}{path}"
    resp = session.get(url, headers=HEADERS, timeout=DEFAULT_TIMEOUT)
    if resp.status_code >= 400:
        raise ShopifyError(f"GET {path} -> {resp.status_code}: {resp.text}")
    return resp.json()

def api_post(path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{API_BASE}{path}"
    resp = session.post(url, headers=HEADERS, json=payload, timeout=DEFAULT_TIMEOUT)
    if resp.status_code >= 400:
        raise ShopifyError(f"POST {path} -> {resp.status_code}: {resp.text}")
    return resp.json()

def api_put(path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{API_BASE}{path}"
    resp = session.put(url, headers=HEADERS, json=payload, timeout=DEFAULT_TIMEOUT)
    if resp.status_code >= 400:
        raise ShopifyError(f"PUT {path} -> {resp.status_code}: {resp.text}")
    return resp.json()

# -----------------------
# UI – Sidebar
# -----------------------
with st.sidebar:
    st.header("Impostazioni")
    st.caption(f"Store: `{SHOPIFY_STORE}` — API: `{API_VERSION}`")
    default_vendor = st.text_input("Vendor predefinito", value="Brand")
    default_product_type = st.text_input("Product Type predefinito", value="Altro")
    default_price = st.number_input("Prezzo predefinito (mancando nel CSV)", min_value=0.0, value=0.0, step=0.01, help="Shopify richiede almeno una variante con prezzo. Se il CSV non lo contiene, userò questo.")
    default_status = st.selectbox("Status prodotto", options=["active", "draft"], index=0)
    inventory_policy = st.selectbox("Inventory policy", options=["deny", "continue"], index=0, help="Se esaurito: 'deny' blocca, 'continue' consente.")
    inventory_qty_default = st.number_input("Quantità inventario di default", min_value=0, value=0, step=1)
    max_images_per_product = st.number_input("Max immagini per prodotto", min_value=0, value=10, step=1, help="Per ridurre tempi e timeout.")
    st.caption("Le immagini sono abbinate per **SKU** o **Handle URL** (il filename contiene SKU o handle).")
    if st.button("Test connessione Shopify"):
        try:
            info = api_get("/shop.json")
            st.success(f"Connessione OK · Shop: {info.get('shop', {}).get('name', 'sconosciuto')}")
        except Exception as e:
            st.error(f"Connessione fallita: {e}")

# -----------------------
# UI – Main
# -----------------------
st.title("CSV → Shopify + Immagini .zip (anti-timeout)")
st.write("L’app crea prima il prodotto **senza immagini** e poi carica le immagini **una per una** per evitare timeout.")

csv_file = st.file_uploader("Carica CSV", type=["csv"])
zip_file = st.file_uploader("Carica immagini (.zip)", type=["zip"])

st.markdown("**Colonne attese nel CSV:** `Titolo Prodotto`, `SKU`, `Descrizione`, `Collezioni`, `Tag`, `Titolo della pagina`, `Meta descrizione`, `Handle URL`.")
st.caption("Se mancano colonne, l'app usa fallback sensati (es. handle generato).")

if csv_file:
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
    supported_ext = (".jpg", ".jpeg", ".png", ".gif", ".webp")
    index = {}
    for name in zf.namelist():
        if name.lower().endswith(supported_ext) and not name.endswith("/"):
            with zf.open(name) as f:
                index[name.split('/')[-1].lower()] = f.read()
    return index

def find_images_for_product(index: Dict[str, bytes], keys: List[str]) -> List[Dict[str, Any]]:
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

@retry(
    reraise=True,
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=20),
    retry=retry_if_exception_type(ShopifyError),
)
def attach_image(product_id: int, image_payload: Dict[str, Any]) -> Dict[str, Any]:
    return api_post(f"/products/{product_id}/images.json", {"image": image_payload})

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

        # Pre-calc immagini ma NON le invio durante la creazione prodotto
        images_found = []
        if image_index:
            keys = [k for k in [sku, handle] if k]
            images_found = find_images_for_product(image_index, keys)
            if max_images_per_product and len(images_found) > max_images_per_product:
                images_found = images_found[:max_images_per_product]

        # === 1) CREA PRODOTTO SENZA IMMAGINI (payload leggero) ===
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

        try:
            res = create_product(product_payload)
            prod = res.get("product", {})
            product_id = prod.get("id")
            try:
                update_product_metafields(product_id, seo_title, seo_desc)
            except ShopifyError as e:
                st.info(f"SEO non aggiornato per {title}: {e}")

            # === 2) ALLEGA IMMAGINI UNA AD UNA ===
            attached = 0
            for img in images_found:
                try:
                    attach_image(product_id, img)
                    attached += 1
                except ShopifyError as e:
                    logs.append({
                        "row": i,
                        "title": title,
                        "sku": sku,
                        "product_id": product_id,
                        "status": "image_error",
                        "error": str(e)[:300],
                        "filename": img.get("filename")
                    })

            logs.append({
                "row": i,
                "title": title,
                "sku": sku,
                "product_id": product_id,
                "handle": prod.get("handle"),
                "status": "created",
                "images_attached": attached,
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
