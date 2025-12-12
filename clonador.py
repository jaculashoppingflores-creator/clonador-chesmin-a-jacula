import os
import time
import json
import requests

API_BASE = "https://api.tiendanube.com/v1"

# ==============================
# CONFIG (por GitHub Secrets)
# ==============================
CHESMIN_STORE_ID = int(os.getenv("CHESMIN_STORE_ID", "1610487"))
JACULA_STORE_ID  = int(os.getenv("JACULA_STORE_ID", "6889084"))

CHESMIN_ACCESS_TOKEN = os.getenv("CHESMIN_ACCESS_TOKEN", "")
JACULA_ACCESS_TOKEN  = os.getenv("JACULA_ACCESS_TOKEN", "")

USER_AGENT = os.getenv("USER_AGENT", "Clonador Chesmin a Jacula (jaculashoppingflores@gmail.com)")

EXCLUDED_CATEGORY_NAME = "Capsula Jacula ✿"
PRICE_FACTOR = float(os.getenv("PRICE_FACTOR", "1.28"))

MAX_RETRIES = 8


def make_headers(access_token: str) -> dict:
    return {
        "Authentication": f"bearer {access_token}",
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    }


CHESMIN_HEADERS = make_headers(CHESMIN_ACCESS_TOKEN)
JACULA_HEADERS  = make_headers(JACULA_ACCESS_TOKEN)


# ==============================
# HTTP con retry (429 / 5xx)
# ==============================
def request_with_retry(method, url, headers=None, params=None, json_body=None):
    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.request(method, url, headers=headers, params=params, json=json_body, timeout=60)

        # Rate limit
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            wait = int(retry_after) if (retry_after and retry_after.isdigit()) else min(2 ** attempt, 60)
            print(f"[429] Esperando {wait}s... (intento {attempt}/{MAX_RETRIES})")
            time.sleep(wait)
            continue

        # Errores temporales
        if resp.status_code in (500, 502, 503, 504):
            wait = min(2 ** attempt, 60)
            print(f"[{resp.status_code}] Esperando {wait}s... (intento {attempt}/{MAX_RETRIES})")
            time.sleep(wait)
            continue

        return resp

    return resp


def get_all_products(store_id, headers):
    products = []
    page = 1

    while True:
        resp = request_with_retry(
            "GET",
            f"{API_BASE}/{store_id}/products",
            headers=headers,
            params={"page": page, "per_page": 200},
        )

        # En TN a veces 404 significa "no hay más páginas"
        if resp.status_code == 404:
            break

        resp.raise_for_status()
        data = resp.json()
        if not data:
            break

        products.extend(data)
        page += 1

    return products


# ==============================
# Helpers
# ==============================
def product_has_excluded_category(product: dict) -> bool:
    for cat in product.get("categories", []) or []:
        # cat suele venir como dict con "name" multi-idioma
        if isinstance(cat, dict):
            name_dict = cat.get("name", {}) or {}
            for lang_name in name_dict.values():
                if lang_name == EXCLUDED_CATEGORY_NAME:
                    return True
    return False


def build_product_key(product: dict):
    # 1) SKU (mejor)
    for v in product.get("variants", []) or []:
        sku = v.get("sku")
        if sku:
            return sku.strip()

    # 2) fallback: nombre ES
    name = product.get("name") or {}
    if isinstance(name, dict):
        return (name.get("es") or next(iter(name.values()), None))

    return None


def categories_to_ids(src_categories):
    """
    IMPORTANTÍSIMO: en create/update mandamos SOLO IDs.
    """
    out = []
    for c in src_categories or []:
        if isinstance(c, int):
            out.append(c)
        elif isinstance(c, dict) and "id" in c:
            out.append(int(c["id"]))
    return out


def dedupe_variant_values(values):
    """
    Soluciona el 422: 'Variant values should not be repeated'
    Deduplicamos valores repetidos dentro de la variante.
    """
    if not values:
        return []

    out = []
    seen = set()

    for val in values:
        # val suele ser dict; usamos id si existe, sino su representación estable
        if isinstance(val, dict):
            key = val.get("id")
            if key is None:
                key = json.dumps(val, sort_keys=True, ensure_ascii=False)
        else:
            key = str(val)

        if key in seen:
            continue
        seen.add(key)
        out.append(val)

    return out


def adjust_prices(src_price, src_promo):
    price = float(src_price)
    new_price = round(price * PRICE_FACTOR)

    if src_promo is None:
        return new_price, None

    promo = float(src_promo)
    # mismo % promo que en Chesmin
    discount_factor = promo / price if price else 1.0
    new_promo = round(new_price * discount_factor)
    return new_price, new_promo


def build_payload_from_chesmin(src_product: dict, full_update: bool) -> dict:
    """
    full_update=True: actualiza todo (imagenes, variantes, etc)
    full_update=False: SOLO sync de published (para ocultar)
    """
    payload = {
        "published": bool(src_product.get("published")),
    }

    if not full_update:
        return payload

    # Variantes
    variants_out = []
    for v in src_product.get("variants", []) or []:
        new_price, new_promo = adjust_prices(v.get("price"), v.get("promotional_price"))

        values_clean = dedupe_variant_values(v.get("values"))

        variant_out = {
            "name": v.get("name"),
            "sku": v.get("sku"),
            "price": new_price,
            "stock": v.get("stock"),
            "weight": v.get("weight"),
        }

        # Solo mandamos values si existen (y ya dedupeados)
        if values_clean:
            variant_out["values"] = values_clean

        if new_promo is not None:
            variant_out["promotional_price"] = new_promo

        variants_out.append(variant_out)

    # Imágenes (copiamos URLs)
    images_out = []
    for img in src_product.get("images", []) or []:
        src = img.get("src")
        if src:
            images_out.append({"src": src})

    payload.update({
        "name": src_product.get("name"),
        "description": src_product.get("description"),
        "categories": categories_to_ids(src_product.get("categories")),
        "tags": src_product.get("tags"),
        "variants": variants_out,
        "images": images_out,
    })

    return payload


# ==============================
# Sync principal
# ==============================
def sync_chesmin_to_jacula():
    if not CHESMIN_ACCESS_TOKEN or not JACULA_ACCESS_TOKEN:
        raise RuntimeError("Faltan tokens. Configurá CHESMIN_ACCESS_TOKEN y JACULA_ACCESS_TOKEN como Secrets/env vars.")

    print("Descargando productos de Chesmin (origen)...")
    chesmin_products = get_all_products(CHESMIN_STORE_ID, CHESMIN_HEADERS)
    total = len(chesmin_products)
    visible = [p for p in chesmin_products if p.get("published") is True]
    print(f"  → {total} productos totales en Chesmin")
    print(f"  → {len(visible)} productos visibles en Chesmin (a clonar/actualizar)")

    print("Descargando productos de Jacula (destino)...")
    jacula_products = get_all_products(JACULA_STORE_ID, JACULA_HEADERS)
    print(f"  → {len(jacula_products)} productos totales en Jacula")

    jacula_by_key = {}
    for p in jacula_products:
        key = build_product_key(p)
        if key:
            jacula_by_key[key] = p

    # 1) Crear/Actualizar SOLO visibles
    for src in visible:
        key = build_product_key(src)
        if not key:
            print("[SKIP] Chesmin sin key (sin SKU y sin nombre).")
            continue

        dst = jacula_by_key.get(key)

        # Si existe y está en Capsula Jacula ✿ -> no tocar
        if dst and product_has_excluded_category(dst):
            print(f"[SKIP] (Capsula Jacula ✿) key={key}")
            continue

        payload = build_payload_from_chesmin(src, full_update=True)

        if dst:
            product_id = dst["id"]
            print(f"[UPDATE] key={key} id={product_id}")
            resp = request_with_retry(
                "PUT",
                f"{API_BASE}/{JACULA_STORE_ID}/products/{product_id}",
                headers=JACULA_HEADERS,
                json_body=payload
            )
            if resp.status_code >= 400:
                print(f"  → ERROR {resp.status_code}: {resp.text[:300]}")
            else:
                print(f"  → OK {resp.status_code}")

        else:
            print(f"[CREATE] key={key}")
            resp = request_with_retry(
                "POST",
                f"{API_BASE}/{JACULA_STORE_ID}/products",
                headers=JACULA_HEADERS,
                json_body=payload
            )
            if resp.status_code >= 400:
                print(f"  → ERROR {resp.status_code}: {resp.text[:300]}")
            else:
                print(f"  → OK {resp.status_code}")

        time.sleep(0.25)  # un mini pacing para evitar 429

    # 2) Propagar ocultado (published=False) SOLO si el producto ya existe en Jacula
    hidden = [p for p in chesmin_products if p.get("published") is False]
    for src in hidden:
        key = build_product_key(src)
        if not key:
            continue

        dst = jacula_by_key.get(key)
        if not dst:
            continue  # IMPORTANTÍSIMO: no “inventamos” ocultar cosas nuevas

        if product_has_excluded_category(dst):
            continue

        product_id = dst["id"]
        payload = build_payload_from_chesmin(src, full_update=False)  # solo published
        print(f"[HIDE-SYNC] key={key} id={product_id} -> published={payload['published']}")

        resp = request_with_retry(
            "PUT",
            f"{API_BASE}/{JACULA_STORE_ID}/products/{product_id}",
            headers=JACULA_HEADERS,
            json_body=payload
        )
        if resp.status_code >= 400:
            print(f"  → ERROR {resp.status_code}: {resp.text[:300]}")
        else:
            print(f"  → OK {resp.status_code}")

        time.sleep(0.25)

    print("Sincronización terminada.")


if __name__ == "__main__":
    sync_chesmin_to_jacula()
