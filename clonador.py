import os
import time
import requests

API_BASE = "https://api.tiendanube.com/v1"
USER_AGENT = "Clonador Chesmin a Jacula (jaculashoppingflores@gmail.com)"

# Lee secrets del workflow
CHESMIN_STORE_ID = int(os.environ["CHESMIN_STORE_ID"])
JACULA_STORE_ID = int(os.environ["JACULA_STORE_ID"])
CHESMIN_ACCESS_TOKEN = os.environ["CHESMIN_ACCESS_TOKEN"]
JACULA_ACCESS_TOKEN = os.environ["JACULA_ACCESS_TOKEN"]

PRICE_FACTOR = float(os.environ.get("PRICE_FACTOR", "1.0"))
EXCLUDED_CATEGORY_NAME = os.environ.get("EXCLUDED_CATEGORY_NAME", "Capsula Jacula ✿")

MAX_RETRIES = 8


def make_headers(token: str) -> dict:
    return {
        "Authentication": f"bearer {token}",
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    }


CHESMIN_HEADERS = make_headers(CHESMIN_ACCESS_TOKEN)
JACULA_HEADERS = make_headers(JACULA_ACCESS_TOKEN)


def request_with_retry(method, url, headers=None, params=None, json=None):
    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.request(method, url, headers=headers, params=params, json=json)

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            wait = int(retry_after) if retry_after and retry_after.isdigit() else min(2 ** attempt, 60)
            print(f"[429] Esperando {wait}s... (intento {attempt}/{MAX_RETRIES})")
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
        if resp.status_code == 404:
            break
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        products.extend(data)
        page += 1
    return products


def get_all_categories(store_id, headers):
    cats = []
    page = 1
    while True:
        resp = request_with_retry(
            "GET",
            f"{API_BASE}/{store_id}/categories",
            headers=headers,
            params={"page": page, "per_page": 200},
        )
        if resp.status_code == 404:
            break
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        cats.extend(data)
        page += 1
    return cats


def category_display_name(cat: dict) -> str:
    name_dict = cat.get("name") or {}
    return name_dict.get("es") or next(iter(name_dict.values()), "")


def product_has_excluded_category(product: dict) -> bool:
    for cat in product.get("categories", []):
        if isinstance(cat, dict):
            if category_display_name(cat) == EXCLUDED_CATEGORY_NAME:
                return True
    return False


def build_product_key(product: dict):
    for v in product.get("variants", []):
        sku = v.get("sku")
        if sku:
            return sku
    name = product.get("name") or {}
    return name.get("es") or next(iter(name.values()), None)


def adjust_prices_from_variant(src_variant: dict):
    price = float(src_variant["price"])
    promo = src_variant.get("promotional_price")

    new_price = round(price * PRICE_FACTOR)

    if promo:
        promo = float(promo)
        discount_factor = promo / price if price else 1.0
        new_promo = round(new_price * discount_factor)
    else:
        new_promo = None

    return new_price, new_promo


def safe_variant_values(v: dict):
    """
    Evita el 422 "Variant values should not be repeated" y "wrong number of elements".
    Tiendanube es sensible con values: si viene raro, mandamos [].
    """
    vals = v.get("values")
    if not isinstance(vals, list):
        return []
    # dedupe simple por (es/pt/en o string)
    seen = set()
    out = []
    for item in vals:
        key = str(item)
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def map_categories_by_name(src_product: dict, jacula_cat_by_name: dict):
    """
    Convierte categorías de Chesmin -> IDs existentes en Jacula según nombre.
    Devuelve lista de IDs (int) para Jacula.
    """
    out_ids = []
    for cat in src_product.get("categories", []):
        if not isinstance(cat, dict):
            continue
        name = category_display_name(cat)
        if not name:
            continue
        jac_id = jacula_cat_by_name.get(name)
        if isinstance(jac_id, int):
            out_ids.append(jac_id)
    # sin repetidos
    return list(dict.fromkeys(out_ids))


def build_jacula_payload_from_chesmin(src_product: dict, jacula_cat_by_name: dict) -> dict:
    variants_out = []
    for v in src_product.get("variants", []):
        new_price, new_promo = adjust_prices_from_variant(v)
        variant_out = {
            "name": v.get("name"),
            "sku": v.get("sku"),
            "price": new_price,
            "stock": v.get("stock"),
            "weight": v.get("weight"),
            "values": safe_variant_values(v),
        }
        if new_promo is not None:
            variant_out["promotional_price"] = new_promo
        variants_out.append(variant_out)

    images_out = []
    for img in src_product.get("images", []):
        src = img.get("src")
        if src:
            images_out.append({"src": src})

    # categorías mapeadas por nombre -> IDs de Jacula
    mapped_category_ids = map_categories_by_name(src_product, jacula_cat_by_name)

    payload = {
        "name": src_product.get("name"),
        "description": src_product.get("description"),
        # SOLO visibles se clonan; si el producto existe y luego lo ocultás en Chesmin, se oculta en Jacula
        "published": src_product.get("published"),
        "tags": src_product.get("tags"),
        "variants": variants_out,
        "images": images_out,
    }

    # Solo mandamos categorías si pudimos mapearlas a IDs existentes en Jacula
    if mapped_category_ids:
        payload["categories"] = mapped_category_ids

    return payload


def sync_chesmin_to_jacula():
    print("Descargando productos de Chesmin (origen)...")
    chesmin_products_all = get_all_products(CHESMIN_STORE_ID, CHESMIN_HEADERS)
    visibles = [p for p in chesmin_products_all if p.get("published") is True]
    print(f"  → {len(chesmin_products_all)} productos totales en Chesmin")
    print(f"  → {len(visibles)} productos visibles en Chesmin (a clonar/actualizar)")

    print("Descargando productos de Jacula (destino)...")
    jacula_products = get_all_products(JACULA_STORE_ID, JACULA_HEADERS)
    print(f"  → {len(jacula_products)} productos encontrados en Jacula")

    print("Descargando categorías de Jacula para mapear por nombre...")
    jacula_cats = get_all_categories(JACULA_STORE_ID, JACULA_HEADERS)
    jacula_cat_by_name = {category_display_name(c): c.get("id") for c in jacula_cats if c.get("id")}

    jacula_by_key = {}
    for p in jacula_products:
        key = build_product_key(p)
        if key:
            jacula_by_key[key] = p

    for src in visibles:
        key = build_product_key(src)
        if not key:
            print("Saltando producto de Chesmin SIN clave (sin sku ni nombre)")
            continue

        dst = jacula_by_key.get(key)
        payload = build_jacula_payload_from_chesmin(src, jacula_cat_by_name)

        if dst:
            if product_has_excluded_category(dst):
                print(f"[SKIP] '{EXCLUDED_CATEGORY_NAME}' no se toca. Key={key}")
                continue

            product_id = dst["id"]
            print(f"[UPDATE] id={product_id} key={key}")
            resp = request_with_retry(
                "PUT",
                f"{API_BASE}/{JACULA_STORE_ID}/products/{product_id}",
                headers=JACULA_HEADERS,
                json=payload,
            )
            print(f"  → {resp.status_code} {resp.text[:160]}")
        else:
            print(f"[CREATE] key={key}")
            resp = request_with_retry(
                "POST",
                f"{API_BASE}/{JACULA_STORE_ID}/products",
                headers=JACULA_HEADERS,
                json=payload,
            )
            print(f"  → {resp.status_code} {resp.text[:160]}")

    print("Sincronización terminada.")


if __name__ == "__main__":
    sync_chesmin_to_jacula()
