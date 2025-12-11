import os
import time
import requests

API_BASE = "https://api.tiendanube.com/v1"

CHESMIN_STORE_ID = 1610487
CHESMIN_ACCESS_TOKEN = "0e6586bece80829a7050a3ecf5b6e084a8ee0a58"

JACULA_STORE_ID = 6889084
JACULA_ACCESS_TOKEN = "3c9c872098bb3a1469834dd8a7216880216f4cc1"

EXCLUDED_CATEGORY_NAME = os.environ.get("EXCLUDED_CATEGORY_NAME", "Capsula Jacula ✿")
PRICE_FACTOR = float(os.environ.get("PRICE_FACTOR", "1.28"))

USER_AGENT = os.environ.get("USER_AGENT", "Clonador Chesmin a Jacula")

PER_PAGE = 200
MAX_RETRIES = 6


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
            print(f"[429] Esperando {wait}s... (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(wait)
            continue

        if resp.status_code in (500, 502, 503, 504):
            wait = min(2 ** attempt, 30)
            print(f"[{resp.status_code}] Error temporal. Esperando {wait}s... (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(wait)
            continue

        return resp

    return resp


def get_all_products(store_id: int, headers: dict):
    products = []
    page = 1
    while True:
        url = f"{API_BASE}/{store_id}/products"
        resp = request_with_retry("GET", url, headers=headers, params={"page": page, "per_page": PER_PAGE})

        if resp.status_code == 404:
            break

        resp.raise_for_status()
        data = resp.json()
        if not data:
            break

        products.extend(data)
        page += 1

    return products


def build_product_key(product: dict) -> str | None:
    for v in product.get("variants", []):
        sku = v.get("sku")
        if sku:
            return sku

    name = product.get("name") or {}
    return name.get("es") or next(iter(name.values()), None)


def product_has_excluded_category(product: dict) -> bool:
    for cat in product.get("categories", []):
        name_dict = cat.get("name", {}) or {}
        for lang_name in name_dict.values():
            if lang_name == EXCLUDED_CATEGORY_NAME:
                return True
    return False


def dedupe_values(values):
    if not values:
        return values
    seen = set()
    out = []
    for item in values:
        if isinstance(item, dict):
            key = tuple(sorted(item.items()))
        else:
            key = str(item)
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def adjust_prices(price: float, promo: float | None):
    new_price = round(price * PRICE_FACTOR)

    if promo is not None and price > 0:
        discount_factor = promo / price
        new_promo = round(new_price * discount_factor)
    else:
        new_promo = None

    return new_price, new_promo


def build_jacula_payload_from_chesmin(src_product: dict) -> dict:
    variants_out = []
    for v in src_product.get("variants", []):
        price = float(v.get("price") or 0)
        promo = v.get("promotional_price")
        promo = float(promo) if promo not in (None, "") else None

        new_price, new_promo = adjust_prices(price, promo)

        values = dedupe_values(v.get("values"))

        variant_out = {
            "name": v.get("name"),
            "sku": v.get("sku"),
            "price": new_price,
            "stock": v.get("stock"),
            "weight": v.get("weight"),
            "values": values,
        }
        if new_promo is not None:
            variant_out["promotional_price"] = new_promo

        variants_out.append(variant_out)

    images_out = []
    for img in src_product.get("images", []):
        src = img.get("src")
        if src:
            images_out.append({"src": src})

    payload = {
        "name": src_product.get("name"),
        "description": src_product.get("description"),
        "categories": src_product.get("categories"),
        "tags": src_product.get("tags"),
        "variants": variants_out,
        "images": images_out,
    }
    return payload


def set_published_in_jacula(product_id: int, published: bool):
    url = f"{API_BASE}/{JACULA_STORE_ID}/products/{product_id}"
    resp = request_with_retry("PUT", url, headers=JACULA_HEADERS, json={"published": published})
    return resp


def sync_chesmin_to_jacula():
    print("Descargando productos de Chesmin...")
    chesmin_products = get_all_products(CHESMIN_STORE_ID, CHESMIN_HEADERS)

    visibles = [p for p in chesmin_products if p.get("published") is True]
    ocultos = [p for p in chesmin_products if p.get("published") is False]

    print(f"  → {len(chesmin_products)} productos totales en Chesmin")
    print(f"  → {len(visibles)} productos visibles en Chesmin (a clonar)")
    print(f"  → {len(ocultos)} productos ocultos en Chesmin (solo ocultan si ya existen en Jacula)")

    print("Descargando productos de Jacula...")
    jacula_products = get_all_products(JACULA_STORE_ID, JACULA_HEADERS)
    print(f"  → {len(jacula_products)} productos totales en Jacula")

    jacula_by_key = {}
    for p in jacula_products:
        key = build_product_key(p)
        if key:
            jacula_by_key[key] = p

    # 1) Crear/actualizar SOLO VISIBLES
    for src in visibles:
        key = build_product_key(src)
        if not key:
            print("[SKIP] Chesmin sin key (sin sku ni nombre).")
            continue

        dst = jacula_by_key.get(key)

        if dst and product_has_excluded_category(dst):
            print(f"[SKIP] '{EXCLUDED_CATEGORY_NAME}' no se toca. key={key}")
            continue

        payload = build_jacula_payload_from_chesmin(src)
        payload["published"] = True

        if dst:
            product_id = dst["id"]
            print(f"[UPDATE] key={key} id={product_id}")
            url = f"{API_BASE}/{JACULA_STORE_ID}/products/{product_id}"
            resp = request_with_retry("PUT", url, headers=JACULA_HEADERS, json=payload)
            print(f"  → {resp.status_code} {resp.text[:180]}")
        else:
            print(f"[CREATE] key={key}")
            url = f"{API_BASE}/{JACULA_STORE_ID}/products"
            resp = request_with_retry("POST", url, headers=JACULA_HEADERS, json=payload)
            print(f"  → {resp.status_code} {resp.text[:180]}")

    # 2) Ocultar en Jacula SOLO si:
    #    - en Chesmin está oculto
    #    - y ya existe en Jacula
    #    (no crea ocultos nuevos)
    for src in ocultos:
        key = build_product_key(src)
        if not key:
            continue

        dst = jacula_by_key.get(key)
        if not dst:
            continue

        if product_has_excluded_category(dst):
            continue

        product_id = dst["id"]
        if dst.get("published") is False:
            continue

        print(f"[HIDE] key={key} id={product_id}")
        resp = set_published_in_jacula(product_id, False)
        print(f"  → {resp.status_code} {resp.text[:180]}")

    print("✅ Sincronización terminada.")


if __name__ == "__main__":
    sync_chesmin_to_jacula()
