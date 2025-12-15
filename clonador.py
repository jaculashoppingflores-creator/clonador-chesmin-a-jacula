import os
import time
import requests

API_BASE = "https://api.tiendanube.com/v1"
USER_AGENT = "Clonador Chesmin a Jacula (jaculashoppingflores@gmail.com)"

# No tocar productos dentro de esta categoría en Jacula
EXCLUDED_CATEGORY_NAME = "Capsula Jacula ✿"

# Lee secrets desde el workflow env:
CHESMIN_STORE_ID = int(os.environ["CHESMIN_STORE_ID"])
JACULA_STORE_ID = int(os.environ["JACULA_STORE_ID"])
CHESMIN_ACCESS_TOKEN = os.environ["CHESMIN_ACCESS_TOKEN"]
JACULA_ACCESS_TOKEN = os.environ["JACULA_ACCESS_TOKEN"]
PRICE_FACTOR = float(os.environ.get("PRICE_FACTOR", "1.0"))

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


def get_all_products(store_id: int, headers: dict):
    products = []
    page = 1

    while True:
        resp = request_with_retry(
            "GET",
            f"{API_BASE}/{store_id}/products",
            headers=headers,
            params={"page": page, "per_page": 200},
        )

        # Tiendanube a veces devuelve 404 cuando te pasás de página
        if resp.status_code == 404:
            break

        resp.raise_for_status()
        data = resp.json()
        if not data:
            break

        products.extend(data)
        page += 1

    return products


def product_has_excluded_category(product: dict) -> bool:
    for cat in product.get("categories", []):
        name_dict = cat.get("name", {}) or {}
        for lang_name in name_dict.values():
            if lang_name == EXCLUDED_CATEGORY_NAME:
                return True
    return False


def build_product_key(product: dict) -> str | None:
    # Prioridad: SKU de la primera variante con sku
    for v in product.get("variants", []):
        sku = v.get("sku")
        if sku:
            return sku

    # Fallback: nombre
    name = product.get("name") or {}
    return name.get("es") or next(iter(name.values()), None)


def extract_category_ids(src_product: dict):
    ids = []
    for c in src_product.get("categories", []):
        if isinstance(c, dict) and "id" in c:
            ids.append(int(c["id"]))
        elif isinstance(c, int):
            ids.append(c)
    return ids


def normalize_variant_values(v: dict):
    """
    Tiendanube espera values como lista de IDs (enteros) y sin repetidos.
    """
    values = v.get("values") or []
    ids = []
    for it in values:
        if isinstance(it, dict) and "id" in it:
            ids.append(int(it["id"]))
        elif isinstance(it, int):
            ids.append(it)

    # dedupe manteniendo orden
    seen = set()
    out = []
    for x in ids:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


def adjust_prices(price: float, promo: float | None):
    new_price = round(price * PRICE_FACTOR)
    if promo is None:
        return new_price, None

    # Mantener mismo % promo que Chesmin
    discount_factor = promo / price if price else 1
    new_promo = round(new_price * discount_factor)
    return new_price, new_promo


def build_jacula_payload_from_chesmin(src_product: dict) -> dict:
    # Variantes
    variants_out = []
    for v in src_product.get("variants", []):
        price = float(v["price"])
        promo = v.get("promotional_price")
        promo = float(promo) if promo not in (None, "", 0) else None

        new_price, new_promo = adjust_prices(price, promo)

        variant_out = {
            "name": v.get("name"),
            "sku": v.get("sku"),
            "price": new_price,
            "stock": v.get("stock"),
            "weight": v.get("weight"),
            "values": normalize_variant_values(v),  # ✅ ARREGLADO
        }
        if new_promo is not None:
            variant_out["promotional_price"] = new_promo

        variants_out.append(variant_out)

    # Imágenes
    images_out = []
    for img in src_product.get("images", []):
        src = img.get("src")
        if src:
            images_out.append({"src": src})

    payload = {
        "name": src_product.get("name"),
        "description": src_product.get("description"),
        "published": bool(src_product.get("published")),
        "categories": extract_category_ids(src_product),  # ✅ ARREGLADO (lista de ints)
        "tags": src_product.get("tags"),
        "variants": variants_out,
        "images": images_out,
    }

    return payload


def update_product_published_only(jacula_product_id: int, published: bool):
    payload = {"published": published}
    resp = request_with_retry(
        "PUT",
        f"{API_BASE}/{JACULA_STORE_ID}/products/{jacula_product_id}",
        headers=JACULA_HEADERS,
        json=payload,
    )
    print(f"  → {resp.status_code} {resp.text[:200]}")


def sync_chesmin_to_jacula():
    print("Descargando productos de Chesmin (origen)...")
    chesmin_products = get_all_products(CHESMIN_STORE_ID, CHESMIN_HEADERS)
    total_chesmin = len(chesmin_products)
    visibles = [p for p in chesmin_products if p.get("published") is True]
    print(f"  → {total_chesmin} productos totales en Chesmin")
    print(f"  → {len(visibles)} productos visibles en Chesmin (a clonar/actualizar)")

    print("Descargando productos de Jacula (destino)...")
    jacula_products = get_all_products(JACULA_STORE_ID, JACULA_HEADERS)
    print(f"  → {len(jacula_products)} productos encontrados en Jacula")

    jacula_by_key = {}
    for p in jacula_products:
        key = build_product_key(p)
        if key:
            jacula_by_key[key] = p

    chesmin_by_key = {}
    for p in chesmin_products:
        key = build_product_key(p)
        if key:
            chesmin_by_key[key] = p

    # 1) Crear/Actualizar SOLO visibles
    for src in visibles:
        key = build_product_key(src)
        if not key:
            continue

        dst = jacula_by_key.get(key)
        payload = build_jacula_payload_from_chesmin(src)

        if dst:
            if product_has_excluded_category(dst):
                print(f"[SKIP] {EXCLUDED_CATEGORY_NAME} key={key}")
                continue

            product_id = dst["id"]
            print(f"[UPDATE] key={key} id={product_id}")
            resp = request_with_retry(
                "PUT",
                f"{API_BASE}/{JACULA_STORE_ID}/products/{product_id}",
                headers=JACULA_HEADERS,
                json=payload,
            )
            print(f"  → {resp.status_code} {resp.text[:200]}")

        else:
            print(f"[CREATE] key={key}")
            resp = request_with_retry(
                "POST",
                f"{API_BASE}/{JACULA_STORE_ID}/products",
                headers=JACULA_HEADERS,
                json=payload,
            )
            print(f"  → {resp.status_code} {resp.text[:200]}")

    # 2) Ocultar en Jacula los que en Chesmin estén ocultos (solo si existen en Jacula)
    print("Sincronizando ocultamientos (Chesmin → Jacula)...")
    for key, dst in jacula_by_key.items():
        if product_has_excluded_category(dst):
            continue

        src = chesmin_by_key.get(key)
        if not src:
            continue

        if src.get("published") is False and dst.get("published") is True:
            print(f"[HIDE] key={key} id={dst['id']}")
            update_product_published_only(dst["id"], False)

    print("Sincronización terminada.")


if __name__ == "__main__":
    sync_chesmin_to_jacula()
