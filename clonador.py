import time
import requests

# ============================================
# CONFIGURACIÓN
# ============================================

API_BASE = "https://api.tiendanube.com/v1"

# --- Chesmin (ORIGEN) ---
CHESMIN_STORE_ID = 1610487
CHESMIN_ACCESS_TOKEN = "0e6586bece80829a7050a3ecf5b6e084a8ee0a58"

# --- Jacula (DESTINO) ---
JACULA_STORE_ID = 6889084
JACULA_ACCESS_TOKEN = "3c9c872098bb3a1469834dd8a7216880216f4cc1"

USER_AGENT = "Clonador Chesmin a Jacula (jaculashoppingflores@gmail.com)"

# No tocar productos de Jacula que estén en esta categoría
EXCLUDED_CATEGORY_NAME = "Capsula Jacula ✿"

# Margen extra Jacula vs Chesmin (1.28 = +28%)
PRICE_FACTOR = 1.28

# Retry / rate limit
MAX_RETRIES = 8


def make_headers(access_token: str) -> dict:
    return {
        "Authentication": f"bearer {access_token}",
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    }


CHESMIN_HEADERS = make_headers(CHESMIN_ACCESS_TOKEN)
JACULA_HEADERS = make_headers(JACULA_ACCESS_TOKEN)


def request_with_retry(method, url, headers=None, params=None, json=None):
    last = None
    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.request(method, url, headers=headers, params=params, json=json)
        last = resp

        # Rate limit
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            wait = int(retry_after) if retry_after and retry_after.isdigit() else min(2 ** attempt, 60)
            print(f"[429] Esperando {wait}s... (intento {attempt}/{MAX_RETRIES})")
            time.sleep(wait)
            continue

        return resp

    return last


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

        # Tiendanube a veces responde 404 cuando pedís una página que no existe
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
    # 1) SKU de la primera variante
    for variant in product.get("variants", []):
        sku = variant.get("sku")
        if sku:
            return sku

    # 2) Nombre
    name = product.get("name") or {}
    return name.get("es") or next(iter(name.values()), None)


def normalize_categories_ids(src_product: dict) -> list[int]:
    out = []
    for c in src_product.get("categories", []) or []:
        cid = c.get("id") if isinstance(c, dict) else None
        if isinstance(cid, int):
            out.append(cid)
    return out


def normalize_variant_values(values):
    """
    En la API, a veces values viene como lista de dicts.
    Para crear/actualizar, mandamos lista simple de strings (lo más compatible).
    """
    if not values:
        return None

    out = []
    for v in values:
        if isinstance(v, dict):
            # preferimos 'es' si existe
            if "es" in v and v["es"] is not None:
                out.append(str(v["es"]))
            else:
                # primer valor disponible
                out.append(str(next(iter(v.values()))))
        else:
            out.append(str(v))

    return out


def adjust_prices_from_variant(src_variant: dict):
    price = float(src_variant["price"])
    promo = src_variant.get("promotional_price")

    new_price = round(price * PRICE_FACTOR)

    if promo:
        promo = float(promo)
        discount_factor = promo / price  # mismo % descuento
        new_promo = round(new_price * discount_factor)
    else:
        new_promo = None

    return new_price, new_promo


def build_jacula_payload_from_chesmin(src_product: dict) -> dict:
    variants_out = []
    for v in src_product.get("variants", []) or []:
        new_price, new_promo = adjust_prices_from_variant(v)

        values_norm = normalize_variant_values(v.get("values"))

        variant_out = {
            "name": v.get("name"),
            "sku": v.get("sku"),
            "price": new_price,
            "stock": v.get("stock"),
            "weight": v.get("weight"),
        }

        # Solo mandamos values si existe (y normalizado)
        if values_norm is not None:
            variant_out["values"] = values_norm

        if new_promo is not None:
            variant_out["promotional_price"] = new_promo

        variants_out.append(variant_out)

    images_out = []
    for img in src_product.get("images", []) or []:
        src = img.get("src")
        if src:
            images_out.append({"src": src})

    payload = {
        "name": src_product.get("name"),
        "description": src_product.get("description"),
        # published lo seteamos afuera según lógica visible/oculto
        "tags": src_product.get("tags"),
        "variants": variants_out,
        "images": images_out,
    }

    # Categorías: SOLO IDs (esto evita el 422 de categories)
    cat_ids = normalize_categories_ids(src_product)
    if cat_ids:
        payload["categories"] = cat_ids

    return payload


def set_jacula_published(product_id: int, published: bool):
    resp = request_with_retry(
        "PUT",
        f"{API_BASE}/{JACULA_STORE_ID}/products/{product_id}",
        headers=JACULA_HEADERS,
        json={"published": published},
    )
    print(f"  → {resp.status_code} {resp.text[:200]}")
    return resp


def sync_chesmin_to_jacula():
    print("Descargando productos de Chesmin (origen)...")
    chesmin_products = get_all_products(CHESMIN_STORE_ID, CHESMIN_HEADERS)
    print(f"  → {len(chesmin_products)} productos totales en Chesmin")

    visible_chesmin = [p for p in chesmin_products if p.get("published") is True]
    print(f"  → {len(visible_chesmin)} productos visibles en Chesmin (a clonar/actualizar)")

    print("Descargando productos de Jacula (destino)...")
    jacula_products = get_all_products(JACULA_STORE_ID, JACULA_HEADERS)
    print(f"  → {len(jacula_products)} productos encontrados en Jacula")

    jacula_by_key: dict[str, dict] = {}
    for p in jacula_products:
        key = build_product_key(p)
        if key:
            jacula_by_key[key] = p

    # 1) CREAR / ACTUALIZAR SOLO VISIBLES
    for src in visible_chesmin:
        key = build_product_key(src)
        if not key:
            print("Saltando producto de Chesmin SIN clave (sin sku ni nombre)")
            continue

        dst = jacula_by_key.get(key)
        payload = build_jacula_payload_from_chesmin(src)
        payload["published"] = True  # visibles siempre van publicados en Jacula

        if dst:
            if product_has_excluded_category(dst):
                print(f"[SKIP] '{EXCLUDED_CATEGORY_NAME}' no se toca. Key={key}")
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

    # 2) OCULTAR EN JACULA LOS QUE ESTÁN OCULTOS EN CHESMIN (PERO SOLO SI YA EXISTEN EN JACULA)
    hidden_chesmin = [p for p in chesmin_products if p.get("published") is False]
    print(f"  → {len(hidden_chesmin)} productos ocultos en Chesmin (solo despublicar si existen en Jacula)")

    for src in hidden_chesmin:
        key = build_product_key(src)
        if not key:
            continue

        dst = jacula_by_key.get(key)
        if not dst:
            # IMPORTANTÍSIMO: si está oculto en Chesmin y no existe en Jacula,
            # NO lo creamos (así no te trae temporadas viejas)
            continue

        if product_has_excluded_category(dst):
            continue

        # si existe, lo despublicamos
        product_id = dst["id"]
        if dst.get("published") is True:
            print(f"[HIDE] key={key} id={product_id} (porque en Chesmin está oculto)")
            set_jacula_published(product_id, False)

    print("Sincronización terminada.")


if __name__ == "__main__":
    sync_chesmin_to_jacula()
