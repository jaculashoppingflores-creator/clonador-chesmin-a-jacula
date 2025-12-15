import os
import time
import json
import requests

API_BASE = "https://api.tiendanube.com/v1"
USER_AGENT = "Clonador Chesmin a Jacula (jaculashoppingflores@gmail.com)"

# Tag para marcar productos sincronizados (así NO tocamos los de Jacula propios)
SYNC_TAG = "SYNC_CHESMIN"

# Lee config desde variables de entorno (GitHub Actions env/secrets)
CHESMIN_STORE_ID = int(os.getenv("CHESMIN_STORE_ID", "0"))
JACULA_STORE_ID = int(os.getenv("JACULA_STORE_ID", "0"))
CHESMIN_ACCESS_TOKEN = os.getenv("CHESMIN_ACCESS_TOKEN", "")
JACULA_ACCESS_TOKEN = os.getenv("JACULA_ACCESS_TOKEN", "")
PRICE_FACTOR = float(os.getenv("PRICE_FACTOR", "1.28"))

EXCLUDED_CATEGORY_NAME = os.getenv("EXCLUDED_CATEGORY_NAME", "Capsula Jacula ✿")

MAX_RETRIES = 8


def make_headers(token: str) -> dict:
    return {
        "Authentication": f"bearer {token}",
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    }


CHESMIN_HEADERS = make_headers(CHESMIN_ACCESS_TOKEN)
JACULA_HEADERS = make_headers(JACULA_ACCESS_TOKEN)


def request_with_retry(method, url, headers=None, params=None, payload=None):
    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.request(method, url, headers=headers, params=params, json=payload)

        # Rate limit
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

        # Tiendanube a veces tira 404 cuando pedís página fuera de rango
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
    # 1) SKU de la primera variante con sku
    for v in product.get("variants", []):
        sku = v.get("sku")
        if sku:
            return sku

    # 2) nombre ES
    name = product.get("name") or {}
    return name.get("es") or next(iter(name.values()), None)


def normalize_categories(src_product: dict):
    """
    La API para crear/editar espera CATEGORIES = [id, id, id]
    pero el GET devuelve objetos. Convertimos a IDs enteros.
    """
    out = []
    for c in (src_product.get("categories") or []):
        if isinstance(c, int):
            out.append(c)
        elif isinstance(c, dict) and "id" in c:
            out.append(int(c["id"]))
    return out


def normalize_variant_values(v: dict, expected_len: int | None):
    """
    Intenta limpiar values (quita duplicados).
    Si la cantidad no coincide con expected_len (cuando existe), devuelve None (para no romper).
    """
    vals = v.get("values") or []
    uniq = []
    seen = set()

    for it in vals:
        # Armamos una key estable para deduplicar
        if isinstance(it, dict):
            k = it.get("id")
            if not k:
                # usa nombre (es) si existe, si no, json
                nm = it.get("name") or {}
                k = nm.get("es") or json.dumps(it, sort_keys=True)
        else:
            k = str(it)

        if k in seen:
            continue
        seen.add(k)
        uniq.append(it)

    if expected_len is not None:
        if len(uniq) != expected_len:
            # si sobran, intentamos recortar
            if len(uniq) > expected_len:
                uniq = uniq[:expected_len]
            else:
                # faltan -> mejor no mandar values (evita “wrong number of elements”)
                return None

    return uniq


def adjust_prices(src_variant: dict):
    price = float(src_variant.get("price") or 0)
    promo = src_variant.get("promotional_price")

    new_price = round(price * PRICE_FACTOR)

    if promo:
        promo = float(promo)
        discount_factor = promo / price if price else None
        new_promo = round(new_price * discount_factor) if discount_factor else None
    else:
        new_promo = None

    return new_price, new_promo


def ensure_sync_tag(tags):
    tags = tags or []
    if SYNC_TAG not in tags:
        tags.append(SYNC_TAG)
    return tags


def build_payload(src_product: dict) -> dict:
    options = src_product.get("options") or []
    expected_len = len(options) if isinstance(options, list) and len(options) > 0 else None

    variants_out = []
    for v in (src_product.get("variants") or []):
        new_price, new_promo = adjust_prices(v)

        out = {
            "name": v.get("name"),
            "sku": v.get("sku"),
            "price": new_price,
            "stock": v.get("stock"),
            "weight": v.get("weight"),
        }

        norm_values = normalize_variant_values(v, expected_len)
        if norm_values is not None:
            out["values"] = norm_values

        if new_promo is not None:
            out["promotional_price"] = new_promo

        variants_out.append(out)

    images_out = []
    for img in (src_product.get("images") or []):
        src = img.get("src")
        if src:
            images_out.append({"src": src})

    payload = {
        "name": src_product.get("name"),
        "description": src_product.get("description"),
        # IMPORTANTE: nosotros SOLO vamos a crear visibles.
        # Pero si un producto ya está sincronizado y Chesmin lo oculta, lo vamos a ocultar en update.
        "published": src_product.get("published", True),
        "categories": normalize_categories(src_product),
        "tags": ensure_sync_tag(src_product.get("tags")),
        "variants": variants_out,
        "images": images_out,
    }

    # Si existen options en el producto, las pasamos (cuando viene bien armado desde la API)
    if options:
        payload["options"] = options

    return payload


def is_synced_product(jacula_product: dict) -> bool:
    tags = jacula_product.get("tags") or []
    return SYNC_TAG in tags


def create_or_update(jacula_product, payload):
    if jacula_product:
        product_id = jacula_product["id"]
        resp = request_with_retry(
            "PUT",
            f"{API_BASE}/{JACULA_STORE_ID}/products/{product_id}",
            headers=JACULA_HEADERS,
            payload=payload,
        )
        return resp
    else:
        resp = request_with_retry(
            "POST",
            f"{API_BASE}/{JACULA_STORE_ID}/products",
            headers=JACULA_HEADERS,
            payload=payload,
        )
        return resp


def fallback_payload_single_variant(payload: dict) -> dict:
    """
    Si la API devuelve 422 por variants/values, creamos una versión “mínima” (1 variante sin values)
    para por lo menos levantar el producto (y después lo corregimos manualmente si hace falta).
    """
    new_payload = dict(payload)
    variants = payload.get("variants") or []
    if variants:
        v0 = dict(variants[0])
        v0.pop("values", None)
        new_payload["variants"] = [v0]
    return new_payload


def sync():
    print("Descargando productos de Chesmin (origen)...")
    chesmin_all = get_all_products(CHESMIN_STORE_ID, CHESMIN_HEADERS)
    chesmin_by_key = {build_product_key(p): p for p in chesmin_all if build_product_key(p)}
    chesmin_visible = [p for p in chesmin_all if p.get("published") is True]

    print(f"  → {len(chesmin_all)} productos totales en Chesmin")
    print(f"  → {len(chesmin_visible)} productos visibles en Chesmin (a clonar/actualizar)")

    print("Descargando productos de Jacula (destino)...")
    jacula_all = get_all_products(JACULA_STORE_ID, JACULA_HEADERS)
    print(f"  → {len(jacula_all)} productos encontrados en Jacula")

    jacula_by_key = {}
    for p in jacula_all:
        k = build_product_key(p)
        if k:
            jacula_by_key[k] = p

    # 1) UPSERT: solo VISIBLES de Chesmin
    for src in chesmin_visible:
        key = build_product_key(src)
        if not key:
            print("[SKIP] producto Chesmin sin key (sin sku ni nombre)")
            continue

        dst = jacula_by_key.get(key)

        # No tocar Capsula Jacula ✿
        if dst and product_has_excluded_category(dst):
            print(f"[SKIP] '{EXCLUDED_CATEGORY_NAME}' no se toca. key={key}")
            continue

        payload = build_payload(src)
        payload["published"] = True  # acá forzamos: si está en visibles, queda visible

        action = "UPDATE" if dst else "CREATE"
        print(f"[{action}] key={key}")

        resp = create_or_update(dst, payload)

        if resp.status_code == 422:
            print(f"  → 422, intentando fallback (1 variante sin values) key={key}")
            fb = fallback_payload_single_variant(payload)
            resp = create_or_update(dst, fb)

        if resp.status_code >= 400:
            print(f"  → ERROR {resp.status_code}: {resp.text[:300]}")
        else:
            print(f"  → OK {resp.status_code}")

    # 2) OCULTAR en Jacula: SOLO productos sincronizados
    #    Si el producto en Chesmin ya no está publicado -> lo ocultamos en Jacula
    print("Aplicando ocultamientos (solo productos sincronizados)...")
    for dst in jacula_all:
        if product_has_excluded_category(dst):
            continue
        if not is_synced_product(dst):
            continue

        key = build_product_key(dst)
        if not key:
            continue

        src = chesmin_by_key.get(key)
        if src is None:
            # Si no existe en Chesmin, NO lo borramos. Solo lo dejamos como está.
            continue

        if src.get("published") is False and dst.get("published") is True:
            print(f"[HIDE] key={key} (Chesmin oculto -> Jacula oculto)")
            payload = {"published": False}
            resp = request_with_retry(
                "PUT",
                f"{API_BASE}/{JACULA_STORE_ID}/products/{dst['id']}",
                headers=JACULA_HEADERS,
                payload=payload,
            )
            if resp.status_code >= 400:
                print(f"  → ERROR {resp.status_code}: {resp.text[:200]}")
            else:
                print(f"  → OK {resp.status_code}")

    print("Sincronización terminada.")


if __name__ == "__main__":
    sync()
