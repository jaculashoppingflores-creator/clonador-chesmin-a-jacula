import os
import time
import requests

API_BASE = "https://api.tiendanube.com/v1"

USER_AGENT = os.getenv("USER_AGENT", "Clonador Chesmin a Jacula (jaculashoppingflores@gmail.com)")
EXCLUDED_CATEGORY_NAME = os.getenv("EXCLUDED_CATEGORY_NAME", "Capsula Jacula ✿")
PRICE_FACTOR = float(os.getenv("PRICE_FACTOR", "1.28"))

CHESMIN_STORE_ID = int(os.getenv("CHESMIN_STORE_ID"))
CHESMIN_ACCESS_TOKEN = os.getenv("CHESMIN_ACCESS_TOKEN")

JACULA_STORE_ID = int(os.getenv("JACULA_STORE_ID"))
JACULA_ACCESS_TOKEN = os.getenv("JACULA_ACCESS_TOKEN")

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
    last = None
    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.request(method, url, headers=headers, params=params, json=json)
        last = resp

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
    for cat in product.get("categories", []) or []:
        name_dict = (cat.get("name") or {})
        for lang_name in name_dict.values():
            if lang_name == EXCLUDED_CATEGORY_NAME:
                return True
    return False


def build_product_key(product: dict) -> str | None:
    for v in product.get("variants", []) or []:
        sku = v.get("sku")
        if sku:
            return sku
    name = product.get("name") or {}
    return name.get("es") or next(iter(name.values()), None)


def normalize_categories_ids(src_product: dict) -> list[int]:
    out = []
    for c in src_product.get("categories", []) or []:
        if isinstance(c, dict) and isinstance(c.get("id"), int):
            out.append(c["id"])
    return out


def get_attribute_names(src_product: dict) -> list[str]:
    """
    Lee nombres de atributos (Color / Talle / etc) para poder desambiguar duplicados.
    """
    attrs = src_product.get("attributes") or []
    names = []
    for a in attrs:
        nd = a.get("name") or {}
        names.append(nd.get("es") or next(iter(nd.values()), "Atributo"))
    return names


def adjust_prices(price, promo):
    price = float(price)
    new_price = round(price * PRICE_FACTOR)
    if promo is None:
        return new_price, None
    promo = float(promo)
    discount_factor = promo / price
    new_promo = round(new_price * discount_factor)
    return new_price, new_promo


def fix_variant_values(values, attr_names):
    """
    - Asegura que len(values) == len(attributes)
    - Si hay repetidos dentro de la misma variante, los renombra tipo "Talle: Único"
      para evitar 422 "values should not be repeated".
    """
    values = list(values or [])
    n = len(attr_names)

    # ajustar largo
    if n > 0:
        if len(values) < n:
            values += ["Único"] * (n - len(values))
        elif len(values) > n:
            values = values[:n]

    seen = set()
    fixed = []
    for i, val in enumerate(values):
        val_str = str(val) if val is not None else "Único"
        if val_str in seen:
            prefix = attr_names[i] if i < len(attr_names) else "Atributo"
            val_str = f"{prefix}: {val_str}"
        seen.add(val_str)
        fixed.append(val_str)

    return fixed


def build_payload(src_product: dict) -> dict:
    attr_names = get_attribute_names(src_product)
    variants_out = []

    for v in src_product.get("variants", []) or []:
        new_price, new_promo = adjust_prices(v.get("price"), v.get("promotional_price"))
        values_fixed = fix_variant_values(v.get("values"), attr_names)

        variant_out = {
            "name": v.get("name"),
            "sku": v.get("sku"),
            "price": new_price,
            "stock": v.get("stock"),
            "weight": v.get("weight"),
            "values": values_fixed,
        }
        if new_promo is not None:
            variant_out["promotional_price"] = new_promo

        variants_out.append(variant_out)

    images_out = []
    for img in src_product.get("images", []) or []:
        if img.get("src"):
            images_out.append({"src": img["src"]})

    payload = {
        "name": src_product.get("name"),
        "description": src_product.get("description"),
        "tags": src_product.get("tags"),
        "variants": variants_out,
        "images": images_out,
    }

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


def sync():
    print("Descargando productos de Chesmin...")
    chesmin_products = get_all_products(CHESMIN_STORE_ID, CHESMIN_HEADERS)
    print(f"  → {len(chesmin_products)} totales en Chesmin")

    visible_chesmin = [p for p in chesmin_products if p.get("published") is True]
    hidden_chesmin = [p for p in chesmin_products if p.get("published") is False]
    print(f"  → {len(visible_chesmin)} visibles (se crean/actualizan)")
    print(f"  → {len(hidden_chesmin)} ocultos (solo se ocultan si ya existen en Jacula)")

    print("Descargando productos de Jacula...")
    jacula_products = get_all_products(JACULA_STORE_ID, JACULA_HEADERS)
    print(f"  → {len(jacula_products)} totales en Jacula")

    jacula_by_key = {}
    for p in jacula_products:
        key = build_product_key(p)
        if key:
            jacula_by_key[key] = p

    # 1) Crear/actualizar solo VISIBLES
    for src in visible_chesmin:
        key = build_product_key(src)
        if not key:
            continue

        dst = jacula_by_key.get(key)

        payload = build_payload(src)
        payload["published"] = True

        if dst:
            if product_has_excluded_category(dst):
                print(f"[SKIP] Capsula Jacula ✿  key={key}")
                continue

            pid = dst["id"]
            print(f"[UPDATE] key={key} id={pid}")
            resp = request_with_retry(
                "PUT",
                f"{API_BASE}/{JACULA_STORE_ID}/products/{pid}",
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

    # 2) Ocultar en Jacula lo que ocultás en Chesmin (solo si existe en Jacula)
    for src in hidden_chesmin:
        key = build_product_key(src)
        if not key:
            continue

        dst = jacula_by_key.get(key)
        if not dst:
            continue

        if product_has_excluded_category(dst):
            continue

        pid = dst["id"]
        if dst.get("published") is True:
            print(f"[HIDE] key={key} id={pid}")
            set_jacula_published(pid, False)

    print("OK. Sincronización terminada.")


if __name__ == "__main__":
    sync()
