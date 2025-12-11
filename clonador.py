import requests

API_BASE = "https://api.tiendanube.com/v1"

# --- Chesmin (ORIGEN) ---
CHESMIN_STORE_ID = 1610487
CHESMIN_ACCESS_TOKEN = "0e6586bece80829a7050a3ecf5b6e084a8ee0a58"

# --- Jacula (DESTINO) ---
JACULA_STORE_ID = 6889084
JACULA_ACCESS_TOKEN = "3c9c872098bb3a1469834dd8a7216880216f4cc1"

USER_AGENT = "Clonador Chesmin a Jacula (jaculashoppingflores@gmail.com)"

EXCLUDED_CATEGORY_NAME = "Capsula Jacula ✿"

# Ej: 1.28 = Jacula +28% vs Chesmin
PRICE_FACTOR = 1.28


def make_headers(access_token: str) -> dict:
    return {
        "Authentication": f"bearer {access_token}",
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    }


CHESMIN_HEADERS = make_headers(CHESMIN_ACCESS_TOKEN)
JACULA_HEADERS = make_headers(JACULA_ACCESS_TOKEN)


def get_all_products(store_id, headers):
    products = []
    page = 1
    while True:
        resp = requests.get(
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


def is_visible(product: dict) -> bool:
    # Tiendanube suele traer published True/False.
    # Dejamos fallback por si cambia.
    if product.get("published") is True:
        return True
    if product.get("status") == "published":
        return True
    return False


def product_has_excluded_category(product: dict) -> bool:
    for cat in product.get("categories", []):
        name_dict = cat.get("name", {}) or {}
        for lang_name in name_dict.values():
            if lang_name == EXCLUDED_CATEGORY_NAME:
                return True
    return False


def build_product_key(product: dict) -> str | None:
    for variant in product.get("variants", []):
        sku = variant.get("sku")
        if sku:
            return sku

    name = product.get("name") or {}
    return name.get("es") or next(iter(name.values()), None)


def dedupe_variant_values(values):
    """
    FIX 422: "Variant values should not be repeated"
    Deduplicamos los values dentro de la misma variante.
    """
    if not isinstance(values, list):
        return values

    seen = set()
    out = []
    for v in values:
        if not isinstance(v, dict):
            k = ("raw", str(v))
        else:
            if "option_id" in v:
                k = ("option_id", v.get("option_id"))
            elif "id" in v:
                k = ("id", v.get("id"))
            elif "name" in v:
                k = ("name", str(v.get("name")))
            elif "value" in v:
                k = ("value", str(v.get("value")))
            else:
                k = ("dict", tuple(sorted(v.items())))

        if k in seen:
            continue
        seen.add(k)
        out.append(v)

    return out


def adjust_prices_from_variant(src_variant: dict):
    price = float(src_variant["price"])
    promo = src_variant.get("promotional_price")

    new_price = round(price * PRICE_FACTOR)

    if promo:
        promo = float(promo)
        discount_factor = promo / price
        new_promo = round(new_price * discount_factor)
    else:
        new_promo = None

    return new_price, new_promo


def build_jacula_payload_from_chesmin(src_product: dict, force_published: bool | None = None) -> dict:
    variants_out = []
    for v in src_product.get("variants", []):
        new_price, new_promo = adjust_prices_from_variant(v)
        values = dedupe_variant_values(v.get("values", []))

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

    if force_published is None:
        payload["published"] = src_product.get("published")
    else:
        payload["published"] = force_published

    return payload


def sync_chesmin_to_jacula():
    print("Descargando productos de Chesmin...")
    chesmin_products = get_all_products(CHESMIN_STORE_ID, CHESMIN_HEADERS)
    print(f"  → {len(chesmin_products)} productos totales en Chesmin")

    print("Descargando productos de Jacula...")
    jacula_products = get_all_products(JACULA_STORE_ID, JACULA_HEADERS)
    print(f"  → {len(jacula_products)} productos totales en Jacula")

    jacula_by_key: dict[str, dict] = {}
    for p in jacula_products:
        key = build_product_key(p)
        if key:
            jacula_by_key[key] = p

    # 1) VISIBLES: crear/actualizar
    visibles = [p for p in chesmin_products if is_visible(p)]
    print(f"  → {len(visibles)} productos visibles en Chesmin (se clonan)")

    for src in visibles:
        key = build_product_key(src)
        if not key:
            print("[SKIP] Chesmin sin key (sin sku y sin nombre)")
            continue

        dst = jacula_by_key.get(key)

        # Clonamos visibles como visibles sí o sí
        payload = build_jacula_payload_from_chesmin(src, force_published=True)

        if dst:
            if product_has_excluded_category(dst):
                print(f"[SKIP] Está en '{EXCLUDED_CATEGORY_NAME}', no se toca. key={key}")
                continue

            product_id = dst["id"]
            print(f"[UPDATE] {key} -> Jacula id={product_id}")
            resp = requests.put(
                f"{API_BASE}/{JACULA_STORE_ID}/products/{product_id}",
                headers=JACULA_HEADERS,
                json=payload,
            )
            if resp.status_code >= 400:
                print(f"  → ERROR {resp.status_code}: {resp.text[:300]}")

        else:
            print(f"[CREATE] {key}")
            resp = requests.post(
                f"{API_BASE}/{JACULA_STORE_ID}/products",
                headers=JACULA_HEADERS,
                json=payload,
            )
            if resp.status_code >= 400:
                print(f"  → ERROR {resp.status_code}: {resp.text[:300]}")

    # 2) OCULTOS: NO crear, solo ocultar si ya existe en Jacula
    ocultos = [p for p in chesmin_products if not is_visible(p)]
    print(f"  → {len(ocultos)} productos ocultos en Chesmin (NO se crean; solo se ocultan si existen)")

    for src in ocultos:
        key = build_product_key(src)
        if not key:
            continue

        dst = jacula_by_key.get(key)
        if not dst:
            # Importante: NO lo creamos
            continue

        if product_has_excluded_category(dst):
            continue

        # Solo ocultamos (no tocamos variantes/imagenes en ocultos)
        if is_visible(dst):
            print(f"[HIDE] {key} -> ocultando en Jacula id={dst['id']}")
            resp = requests.put(
                f"{API_BASE}/{JACULA_STORE_ID}/products/{dst['id']}",
                headers=JACULA_HEADERS,
                json={"published": False},
            )
            if resp.status_code >= 400:
                print(f"  → ERROR {resp.status_code}: {resp.text[:300]}")

    print("Sincronización terminada.")


if __name__ == "__main__":
    sync_chesmin_to_jacula()
