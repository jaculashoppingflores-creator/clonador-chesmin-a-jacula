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

EXCLUDED_CATEGORY_NAME = "Capsula Jacula ✿"

PRICE_FACTOR = 1.28


def make_headers(access_token: str) -> dict:
    return {
        "Authentication": f"bearer {access_token}",
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    }


CHESMIN_HEADERS = make_headers(CHESMIN_ACCESS_TOKEN)
JACULA_HEADERS = make_headers(JACULA_ACCESS_TOKEN)

# ============================================
# FUNCIONES AUXILIARES
# ============================================

def get_all_products(store_id, headers):
    """Descarga todos los productos de una tienda paginando."""
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


def product_has_excluded_category(product: dict) -> bool:
    """Devuelve True si el producto está en 'Capsula Jacula ✿'."""
    for cat in product.get("categories", []):
        for name in cat.get("name", {}).values():
            if name == EXCLUDED_CATEGORY_NAME:
                return True
    return False


def build_product_key(product: dict) -> str | None:
    """Clave única: prioriza SKU, si no nombre."""
    for v in product.get("variants", []):
        if v.get("sku"):
            return v["sku"]

    name = product.get("name") or {}
    return name.get("es") or next(iter(name.values()), None)


def is_visible(product: dict) -> bool:
    """Determina si el producto está visible/publicado."""
    if product.get("published") is True:
        return True
    if product.get("status") == "published":
        return True
    return False


# ---------- Precios ----------

def adjust_prices_from_variant(src_variant: dict):
    """Ajusta precios copiando el descuento pero aplicando PRICE_FACTOR."""
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


def build_jacula_payload_from_chesmin(src_product: dict) -> dict:
    """Construye payload para Jacula basado en Chesmin."""

    # Variantes
    variants_out = []
    for v in src_product.get("variants", []):
        new_price, new_promo = adjust_prices_from_variant(v)
        out = {
            "name": v.get("name"),
            "sku": v.get("sku"),
            "price": new_price,
            "stock": v.get("stock"),
            "weight": v.get("weight"),
            "values": v.get("values"),
        }
        if new_promo is not None:
            out["promotional_price"] = new_promo
        variants_out.append(out)

    # Imágenes
    images_out = [{"src": img.get("src")} for img in src_product.get("images", []) if img.get("src")]

    payload = {
        "name": src_product.get("name"),
        "description": src_product.get("description"),
        "categories": src_product.get("categories"),
        "tags": src_product.get("tags"),
        "variants": variants_out,
        "images": images_out,
        "published": True,   # Se fuerza visible si Chesmin lo tiene visible
    }

    return payload


# ============================================
# LÓGICA PRINCIPAL
# ============================================

def sync_chesmin_to_jacula():

    print("Descargando productos de Chesmin...")
    all_chesmin = get_all_products(CHESMIN_STORE_ID, CHESMIN_HEADERS)
    print(f"  → {len(all_chesmin)} productos totales en Chesmin")

    # Filtramos solo visibles
    chesmin_visible = [p for p in all_chesmin if is_visible(p)]
    print(f"  → {len(chesmin_visible)} productos visibles en Chesmin (a clonar)")

    print("Descargando productos de Jacula...")
    jacula_products = get_all_products(JACULA_STORE_ID, JACULA_HEADERS)
    print(f"  → {len(jacula_products)} productos totales en Jacula")

    # Mapas por clave
    jacula_by_key = {}
    for p in jacula_products:
        if product_has_excluded_category(p):
            continue
        key = build_product_key(p)
        if key:
            jacula_by_key[key] = p

    # ---------------------------
    # CREAR / ACTUALIZAR VISIBLES
    # ---------------------------

    visible_keys = set()

    for src in chesmin_visible:
        key = build_product_key(src)
        if not key:
            continue

        visible_keys.add(key)
        dst = jacula_by_key.get(key)

        payload = build_jacula_payload_from_chesmin(src)

        if dst:
            print(f"[UPDATE] {key}")
            resp = requests.put(
                f"{API_BASE}/{JACULA_STORE_ID}/products/{dst['id']}",
                headers=JACULA_HEADERS,
                json=payload,
            )
        else:
            print(f"[CREATE] {key}")
            resp = requests.post(
                f"{API_BASE}/{JACULA_STORE_ID}/products",
                headers=JACULA_HEADERS,
                json=payload,
            )

        try:
            resp.raise_for_status()
        except:
            print("  → ERROR:", resp.status_code, resp.text[:200])

    # ---------------------------
    # OCULTAR LO QUE YA NO ESTÁ VISIBLE
    # ---------------------------

    for p in jacula_products:
        if product_has_excluded_category(p):
            continue

        key = build_product_key(p)
        if not key:
            continue

        if key in visible_keys:
            continue  # Debe permanecer visible

        # Si está visible en Jacula pero NO en Chesmin → ocultar
        if is_visible(p):
            print(f"[HIDE] Ocultando {key}")
            resp = requests.put(
                f"{API_BASE}/{JACULA_STORE_ID}/products/{p['id']}",
                headers=JACULA_HEADERS,
                json={"published": False},
            )
            try:
                resp.raise_for_status()
            except:
                print("  → ERROR ocultando:", resp.status_code, resp.text[:200])

    print("Sincronización terminada.")


if __name__ == "__main__":
    sync_chesmin_to_jacula()

