import requests

# ============================================
# CONFIGURACIÓN
# ============================================

API_BASE = "https://api.tiendanube.com/v1"

# Estos datos salen de las llamadas curl que hicimos.
# Si ves que clona al revés, simplemente intercambiá estos pares.

# --- Chesmin (ORIGEN) ---
CHESMIN_STORE_ID = 6889084
CHESMIN_ACCESS_TOKEN = "3c9c872098bb3a1469834dd8a7216880216f4cc1"

# --- Jacula (DESTINO) ---
JACULA_STORE_ID = 1610487
JACULA_ACCESS_TOKEN = "0e6586bece80829a7050a3ecf5b6e084a8ee0a58"

# User-Agent requerido por Tiendanube (podés dejarlo así)
USER_AGENT = "Clonador Chesmin a Jacula (jaculashoppingflores@gmail.com)"

# Nombre EXACTO de la categoría que NO queremos tocar en Jacula
EXCLUDED_CATEGORY_NAME = "Capsula Jacula ✿"

# Factor extra de margen para Jacula.
# Ejemplo: 1.28 = Jacula cobra ~28% más que Chesmin.
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


def get_all_products(store_id: int, headers: dict):
    """Descarga TODOS los productos de una tienda, paginando."""
    products = []
    page = 1

    while True:
        resp = requests.get(
            f"{API_BASE}/{store_id}/products",
            headers=headers,
            params={"per_page": 200, "page": page},
        )
        resp.raise_for_status()
        chunk = resp.json()
        if not chunk:
            break
        products.extend(chunk)
        page += 1

    return products


def product_has_excluded_category(product: dict) -> bool:
    """Devuelve True si el producto de Jacula está en 'Capsula Jacula ✿'."""
    for cat in product.get("categories", []):
        name_dict = cat.get("name", {}) or {}
        # name_dict es algo como {"es": "Capsula Jacula ✿", "pt": "...", etc}
        for lang_name in name_dict.values():
            if lang_name == EXCLUDED_CATEGORY_NAME:
                return True
    return False


def build_product_key(product: dict) -> str | None:
    """
    Genera una "llave" para emparejar productos entre tiendas.
    Priorizamos el SKU de la primera variante; si no hay, usamos el nombre ES.
    """
    for variant in product.get("variants", []):
        sku = variant.get("sku")
        if sku:
            return sku

    name = product.get("name") or {}
    return name.get("es") or next(iter(name.values()), None)


def adjust_prices_from_variant(src_variant: dict):
    """
    A partir de una variante de Chesmin, devuelve (precio_jacula, promo_jacula)
    aplicando PRICE_FACTOR y manteniendo el mismo % de descuento promocional.
    """
    price = float(src_variant["price"])
    promo = src_variant.get("promotional_price")

    new_price = round(price * PRICE_FACTOR)

    if promo:
        promo = float(promo)
        discount_factor = promo / price  # mismo % de descuento que Chesmin
        new_promo = round(new_price * discount_factor)
    else:
        new_promo = None

    return new_price, new_promo


def build_jacula_payload_from_chesmin(src_product: dict) -> dict:
    """
    Arma el cuerpo del producto para Jacula, copiando datos de Chesmin
    y ajustando precios con el margen.
    """
    # Variantes
    variants_out = []
    for v in src_product.get("variants", []):
        new_price, new_promo = adjust_prices_from_variant(v)
        variant_out = {
            "name": v.get("name"),
            "sku": v.get("sku"),
            "price": new_price,
            "stock": v.get("stock"),
            "weight": v.get("weight"),
            "values": v.get("values"),
        }
        if new_promo is not None:
            variant_out["promotional_price"] = new_promo

        variants_out.append(variant_out)

    # Imágenes (copiamos las URLs de Chesmin)
    images_out = []
    for img in src_product.get("images", []):
        src = img.get("src")
        if src:
            images_out.append({"src": src})

    payload = {
        "name": src_product.get("name"),
        "description": src_product.get("description"),
        "published": src_product.get("published"),  # si está oculto en Chesmin, se oculta en Jacula
        "categories": src_product.get("categories"),
        "tags": src_product.get("tags"),
        "variants": variants_out,
        "images": images_out,
    }

    return payload


# ============================================
# LÓGICA PRINCIPAL DE SINCRONIZACIÓN
# ============================================


def sync_chesmin_to_jacula():
    print("Descargando productos de Chesmin (origen)...")
    chesmin_products = get_all_products(CHESMIN_STORE_ID, CHESMIN_HEADERS)
    print(f"  → {len(chesmin_products)} productos encontrados en Chesmin")

    print("Descargando productos de Jacula (destino)...")
    jacula_products = get_all_products(JACULA_STORE_ID, JACULA_HEADERS)
    print(f"  → {len(jacula_products)} productos encontrados en Jacula")

    # Mapa key → producto de Jacula
    jacula_by_key: dict[str, dict] = {}
    for p in jacula_products:
        key = build_product_key(p)
        if key:
            jacula_by_key[key] = p

    # Recorremos todos los productos de Chesmin y clonamos / actualizamos
    for src in chesmin_products:
        key = build_product_key(src)
        if not key:
            print("Saltando producto de Chesmin SIN clave (sin sku ni nombre)")
            continue

        dst = jacula_by_key.get(key)
        payload = build_jacula_payload_from_chesmin(src)

        if dst:
            # Ya existe en Jacula → actualizar (salvo que sea Capsula Jacula ✿)
            if product_has_excluded_category(dst):
                print(f"[SKIP] Producto en categoría '{EXCLUDED_CATEGORY_NAME}', no se toca. Key={key}")
                continue

            product_id = dst["id"]
            print(f"[UPDATE] Actualizando producto Jacula id={product_id} key={key}")
            resp = requests.put(
                f"{API_BASE}/{JACULA_STORE_ID}/products/{product_id}",
                headers=JACULA_HEADERS,
                json=payload,
            )
            print(f"  → {resp.status_code} {resp.text[:200]}")

        else:
            # No existe en Jacula → crear producto nuevo
            print(f"[CREATE] Creando producto nuevo en Jacula key={key}")
            resp = requests.post(
                f"{API_BASE}/{JACULA_STORE_ID}/products",
                headers=JACULA_HEADERS,
                json=payload,
            )
            print(f"  → {resp.status_code} {resp.text[:200]}")

    print("Sincronización terminada.")


if __name__ == "__main__":
    sync_chesmin_to_jacula()
