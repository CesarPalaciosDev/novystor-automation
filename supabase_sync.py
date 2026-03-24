"""
supabase_sync.py — Dual-write para novy-api

Escribe checkouts y sus items directamente en Supabase (schema novy)
inmediatamente después del write a MySQL, sin esperar el cron de novy-upsert.

Usa PostgREST vía HTTP (no cliente oficial de Supabase).
Requiere /app/.supabase_env con SUPABASE_URL y SUPABASE_SERVICE_KEY.
"""

import os
import logging
import requests
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# --- Configuración ---
# Lee de env vars (configuradas en Coolify). Fallback a archivo .supabase_env para compatibilidad.
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    try:
        from dotenv import dotenv_values
        _config = dotenv_values("/app/.supabase_env")
        SUPABASE_URL = SUPABASE_URL or _config.get("SUPABASE_URL", "https://supabase.novaq.cl")
        SUPABASE_SERVICE_KEY = SUPABASE_SERVICE_KEY or _config.get("SUPABASE_SERVICE_KEY", "")
    except Exception:
        pass

_HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
    "Accept-Profile": "novy",
    "Content-Profile": "novy",
    "Prefer": "resolution=merge-duplicates,return=minimal",
}


def _iso(value) -> str | None:
    """Convierte datetime o string a ISO 8601. Retorna None si es vacío."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    s = str(value).strip()
    return s if s else None


def _upsert(table: str, payload: dict, on_conflict: str = "") -> bool:
    """POST a PostgREST con upsert. Retorna True si exitoso."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    if on_conflict:
        url = f"{url}?on_conflict={on_conflict}"
    try:
        resp = requests.post(url, json=payload, headers=_HEADERS, timeout=10)
        if resp.status_code in (200, 201):
            return True
        logger.warning(
            "[SupabaseSync] upsert %s falló — status %s: %s",
            table, resp.status_code, resp.text[:200],
        )
        return False
    except requests.RequestException as e:
        logger.error("[SupabaseSync] Error de red al upsert %s: %s", table, e)
        return False


def sync_checkout(tmp: dict, productos: list) -> None:
    """
    Sincroniza un checkout y sus items con Supabase novy schema.

    Args:
        tmp: dict con los datos del checkout (tal como se construye en utils.py)
        productos: list de dicts con los items del checkout
    """
    if not SUPABASE_SERVICE_KEY:
        logger.warning("[SupabaseSync] SUPABASE_SERVICE_KEY no configurado — skip sync")
        return

    # Tomar el último estado de pago (igual que el DataFrame processing)
    estados = tmp.get("estado venta", [])
    estado_venta = estados[-1] if estados else None

    checkout_payload = {
        "costo_envio": tmp.get("costo de envio"),
        "estado_boleta": tmp.get("estado boleta"),
        "estado_entrega": tmp.get("estado entrega"),
        "estado_venta": estado_venta,
        "fecha": _iso(tmp.get("fecha")),
        "mail": tmp.get("mail"),
        "market": tmp.get("market"),
        "n_venta": tmp.get("n venta"),
        "nombre_cliente": tmp.get("nombre"),
        "phone": tmp.get("phone"),
        "url_boleta": tmp.get("url boleta"),
        "n_seguimiento": tmp.get("N seguimiento"),
        "codigo": tmp.get("codigo"),
        "codigo_venta": tmp.get("codigo venta"),
        "courier": tmp.get("courier") or "Empty",
        "clase_de_envio": tmp.get("clase de envio"),
        "delivery_status": tmp.get("delivery status"),
        "direccion": tmp.get("direccion"),
        "impresion_etiqueta": tmp.get("estado impresion etiqueta") or "not_printed",
        "fecha_despacho": _iso(tmp.get("fecha despacho")),
        "fecha_promesa": _iso(tmp.get("fecha promesa")),
        "id_venta": tmp.get("id venta"),
        "status_etiqueta": tmp.get("status etiqueta"),
    }

    ok = _upsert("checkouts_full", checkout_payload)
    n_venta = tmp.get("n venta", "?")
    if ok:
        logger.info("[SupabaseSync] checkouts_full sincronizado — n_venta=%s", n_venta)
    else:
        logger.warning("[SupabaseSync] checkouts_full sync falló — n_venta=%s", n_venta)

    # Sync items
    items_ok = 0
    for item in productos:
        item_payload = {
            "codigo_producto": item.get("codigo producto"),
            "nombre_producto": item.get("nombre producto"),
            "id_padre_producto": item.get("id padre producto"),
            "id_hijo_producto": item.get("id hijo producto"),
            "cantidad": item.get("cantidad"),
            "precio": item.get("precio"),
            "id_venta": item.get("id venta"),
        }
        if _upsert("checkout_items", item_payload, on_conflict="id_venta,id_hijo_producto"):
            items_ok += 1

    logger.info(
        "[SupabaseSync] checkout_items sincronizados: %d/%d — n_venta=%s",
        items_ok, len(productos), n_venta,
    )
