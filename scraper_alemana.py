# -*- coding: utf-8 -*-
import mysql.connector
import requests
import json
import re
import os # <--- OBLIGATORIO PARA SEGURIDAD EN NUBE
from bs4 import BeautifulSoup
import time
from mysql.connector import Error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# --- 1. CONFIGURACIÓN HÍBRIDA (NUBE + LOCAL) ---
# Intenta leer de GitHub Actions (Secretos). Si no los encuentra (tu PC), usa el valor por defecto.
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'gateway01.us-east-1.prod.aws.tidbcloud.com'),
    'user': os.getenv('DB_USER', '4AnCu27X8wGRLCd.root'),
    'password': os.getenv('DB_PASS', 'R2f5aC0Sf2ThBSVT'),
    'database': 'datos',
    'port': int(os.getenv('DB_PORT', 4000))
}

MAX_WORKERS = 20

# --- 2. FUNCIONES DE BÚSQUEDA ---
def find_any_price(data):
    POSIBLES_CLAVES_PRECIO = ['price', 'lowPrice', 'highPrice', 'sellingPrice', 'Price']
    if isinstance(data, dict):
        for key, value in data.items():
            if key in POSIBLES_CLAVES_PRECIO and isinstance(value, (int, float)):
                return value
            result = find_any_price(value)
            if result is not None:
                return result
    elif isinstance(data, list):
        for item in data:
            result = find_any_price(item)
            if result is not None:
                return result
    return None

def get_product_prices(codigo):
    # URL de búsqueda de Tu Droguería Virtual (Alemana)
    product_url = f"https://www.tudrogueriavirtual.com/{codigo}?_q={codigo}&map=ft"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    }
    
    selling_price = "No encontrado"
    list_price = "No encontrado"
    
    try:
        response = requests.get(product_url, headers=headers, timeout=15)
        if response.status_code != 200:
            return "No encontrado", "No encontrado"

        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Intento 1: JSON-LD (Estructura de datos)
        script_tag = soup.find('script', {'type': 'application/ld+json'})
        if script_tag:
            try:
                data = json.loads(script_tag.string)
                price_found = find_any_price(data)
                if price_found:
                    selling_price = int(price_found)
            except json.JSONDecodeError:
                pass
        
        # Intento 2: Regex directo en el HTML (ListPrice)
        match = re.search(r'"ListPrice":(\d+\.?\d*)', response.text)
        if match:
            list_price = int(float(match.group(1)))
            
        # Intento 3: Regex de respaldo para Price si falló JSON-LD
        if not isinstance(selling_price, int):
            match_price = re.search(r'"Price":(\d+\.?\d*)', response.text)
            if match_price:
                selling_price = int(float(match_price.group(1)))

        if isinstance(selling_price, int):
            return selling_price, list_price
        else:
            return "No encontrado", list_price

    except Exception:
        return "Error", "Error"

# --- 3. FUNCIÓN DE PROCESAMIENTO ---
def procesar_fila(row):
    try:
        producto_id = row['id']
        codigo_celda = row['BARRAS'] if row['BARRAS'] else ""
        presentacion_str = row['PRES'] if row['PRES'] else "1"
        criterio = row['CRITERIO_LA_ALEMANA'].strip().upper() if row['CRITERIO_LA_ALEMANA'] else ""

        if not codigo_celda or codigo_celda.isspace():
            return None

        codigos_a_buscar = [c.strip() for c in codigo_celda.split(';') if c.strip()]
        precio_calculado_final = "No encontrado"
        precio_original_final = "No encontrado"

        for codigo_individual in codigos_a_buscar:
            price_val, list_price_val = get_product_prices(codigo_individual)
            if isinstance(price_val, int):
                precio_calculado_final = price_val
                precio_original_final = list_price_val
                break
            else:
                precio_calculado_final = price_val
                precio_original_final = list_price_val
                
        # --- MATEMÁTICA DE CRITERIOS ---
        if criterio == "BLOQUEAR":
            precio_calculado_final = "BLOQUEADO"
            precio_original_final = "BLOQUEADO"
        
        elif isinstance(precio_calculado_final, int):
            try:
                presentacion_num = int(presentacion_str) if str(presentacion_str).isdigit() else 1
                
                if criterio == "MULTIPLICAR":
                    precio_calculado_final = precio_calculado_final * presentacion_num
                    if isinstance(precio_original_final, int):
                        precio_original_final = precio_original_final * presentacion_num
                        
                elif criterio.startswith("DIVIDIR;"):
                    try:
                        divisor = int(criterio.split(';')[1])
                        if divisor != 0:
                            precio_calculado_final = round((precio_calculado_final / divisor) * presentacion_num)
                            if isinstance(precio_original_final, int):
                                precio_original_final = round((precio_original_final / divisor) * presentacion_num)
                    except (ValueError, IndexError):
                        pass
            except ValueError:
                pass
        
        # Limpieza final
        if isinstance(precio_calculado_final, int) and isinstance(precio_original_final, int):
            if precio_calculado_final == precio_original_final:
                precio_original_final = "no encontrado"

        return (
            producto_id,
            "Drogueria Alemana",
            str(precio_calculado_final), 
            str(precio_original_final), 
            datetime.now()
        )
    
    except Exception as e:
        print(f"Error procesando ID {row.get('id', '??')}: {e}")
        return None

# --- 5. PROCESO PRINCIPAL (GESTIÓN DE CONEXIÓN INTELIGENTE) ---
def procesar_productos_en_paralelo():
    productos_a_procesar = []
    
    # ---------------------------------------------------------
    # FASE 1: LEER PRODUCTOS (Conexión rápida)
    # ---------------------------------------------------------
    print("🔌 FASE 1: Conectando a la Nube para LEER lista de productos...")
    try:
        conn_read = mysql.connector.connect(**DB_CONFIG)
        cursor_read = conn_read.cursor(dictionary=True)
        
        # FILTRO CRÍTICO: vip_monitoreo = 1
        sql_lectura = """
            SELECT id, BARRAS, PRES, CRITERIO_LA_ALEMANA 
            FROM productos_master
            WHERE BARRAS IS NOT NULL 
            AND BARRAS != '' 
            AND vip_monitoreo = 1
        """
        cursor_read.execute(sql_lectura)
        productos_a_procesar = cursor_read.fetchall()
        print(f"✅ Lectura completada: {len(productos_a_procesar)} productos VIP encontrados.")
        
        cursor_read.close()
        conn_read.close() # <--- ¡CERRAMOS LA CONEXIÓN AQUÍ!
        print("🔌 Conexión cerrada para ahorrar recursos.")

    except Error as e:
        print(f"❌ Error leyendo DB: {e}")
        return

    if not productos_a_procesar: return

    # ---------------------------------------------------------
    # FASE 2: TRABAJO PESADO (Sin gastar conexión a DB)
    # ---------------------------------------------------------
    print(f"🚀 Iniciando scraping masivo de Alemana con {MAX_WORKERS} hilos...")
    resultados_para_insertar = []
    total_productos = len(productos_a_procesar)
    contador_completados = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(procesar_fila, row) for row in productos_a_procesar]
        
        for future in as_completed(futures):
            result = future.result()
            if result:
                resultados_para_insertar.append(result)
            
            contador_completados += 1
            if contador_completados % 10 == 0 or contador_completados == total_productos:
                print(f" -> Progreso: {contador_completados}/{total_productos}", end='\r')

    print(f"\n\n✅ Recolección finalizada. Preparando datos para subir...")

    # ---------------------------------------------------------
    # FASE 3: GUARDAR (Reconexión fresca)
    # ---------------------------------------------------------
    if resultados_para_insertar:
        print("🔌 FASE 3: Reconectando a la Nube para GUARDAR datos...")
        conn_write = None
        try:
            conn_write = mysql.connector.connect(**DB_CONFIG) # <--- NUEVA CONEXIÓN
            cursor_write = conn_write.cursor()
            
            # Update Maestro
            updates = [(r[2], r[3], r[0]) for r in resultados_para_insertar]
            sql_update = "UPDATE productos_master SET P_DROGUERIA_ALEMANA_Calculado=%s, P_DROGUERIA_ALEMANA_Original=%s WHERE id=%s"
            cursor_write.executemany(sql_update, updates)
            
            # Insert Historial
            sql_insert = "INSERT INTO precios_historicos (producto_id, drogueria_nombre, precio_oferta, precio_normal, fecha_actualizacion) VALUES (%s, %s, %s, %s, %s)"
            cursor_write.executemany(sql_insert, resultados_para_insertar)
            
            conn_write.commit()
            print(f"\n🎉 ¡ÉXITO! Droguería Alemana sincronizada en www.sniperpharma.com.co")
            
        except Error as e:
            print(f"❌ Error guardando en MySQL: {e}")
        finally:
            if conn_write and conn_write.is_connected():
                conn_write.close()
    else:
        print("⚠️ El proceso terminó pero no se encontraron precios válidos.")

if __name__ == "__main__":
    start_time = time.time()
    procesar_productos_en_paralelo()
    end_time = time.time()
    print(f"\n⏱️ Tiempo total: {end_time - start_time:.2f} segundos.")