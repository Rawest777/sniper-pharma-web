# -*- coding: utf-8 -*-
import mysql.connector
import requests
import json
import os  # <--- IMPORTANTE: Necesario para leer secretos de la nube
from mysql.connector import Error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import time

# --- 1. CONFIGURACIÓN HÍBRIDA (NUBE + LOCAL) ---
# El script intentará leer las claves de GitHub. 
# Si no las encuentra (ej. en tu PC), usará los valores por defecto (segundo parámetro).
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'gateway01.us-east-1.prod.aws.tidbcloud.com'),
    'user': os.getenv('DB_USER', '4AnCu27X8wGRLCd.root'),
    'password': os.getenv('DB_PASS', 'R2f5aC0Sf2ThBSVT'),
    'database': 'datos',
    'port': int(os.getenv('DB_PORT', 4000))
}

# Ajusta la velocidad según tu PC. 20 es rápido y seguro.
MAX_WORKERS = 20 

# --- 2. FUNCIONES DE BÚSQUEDA ---

def find_specific_key(data, target_key):
    """Busca una llave recursivamente en un JSON complejo"""
    if isinstance(data, dict):
        for key, value in data.items():
            if key == target_key:
                return value
            result = find_specific_key(value, target_key)
            if result is not None:
                return result
    elif isinstance(data, list):
        for item in data:
            result = find_specific_key(item, target_key)
            if result is not None:
                return result
    return None

def get_colsubsidio_prices(codigo):
    """
    Consulta la API 'Intelligent Search' de VTEX.
    """
    api_url = f"https://www.drogueriascolsubsidio.com/api/io/_v/api/intelligent-search/product_search?query={codigo}&count=1"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    }
    
    try:
        response = requests.get(api_url, headers=headers, timeout=15)
        
        if response.status_code != 200:
            return "No encontrado", "no encontrado"
            
        data = response.json()
        
        # Validación de estructura VTEX Intelligent Search
        if isinstance(data, dict) and 'products' in data:
            lista_productos = data['products']
            if not lista_productos:
                return "No encontrado", "no encontrado"
            primer_resultado = lista_productos[0]
        elif isinstance(data, list) and data:
            primer_resultado = data[0]
        else:
             return "No encontrado", "no encontrado"

        # Extracción de precios usando búsqueda recursiva
        price_normal = find_specific_key(primer_resultado, "Price")
        price_without_discount = find_specific_key(primer_resultado, "PriceWithoutDiscount")
        
        # --- Lógica de Rescate (Fallbacks) ---
        if price_normal is None: 
            price_normal = find_specific_key(primer_resultado, "sellingPrice")
        
        if price_normal is None: 
            return "Sin precio", "no encontrado"
        
        # Conversión a enteros
        p_normal = int(price_normal)
        p_old = "no encontrado"
        
        # Validar Agotado
        if p_normal == 0:
            return "Agotado", "Agotado"

        # Lógica de Precio Oferta vs Precio Lista
        if price_without_discount is not None:
            p_old_int = int(price_without_discount)
            if p_normal != p_old_int and p_old_int > 0:
                p_old = p_old_int
        else:
             # Intento secundario de encontrar precio de lista
             price_list = find_specific_key(primer_resultado, "listPrice")
             if price_list and int(price_list) != p_normal:
                 p_old = int(price_list)

        return p_normal, p_old

    except Exception:
        return "Error", "no encontrado"

# --- 3. FUNCIÓN DE PROCESAMIENTO ---
def procesar_fila(row):
    try:
        producto_id = row['id']
        codigo_celda = row['BARRAS'] if row['BARRAS'] else ""
        presentacion_str = row['PRES'] if row['PRES'] else "1"
        
        criterio = row['CRITERIO_COLSUPSIDIO'].strip().upper() if row['CRITERIO_COLSUPSIDIO'] else ""

        if not codigo_celda or codigo_celda.isspace():
            return None

        codigos_a_buscar = [c.strip() for c in codigo_celda.split(';') if c.strip()]
        
        precio_normal_final = "No encontrado"
        precio_viejo_final = "no encontrado"

        for codigo_individual in codigos_a_buscar:
            precio_normal, precio_viejo = get_colsubsidio_prices(codigo_individual)
            
            if isinstance(precio_normal, int) or precio_normal == "Agotado":
                precio_normal_final = precio_normal
                precio_viejo_final = precio_viejo
                break 
            else:
                precio_normal_final = precio_normal
                precio_viejo_final = precio_viejo
        
        # --- APLICACIÓN DE CRITERIOS ---
        if criterio == "BLOQUEAR":
            precio_normal_final = "BLOQUEADO"
            precio_viejo_final = "BLOQUEADO"
        elif isinstance(precio_normal_final, int):
            try:
                presentacion_num = int(presentacion_str) if str(presentacion_str).isdigit() else 1
                
                if criterio == "MULTIPLICAR":
                    precio_normal_final = precio_normal_final * presentacion_num
                    if isinstance(precio_viejo_final, int):
                        precio_viejo_final = precio_viejo_final * presentacion_num
                        
                elif criterio.startswith("DIVIDIR;"):
                    try:
                        divisor = int(criterio.split(';')[1])
                        if divisor != 0:
                            precio_normal_final = round((precio_normal_final / divisor) * presentacion_num)
                            if isinstance(precio_viejo_final, int):
                                precio_viejo_final = round((precio_viejo_final / divisor) * presentacion_num)
                    except Exception:
                        pass
            except ValueError:
                pass

        return (
            producto_id,
            "Colsubsidio", 
            str(precio_normal_final), 
            str(precio_viejo_final), 
            datetime.now()
        )
    
    except Exception as e:
        print(f"Error procesando ID {row.get('id', '??')}: {e}")
        return None

# --- 5. PROCESO PRINCIPAL ---
def procesar_productos_en_paralelo():
    resultados_para_insertar = [] 
    db_connection = None
    cursor = None
    
    try:
        print("🔌 Conectando con MySQL...")
        db_connection = mysql.connector.connect(**DB_CONFIG)
        cursor = db_connection.cursor(dictionary=True)
        print("✅ Conexión exitosa.")
        
        # --- LECTURA ---
        print("⏳ Leyendo SOLO productos VIP...")
        
        sql_lectura = """
            SELECT id, BARRAS, PRES, CRITERIO_COLSUPSIDIO 
            FROM productos_master 
            WHERE BARRAS IS NOT NULL 
            AND BARRAS != '' 
            AND vip_monitoreo = 1
        """
        
        cursor.execute(sql_lectura)
        productos_a_procesar = cursor.fetchall()
        total_productos = len(productos_a_procesar)
        
        if not productos_a_procesar:
            print("❌ No se encontraron productos VIP con código de barras.")
            return

        print(f"ℹ️  Procesando {total_productos} productos VIP...")
        
        contador_completados = 0
        encontrados = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(procesar_fila, row) for row in productos_a_procesar]
            
            for future in as_completed(futures):
                result = future.result()
                if result:
                    resultados_para_insertar.append(result)
                    if result[2] != "No encontrado" and result[2] != "Error":
                        encontrados += 1
                
                contador_completados += 1
                if contador_completados % 5 == 0 or contador_completados == total_productos:
                    print(f" -> Progreso: {contador_completados}/{total_productos} | Precios Hallados: {encontrados}", end='\r')

        print(f"\n\n✅ Recolección finalizada. Guardando en la Nube...")

        if resultados_para_insertar:
            
            # --- 6a. ACTUALIZAR MAESTRA ---
            print(f"--- Actualizando tabla maestra en la nube...")
            updates_master = [(r[2], r[3], r[0]) for r in resultados_para_insertar]
            
            sql_update = """
            UPDATE productos_master SET 
                P_COLSUBSIDIO_Calculado = %s,
                P_COLSUBSIDIO_Original = %s
            WHERE id = %s
            """
            cursor.executemany(sql_update, updates_master)
            
            # --- 6b. INSERTAR HISTORIAL ---
            print(f"--- Insertando historial en la nube...")
            sql_insert = """
            INSERT INTO precios_historicos 
            (producto_id, drogueria_nombre, precio_oferta, precio_normal, fecha_actualizacion)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.executemany(sql_insert, resultados_para_insertar)
            
            db_connection.commit()
            print(f"\n🎉 ¡ÉXITO! Datos sincronizados con www.sniperpharma.com.co")
            
        else:
            print("⚠️ No se generaron datos para guardar.")

    except Error as e:
        print(f"❌ Error de conexión: {e}")
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        
    finally:
        if cursor: cursor.close()
        if db_connection: db_connection.close()

if __name__ == "__main__":
    start_time = time.time()
    procesar_productos_en_paralelo()
    end_time = time.time()
    print(f"\n⏱️ Tiempo total: {end_time - start_time:.2f} segundos.")