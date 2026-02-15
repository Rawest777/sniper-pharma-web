# -*- coding: utf-8 -*-
import mysql.connector
import requests
import json
import time 
import os # <--- OBLIGATORIO PARA LA NUBE
from mysql.connector import Error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# --- 1. CONFIGURACIÓN HÍBRIDA (NUBE + LOCAL) ---
# El sistema buscará primero en los Secretos de GitHub.
# Si no los encuentra (tu PC), usará los valores por defecto.
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'gateway01.us-east-1.prod.aws.tidbcloud.com'),
    'user': os.getenv('DB_USER', '4AnCu27X8wGRLCd.root'),
    'password': os.getenv('DB_PASS', 'R2f5aC0Sf2ThBSVT'),
    'database': 'datos',
    'port': int(os.getenv('DB_PORT', 4000))
}

MAX_WORKERS = 20

# --- 2. FUNCIÓN DE BÚSQUEDA (MOTOR API) ---
def get_larebaja_prices(codigo):
    # Endpoint oficial de búsqueda VTEX de La Rebaja
    api_url = f"https://www.larebajavirtual.com/{codigo}?_q={codigo}&map=ft&__pickRuntime=appsEtag%2Cblocks%2CblocksTree%2Ccomponents%2CcontentMap%2Cextensions%2Cmessages%2Cpage%2Cpages%2Cquery%2CqueryData%2Croute%2CruntimeMeta%2Csettings"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    }
    
    try:
        response = requests.get(api_url, headers=headers, timeout=15)
        if response.status_code != 200:
            return "No encontrado", "no encontrado"
            
        main_json = response.json()
        
        # Validación extra por si la estructura cambia
        if 'queryData' not in main_json or not main_json['queryData']:
             return "No encontrado", "no encontrado"

        product_data_string = main_json['queryData'][0]['data']
        product_data = json.loads(product_data_string)
        
        if 'productSearch' not in product_data or 'products' not in product_data['productSearch']:
            return "No encontrado", "no encontrado"

        lista_productos = product_data['productSearch']['products']
        
        price_pairs = []
        for producto in lista_productos:
            # La Rebaja a veces tiene múltiples vendedores, tomamos el primero
            if 'items' in producto and len(producto['items']) > 0:
                oferta = producto['items'][0]['sellers'][0]['commertialOffer']
                price_normal = oferta['Price']
                price_viejo = oferta['ListPrice']
                price_pairs.append((price_normal, price_viejo))
        
        if not price_pairs:
            return "No encontrado", "no encontrado"
        
        # Seleccionamos la mejor opción (el precio más alto suele ser el real si hay variantes)
        best_pair = max(price_pairs, key=lambda item: item[0])
        final_price_normal, final_price_viejo = int(best_pair[0]), int(best_pair[1])
        
        if final_price_normal != final_price_viejo:
            return final_price_normal, final_price_viejo
        else:
            return final_price_normal, "no encontrado"
            
    except (KeyError, IndexError, TypeError, json.JSONDecodeError):
        return "No encontrado", "no encontrado"
    except Exception:
        return "Error", "no encontrado"

# --- 3. FUNCIÓN DE PROCESAMIENTO ---
def procesar_fila(row):
    try:
        producto_id = row['id']
        codigo_celda = row['BARRAS'] if row['BARRAS'] else ""
        presentacion_str = row['PRES'] if row['PRES'] else "1"
        criterio = row['CRITERIO_LA_REBAJA'].strip().upper() if row['CRITERIO_LA_REBAJA'] else ""

        if not codigo_celda or codigo_celda.isspace():
            return None 

        codigos_a_buscar = [c.strip() for c in codigo_celda.split(';') if c.strip()]
        precio_normal_final = "No encontrado"
        precio_viejo_final = "no encontrado"

        for codigo_individual in codigos_a_buscar:
            precio_normal, precio_viejo = get_larebaja_prices(codigo_individual)
            if isinstance(precio_normal, int) or precio_normal == "Agotado":
                precio_normal_final = precio_normal
                precio_viejo_final = precio_viejo
                break
            else:
                precio_normal_final = precio_normal
                precio_viejo_final = precio_viejo
        
        # --- MATEMÁTICA DE CRITERIOS ---
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
            "La Rebaja", 
            str(precio_normal_final), 
            str(precio_viejo_final), 
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
        
        sql_lectura = """
            SELECT id, BARRAS, PRES, CRITERIO_LA_REBAJA 
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
    print(f"🚀 Iniciando scraping masivo con {MAX_WORKERS} hilos...")
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

    print(f"\n✅ Recolección finalizada. Preparando datos para subir...")

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
            sql_update = "UPDATE productos_master SET P_LAREBAJA_Calculado=%s, P_LAREBAJA_Original=%s WHERE id=%s"
            cursor_write.executemany(sql_update, updates)
            
            # Insert Historial
            sql_insert = "INSERT INTO precios_historicos (producto_id, drogueria_nombre, precio_oferta, precio_normal, fecha_actualizacion) VALUES (%s, %s, %s, %s, %s)"
            cursor_write.executemany(sql_insert, resultados_para_insertar)
            
            conn_write.commit()
            print(f"\n🎉 ¡ÉXITO! Se actualizaron {len(updates)} productos en www.sniperpharma.com.co")
            
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