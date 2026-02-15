# -*- coding: utf-8 -*-
import mysql.connector
import re
import os
import shutil
import time
import random
from mysql.connector import Error
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options 
from selenium_stealth import stealth
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# --- CONFIGURACIÓN HÍBRIDA ---
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'gateway01.us-east-1.prod.aws.tidbcloud.com'),
    'user': os.getenv('DB_USER', '4AnCu27X8wGRLCd.root'),
    'password': os.getenv('DB_PASS', 'R2f5aC0Sf2ThBSVT'),
    'database': 'datos',
    'port': int(os.getenv('DB_PORT', 4000))
}

MAX_WORKERS = 5
HTML_DIRECTORY = "html_cruzverde"

# --- LIMPIEZA ---
def limpiar_directorio_html():
    if os.path.exists(HTML_DIRECTORY):
        try:
            for filename in os.listdir(HTML_DIRECTORY):
                file_path = os.path.join(HTML_DIRECTORY, filename)
                if os.path.isfile(file_path): os.unlink(file_path)
        except: pass
    else: os.makedirs(HTML_DIRECTORY)

def limpiar_precio(texto_precio):
    if not texto_precio: return None
    numeros = re.findall(r'\d+', texto_precio)
    return int("".join(numeros)) if numeros else None

# --- ANÁLISIS ---
def analizar_html_producto(soup):
    precio_normal, precio_oferta = None, None
    product_card = soup.find('ml-card-product')
    if not product_card: return "N/A", "N/A"

    normal_price_tag = product_card.find(class_='line-through')
    if normal_price_tag: precio_normal = limpiar_precio(normal_price_tag.get_text())

    offer_price_tag = product_card.select_one("div#club-price span")
    if not offer_price_tag: offer_price_tag = product_card.select_one(".text-prices")
    if offer_price_tag: precio_oferta = limpiar_precio(offer_price_tag.get_text())

    if precio_oferta and not precio_normal:
        default_tag = product_card.find(class_='text-12 sm:text-14 order-3')
        if default_tag and limpiar_precio(default_tag.get_text()) != precio_oferta:
            precio_normal = limpiar_precio(default_tag.get_text())

    return (precio_normal if precio_normal else "N/A"), (precio_oferta if precio_oferta else "N/A")

# --- SCRAPING (HEADLESS) ---
def recolectar_html_cruzverde(productos):
    print("\n🕵️ INICIANDO MODO FANTASMA (HEADLESS)...")
    chrome_options = Options()
    chrome_options.add_argument("--headless=new") 
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    
    driver = webdriver.Chrome(options=chrome_options)
    stealth(driver, languages=["es-ES", "es"], vendor="Google Inc.", platform="Win32", fix_hairline=True)
    wait = WebDriverWait(driver, 15)

    try:
        driver.get("https://www.cruzverde.com.co/")
        time.sleep(3)
    except: pass

    total = len(productos)
    for i, row in enumerate(productos):
        p_id = row['id']
        codigos = [c.strip() for c in row['BARRAS'].split(';') if c.strip()]
        ruta = os.path.join(HTML_DIRECTORY, f"producto_{p_id}.html")
        
        # Feedback visual en terminal
        nombre_corto = (row['NOMBRE'][:30] + '..') if len(row['NOMBRE']) > 30 else row['NOMBRE']
        print(f"🔎 ({i+1}/{total}) Buscando: {nombre_corto:<35}", end='\r')

        for codigo in codigos:
            try:
                search_box = wait.until(EC.visibility_of_element_located((By.XPATH, "//input[@placeholder='Buscar...']")))
                driver.execute_script("arguments[0].value = '';", search_box)
                search_box.send_keys(codigo)
                search_box.send_keys(Keys.RETURN)
                time.sleep(4) 

                if 'Lo sentimos, no encontramos resultados' in driver.page_source: continue
                
                with open(ruta, 'w', encoding='utf-8') as f: f.write(driver.page_source)
                break 
            except: pass
    driver.quit()

# --- PROCESAMIENTO ---
def procesar_fila(row):
    try:
        p_id = row['id']
        ruta = os.path.join(HTML_DIRECTORY, f"producto_{p_id}.html")
        
        # Valores por defecto
        p_fin, n_fin = "No encontrado", "No encontrado"
        web_oferta = "N/A" # Para mostrar en consola
        
        if os.path.exists(ruta): 
            with open(ruta, 'r', encoding='utf-8') as f: content = f.read()
            p_normal_raw, p_oferta_raw = analizar_html_producto(BeautifulSoup(content, 'lxml'))
            
            p_fin, n_fin = p_oferta_raw, p_normal_raw
            web_oferta = p_oferta_raw # Guardamos el dato crudo para mostrarlo
            
            # LOGICA DE NEGOCIO
            crit = row['CRITERIO_CRUZ_VERDE'].strip().upper() if row['CRITERIO_CRUZ_VERDE'] else ""
            pres = int(row['PRES']) if str(row['PRES']).isdigit() else 1

            if crit == "BLOQUEAR":
                p_fin, n_fin = "BLOQUEADO", "BLOQUEADO"
            elif isinstance(p_oferta_raw, int):
                if crit == "MULTIPLICAR":
                    p_fin = p_oferta_raw * pres
                    if isinstance(p_normal_raw, int): n_fin = p_normal_raw * pres
                elif crit.startswith("DIVIDIR;"):
                    try:
                        div = int(crit.split(';')[1])
                        if div != 0:
                            p_fin = round((p_oferta_raw / div) * pres)
                            if isinstance(p_normal_raw, int): n_fin = round((p_normal_raw / div) * pres)
                    except: pass
            
            if p_fin != "N/A" and n_fin == "N/A": n_fin = "no encontrado"

        # Retornamos TUPLA + INFO EXTRA para imprimir
        # Estructura: (id, nombre_droguera, precio_of, precio_norm, fecha, NOMBRE_PROD, PRECIO_WEB)
        return (p_id, "Cruz Verde", str(p_fin), str(n_fin), datetime.now(), row['NOMBRE'], str(web_oferta))
    except: return None

# --- MAIN ---
def main():
    print("🔌 Conectando a TiDB (Nube)...")
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        # Puedes cambiar LIMIT 1000 a LIMIT 5 si quieres probar rápido
        cursor.execute("SELECT id, NOMBRE, BARRAS, PRES, CRITERIO_CRUZ_VERDE FROM productos_master WHERE vip_monitoreo = 1")
        productos = cursor.fetchall()
        conn.close()
    except Error as e:
        print(f"❌ Error BD: {e}")
        return

    if not productos:
        print("⚠️ No hay productos VIP.")
        return

    print(f"🚀 Iniciando scraping masivo de {len(productos)} productos...")
    limpiar_directorio_html()
    recolectar_html_cruzverde(productos)

    print("\n\n⚙️ Analizando datos...")
    resultados_completos = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(procesar_fila, row) for row in productos]
        for f in as_completed(futures):
            res = f.result()
            if res: resultados_completos.append(res)

    # --- IMPRIMIR TABLA EN TERMINAL ---
    print("\n📊 RESUMEN DE EJECUCIÓN:")
    print("="*100)
    print(f"{'PRODUCTO':<30} | {'WEB':<10} | {'FINAL':<10} | {'ESTADO':<15}")
    print("="*100)
    
    datos_para_guardar = []
    
    for r in resultados_completos:
        # Desempaquetamos los datos extra que agregamos en procesar_fila
        p_id, drog, p_off, p_norm, fecha, nombre_prod, web_price = r
        
        estado = "✅ OK"
        if p_off == "No encontrado" or p_off == "N/A": estado = "❌ Falta"
        if p_off == "BLOQUEADO": estado = "🔒 Block"
        
        # Imprimimos
        n_print = (nombre_prod[:28] + '..') if len(nombre_prod) > 28 else nombre_prod
        print(f"{n_print:<30} | {web_price:<10} | {p_off:<10} | {estado:<15}")
        
        # Guardamos solo lo necesario para SQL (quitamos nombre y web_price)
        datos_para_guardar.append((p_id, drog, p_off, p_norm, fecha))

    print("="*100)

    # --- GUARDADO ---
    if datos_para_guardar:
        try:
            print(f"💾 Guardando {len(datos_para_guardar)} registros en la Base de Datos...")
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor()
            
            sql_up = "UPDATE productos_master SET P_CRUZ_VERDE_Calculado=%s, P_CRUZ_VERDE_Original=%s WHERE id=%s"
            # Reordenamos para el Update: (p_off, p_norm, p_id)
            cursor.executemany(sql_up, [(x[2], x[3], x[0]) for x in datos_para_guardar])
            
            sql_ins = "INSERT INTO precios_historicos (producto_id, drogueria_nombre, precio_oferta, precio_normal, fecha_actualizacion) VALUES (%s, %s, %s, %s, %s)"
            cursor.executemany(sql_ins, datos_para_guardar)
            
            conn.commit()
            conn.close()
            print(f"✅ ¡HECHO! Base de datos actualizada.")
        except Exception as e: print(f"❌ Error guardando: {e}")
    else:
        print("⚠️ No se encontraron datos válidos para guardar.")

if __name__ == "__main__":
    main()