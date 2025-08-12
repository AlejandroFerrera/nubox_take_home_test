import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.handler import DBHandler
from config.config import config
from sqlalchemy import text
from config.logger import setup_logger

logger = setup_logger('INSIGHTS')


def get_pm25_daily_average_last_7_days(country: str, locality: str, db_client: DBHandler) -> None:
    """1. Promedio diario de PM2.5 para los últimos 7 días."""
    query = """
        SELECT dl.locality_name         AS localidad,
               dl.country_name          AS pais,
               aqm.parameter            AS contaminante,
               aqm.unit                 AS unidad_de_medida,
               ROUND(AVG(aqm.value), 2) AS promedio_diario_pm25
        FROM fact_air_quality_measurement aqm
                 INNER JOIN dim_station dm ON aqm.station_sk = dm.station_sk
                 INNER JOIN dim_locality dl ON dl.locality_sk = dm.locality_sk
        WHERE aqm.parameter = 'pm25'
          AND aqm.measurement_timestamp >= NOW() - INTERVAL '7 days'
          AND LOWER(dl.country_name) = LOWER(:country)
          AND LOWER(dl.locality_name) = LOWER(:locality)
        GROUP BY dl.locality_name, dl.country_name, aqm.parameter, aqm.unit;
    """
    
    result = db_client.conn.execute(text(query), {"country": country, "locality": locality})
    row = result.fetchone()
    
    if row:
        logger.info(
            f"1. PROMEDIO DIARIO PM2.5 (últimos 7 días) - Localidad: {row.localidad}, País: {row.pais}, "
            f"Contaminante: {row.contaminante}, Unidad: {row.unidad_de_medida}, "
            f"Promedio: {row.promedio_diario_pm25}"
        )
    else:
        logger.warning(f"1. No se encontraron datos de PM2.5 para {locality}, {country}")


def get_days_exceeding_pm25_who_limit(country: str, locality: str, db_client: DBHandler) -> None:
    """2. Días donde se superó el valor de 25 µg/m³ de PM2.5 (según la OMS)."""
    query = """
        WITH dias_superados AS (
            SELECT DISTINCT
                   dl.locality_name AS localidad,
                   dl.country_name  AS pais,
                   faqm.parameter   AS contaminante,
                   faqm.unit        AS unidad_de_medida,
                   CAST(faqm.measurement_timestamp AS DATE) AS fecha
            FROM fact_air_quality_measurement faqm
            INNER JOIN dim_station dm ON faqm.station_sk = dm.station_sk
            INNER JOIN dim_locality dl ON dl.locality_sk = dm.locality_sk
            WHERE faqm.parameter = 'pm25'
              AND faqm.value > 25
              AND LOWER(dl.country_name) = LOWER(:country)
              AND LOWER(dl.locality_name) = LOWER(:locality)
        )
        SELECT localidad,
               pais,
               contaminante,
               unidad_de_medida,
               STRING_AGG(CAST(fecha AS VARCHAR), ', ' ORDER BY fecha) AS dias_superados,
               COUNT(*) AS cantidad_dias
        FROM dias_superados
        GROUP BY localidad, pais, contaminante, unidad_de_medida;
    """
    
    result = db_client.conn.execute(text(query), {"country": country, "locality": locality})
    row = result.fetchone()
    
    if row:
        logger.info(
            f"2. DÍAS QUE SUPERARON LÍMITE OMS PM2.5 (25 µg/m³) - Localidad: {row.localidad}, País: {row.pais}, "
            f"Contaminante: {row.contaminante}, Unidad: {row.unidad_de_medida}, "
            f"Cantidad de días: {row.cantidad_dias}, Fechas: {row.dias_superados}"
        )
    else:
        logger.warning(f"2. No se encontraron días con PM2.5 > 25 µg/m³ para {locality}, {country}")


def get_station_highest_no2_last_3_days(country: str, locality: str, db_client: DBHandler) -> None:
    """3. Estación con mayor promedio de NO2 durante los últimos 3 días."""
    query = """
        WITH no2_avg_ranked_by_station AS (
            SELECT
                dl.locality_name AS localidad,
                dl.country_name  AS pais,
                dm.station_name  AS estacion,
                faqm.parameter   AS contaminante,
                faqm.unit        AS unidad_de_medida,
                ROUND(AVG(faqm.value), 2)  AS promedio_no2_3d,
                RANK() OVER (
                    PARTITION BY dl.locality_name, dl.country_name
                    ORDER BY AVG(faqm.value) DESC
                ) AS rnk
            FROM fact_air_quality_measurement faqm
            JOIN dim_station dm  ON faqm.station_sk  = dm.station_sk
            JOIN dim_locality dl ON dl.locality_sk   = dm.locality_sk
            WHERE faqm.parameter = 'no2'
              AND faqm.measurement_timestamp >= NOW() - INTERVAL '3 days'
              AND LOWER(dl.country_name) = LOWER(:country)
              AND LOWER(dl.locality_name) = LOWER(:locality)
            GROUP BY localidad, pais, estacion, contaminante, unidad_de_medida
        )
        SELECT localidad, pais, estacion, contaminante, unidad_de_medida, promedio_no2_3d
        FROM no2_avg_ranked_by_station
        WHERE rnk = 1;
    """
    
    result = db_client.conn.execute(text(query), {"country": country, "locality": locality})
    row = result.fetchone()
    
    if row:
        logger.info(
            f"3. ESTACIÓN CON MAYOR PROMEDIO NO2 (últimos 3 días) - Localidad: {row.localidad}, País: {row.pais}, "
            f"Estación: {row.estacion}, Contaminante: {row.contaminante}, "
            f"Unidad: {row.unidad_de_medida}, Promedio: {row.promedio_no2_3d}"
        )
    else:
        logger.warning(f"3. No se encontraron datos de NO2 para {locality}, {country}")


def generate_air_quality_insights(country: str, locality: str, db_client: DBHandler) -> None:
    logger.info(f"=== INSIGHTS DE CALIDAD DEL AIRE: {locality}, {country} ===")
    
    # 1. Promedio diario de PM2.5 para los últimos 7 días
    get_pm25_daily_average_last_7_days(country, locality, db_client)
    
    # 2. Días donde se superó el valor de 25 µg/m³ de PM2.5 (según la OMS)
    get_days_exceeding_pm25_who_limit(country, locality, db_client)
    
    # 3. Estación con mayor promedio de NO2 durante los últimos 3 días
    get_station_highest_no2_last_3_days(country, locality, db_client)
    
    logger.info("=== FIN DE INSIGHTS ===")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate air quality insights for technical test")
    parser.add_argument("--country", help="Country name", default="Chile")
    parser.add_argument("--locality", help="Locality name", default="Santiago")
    args = parser.parse_args()
    
    db_client = DBHandler(config)
    try:
        generate_air_quality_insights(args.country, args.locality, db_client)
    finally:
        db_client.close()
