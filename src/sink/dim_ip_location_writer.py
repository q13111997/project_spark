from psycopg2.extras import execute_values
from pyspark import SparkFiles
import psycopg2
import IP2Location

# cache per executor
_ip2loc_instance = None

def get_ip2loc(bin_file):
    global _ip2loc_instance
    if _ip2loc_instance is None:
        path = SparkFiles.get(bin_file)
        _ip2loc_instance = IP2Location.IP2Location(path)
    return _ip2loc_instance


def dim_ip_location_writer(batch_df, postgres_conf, bin_file):
    sql = """
        INSERT INTO dim_ip_location (
            ip,
            country_name_short,
            country_name_long,
            region_name,
            city_name
        )
        VALUES %s
        ON CONFLICT (ip) DO NOTHING
    """

    def write_partition(rows):
        ip2loc = get_ip2loc(bin_file)

        conn = psycopg2.connect(
            host=postgres_conf["host"],
            database=postgres_conf["database"],
            user=postgres_conf["user"],
            password=postgres_conf["password"]
        )
        cursor = conn.cursor()

        batch = []
        batch_size = 5000

        for row in rows:
            try:
                rec = ip2loc.get_all(row.ip)

                batch.append((
                    row.ip,
                    rec.country_short,
                    rec.country_long,
                    rec.region,
                    rec.city
                ))
            except:
                batch.append((row.ip, None, None, None, None))

            if len(batch) >= batch_size:
                execute_values(cursor, sql, batch)
                conn.commit()
                batch.clear()

        if batch:
            execute_values(cursor, sql, batch)
            conn.commit()

        cursor.close()
        conn.close()

    batch_df.foreachPartition(write_partition)