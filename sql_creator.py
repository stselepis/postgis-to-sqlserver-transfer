from sqlalchemy import create_engine
import pyodbc, os
import zipfile
from shapely.geometry.base import BaseGeometry
from shapely import wkt
from urllib.parse import quote_plus
from dotenv import load_dotenv
import geopandas as gpd
import pandas as pd
from pandas.util import hash_pandas_object
import numpy as np
from shapely.geometry import Polygon, MultiPolygon
from shapely.geometry.polygon import orient
from shapely.wkt import dumps as wkt_dumps
from datetime import datetime
import math
import warnings
warnings.filterwarnings('ignore')
# Load environment variables from .env
load_dotenv()

# Connections
# --- MT
pg_config = {
    'username': 'analyst_ddl',
    'password': quote_plus(os.getenv('PG_PASSWORD')),
    'host': 'maritime-assets-db1-dev-geospatial.cluster-cinsmmsxwkgg.eu-west-1.rds.amazonaws.com',
    'port': 5432,
    'database': 'maritime_assets'
}
dbdev_conn_str = ("DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=192.168.100.130,1437;"
    "DATABASE=ais;"
    f"UID=kp_daan;PWD={os.getenv('SQL_SERVER_PASSWORD')}"
                 )
dbprim03_conn_str = ("DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=192.168.100.151,1433;"
    "DATABASE=ais;"
    f"UID=kp_daan;PWD={os.getenv('SQL_SERVER_PASSWORD')}"
                    )
dbprim_conn_str = ("DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=vpn-mt-prim-db.ad.shared.galil.io,1433;"
    "DATABASE=ais;"
    f"UID=kp_daan;PWD={os.getenv('SQL_SERVER_PASSWORD')}"
                 )
# --- PG
pg_url = (
    f"postgresql://{pg_config['username']}:{pg_config['password']}@"
    f"{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"
)

# Create functions
# Fix functions
# Ensure correct ring orientation for polygons and multipolygons
def correct_orientation(geom):
    if isinstance(geom, Polygon):
        return orient(geom)
    elif isinstance(geom, MultiPolygon):
        return MultiPolygon([orient(p) for p in geom.geoms])
    else:
        return geom
# Fill ids from next id (for inserts)
def fill_column_with_increment(df, column_name, start_number):
    df = df.copy()  # Avoid modifying original
    mask = df[column_name].isnull()
    num_nulls = mask.sum()
    df.loc[mask, column_name] = range(start_number, start_number + num_nulls)
    return df
# Fix related port and anch fields
def fix_anch_relations(gdf):
    all_related_zone_ids = list(set(list(gdf[['related_zone_port_id']].dropna()['related_zone_port_id']) + list(gdf[['related_zone_anch_id']].dropna()['related_zone_anch_id'])))
    zone_ids_in_ports = list(gdf['zone_id'])
    related_zone_ids_not_existing = list(set(all_related_zone_ids)-set(zone_ids_in_ports))
    if len(related_zone_ids_not_existing)>0:
        raise ValueError(f"Zone IDs: [{', '.join(str(int(i)) for i in related_zone_ids_not_existing)}] not found in lookup table. Add them in input port list.")
    else:
        # set index to zone_id for quick lookup
        zone_port_map = gdf.set_index('zone_id')['port_id']
        # map related_zone_anch_id to port_id → this gives related_anch_id
        gdf['related_anch_id'] = gdf['related_zone_anch_id'].map(zone_port_map)
        # map related_zone_port_id to port_id → this gives related_port_id
        gdf['related_port_id'] = gdf['related_zone_port_id'].map(zone_port_map)
        return gdf
# fix port_id of terminals
def fix_terminal_relations(gdf):
    all_related_zone_ids = list(set(list(gdf[['port_zone_id']].dropna()['port_zone_id'])))
    zone_ids_in_ports = list(gdf_PG_ports['zone_id'])
    related_zone_ids_not_existing = list(set(all_related_zone_ids)-set(zone_ids_in_ports))
    if len(related_zone_ids_not_existing)>0:
        raise ValueError(f"Zone IDs: [{', '.join(str(int(i)) for i in related_zone_ids_not_existing)}] not found in lookup table.")
    else:
        # set index to zone_id for quick lookup
        zone_port_map = gdf_PG_ports.set_index('zone_id')['port_id']
        # map port_id of terminals
        gdf['port_id'] = gdf['port_zone_id'].map(zone_port_map)
        return gdf
# fix port_id and terminal_id of berths
def fix_berth_relations(gdf):
    all_related_zone_ids = list(set(list(gdf[['port_zone_id']].dropna()['port_zone_id'])))
    zone_ids_in_ports = list(gdf_PG_ports['zone_id'])
    related_zone_ids_not_existing = list(set(all_related_zone_ids)-set(zone_ids_in_ports))
    if len(related_zone_ids_not_existing)>0:
        raise ValueError(f"Zone IDs: [{', '.join(str(int(i)) for i in related_zone_ids_not_existing)}] not found in PG ports table.")
    else:
        # set index to zone_id for quick lookup
        zone_port_map = gdf_PG_ports.set_index('zone_id')['port_id']
        # map port_id of terminals
        gdf['port_id'] = gdf['port_zone_id'].map(zone_port_map)

    all_related_zone_ids = list(set(list(gdf[['terminal_zone_id']].dropna()['terminal_zone_id'])))
    zone_ids_in_terminals = list(df_PG_terminals['zone_id'])
    related_zone_ids_not_existing = list(set(all_related_zone_ids)-set(zone_ids_in_terminals))
    if len(related_zone_ids_not_existing)>0:
        raise ValueError(f"Zone IDs: [{', '.join(str(int(i)) for i in related_zone_ids_not_existing)}] not found in PG terminal table.")
    else:
        # set index to zone_id for quick lookup
        zone_terminal_map = (
            df_PG_terminals[['zone_id', 'terminal_id']]
            .drop_duplicates(subset='zone_id', keep='first')
            .set_index('zone_id')['terminal_id']
        )
        gdf['terminal_id'] = gdf['terminal_zone_id'].map(zone_terminal_map)
    return gdf
# fix port_id of altnames
def fix_altnames_relations(gdf):
    all_related_zone_ids = list(set(list(gdf[['port_zone_id']].dropna()['port_zone_id'])))
    zone_ids_in_ports = list(gdf_PG_ports['zone_id'])
    related_zone_ids_not_existing = list(set(all_related_zone_ids)-set(zone_ids_in_ports))
    if len(related_zone_ids_not_existing)>0:
        raise ValueError(f"Zone IDs: [{', '.join(str(int(i)) for i in related_zone_ids_not_existing)}] not found in lookup table.")
    else:
        # set index to zone_id for quick lookup
        zone_port_map = gdf_PG_ports.set_index('zone_id')['port_id']
        # map port_id of terminals
        gdf['port_id'] = gdf['port_zone_id'].map(zone_port_map)
        return gdf

def check_sandbox_mt_same():
    # sandbox mt ports
    pg_engine = create_engine(pg_url)
    sql_conn = pyodbc.connect(selected_conn)
    q = f"""
    select pb."PORT_ID" as port_id, pb.geometry 
    from sandbox."MT_Ports" pb 
    where pb."MOVING_SHIP_ID" is null
    """
    gdf_sandbox_p = gpd.read_postgis(
        sql=q,
        con=pg_engine,
        geom_col='geometry'
    )
    gdf_sandbox_p['port_id'] = gdf_sandbox_p['port_id'].astype('int64')
    gdf_sandbox_p = gdf_sandbox_p.reset_index(drop=True)
    #sandbox mt berths
    q = f"""
    select mb."BERTH_ID" as berth_id, mb.geometry 
    from sandbox."MT_Berths" mb 
    """
    gdf_sandbox_b = gpd.read_postgis(
        sql=q,
        con=pg_engine,
        geom_col='geometry'
    )
    gdf_sandbox_b['berth_id'] = gdf_sandbox_b['berth_id'].astype('int64')
    gdf_sandbox_b = gdf_sandbox_b.reset_index(drop=True)
    #mt ports
    query = f"""
    select port_id, POLYGON.STAsText() as geometry
    from ais.dbo.ports 
    where confirmed=1
    and MOVING_SHIP_ID is null
    """
    gdf_mt_p = pd.read_sql_query(query, sql_conn)
    gdf_mt_p["geometry"] = gdf_mt_p["geometry"].apply(lambda s: wkt.loads(s) if pd.notna(s) else None)
    gdf_mt_p = gpd.GeoDataFrame(gdf_mt_p, geometry="geometry", crs="EPSG:4326")
    gdf_mt_p['port_id'] = gdf_mt_p['port_id'].astype('Int64')
    gdf_mt_p = gdf_mt_p.reset_index(drop=True)
    #mt berths
    query = f"""
    select berth_id, POLYGON.STAsText() as geometry
    from ais.dbo.port_berths
    """
    gdf_mt_b = pd.read_sql_query(query, sql_conn)
    gdf_mt_b["geometry"] = gdf_mt_b["geometry"].apply(lambda s: wkt.loads(s) if pd.notna(s) else None)
    gdf_mt_b = gpd.GeoDataFrame(gdf_mt_b, geometry="geometry", crs="EPSG:4326")
    gdf_mt_b['berth_id'] = gdf_mt_b['berth_id'].astype('Int64')
    gdf_mt_b = gdf_mt_b.reset_index(drop=True)
    #df_of_differences
    gdf_p_merged = gdf_sandbox_p.merge(gdf_mt_p, on='port_id', how='outer')
    gdf_p_diff = gdf_p_merged[gdf_p_merged['geometry_x']!=gdf_p_merged['geometry_y']]
    gdf_b_merged = gdf_sandbox_b.merge(gdf_mt_b, on='berth_id', how='outer')
    gdf_b_diff = gdf_b_merged[gdf_b_merged['geometry_x']!=gdf_b_merged['geometry_y']]
    #ids
    diff_ids = {'port_id_diff':list(gdf_p_diff['port_id']), 'berth_id_diff':list(gdf_b_diff['berth_id'])}
    print(instance)
    print('Port diffs', len(gdf_p_diff), 'Berth diffs', len(gdf_b_diff),)
    pg_engine.dispose()
    sql_conn.close()
    return [gdf_p_diff, gdf_b_diff]


def mt_ports_intersecting_not_included(port_zone_id_list):
    if isinstance(port_zone_id_list, list):
        id_str = ', '.join(str(i) for i in port_zone_id_list)
    else:
        raise ValueError(f"No zone_id list")
    q = f"""
    with mt_ports as (
    	select "PORT_ID" as port_id, "PORT_NAME" as port_name, geometry
    	from sandbox."MT_Ports"
    	where "CONFIRMED" = True
    	and "MOVING_SHIP_ID" is null
    ),
    matched_ports as (
    	select mt_id as port_id, zone_id as matched_zone_id
    	from sandbox.mview_mt_matching
    	where mt_table = 'ports'
    ),
    mt_ports_matched as (
    	select p.*, m.matched_zone_id
    	from mt_ports p
    	left join matched_ports m on m.port_id = p.port_id 
    ),
    port_zone_list as (
    	select zone_id as overlap_zone_id, polygon_geom
    	from geospatial.zones z 
    	where zone_id in ({id_str})
    )
    select a.port_id, a.port_name, a.matched_zone_id, b.overlap_zone_id 
    from mt_ports_matched a
    JOIN port_zone_list b ON ST_Intersects(a.geometry, b.polygon_geom) AND NOT ST_Touches(a.geometry, b.polygon_geom)
    where a.matched_zone_id not in ({id_str}) or a.matched_zone_id is null
    """
    pg_engine = create_engine(pg_url)
    df = pd.read_sql(
        sql = q,
        con = pg_engine)
    pg_engine.dispose()
    df['matched_zone_id'] = df['matched_zone_id'].astype('Int64')
    return df.reset_index(drop=True)


# PG reads
# PG ports
def read_PG_ports(port_list = None):
    if isinstance(port_list, list):
        id_str = ', '.join(str(i) for i in port_list)
        sql_where = f'mp.zone_id in ({id_str})'
    else:
        sql_where = '1=1'
    q = f"""
    SELECT
        mp.zone_id,
        mt_id as port_id,
        mp.name as port_name,
        mp.name as standard_name,
        z.zone_name,
        (coalesce(alternative_names, '{{}}') || coalesce(alternative_unlocodes, '{{}}'))[1] as altname1,
      	(coalesce(alternative_names, '{{}}') || coalesce(alternative_unlocodes, '{{}}'))[2] as altname2,
      	(coalesce(alternative_names, '{{}}') || coalesce(alternative_unlocodes, '{{}}'))[3] as altname3,
      	(coalesce(alternative_names, '{{}}') || coalesce(alternative_unlocodes, '{{}}'))[4] as altname4,
        mp.zone_type,
        mp.unlocode,
        mp.country_code,
        mp.timezone_name as timezone,
        mp.dst_id as dst,
        CASE 
            WHEN mp.zone_id IN (
                SELECT zone_id 
                FROM geospatial.zones 
                WHERE parent_zone_id IS NULL
            ) THEN 1
            ELSE 0
        END AS enable_calls,
        mp.confirmed,
        round(("CENTERY" - greatest(abs("CENTERY" - st_ymin(mp.polygon_geom)), abs("CENTERY" - st_ymax(mp.polygon_geom))))::numeric, 5) as sw_y,
        round(("CENTERX" - greatest(abs("CENTERX" - st_xmin(mp.polygon_geom)), abs("CENTERX" - st_xmax(mp.polygon_geom))))::numeric, 5) as sw_x,
        round(("CENTERY" + greatest(abs("CENTERY" - st_ymin(mp.polygon_geom)), abs("CENTERY" - st_ymax(mp.polygon_geom))))::numeric, 5) as ne_y,
        round(("CENTERX" + greatest(abs("CENTERX" - st_xmin(mp.polygon_geom)), abs("CENTERX" - st_xmax(mp.polygon_geom))))::numeric, 5) as ne_x,
        mp.alternative_names,
        mp.alternative_unlocodes,
        ap.primary_related_zone_anch_id AS related_zone_anch_id,
        ap.primary_related_zone_port_id AS related_zone_port_id,
        mp.polygon_geom as polygon
    FROM sandbox.mview_master_ports mp
    left join sandbox.v_port_anchorage_primary ap on ap.zone_id = mp.zone_id
    left join geospatial.zones z on z.zone_id = mp.zone_id
    where {sql_where}
    """
    pg_engine = create_engine(pg_url)
    gdf = gpd.read_postgis(
        sql=q,
        con=pg_engine,
        geom_col='polygon'
    )
    pg_engine.dispose()
    # geometry orientation
    gdf['polygon'] = gdf['polygon'].apply(lambda geom: correct_orientation(geom) if geom and geom.is_valid else geom)
    # new mt port_ids
    gdf = fill_column_with_increment(gdf, 'port_id', next_ids['dbo.ports'])
    # relations
    gdf = fix_anch_relations(gdf)
    #float rounding
    gdf['sw_x'] = gdf['sw_x'].round(6)
    gdf['sw_y'] = gdf['sw_y'].round(6)
    gdf['ne_x'] = gdf['ne_x'].round(6)
    gdf['ne_y'] = gdf['ne_y'].round(6)
    # dtypes
    gdf['port_id'] = gdf['port_id'].astype('int64')
    gdf['related_anch_id'] = gdf['related_anch_id'].astype('Int64')
    gdf['related_port_id'] = gdf['related_port_id'].astype('Int64')
    # port types
    PORT_TYPE_MAPPING = {
            'Port': 'P',
            'Marina': 'M',
            'Anchorage': 'A',
            'Offshore Terminal': 'T',
            'Canal': 'C'
        }    
    gdf['port_type'] = gdf['zone_type'].map(PORT_TYPE_MAPPING)
    if gdf['port_type'].isnull().any():
        raise ValueError("Port type has null")
    # name handling
    def fix_name(row):
        name = row['port_name']
        port_type = row['port_type']
        if not isinstance(name, str) or name is None:  # skip non-strings/NaN/None
            return name
        # apply type-specific replacements #bookmark
        if port_type == 'P':
            name = name.replace(" PORT", "")  # drop ' PORT'
        elif port_type == 'A':
            name = name.replace("ANCHORAGE", "ANCH")  # shorten anchorage
        elif port_type == 'M':
            name = name.replace(" MARINA", "")  # shorten anchorage
        elif port_type == 'T':
            name = name.replace(" OT", "")  # drop ' OT'
        return name        
    for col in ['port_name', 'altname1', 'altname2', 'altname3', 'altname4']:
        gdf[col] = gdf.apply(lambda row: fix_name({'port_name': row[col], 'port_type': row['port_type']}), axis=1)
    gdf['name_trimmed'] = gdf['port_name'].apply(len)>20
    gdf['port_name'] = gdf['port_name'].str[:20]
    gdf['altname1'] = gdf['altname1'].str[:20]
    gdf['altname2'] = gdf['altname2'].str[:20]
    gdf['altname3'] = gdf['altname3'].str[:20]
    gdf['altname4'] = gdf['altname4'].str[:20]
    print(len(gdf), "PG ports read")
    return gdf.reset_index(drop=True)
# PG terminals
def read_PG_terminals(port_list = None):
    if isinstance(port_list, list):
        id_str = ', '.join(str(i) for i in port_list)
        sql_where = f'port_id in ({id_str})'
    else:
        sql_where = '1=1'
    q = f"""
    SELECT 
        mtt.zone_id,
        port_id as port_zone_id,
        terminal_id,
        name as terminal_name,
        name as standard_name,
        z.zone_name,
        unlocode,
        terminal_code,
        terminal_facility_name,
        terminal_company_name,
        lat,
        lon,
        smdg_listing_date,
        smdg_unlisting_date,
        smdg_updated_date,
        terminal_website,
        terminal_address,
        remarks,
        zone_type
    FROM sandbox.mview_master_terminals_mt mtt
    left join geospatial.zones z on z.zone_id = mtt.zone_id
    where {sql_where}
    """
    pg_engine = create_engine(pg_url)
    df = pd.read_sql(
        sql = q,
        con = pg_engine)
    pg_engine.dispose()
    # fix relations
    df = fix_terminal_relations(df)
    # fill new mt terminal_ids
    df = df.sort_values('port_id')
    df = fill_column_with_increment(df, 'terminal_id', next_ids['dbo.port_terminals'])
    # fix string lengths
    df['name_trimmed'] = df['terminal_name'].apply(len)>50
    df['terminal_name'] = df['terminal_name'].str[:50]
    # dtypes
    df['terminal_id'] = df['terminal_id'].astype('int64')
    df['port_id'] = df['port_id'].astype('int64')
    print(len(df), "PG terminals read")
    return df.reset_index(drop=True)
#PG berths
def read_PG_berths(port_list = None):
    if isinstance(port_list, list):
        id_str = ', '.join(str(i) for i in port_list)
        sql_where = f'port_id in ({id_str})'
    else:
        sql_where = '1=1'
    q = f"""
    SELECT 
        mb.zone_id,
        mt_id as berth_id,
        terminal_id as terminal_zone_id,
        port_id as port_zone_id,
        name as berth_name,
        name as standard_name,
        z.zone_name,
        zone_type,
        "MAX_LENGTH" as max_length,
        "MAX_DRAUGHT" as max_draught,
        "MAX_BREADTH" as max_breadth,
        "LIFTING_GEAR" as lifting_gear,
        "BULK_CAPACITY" as bulk_capacity,
        "DESCRIPTION" as description,
        "MAX_TIDAL_DRAUGHT" as max_tidal_draught,
         mb.polygon_geom as polygon
    FROM sandbox.mview_master_berths mb
    left join geospatial.zones z on z.zone_id = mb.zone_id    
    where {sql_where}
    """
    pg_engine = create_engine(pg_url)
    gdf = gpd.read_postgis(
        sql=q,
        con=pg_engine,
        geom_col='polygon'
    )
    pg_engine.dispose()
    # Check for ring orientation issues SQL server requires : outer ring - anti clockwise & inner ring-clockwise
    gdf['polygon'] = gdf['polygon'].apply(lambda geom: correct_orientation(geom) if geom and geom.is_valid else geom)
    # relations
    gdf = fix_berth_relations(gdf)
    # new mt berth_ids
    gdf = gdf.sort_values(['port_id', 'terminal_id'])
    gdf = fill_column_with_increment(gdf, 'berth_id', next_ids['dbo.port_berths'])
    # string lengths
    gdf['name_trimmed'] = gdf['berth_name'].apply(len)>30
    gdf['berth_name'] = gdf['berth_name'].str[:30]
    # dtypes
    gdf['berth_id'] = gdf['berth_id'].astype('Int64')
    gdf['terminal_id'] = gdf['terminal_id'].astype('Int64')
    gdf['port_id'] = gdf['port_id'].astype('Int64')
    print(len(gdf), "PG berths read")
    return gdf.reset_index(drop=True)
# PG altnames
def read_PG_altnames(port_list = None):
    if isinstance(port_list, list):
        id_str = ', '.join(str(i) for i in port_list)
        sql_where = f'zone_id in ({id_str})'
    else:
        sql_where = '1=1'
    pg_Port_alias_query = f"""
    select 
        zone_id as port_zone_id,
        unnest(coalesce(alternative_names, '{{}}') || coalesce(alternative_unlocodes, '{{}}')) as alias_name
    from sandbox.mview_master_ports where {sql_where}
    """
    pg_engine = create_engine(pg_url)
    df = pd.read_sql(
        sql = pg_Port_alias_query,
        con = pg_engine)
    pg_engine.dispose()
    # fix relations
    df = fix_altnames_relations(df)
    # fix string lengths
    df['alias_name'] = df['alias_name'].str[:20]
    # dtypes
    df['port_id'] = df['port_id'].astype('int64')
    print(len(df), "PG alt-names read")
    return df.reset_index(drop=True)
# PG quality errors
def read_errors():
    all_zone_ids = list(gdf_PG_ports['zone_id']) + list(df_PG_terminals['zone_id']) + list(gdf_PG_berths['zone_id'])
    if isinstance(all_zone_ids, list):
        id_str = ', '.join(str(i) for i in all_zone_ids)
    else:
        raise ValueError(f"No zone_id list")
    pg_quality_errors = f"""
    select *
    from sandbox.mview_geoquality_checks
    where zone_id in ({id_str})
    or zone_id_b in ({id_str})
    """
    pg_engine = create_engine(pg_url)
    df = pd.read_sql(
        sql = pg_quality_errors,
        con = pg_engine)
    pg_engine.dispose()
    print(len(df), "PG quality errors read")
    return df.reset_index(drop=True)
# Name check
def name_check():
    dfp = gdf_PG_ports[['zone_id', 'zone_name', 'zone_type', 'standard_name', 'port_type', 'port_name', 'name_trimmed']].rename(columns={'port_type':'mt_type', 'port_name':'name'})
    dft = df_PG_terminals[['zone_id', 'zone_name','zone_type', 'standard_name', 'terminal_name', 'name_trimmed']].rename(columns={'terminal_name':'name'})
    dft['mt_type'] = 'terminal'
    dfb = gdf_PG_berths[['zone_id', 'zone_name', 'zone_type', 'standard_name', 'berth_name', 'name_trimmed']].rename(columns={'berth_name':'name'})
    dfb['mt_type'] = 'berth'
    df_name_check = pd.concat([dfp, dft, dfb])
    print(sum(df_name_check['name_trimmed']), 'names trimmed')
    return df_name_check.reset_index(drop=True)

# MT reads
# next id
def get_next_identity():
    sql_conn = pyodbc.connect(selected_conn)
    cursor = sql_conn.cursor()
    next_ids = {}
    query = "SELECT IDENT_CURRENT('dbo.ports') + IDENT_INCR('dbo.ports') AS next_identity;"
    cursor.execute(query)
    next_ids['dbo.ports'] = int(cursor.fetchone()[0])
    query = "SELECT IDENT_CURRENT('dbo.port_terminals') + IDENT_INCR('dbo.port_terminals') AS next_identity;"
    cursor.execute(query)
    next_ids['dbo.port_terminals'] = int(cursor.fetchone()[0])
    query = "SELECT IDENT_CURRENT('dbo.port_berths') + IDENT_INCR('dbo.port_berths') AS next_identity;"
    cursor.execute(query)
    next_ids['dbo.port_berths'] = int(cursor.fetchone()[0])
    cursor.close()
    sql_conn.close()
    return next_ids
# MT ports
def read_mt_ports():
    mt_port_ids = list(gdf_PG_ports[['port_id']].dropna()['port_id'].astype('Int64'))
    if len(mt_port_ids) == 0:
        id_str ="''"
    else:
        id_str = ', '.join(str(i) for i in mt_port_ids)
    query = f"""
    select 
        p.port_id, 
        p.port_name, 
        p.port_type,
        p.country_code,
        p.timezone, 
        p.dst,
        p.unlocode,
        p.related_anch_id, 
        p.related_port_id, 
        p.sw_x, p.sw_y, p.ne_x, p.ne_y,
        p.altname1, p.altname2, p.altname3, p.altname4,
        CONVERT(int, p.confirmed) AS confirmed,
        CONVERT(int, p.enable_calls) AS enable_calls,
        p.polygon.STAsText() as polygon
    from dbo.ports p
    where port_id in ({id_str})
    """
    sql_conn = pyodbc.connect(selected_conn)
    df = pd.read_sql_query(query, sql_conn)
    sql_conn.close()
    # proper geometry type
    df["polygon"] = df["polygon"].apply(lambda s: wkt.loads(s) if pd.notna(s) else None)
    gdf = gpd.GeoDataFrame(df, geometry="polygon", crs="EPSG:4326")
    gdf['related_anch_id'] = gdf['related_anch_id'].astype('Int64')
    gdf['related_port_id'] = gdf['related_port_id'].astype('Int64')
    print(len(gdf), "MT ports read")
    return gdf
#MT altnames
def read_mt_r_port_altnames():
    mt_port_ids = list(gdf_PG_ports[['port_id']].dropna()['port_id'].astype('Int64'))
    if len(mt_port_ids) == 0:
        id_str ="''"
    else:
        id_str = ', '.join(str(i) for i in mt_port_ids)
    query = f"""
    select am.*
    from dbo.R_PORT_ALTNAMES am
    join ais.dbo.PORTS p on p.port_id = am.port_id
    where am.port_id in ({id_str})
    """
    sql_conn = pyodbc.connect(selected_conn)
    df = pd.read_sql_query(query, sql_conn)
    sql_conn.close()
    print(len(df), "MT r_altnames read")
    return df
#MT terminals
def read_mt_terminals():
    # comma seperated port_ids 
    mt_port_ids = list(gdf_PG_ports[['port_id']].dropna()['port_id'].astype('Int64'))
    if len(mt_port_ids) == 0:
        port_id_str ="''"
    else:
        port_id_str = ', '.join(str(i) for i in mt_port_ids)

    #comma separated terminal_ids
    mt_terminal_ids = list(df_PG_terminals[['terminal_id']].dropna()['terminal_id'].astype('Int64'))
    if len(mt_terminal_ids) == 0:
        terminal_id_str ="''"
    else:
        terminal_id_str = ', '.join(str(i) for i in mt_terminal_ids)
    query = f"""
    select 
        terminal_id,
        terminal_name,
        port_id
    from dbo.port_terminals 
    where port_id in ({port_id_str})
    or terminal_id in (select terminal_id from dbo.port_berths where port_id in ({port_id_str}))
    or terminal_id in ({terminal_id_str})
    """
    sql_conn = pyodbc.connect(selected_conn)
    df = pd.read_sql_query(query, sql_conn)
    sql_conn.close()
    df['port_id'] = df['port_id'].astype('Int64')
    print(len(df), "MT terminals read")
    return df
#MT berths
def read_mt_berths():
    # comma seperated port_ids 
    mt_port_ids = list(gdf_PG_ports[['port_id']].dropna()['port_id'].astype('Int64'))
    if len(mt_port_ids) == 0:
        port_id_str ="''"
    else:
        port_id_str = ', '.join(str(i) for i in mt_port_ids)
    #comma separated berth_ids
    mt_berth_ids = list(gdf_PG_berths[['berth_id']].dropna()['berth_id'].astype('Int64'))
    if len(mt_berth_ids) == 0:
        berth_id_str ="''"
    else:
        berth_id_str = ', '.join(str(i) for i in mt_berth_ids)
    query = f"""
    select 
        berth_id,
        berth_name,
        port_id,
        terminal_id,
        max_length,
        max_draught,
        max_breadth,
        lifting_gear,
        bulk_capacity,
        description,
        max_tidal_draught,
        POLYGON.STAsText() as polygon
    from dbo.port_berths 
    where port_id in ({port_id_str})
        or berth_id in ({berth_id_str})
    """
    sql_conn = pyodbc.connect(selected_conn)
    df = pd.read_sql_query(query, sql_conn)
    sql_conn.close()
    df["polygon"] = df["polygon"].apply(lambda s: wkt.loads(s) if pd.notna(s) else None)
    gdf = gpd.GeoDataFrame(df, geometry="polygon", crs="EPSG:4326")
    gdf['port_id'] = gdf['port_id'].astype('Int64')
    gdf['terminal_id'] = gdf['terminal_id'].astype('Int64')
    print(len(gdf), "MT berths read")
    return gdf

# Function to remove from update if there is nothing different
def filter_changed_rows(df1, df2, decimals=3, geom_precision=6):
    """
    Return rows from df1 that are NOT present in df2 (full-row compare),
    allowing tiny float differences by formatting/quantizing to `decimals`.
    Uses row hashes for a fast anti-join and treats NaN == NaN.
    """

    # 1) Align on shared columns (preserve df1 order)
    shared_cols = [c for c in df1.columns if c in df2.columns]
    if not shared_cols:
        # nothing to compare; everything in df1 is "changed"
        return df1.copy()

    df1c = df1[shared_cols].copy().reset_index(drop=True)
    df2c = df2[shared_cols].copy().reset_index(drop=True)

    # 2) Normalize types for comparison

    # Identify numeric columns across both (union)
    def is_numeric(s):
        return pd.api.types.is_numeric_dtype(s)

    num_cols = sorted(
        set([c for c in shared_cols if is_numeric(df1c[c])]) |
        set([c for c in shared_cols if is_numeric(df2c[c])])
    )

    # Identify geometry-bearing columns (any cell a shapely geometry)
    # (vectorized-ish check to avoid scanning all rows twice)
    def col_has_geom(s1, s2):
        # quick short-circuit using dtype if it's a GeoSeries
        if hasattr(s1, "geom_type") or hasattr(s2, "geom_type"):
            return True
        return s1.apply(lambda x: isinstance(x, BaseGeometry)).any() or \
               s2.apply(lambda x: isinstance(x, BaseGeometry)).any()

    geom_cols = [c for c in shared_cols if col_has_geom(df1c[c], df2c[c])]

    # 3) Apply normalization
    # 3a) Geometries -> WKT with precision for stable text compare
    for c in geom_cols:
        df1c[c] = df1c[c].apply(
            lambda g: wkt_dumps(g, rounding_precision=geom_precision, trim=True)
            if isinstance(g, BaseGeometry) else g
        )
        df2c[c] = df2c[c].apply(
            lambda g: wkt_dumps(g, rounding_precision=geom_precision, trim=True)
            if isinstance(g, BaseGeometry) else g
        )

    # 3b) Numeric tolerance: format as fixed-precision strings
    # Using string formatting avoids binary rounding quirks during hashing/merge
    if num_cols:
        fmt = f"{{0:.{decimals}f}}"
        for c in num_cols:
            # leave NaNs as NaN for now; we'll neutralize them next
            df1c[c] = pd.to_numeric(df1c[c], errors="coerce")
            df2c[c] = pd.to_numeric(df2c[c], errors="coerce")
            df1c[c] = df1c[c].apply(lambda v: fmt.format(v) if pd.notna(v) else np.nan)
            df2c[c] = df2c[c].apply(lambda v: fmt.format(v) if pd.notna(v) else np.nan)

    # 3c) Treat NaN == NaN by replacing with a shared sentinel value
    # Use a unique object unlikely to appear otherwise
    SENTINEL = "__<NA>__"
    df1n = df1c.fillna(SENTINEL)
    df2n = df2c.fillna(SENTINEL)

    # 4) Hash rows and anti-join by hash membership (fast + robust)
    h1 = hash_pandas_object(df1n, index=False)
    h2 = hash_pandas_object(df2n, index=False)
    mask = ~h1.isin(h2).to_numpy()

    # 5) Return original (un-normalized) rows from df1 where no match was found
    return df1.iloc[mask].copy()

# Functions: Split/prepare dataframes per handling type
# To insert
def create_df_to_insert(df_new, df_existing, index_col=None):
    # take only the columns from df_existing, same order
    df_new = df_new[df_existing.columns]

    # pick key columns for comparison
    if index_col is None:
        keys = list(df_existing.columns)
    elif isinstance(index_col, str):
        keys = [index_col]
    else:
        keys = list(index_col)

    # make sure keys exist
    for k in keys:
        if k not in df_new.columns:
            raise ValueError(f"Key '{k}' not found in df_new columns")
        if k not in df_existing.columns:
            raise ValueError(f"Key '{k}' not found in df_existing columns")

    # build comparable indexes (reset to avoid length mismatch)
    idx_new = df_new.reset_index(drop=True).set_index(keys).index
    idx_existing = df_existing.reset_index(drop=True).set_index(keys).index

    # keep only rows not already in existing
    mask = ~idx_new.isin(idx_existing)
    return df_new[mask].reset_index(drop=True)
# to update
def create_df_to_update(df_new, df_existing, index_col):
    if len(df_existing) == 0:
        return df_new.head(0)
    # ensure same column order as existing table
    df = df_new[df_existing.columns]
    # keep only rows whose index_col is in the existing df
    df = df[df[index_col].isin(df_existing[index_col])]
    # keep only rows where there is something different per index
    df = filter_changed_rows(df, df_existing)
    return df.reset_index(drop=True)
# to delete
def create_df_to_delete(df_new, df_existing, index_col=None):
    # use only existing's columns (same order)
    out_cols = list(df_existing.columns)
    missing_in_new = [c for c in out_cols if c not in df_new.columns]
    if missing_in_new:
        raise ValueError(f"df_new lacks required columns: {missing_in_new}")
    df_new = df_new[out_cols]
    # decide key columns
    if index_col is None:
        keys = out_cols
    elif isinstance(index_col, str):
        keys = [index_col]
    else:
        keys = list(index_col)
    # sanity: keys must exist on both sides
    for k in keys:
        if k not in out_cols:
            raise ValueError(f"key '{k}' not found in df_existing columns")
        if k not in df_new.columns:
            raise ValueError(f"key '{k}' not found in df_new columns")
    # compare on clean range indexes
    idx_existing = df_existing.reset_index(drop=True).set_index(keys).index
    idx_new = df_new.reset_index(drop=True).set_index(keys).index
    # keep rows that are in existing but not in new
    mask = ~idx_existing.isin(idx_new)
    return df_existing.loc[mask, out_cols].reset_index(drop=True)

#SQL builders
# helper to format SQL-safe values
def sql_format(val):
    try:
        if isinstance(val, BaseGeometry):
            return f"'{val.wkt}'"
        elif isinstance(val, str):
            return f"""'{val.replace("'", "''")}'"""
        elif pd.api.types.is_scalar(val) and pd.isnull(val):
            return 'null'
        else:
            return str(val)
    except Exception:
        return 'null'
# safe detection of geometry columns
def detect_geom_cols(df):
    if df.empty:
        return []
    return [
        col for col in df.columns
        if any(isinstance(v, BaseGeometry) for v in df[col].dropna())
    ]
# generate UPDATE statements as a string
def generate_update_sql(df, id_col, target_table):
    if len(df)==0:
        return ''
    assert id_col in df.columns, f"'{id_col}' not in dataframe columns"

    geom_cols = detect_geom_cols(df)
    non_geom_cols = [col for col in df.columns if col not in geom_cols]
    columns = non_geom_cols + geom_cols

    sql_lines = [f"-- Update statements for table: {target_table}\n"]

    for _, row in df.iterrows():
        id_val = sql_format(row[id_col])
        set_parts = [f"{col} = {sql_format(row[col])}" for col in columns if col != id_col]
        update_sql = f"UPDATE {target_table} SET {', '.join(set_parts)} WHERE {id_col} = {id_val};"
        sql_lines.append(update_sql)

    return '\n'.join(sql_lines)
# generate INSERT statements as a string
def generate_insert_sql(df, target_table, identity_insert=False):
    if len(df) == 0:
        return ''
    geom_cols = detect_geom_cols(df)
    non_geom_cols = [col for col in df.columns if col not in geom_cols]
    columns = non_geom_cols + geom_cols

    sql_lines = [f"-- Insert statements for table: {target_table}\n"]

    if identity_insert:
        sql_lines.append(f"SET IDENTITY_INSERT {target_table} ON;")

    col_names = ', '.join(columns)
    for _, row in df.iterrows():
        col_values = ', '.join(sql_format(row[col]) for col in columns)
        insert_sql = f"INSERT INTO {target_table} ({col_names}) VALUES ({col_values});"
        sql_lines.append(insert_sql)

    if identity_insert:
        sql_lines.append(f"SET IDENTITY_INSERT {target_table} OFF;")

    return '\n'.join(sql_lines)
# generate DELETE statements as a string
def generate_delete_sql(df, target_table, where_cols=None):
    """
    Build one DELETE per row.
    - If where_cols is None, use all df.columns.
    - NULLs use IS NULL; everything else goes through sql_format(...).
    """
    if len(df) == 0:
        return ''

    # decide which columns to match on
    if where_cols is None:
        where_cols = list(df.columns)
    else:
        missing = [c for c in where_cols if c not in df.columns]
        assert not missing, f"Columns not in dataframe: {missing}"

    sql_lines = [f"-- Delete statements for table: {target_table}\n"]

    for _, row in df.iterrows():
        predicates = []
        for col in where_cols:
            val = row[col]
            if pd.isna(val):
                predicates.append(f"{col} IS NULL")
            else:
                predicates.append(f"{col} = {sql_format(val)}")

        where_clause = " AND ".join(predicates)
        sql_lines.append(f"DELETE FROM {target_table} WHERE {where_clause};")

    return "\n".join(sql_lines)
# Generate description in sql
def generate_description_sql():
    sql_lines =[]
    sql_lines.append('/*')
    sql_lines.append(f"Target instance: {instance}")
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    sql_lines.append(f"Export Timestamp: {ts}")
    sql_lines.append('')
    sql_lines.append('Executed per table: SELECT IDENT_CURRENT(target_table) + IDENT_INCR(target_table)')
    sql_lines.append(f"Returned: {next_ids}")
    sql_lines.append('to be used as initial next id in calculations.')
    sql_lines.append('')
    sql_lines.append('Line counts per table and statement:')
    sql_lines.append(f"ports inserts: {len(gdf_ports_to_insert)}")
    sql_lines.append(f"ports updates: {len(gdf_ports_to_update)}")
    sql_lines.append(f"port_terminals inserts: {len(df_terminals_basic_to_insert)}")
    sql_lines.append(f"port_terminals updates: {len(df_terminals_basic_to_update)}")
    sql_lines.append(f"port_terminals deletes: {len(df_terminals_basic_to_delete)}")
    sql_lines.append(f"port_berths inserts: {len(gdf_berths_to_insert)}")
    sql_lines.append(f"port_berths updates: {len(gdf_berths_to_update)}")
    sql_lines.append(f"r_port_altnames inserts: {len(df_alt_names_to_insert)}")
    sql_lines.append(f"r_port_altnames delete: {len(df_alt_names_to_delete)}")
    sql_lines.append('*/')
    return "\n".join(sql_lines)
# combine multiple SQL parts with clear separation
def combine_sql_blocks(*blocks):
    return '\n\n'.join(block.strip() for block in blocks if block.strip())
# Save output
def save_sql(final_sql: str, instance: str, lines_per_part: int = 0):
    """
    save sql into timestamped folder:
    - if lines_per_part == 0 -> save full sql into single file
    - if lines_per_part >= 30 -> split into parts of given size, save separately
    - else -> raise error
    """
    # create base output folder
    base_dir = "output"
    os.makedirs(base_dir, exist_ok=True)
    # create timestamped subfolder
    folder_name = datetime.now().strftime("%Y%m%d_%H%M%S") + f"_{instance}_sql_output"
    folder_path = os.path.join(base_dir, folder_name)
    os.makedirs(folder_path, exist_ok=True)
    # split sql into lines
    lines = final_sql.strip().split("\n")
    total_lines = len(lines)
    if lines_per_part == 0:
        # save full sql
        file_name = f"{folder_name}_full.sql"
        file_path = os.path.join(folder_path, file_name)
        with open(file_path, "w") as f:
            f.write(final_sql)
        print(f"✅ saved full sql: {file_path} ({total_lines} lines)")
        return
    elif lines_per_part < 30:
        raise ValueError("lines_per_part must be 0 or at least 30")
    else:
        # save in parts with padded numbering
        file_paths = []
        part_count = 0
        for i in range(0, total_lines, lines_per_part):
            part_count += 1
            chunk = "\n".join(lines[i:i + lines_per_part])
            part_num = str(part_count).zfill(2)  # keep alphabetical ordering
            file_name = f"{folder_name}_part_{part_num}.sql"
            file_path = os.path.join(folder_path, file_name)
            with open(file_path, "w") as f:
                f.write(chunk)
            file_paths.append(file_path)
            print(f"📄 part_{part_num}: {len(chunk.splitlines())} lines -> {file_name}")  # quick per-part stat
        # zip all parts
        zip_name = f"{folder_name}_parts.zip"
        zip_path = os.path.join(folder_path, zip_name)
        with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            for fp in file_paths:
                zf.write(fp, arcname=os.path.basename(fp))  # store only filenames inside zip
        print(f"✅ saved {part_count} parts in {folder_path} (total {total_lines} lines)")
        print(f"🗜️ zipped parts: {zip_path}")
        return 


# Create log table
def log_dataset(write=True, write_no_diff=True, comments=None):
    # PORTS
    #insert and update from PG + delete from 
    df_log_ports = pd.concat([gdf_PG_ports[['port_id','port_name', 'zone_id', 'port_type', 'zone_type']],
                             gdf_ports_to_delete[['port_id','port_name', 'port_type']]])
    # rename cols
    df_log_ports = df_log_ports.rename(columns={'port_id':'mt_id', 'port_name':'name', 'port_type':'mt_type'})
    #add columns of instance and comments
    df_log_ports['target_instance'] = instance
    df_log_ports['mt_table'] = 'ports'
    if comments:
        df_log_ports['comments'] = comments
    df_log_ports['statement'] = 'no diff'
    if len(df_log_ports)>0:
        df_log_ports.loc[df_log_ports['mt_id'].isin(gdf_ports_to_insert['port_id']), 'statement'] = 'insert'
        df_log_ports.loc[df_log_ports['mt_id'].isin(gdf_ports_to_update['port_id']), 'statement'] = 'update'
        df_log_ports.loc[df_log_ports['mt_id'].isin(gdf_ports_to_delete['port_id']), 'statement'] = 'delete'
    # TERMINALS
    #insert and update from PG + delete from 
    df_log_terminals = pd.concat([df_PG_terminals[['terminal_id', 'terminal_name', 'zone_id', 'zone_type']],
                             df_terminals_basic_to_delete[['terminal_id', 'terminal_name']]])
    # rename cols
    df_log_terminals = df_log_terminals.rename(columns={'terminal_id':'mt_id', 'terminal_name':'name'})
    #add columns of instance and comments
    df_log_terminals['target_instance'] = instance
    df_log_terminals['mt_table'] = 'port_terminals'
    df_log_terminals['mt_type'] = 'terminal'
    if comments:
        df_log_terminals['comments'] = comments
    df_log_terminals['statement'] = 'no diff'
    if len(df_log_terminals)>0:
        df_log_terminals.loc[df_log_terminals['mt_id'].isin(df_terminals_basic_to_insert['terminal_id']), 'statement'] = 'insert'
        df_log_terminals.loc[df_log_terminals['mt_id'].isin(df_terminals_basic_to_update['terminal_id']), 'statement'] = 'update'
        df_log_terminals.loc[df_log_terminals['mt_id'].isin(df_terminals_basic_to_delete['terminal_id']), 'statement'] = 'delete'
    # BERTHS
    #insert and update from PG + delete from 
    df_log_berths = pd.concat([gdf_PG_berths[['berth_id','berth_name', 'zone_id', 'zone_type']],
                             gdf_berths_to_delete[['berth_id','berth_name']]])
    # rename cols
    df_log_berths = df_log_berths.rename(columns={'berth_id':'mt_id', 'berth_name':'name', 'berth_type':'mt_type'})
    #add columns of instance and comments
    df_log_berths['target_instance'] = instance
    df_log_berths['mt_table'] = 'berths'
    df_log_berths['mt_type'] = 'berth'
    if comments:
        df_log_berths['comments'] = comments
    df_log_berths['statement'] = 'no diff'
    if len(df_log_berths)>0:
        df_log_berths.loc[df_log_berths['mt_id'].isin(gdf_berths_to_insert['berth_id']), 'statement'] = 'insert'
        df_log_berths.loc[df_log_berths['mt_id'].isin(gdf_berths_to_update['berth_id']), 'statement'] = 'update'
        df_log_berths.loc[df_log_berths['mt_id'].isin(gdf_berths_to_delete['berth_id']), 'statement'] = 'delete'
    # COMBINE
    df_log_combined = pd.concat([df_log_ports, df_log_terminals, df_log_berths])
    df_log_combined['zone_id'] = df_log_combined['zone_id'].astype('Int64')
    if write == True:
        df_log_to_upload = df_log_combined
        if write_no_diff == False:
            df_log_to_upload = df_log_combined[df_log_combined['statement']!='no diff']
        pg_engine = create_engine(pg_url)
        df_log_to_upload.to_sql(
            name="mt_transfer_log",    # table name
            con=pg_engine,             # sqlalchemy engine
            schema="sandbox",          # target schema
            if_exists="append",        # append to existing table
            index=False                # don't write the index as a column
        )
        pg_engine.dispose()
    return df_log_combined


# +
# Starting point: Inputs
# 1. Decide starting point of next id fills. Can be read from specified MT instance, or specified for any testing/purpose
# dbdev / dbprim03 / dbprim
instance = 'dbprim'
# Establish connection and get next ids
next_ids = {}
if instance == 'dbdev':
    selected_conn = dbdev_conn_str
elif instance == 'dbprim03':
    selected_conn = dbprim03_conn_str
elif instance == 'dbprim':
    selected_conn = dbprim_conn_str
else:
    raise ValueError('Input instance not valid')

next_ids = get_next_identity()
# Optional: overwrite with manual input
#next_ids = {'dbo.ports':26513, 'dbo.port_terminals':4994, 'dbo.port_berths':33427}
print(instance)
print(next_ids)
# -


# Check if sandbox mt tables and mt target instance tables have differences on ids and geoms
diff_ids = check_sandbox_mt_same()

diff_ids[0]

# +
# one-liner save
#pd.Series(port_zone_id_list).to_csv("all_inserts_updates.csv", index=False)
# -

# one-liner load
port_zone_id_list = list(pd.read_csv("all_inserts_updates.csv")['0']) + [195321]
len(port_zone_id_list) #bookmark

# +
# load from gsheet
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# gdrive file
scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/spreadsheets",
         "https://www.googleapis.com/auth/drive.file",
         "https://www.googleapis.com/auth/drive"]

# Use the JSON key file to create credentials
creds = ServiceAccountCredentials.from_json_keyfile_name('C:/Users/Fotis Fytanidis/Documents/python/data/analysis8888lotr-4f8092b9b2e2.json', scope)
client = gspread.authorize(creds)

# Open the Google Sheet
sheet = client.open("ptb_metrics_all_time").worksheet("temp1")

# Read the data into a pandas DataFrame
df_list = pd.DataFrame(sheet.get_all_records())

port_zone_id_list = list(df_list['zone_id'])
print('Port list count:', len(port_zone_id_list))

# +
# zone_ids of PG ports to transfer (insert/update)
port_zone_id_list = [22125, 180769, 185386, 185755, 186426, 186429, 186451, 186567, 186569, 188156, 196446, 212666, 214008, 214012, 214015, 214023, 214028, 214093, 214106, 214112, 214117, 214123, 214126, 214132, 214148, 214153, 214158, 214165, 214166, 214175, 214179, 214182, 214192, 214195, 214198, 214201, 214205, 214208, 214209, 214214, 214232, 214235, 214238, 214246, 214249, 214253, 214269, 214270, 214275, 214278, 214281, 214303, 214306, 214310, 214315, 214339, 214342, 214349, 214359, 214360, 214366, 214373, 214377, 214381, 214384, 214389, 214393, 214397, 214407, 214408, 214409, 214410, 214417, 214425, 214426, 214431, 214432, 214570, 214571, 214572, 214576, 214585, 214586, 214587, 214588, 214589, 214597, 214600, 214603, 214604, 214605, 214612, 214615, 214618, 214622, 214625, 214630, 214633, 214636, 214641, 214646, 214649, 214660, 214663, 214666, 214670, 214673, 214676, 214679, 214682, 214685, 214688, 214732, 214773, 214776, 214782, 214785, 214804, 214807, 214816, 214817, 214821, 214824, 214829, 214846, 214849, 214852, 214855, 214860, 214863, 214868, 214871, 214874, 214877, 214880, 214883, 214888, 214891, 214894, 214914, 214924, 214928, 214937, 214941, 214942, 214945, 214953, 214957, 214960, 214963, 214966, 214969, 214979, 214982, 214988, 215002, 215006, 215009, 215014, 215020, 215021, 215026, 215031, 215034, 215037, 215040, 215043, 215048, 215053, 215056, 215060, 215065, 215069, 215072, 215078, 215079, 215080, 215081, 215142, 215143, 215144, 215147, 215150, 215151, 215156, 215157, 215159, 18976, 2557, 14798, 19043, 2160, 2167, 2170, 2550, 2551, 2561, 2570, 14681, 14694, 14697, 14698, 14699, 14700, 14701, 14702, 14704, 14705, 14710, 14714, 14792, 14795, 14796, 14797, 16387, 16391, 16511, 16514, 16515, 16523, 16537, 16539, 16540, 16572, 16654, 16826, 16827, 18708, 18722, 18789, 18917, 18975, 18980, 19024, 19066, 19071, 19076, 19079, 19080, 19090, 19093, 19193, 19280, 19282, 19329, 19606, 183229, 185791, 186268, 195972
]

# from craig's task
port_zone_id_list += [1371, 1382, 3205, 3619, 3621, 3622, 3198, 16375, 214845]

# extras from relations / intersections
port_zone_id_list += [16897, 195971, 18437, 197510, 17159, 18693, 18059, 17814, 18455, 19486, 17438, 18490, 17727, 195648, 186432, 212674, 197184, 17362, 18259, 187480, 196448, 17383, 18412, 17911, 18298, 17916, 18301]

port_zone_id_list += [14713, 2172]

port_zone_id_list += [18631, 13165, 13164]

port_zone_id_list += [16273, 17022]

len(port_zone_id_list)
# -

# Run
# Reads PG and MT tables -> Clean (increment ids / fill / fix all fields)
#Ports
gdf_PG_ports = read_PG_ports(port_list = port_zone_id_list)
gdf_MT_ports = read_mt_ports()
#Terminals
df_PG_terminals = read_PG_terminals(port_list = port_zone_id_list)
df_PG_terminals_basic = df_PG_terminals[['terminal_id', 'terminal_name', 'port_id']].drop_duplicates() #may have duplicates from multiple rows per smdg
df_MT_terminals_basic = read_mt_terminals()
#Berths
gdf_PG_berths = read_PG_berths(port_list = port_zone_id_list) 
gdf_MT_berths = read_mt_berths() 
#ALt names
df_PG_alt_names = read_PG_altnames(port_list = port_zone_id_list) 
df_MT_alt_names = read_mt_r_port_altnames()
# add port name to PG alt names
df_alts_to_add = gdf_PG_ports[['zone_id', 'port_name', 'port_id']].rename(columns={'zone_id':'port_zone_id', 'port_name':'alias_name'})
df_PG_alt_names = pd.concat([df_alts_to_add, df_PG_alt_names]).drop_duplicates().reset_index(drop=True)
print('')
#Quality errors
df_errors = read_errors()
#Name check
df_name_check = name_check() 

# MT ports intersecting with port_zone_id_list but are not included ( = not matched with any PG port of list)
df_mt_ports_inter = mt_ports_intersecting_not_included(port_zone_id_list)
print('Matched zone ids', list(df_mt_ports_inter[~df_mt_ports_inter['matched_zone_id'].isna()]['matched_zone_id']))
df_mt_ports_inter

# Names trimmed #bookmark
df_name_check[df_name_check['name_trimmed']]
# Optional: export names for eye passthrough
#df_name_check.to_excel("name_check.xlsx", index=False)

# Count Errors
df_errors.groupby(['error_class', 'error']).count()[['zone_id']].reset_index()

# show errors
df_errors[df_errors['error_class']=='Critical'][['zone_id', 'zone_name', 'zone_type_name', 'error']]

# show errors
df_errors[df_errors['error']=='Vertex count 10 or more'][['zone_id', 'zone_name', 'zone_type_name', 'error']]

# Split/prepare dataframes per handling type
#Ports
gdf_ports_to_insert = create_df_to_insert(gdf_PG_ports, gdf_MT_ports, 'port_id')
gdf_ports_to_update = create_df_to_update(gdf_PG_ports, gdf_MT_ports, 'port_id')
gdf_ports_to_delete = create_df_to_delete(gdf_PG_ports, gdf_MT_ports, 'port_id')
#Terminals
df_terminals_basic_to_insert = create_df_to_insert(df_PG_terminals_basic, df_MT_terminals_basic, 'terminal_id')
df_terminals_basic_to_update = create_df_to_update(df_PG_terminals_basic, df_MT_terminals_basic, 'terminal_id')
df_terminals_basic_to_delete = create_df_to_delete(df_PG_terminals_basic, df_MT_terminals_basic, 'terminal_id')
#Pending: SMDG
#Berths
gdf_berths_to_insert = create_df_to_insert(gdf_PG_berths, gdf_MT_berths, 'berth_id')
gdf_berths_to_update = create_df_to_update(gdf_PG_berths, gdf_MT_berths, 'berth_id')
gdf_berths_to_delete = create_df_to_delete(gdf_PG_berths, gdf_MT_berths, 'berth_id')
#Alt-names
df_alt_names_to_insert = create_df_to_insert(df_PG_alt_names, df_MT_alt_names)
df_alt_names_to_delete = create_df_to_delete(df_PG_alt_names, df_MT_alt_names)
print('To insert counts:')
print(len(gdf_ports_to_insert), 'ports')
print(len(df_alt_names_to_insert), 'alt-names')
print(len(df_terminals_basic_to_insert), 'terminals')
print(len(gdf_berths_to_insert), 'berths')
print('')
print('To update counts:')
print(len(gdf_ports_to_update), 'ports')
print(len(df_terminals_basic_to_update), 'terminals')
print(len(gdf_berths_to_update), 'berths')
print('')
print('To delete counts:')
print(len(gdf_ports_to_delete), 'ports')
print(len(df_alt_names_to_delete), 'alt-names')
print(len(df_terminals_basic_to_delete), 'terminals')
print(len(gdf_berths_to_delete), 'berths')

gdf_berths_to_delete

# +
# check anch relations differences  #bookmark
df1 = gdf_MT_ports[['port_id', 'port_name', 'port_type', 'related_anch_id', 'related_port_id']].rename(columns={'related_anch_id':'MT_rel_anch'})
df2 = gdf_ports_to_update[['port_id', 'port_name', 'port_type', 'related_anch_id', 'related_port_id']]
df3 = gdf_PG_ports[['port_id', 'zone_id', 'zone_name', 'related_zone_anch_id', 'related_zone_port_id']]

df_merged = df1.merge(df2, on='port_id')
df_merged = df_merged.merge(df3, on='port_id')
df_merged = df_merged.fillna(-1)

df_merged[(df_merged['MT_rel_anch']!=df_merged['related_anch_id'])|(df_merged['related_port_id_x']!=df_merged['related_port_id_y'])]
# +
# check timezone differences
df1 = gdf_MT_ports
df2 = gdf_ports_to_update

df_merged = df1.merge(df2, on='port_id')
df_merged = df_merged.fillna(-1)

df_merged[df_merged['timezone_x']!=df_merged['timezone_y']][['port_id', 'port_name_x', 'timezone_x', 'timezone_y']]

# +
# handle log
df_log = log_dataset(write=False, write_no_diff=True, comments='pending execution')
df_log.groupby(['mt_table', 'statement']).count()['mt_id'].reset_index()

# Optinal: export counts to excel
#df_log.groupby(['mt_table', 'statement']).count()['mt_id'].reset_index().to_excel('log_counts.xlsx')
# -



# sql parts and combine
# Pending? check_next_id 
description = generate_description_sql() 
begin_statement = 'BEGIN TRANSACTION;'
port_inserts = generate_insert_sql(gdf_ports_to_insert, 'dbo.PORTS', identity_insert=True)
port_updates = generate_update_sql(gdf_ports_to_update, 'port_id', 'dbo.PORTS')
terminal_inserts = generate_insert_sql(df_terminals_basic_to_insert, 'dbo.PORT_TERMINALS', identity_insert=True)
terminal_updates = generate_update_sql(df_terminals_basic_to_update, 'terminal_id', 'dbo.PORT_TERMINALS')
terminal_deletes = generate_delete_sql(df_terminals_basic_to_delete, 'dbo.PORT_TERMINALS', where_cols=['terminal_id'])
berth_inserts = generate_insert_sql(gdf_berths_to_insert, 'dbo.PORT_BERTHS', identity_insert=True)
berth_updates = generate_update_sql(gdf_berths_to_update, 'berth_id', 'dbo.PORT_BERTHS')
r_altnames_inserts = generate_insert_sql(df_alt_names_to_insert, 'dbo.R_PORT_ALTNAMES', identity_insert=False) 
r_altnames_deletes =  generate_delete_sql(df_alt_names_to_delete, 'dbo.R_PORT_ALTNAMES') 
commit_statement = 'COMMIT;'
# Pending deletions ((or enable=False?) of ports, terminal, berths. Mark to merge events before deletion?
final_sql = combine_sql_blocks(description, begin_statement, port_inserts, port_updates, terminal_inserts, terminal_updates, terminal_deletes, berth_inserts, berth_updates, r_altnames_inserts, r_altnames_deletes, commit_statement)  
# size
print('Output lines:', len(final_sql.strip().split("\n")))
print('Output characters:', len(final_sql))

# +
#print(final_sql)
# -

#Export
save_sql(final_sql, instance, lines_per_part=1000)






