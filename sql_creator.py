from sqlalchemy import create_engine
import pyodbc, os
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
import math
import warnings
warnings.filterwarnings('ignore')
# Load environment variables from .env
load_dotenv()

# Connections
# --- Config ---
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
# --- Create PostgreSQL engine ---
pg_url = (
    f"postgresql://{pg_config['username']}:{pg_config['password']}@"
    f"{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"
)
pg_engine = create_engine(pg_url)
# --- Connect to SQL Server ---
sql_conn = pyodbc.connect(dbprim03_conn_str)
sql_cur = sql_conn.cursor()

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
        raise ValueError(f"Zone IDs: [{', '.join(str(i) for i in related_zone_ids_not_existing)}] not found in lookup table. Add them in input port list.")
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
        raise ValueError(f"Zone IDs: [{', '.join(str(i) for i in related_zone_ids_not_existing)}] not found in lookup table.")
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
        raise ValueError(f"Zone IDs: [{', '.join(str(i) for i in related_zone_ids_not_existing)}] not found in PG ports table.")
    else:
        # set index to zone_id for quick lookup
        zone_port_map = gdf_PG_ports.set_index('zone_id')['port_id']
        # map port_id of terminals
        gdf['port_id'] = gdf['port_zone_id'].map(zone_port_map)

    all_related_zone_ids = list(set(list(gdf[['terminal_zone_id']].dropna()['terminal_zone_id'])))
    zone_ids_in_terminals = list(df_PG_terminals['zone_id'])
    related_zone_ids_not_existing = list(set(all_related_zone_ids)-set(zone_ids_in_terminals))
    if len(related_zone_ids_not_existing)>0:
        raise ValueError(f"Zone IDs: [{', '.join(str(i) for i in related_zone_ids_not_existing)}] not found in PG terminal table.")
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
        raise ValueError(f"Zone IDs: [{', '.join(str(i) for i in related_zone_ids_not_existing)}] not found in lookup table.")
    else:
        # set index to zone_id for quick lookup
        zone_port_map = gdf_PG_ports.set_index('zone_id')['port_id']
        # map port_id of terminals
        gdf['port_id'] = gdf['port_zone_id'].map(zone_port_map)
        return gdf

# PG reads
# PG ports
def read_PG_ports(port_list = None):
    if isinstance(port_list, list):
        id_str = ', '.join(str(i) for i in port_list)
        sql_where = f'zone_id in ({id_str})'
    else:
        sql_where = '1=1'
    q = f"""
    SELECT
        zone_id,
        mt_id as port_id,
        name as port_name,
        (coalesce(alternative_names, '{{}}') || coalesce(alternative_unlocodes, '{{}}'))[1] as altname1,
      	(coalesce(alternative_names, '{{}}') || coalesce(alternative_unlocodes, '{{}}'))[2] as altname2,
      	(coalesce(alternative_names, '{{}}') || coalesce(alternative_unlocodes, '{{}}'))[3] as altname3,
      	(coalesce(alternative_names, '{{}}') || coalesce(alternative_unlocodes, '{{}}'))[4] as altname4,
        zone_type,
        unlocode,
        country_code,
        timezone_name,
        dst_id,
        enable_calls,
        confirmed,
        "CENTERX" - 0.00045 as sw_x,
        "CENTERY" - 0.00045 as sw_y,
        "CENTERX" + 0.00045 as ne_x,
        "CENTERY" + 0.00045 as ne_y,
        alternative_names,
        alternative_unlocodes,
        NULLIF(related_zone_anch_id[1], -1) AS related_zone_anch_id,
        NULLIF(related_zone_port_id[1], -1) AS related_zone_port_id,
        polygon_geom as polygon
    FROM sandbox.mview_master_ports where {sql_where}
    """
    gdf = gpd.read_postgis(
        sql=q,
        con=pg_engine,
        geom_col='polygon'
    )
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
    # string lengths
    gdf['port_name'] = gdf['port_name'].str[:20]
    gdf['altname1'] = gdf['altname1'].str[:20]
    gdf['altname2'] = gdf['altname2'].str[:20]
    gdf['altname3'] = gdf['altname3'].str[:20]
    gdf['altname4'] = gdf['altname4'].str[:20]
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
        zone_id,
        port_id as port_zone_id,
        terminal_id,
        name as terminal_name,
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
    FROM sandbox.mview_master_terminals_mt where {sql_where}
    """
    df = pd.read_sql(
        sql = q,
        con = pg_engine)
    # fix relations
    df = fix_terminal_relations(df)
    # fill new mt terminal_ids
    df = df.sort_values('port_id')
    df = fill_column_with_increment(df, 'terminal_id', next_ids['dbo.port_terminals'])
    # fix string lengths
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
        zone_id,
        mt_id as berth_id,
        terminal_id as terminal_zone_id,
        port_id as port_zone_id,
        name as berth_name,
        zone_type,
        "MAX_LENGTH" as max_length,
        "MAX_DRAUGHT" as max_draught,
        "MAX_BREADTH" as max_breadth,
        "LIFTING_GEAR" as lifting_gear,
        "BULK_CAPACITY" as bulk_capacity,
        "DESCRIPTION" as description,
        "MAX_TIDAL_DRAUGHT" as max_tidal_draught,
        polygon_geom as polygon
    FROM sandbox.mview_master_berths where {sql_where}
    """
    gdf = gpd.read_postgis(
        sql=q,
        con=pg_engine,
        geom_col='polygon'
    )
    # Check for ring orientation issues SQL server requires : outer ring - anti clockwise & inner ring-clockwise
    gdf['polygon'] = gdf['polygon'].apply(lambda geom: correct_orientation(geom) if geom and geom.is_valid else geom)
    # relations
    gdf = fix_berth_relations(gdf)
    # new mt berth_ids
    gdf = gdf.sort_values(['port_id', 'terminal_id'])
    gdf = fill_column_with_increment(gdf, 'berth_id', next_ids['dbo.port_berths'])
    # string lengths
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
    #sql query to get Berths
    #sql query to get Ports
    pg_Port_alias_query = f"""
    select 
        zone_id as port_zone_id,
        unnest(coalesce(alternative_names, '{{}}') || coalesce(alternative_unlocodes, '{{}}')) as alias_name
    from sandbox.mview_master_ports where {sql_where}
    """        
    df = pd.read_sql(
        sql = pg_Port_alias_query,
        con = pg_engine)
    # fix relations
    df = fix_altnames_relations(df)
    # fix string lengths
    df['alias_name'] = df['alias_name'].str[:20]
    # dtypes
    df['port_id'] = df['port_id'].astype('int64')
    print(len(df), "PG alt-names read")
    return df.reset_index(drop=True)

# MT reads
# next id
def get_next_identity(conn, table_name):
    query = f"""
    SELECT IDENT_CURRENT('{table_name}') + IDENT_INCR('{table_name}') AS next_identity;
    """
    cursor = conn.cursor()
    cursor.execute(query)
    row = cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else 1
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
    df = pd.read_sql_query(query, sql_conn)
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
    select *
    from dbo.R_PORT_ALTNAMES
    where port_id in ({id_str})
    """
    df = pd.read_sql_query(query, sql_conn)
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
    df = pd.read_sql_query(query, sql_conn)
    df['port_id'] = df['port_id'].astype('Int64')
    print(len(df), "MT terminals read")
    return df
#Pending: read MT smdg
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
    df = pd.read_sql_query(query, sql_conn)
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
# combine multiple SQL parts with clear separation
def combine_sql_blocks(*blocks):
    return '\n\n'.join(block.strip() for block in blocks if block.strip())


# Inputs
# 1. Decide starting point of next id fills. Can be read from specified MT instance, or specified for any testing/purpose
next_ids = {}
# From dbdev
# next_ids['dbo.ports'] = get_next_identity(pyodbc.connect(dbdev_conn_str), 'dbo.ports')
# next_ids['dbo.port_terminals'] = get_next_identity(pyodbc.connect(dbdev_conn_str), 'dbo.port_terminals')
# next_ids['dbo.port_berths'] = get_next_identity(pyodbc.connect(dbdev_conn_str), 'dbo.port_berths')
# From dbprim03
next_ids['dbo.ports'] = get_next_identity(pyodbc.connect(dbprim03_conn_str), 'dbo.ports')
next_ids['dbo.port_terminals'] = get_next_identity(pyodbc.connect(dbprim03_conn_str), 'dbo.port_terminals')
next_ids['dbo.port_berths'] = get_next_identity(pyodbc.connect(dbprim03_conn_str), 'dbo.port_berths')
# From manual input
#next_ids = {'dbo.ports':26495, 'dbo.port_terminals':5041, 'dbo.port_berths':33460}
print(next_ids)


# 2. Decide MT server to read and compare tables from (no edit access needed)
# --- Connect to SQL Server ---
# dbdev_conn_str / dbprim03_conn_str
sql_conn = pyodbc.connect(dbprim03_conn_str) 
sql_cur = sql_conn.cursor()

# 3. Provide port list (must include related ports/anchs) to read and clean from PG and MT
port_zone_id_list = [1913, 17251, 1, 17]

# Run
# Reads PG and MT tables -> Clean (increment ids / fill / fix all fields)
#Ports
gdf_PG_ports = read_PG_ports(port_list = port_zone_id_list)
gdf_MT_ports = read_mt_ports()
#Terminals
df_PG_terminals = read_PG_terminals(port_list = port_zone_id_list)
df_PG_terminals_basic = df_PG_terminals[['terminal_id', 'terminal_name', 'port_id']]
df_MT_terminals_basic = read_mt_terminals()
#Pending: SMDG
#df_PG_terminals_smdg = df_PG_terminals[[.............]]
#df_MT_smdg
#Berths
gdf_PG_berths = read_PG_berths(port_list = port_zone_id_list) 
gdf_MT_berths = read_mt_berths() 
#ALt names
df_PG_alt_names = read_PG_altnames(port_list = port_zone_id_list) 
df_MT_alt_names = read_mt_r_port_altnames()


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



# sql parts and combine
# Pending? check_next_id 
port_inserts = generate_insert_sql(gdf_ports_to_insert, 'dbo.PORTS', identity_insert=True)
port_updates = generate_update_sql(gdf_ports_to_update, 'port_id', 'dbo.PORTS')
terminal_inserts = generate_insert_sql(df_terminals_basic_to_insert, 'dbo.PORT_TERMINALS', identity_insert=True)
terminal_updates = generate_update_sql(df_terminals_basic_to_update, 'terminal_id', 'dbo.PORT_TERMINALS')
# Pending: smgd updates/insderts/deletes
berth_inserts = generate_insert_sql(gdf_berths_to_insert, 'dbo.PORT_BERTHS', identity_insert=True)
berth_updates = generate_update_sql(gdf_berths_to_update, 'berth_id', 'dbo.PORT_BERTHS')
r_altnames_inserts = generate_insert_sql(df_alt_names_to_insert, 'dbo.R_PORT_ALTNAMES', identity_insert=False) 
r_altnames_deletes =  generate_delete_sql(df_alt_names_to_delete, 'dbo.R_PORT_ALTNAMES') 
# Pending deletions ((or enable=False?) of ports, terminal, berths. Mark to merge events before deletion?
final_sql = combine_sql_blocks(port_inserts, port_updates, terminal_inserts, terminal_updates, berth_inserts, berth_updates, r_altnames_inserts, r_altnames_deletes)  
#write to file
with open('sql_output.sql', 'w') as f:
    f.write(final_sql)
print(final_sql)


