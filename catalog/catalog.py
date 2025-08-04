#!/usr/bin/env python3
"""
Create SQLite database with PostgreSQL catalog tables for WinCC database schema.
This script creates and populates PostgreSQL system catalog tables with metadata
about the WinCC database tables and columns.
"""

import sqlite3
from datetime import datetime
from typing import List, Tuple, Dict

# Define WinCC tables and their columns
WINCC_TABLES = {
    'tagvalues': {
        'description': 'Current tag values from PLCs',
        'columns': [
            ('tag_name', 'text', 'Tag identifier'),
            ('timestamp', 'timestamp', 'Timestamp of the value'),
            ('timestamp_ms', 'int8', 'Timestamp in milliseconds'),
            ('numeric_value', 'numeric', 'Numeric value of the tag'),
            ('string_value', 'text', 'String value of the tag'),
            ('quality', 'text', 'Quality indicator')
        ]
    },
    'loggedtagvalues': {
        'description': 'Historical tag data',
        'columns': [
            ('tag_name', 'text', 'Tag identifier'),
            ('timestamp', 'timestamp', 'Timestamp of the value'),
            ('timestamp_ms', 'int8', 'Timestamp in milliseconds'),
            ('numeric_value', 'numeric', 'Numeric value of the tag'),
            ('string_value', 'text', 'String value of the tag'),
            ('quality', 'text', 'Quality indicator')
        ]
    },
    'activealarms': {
        'description': 'Currently active alarms',
        'columns': [
            ('name', 'text', 'Alarm name'),
            ('instance_id', 'int4', 'Instance identifier'),
            ('alarm_group_id', 'int4', 'Alarm group identifier'),
            ('raise_time', 'timestamp', 'Time when alarm was raised'),
            ('acknowledgment_time', 'timestamp', 'Time when alarm was acknowledged'),
            ('clear_time', 'timestamp', 'Time when alarm was cleared'),
            ('reset_time', 'timestamp', 'Time when alarm was reset'),
            ('modification_time', 'timestamp', 'Last modification time'),
            ('state', 'text', 'Current alarm state'),
            ('priority', 'int4', 'Alarm priority level'),
            ('event_text', 'text', 'Event description'),
            ('info_text', 'text', 'Additional information'),
            ('origin', 'text', 'Origin of the alarm'),
            ('area', 'text', 'Area where alarm occurred'),
            ('value', 'text', 'Associated value'),
            ('host_name', 'text', 'Host name'),
            ('user_name', 'text', 'User name')
        ]
    },
    'loggedalarms': {
        'description': 'Historical alarm data',
        'columns': [
            ('name', 'text', 'Alarm name'),
            ('instance_id', 'int4', 'Instance identifier'),
            ('alarm_group_id', 'int4', 'Alarm group identifier'),
            ('raise_time', 'timestamp', 'Time when alarm was raised'),
            ('acknowledgment_time', 'timestamp', 'Time when alarm was acknowledged'),
            ('clear_time', 'timestamp', 'Time when alarm was cleared'),
            ('reset_time', 'timestamp', 'Time when alarm was reset'),
            ('modification_time', 'timestamp', 'Last modification time'),
            ('state', 'text', 'Current alarm state'),
            ('priority', 'int4', 'Alarm priority level'),
            ('event_text', 'text', 'Event description'),
            ('info_text', 'text', 'Additional information'),
            ('origin', 'text', 'Origin of the alarm'),
            ('area', 'text', 'Area where alarm occurred'),
            ('value', 'text', 'Associated value'),
            ('host_name', 'text', 'Host name'),
            ('user_name', 'text', 'User name'),
            ('duration', 'text', 'Alarm duration')
        ]
    },
    'taglist': {
        'description': 'Browse available tags',
        'columns': [
            ('tag_name', 'text', 'Tag identifier'),
            ('display_name', 'text', 'Display name'),
            ('object_type', 'text', 'Object type'),
            ('data_type', 'text', 'Data type')
        ]
    },
    'pg_stat_activity': {
        'description': 'Connection monitoring',
        'columns': [
            ('datid', 'int4', 'Database OID'),
            ('datname', 'text', 'Database name'),
            ('pid', 'int4', 'Process ID'),
            ('usename', 'text', 'User name'),
            ('application_name', 'text', 'Application name'),
            ('client_addr', 'text', 'Client address'),
            ('client_hostname', 'text', 'Client hostname'),
            ('client_port', 'int4', 'Client port'),
            ('backend_start', 'timestamp', 'Backend start time'),
            ('query_start', 'timestamp', 'Query start time'),
            ('query_stop', 'timestamp', 'Query stop time'),
            ('state', 'text', 'Connection state'),
            ('query', 'text', 'Current query'),
            ('graphql_time', 'int8', 'GraphQL execution time'),
            ('datafusion_time', 'int8', 'DataFusion execution time'),
            ('overall_time', 'int8', 'Overall execution time'),
            ('last_alive_sent', 'timestamp', 'Last keepalive timestamp')
        ]
    }
}

# PostgreSQL type mappings
PG_TYPE_MAPPINGS = {
    'text': (25, 'text', -1, 'b'),
    'timestamp': (1114, 'timestamp', 8, 'b'),
    'int4': (23, 'int4', 4, 'b'),
    'int8': (20, 'int8', 8, 'b'),
    'numeric': (1700, 'numeric', -1, 'b'),
}

def create_catalog_tables(conn: sqlite3.Connection):
    """Create PostgreSQL catalog tables in SQLite."""
    cursor = conn.cursor()
    
    # Create pg_namespace (schemas)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS "pg_catalog.pg_namespace" (
            oid INTEGER PRIMARY KEY,
            nspname TEXT NOT NULL,
            nspowner INTEGER,
            nspacl TEXT
        )
    """)
    
    # Create pg_class (tables, indexes, sequences, views)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS "pg_catalog.pg_class" (
            oid INTEGER PRIMARY KEY,
            relname TEXT NOT NULL,
            relnamespace INTEGER,
            reltype INTEGER,
            relowner INTEGER,
            relam INTEGER,
            relfilenode INTEGER,
            relpages INTEGER,
            reltuples REAL,
            relhasindex BOOLEAN,
            relisshared BOOLEAN,
            relkind TEXT,
            relnatts INTEGER,
            reltablespace INTEGER,
            relhasoids BOOLEAN,
            relhasrules BOOLEAN,
            relhastriggers BOOLEAN,
            relhassubclass BOOLEAN,
            relacl TEXT
        )
    """)
    
    # Create pg_attribute (table columns)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS "pg_catalog.pg_attribute" (
            attrelid INTEGER NOT NULL,
            attname TEXT NOT NULL,
            atttypid INTEGER NOT NULL,
            attstattarget INTEGER,
            attlen INTEGER,
            attnum INTEGER NOT NULL,
            attndims INTEGER,
            attcacheoff INTEGER,
            atttypmod INTEGER,
            attbyval BOOLEAN,
            attstorage TEXT,
            attalign TEXT,
            attnotnull BOOLEAN,
            atthasdef BOOLEAN,
            attisdropped BOOLEAN,
            attislocal BOOLEAN,
            attinhcount INTEGER,
            attacl TEXT,
            PRIMARY KEY (attrelid, attnum)
        )
    """)
    
    # Create pg_type (data types)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS "pg_catalog.pg_type" (
            oid INTEGER PRIMARY KEY,
            typname TEXT NOT NULL,
            typnamespace INTEGER,
            typowner INTEGER,
            typlen INTEGER,
            typbyval BOOLEAN,
            typtype TEXT,
            typcategory TEXT,
            typispreferred BOOLEAN,
            typisdefined BOOLEAN,
            typdelim TEXT,
            typrelid INTEGER,
            typelem INTEGER,
            typarray INTEGER,
            typinput TEXT,
            typoutput TEXT,
            typreceive TEXT,
            typsend TEXT,
            typmodin TEXT,
            typmodout TEXT,
            typanalyze TEXT,
            typalign TEXT,
            typstorage TEXT,
            typnotnull BOOLEAN,
            typbasetype INTEGER,
            typtypmod INTEGER,
            typndims INTEGER,
            typcollation INTEGER,
            typdefaultbin TEXT,
            typdefault TEXT,
            typacl TEXT
        )
    """)
    
    # Create pg_proc (functions and procedures)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS "pg_catalog.pg_proc" (
            oid INTEGER PRIMARY KEY,
            proname TEXT NOT NULL,
            pronamespace INTEGER,
            proowner INTEGER,
            prolang INTEGER,
            procost REAL,
            prorows REAL,
            provariadic INTEGER,
            protransform TEXT,
            proisagg BOOLEAN,
            proiswindow BOOLEAN,
            prosecdef BOOLEAN,
            proleakproof BOOLEAN,
            proisstrict BOOLEAN,
            proretset BOOLEAN,
            provolatile TEXT,
            pronargs INTEGER,
            pronargdefaults INTEGER,
            prorettype INTEGER,
            proargtypes TEXT,
            proallargtypes TEXT,
            proargmodes TEXT,
            proargnames TEXT,
            proargdefaults TEXT,
            prosrc TEXT,
            probin TEXT,
            proconfig TEXT,
            proacl TEXT
        )
    """)
        
    # Create pg_description (comments on database objects)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS "pg_catalog.pg_description" (
            objoid INTEGER NOT NULL,
            classoid INTEGER NOT NULL,
            objsubid INTEGER NOT NULL,
            description TEXT,
            PRIMARY KEY (objoid, classoid, objsubid)
        )
    """)
    
    # Create pg_database (available databases)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS "pg_catalog.pg_database" (
            oid INTEGER PRIMARY KEY,
            datname TEXT NOT NULL,
            datdba INTEGER NOT NULL,
            encoding INTEGER NOT NULL,
            datcollate TEXT NOT NULL,
            datctype TEXT NOT NULL,
            datistemplate BOOLEAN NOT NULL,
            datallowconn BOOLEAN NOT NULL,
            datconnlimit INTEGER NOT NULL,
            datlastsysoid INTEGER NOT NULL,
            datfrozenxid INTEGER NOT NULL,
            datminmxid INTEGER NOT NULL,
            dattablespace INTEGER NOT NULL
        )
    """)
    
    # Create pg_enum (enum values)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS "pg_catalog.pg_enum" (
            oid INTEGER PRIMARY KEY,
            enumtypid INTEGER NOT NULL,
            enumsortorder REAL NOT NULL,
            enumlabel TEXT NOT NULL
        )
    """)
    
    # Create pg_roles (database roles/users)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS "pg_catalog.pg_roles" (
            rolname TEXT NOT NULL,
            rolsuper BOOLEAN,
            rolinherit BOOLEAN,
            rolcreaterole BOOLEAN,
            rolcreatedb BOOLEAN,
            rolcanlogin BOOLEAN,
            rolreplication BOOLEAN,
            rolconnlimit INTEGER,
            rolpassword TEXT,
            rolvaliduntil TIMESTAMP,
            rolbypassrls BOOLEAN,
            rolconfig TEXT,
            oid INTEGER PRIMARY KEY
        )
    """)
        
    conn.commit()

def populate_catalog_tables(conn: sqlite3.Connection):
    """Populate catalog tables with WinCC schema information."""
    cursor = conn.cursor()
    
    # Insert pg_namespace (schemas)
    cursor.execute("""
        INSERT INTO "pg_catalog.pg_namespace" (oid, nspname, nspowner, nspacl)
        VALUES (11, 'pg_catalog', 11, NULL)
    """)

    cursor.execute("""
        INSERT INTO "pg_catalog.pg_namespace" (oid, nspname, nspowner, nspacl)
        VALUES (2200, 'public', 10, NULL)
    """)
    
    # Insert wincc database (referenced by pg_stat_activity)
    database_oid = 13769
    
    # Insert data types
    for type_name, (oid, name, size, align) in PG_TYPE_MAPPINGS.items():
        cursor.execute("""
            INSERT INTO "pg_catalog.pg_type" (oid, typname, typnamespace, typlen, typbyval, 
                               typtype, typcategory, typalign, typstorage)
            VALUES (?, ?, 11, ?, ?, 'b', 'U', ?, 'p')
        """, (oid, name, size, size > 0 and size <= 8, align))
    
    # Insert tables and columns
    table_oid = 20000
    for table_name, table_info in WINCC_TABLES.items():
        # Insert table into pg_class
        cursor.execute("""
            INSERT INTO "pg_catalog.pg_class" (oid, relname, relnamespace, reltype, relkind, 
                                relnatts, relhasindex, relisshared)
            VALUES (?, ?, 2200, 0, 'r', ?, false, false)
        """, (table_oid, table_name, len(table_info['columns'])))
        
        # Insert table description
        #cursor.execute("""
        #    INSERT INTO "pg_catalog.pg_description" (objoid, classoid, objsubid, description)
        #    VALUES (?, 1259, 0, ?)
        #""", (table_oid, table_info['description']))
        
        # Insert columns
        #for attnum, (col_name, col_type, col_desc) in enumerate(table_info['columns'], 1):
        #    type_oid, _, type_len, _ = PG_TYPE_MAPPINGS[col_type]
        #    
        #    cursor.execute("""
        #        INSERT INTO "pg_catalog.pg_attribute" (attrelid, attname, atttypid, attlen, 
        #                                attnum, attnotnull, attisdropped)
        #        VALUES (?, ?, ?, ?, ?, false, false)
        #    """, (table_oid, col_name, type_oid, type_len, attnum))
        #    
        #    # Insert column description
        #    cursor.execute("""
        #        INSERT INTO "pg_catalog.pg_description" (objoid, classoid, objsubid, description)
        #        VALUES (?, 1259, ?, ?)
        #    """, (table_oid, attnum, col_desc))
        
        table_oid += 1
    
    # Insert database information
    cursor.execute("""
        INSERT INTO "pg_catalog.pg_database" (
            oid, datname, datdba, encoding, datcollate, datctype, datistemplate, 
            datallowconn, datconnlimit, datlastsysoid, datfrozenxid, datminmxid, 
            dattablespace 
        ) VALUES 
            (13769, 'postgres', 10, 6, 'en_US.UTF-8', 'en_US.UTF-8', false, 
             true, -1, 0, 0, 0, 0)
    """)        
    
    conn.commit()

def main():
    """Main function to create and populate the database."""
    db_file = '../catalog.db'
    
    # Create connection
    conn = sqlite3.connect(db_file)
    
    try:
        # Create catalog tables
        print("Creating PostgreSQL catalog tables...")
        create_catalog_tables(conn)
        
        # Populate with WinCC schema
        print("Populating catalog tables with WinCC schema...")
        populate_catalog_tables(conn)
        
        # Verify the data
        cursor = conn.cursor()
        
        print("\nCreated catalog tables:")
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name LIKE 'pg_catalog.pg_%'
            ORDER BY name
        """)
        for row in cursor.fetchall():
            print(f"  - {row[0]}")
        
        print("\nWinCC tables in pg_class:")
        cursor.execute("""
            SELECT c.relname, d.description
            FROM "pg_catalog.pg_class" c
            LEFT JOIN "pg_catalog.pg_description" d ON c.oid = d.objoid AND d.objsubid = 0
            WHERE c.relnamespace = 2200 AND c.relkind = 'r'
            ORDER BY c.relname
        """)
        for row in cursor.fetchall():
            print(f"  - {row[0]}: {row[1]}")
        
        print("\nEnum types and values:")
        cursor.execute("""
            SELECT t.typname, e.enumlabel, e.enumsortorder
            FROM "pg_catalog.pg_type" t
            JOIN "pg_catalog.pg_enum" e ON t.oid = e.enumtypid
            ORDER BY t.typname, e.enumsortorder
        """)
        current_type = None
        for row in cursor.fetchall():
            if current_type != row[0]:
                current_type = row[0]
                print(f"  - {current_type}:")
            print(f"    * {row[1]} (order: {row[2]})")
        
        print(f"\nDatabase created successfully: {db_file}")
        
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
