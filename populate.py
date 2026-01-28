#!/usr/bin/env python3
"""
Shipping Data Processor

This script reads shipping data from multiple CSV spreadsheets with different
schemas, combines them, and populates a SQLite database.

Spreadsheets:
- shipping_data_0.csv: Self-contained data with origin, destination, product, quantity
- shipping_data_1.csv: Individual product rows per shipment (count = quantity)
- shipping_data_2.csv: Origin/destination mapping for shipments in data_1
"""

import csv
import sqlite3
from collections import defaultdict


def get_or_create_product(cursor, product_name):
    """
    Get the product ID for a product name. Creates the product if it doesn't exist.
    
    Args:
        cursor: SQLite database cursor
        product_name: Name of the product
        
    Returns:
        int: The product ID
    """
    # Try to find existing product
    cursor.execute("SELECT id FROM product WHERE name = ?", (product_name,))
    result = cursor.fetchone()
    
    if result:
        return result[0]
    
    # Create new product
    cursor.execute("INSERT INTO product (name) VALUES (?)", (product_name,))
    return cursor.lastrowid


def insert_shipment(cursor, product_id, quantity, origin, destination):
    """
    Insert a shipment record into the database.
    
    Args:
        cursor: SQLite database cursor
        product_id: ID of the product
        quantity: Quantity of the product in the shipment
        origin: Origin warehouse identifier
        destination: Destination store identifier
    """
    cursor.execute(
        "INSERT INTO shipment (product_id, quantity, origin, destination) VALUES (?, ?, ?, ?)",
        (product_id, quantity, origin, destination)
    )


def process_shipping_data_0(cursor, filepath):
    """
    Process shipping_data_0.csv - self-contained data that can be directly inserted.
    
    Each row contains: origin_warehouse, destination_store, product, on_time, 
                       product_quantity, driver_identifier
    """
    with open(filepath, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            product_name = row['product']
            quantity = int(row['product_quantity'])
            origin = row['origin_warehouse']
            destination = row['destination_store']
            
            product_id = get_or_create_product(cursor, product_name)
            insert_shipment(cursor, product_id, quantity, origin, destination)


def process_shipping_data_1_and_2(cursor, filepath_1, filepath_2):
    """
    Process shipping_data_1.csv and shipping_data_2.csv together.
    
    Data 1 contains one row per product item (need to count for quantity).
    Data 2 contains origin/destination information keyed by shipment_identifier.
    """
    # First, load shipping_data_2 into a dictionary for quick lookup
    shipment_info = {}
    with open(filepath_2, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            shipment_id = row['shipment_identifier']
            shipment_info[shipment_id] = {
                'origin': row['origin_warehouse'],
                'destination': row['destination_store']
            }
    
    # Process shipping_data_1 and aggregate by (shipment_id, product)
    # Each row represents 1 unit of a product, so count occurrences for quantity
    product_counts = defaultdict(lambda: defaultdict(int))
    
    with open(filepath_1, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            shipment_id = row['shipment_identifier']
            product_name = row['product']
            product_counts[shipment_id][product_name] += 1
    
    # Insert records for each unique (shipment_id, product) combination
    for shipment_id, products in product_counts.items():
        if shipment_id not in shipment_info:
            print(f"Warning: No origin/destination found for shipment {shipment_id}")
            continue
            
        info = shipment_info[shipment_id]
        origin = info['origin']
        destination = info['destination']
        
        for product_name, quantity in products.items():
            product_id = get_or_create_product(cursor, product_name)
            insert_shipment(cursor, product_id, quantity, origin, destination)


def main():
    """Main function to process all shipping data and populate the database."""
    # Connect to the SQLite database
    conn = sqlite3.connect('shipment_database.db')
    cursor = conn.cursor()
    
    try:
        # Process shipping_data_0.csv (self-contained)
        print("Processing shipping_data_0.csv...")
        process_shipping_data_0(cursor, 'data/shipping_data_0.csv')
        
        # Process shipping_data_1.csv and shipping_data_2.csv (dependent)
        print("Processing shipping_data_1.csv with shipping_data_2.csv...")
        process_shipping_data_1_and_2(
            cursor,
            'data/shipping_data_1.csv',
            'data/shipping_data_2.csv'
        )
        
        # Commit all changes
        conn.commit()
        print("Database populated successfully!")
        
        # Print summary statistics
        cursor.execute("SELECT COUNT(*) FROM product")
        product_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM shipment")
        shipment_count = cursor.fetchone()[0]
        
        print(f"\nSummary:")
        print(f"  - Total products: {product_count}")
        print(f"  - Total shipments: {shipment_count}")
        
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
