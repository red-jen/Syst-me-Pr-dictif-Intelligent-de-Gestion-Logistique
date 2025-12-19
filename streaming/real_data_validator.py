"""
Real Data Validation Script
This script reads actual historical orders from the CSV, sends them through the streaming pipeline,
and compares model predictions against actual late delivery outcomes.
"""
import pandas as pd
import socket
import time
import json
from datetime import datetime

# Configuration
CSV_PATH = "/src/data/DataCoSupplyChainDataset.csv"
BRIDGE_HOST = "bridge"
BRIDGE_PORT = 9999
BATCH_SIZE = 100  # Number of orders to send
DELAY_BETWEEN_ORDERS = 0.5  # seconds

def send_real_data_for_validation():
    """
    Load real orders, send to pipeline, and prepare for validation
    """
    print("Loading real data from CSV...")
    
    # Read only necessary columns to avoid memory issues
    columns_to_read = [
        'Order Id', 'Days for shipment (scheduled)', 'Days for shipping (real)',
        'Benefit per order', 'Sales per customer', 'Delivery Status',
        'Late_delivery_risk', 'Category Name', 'Customer City',
        'Customer Country', 'Customer Segment', 'Customer State',
        'Department Name', 'Market', 'Order City', 'Order Country',
        'Order Region', 'Order State', 'Order Status', 'Product Price',
        'Shipping Mode', 'Type', 'Order Item Discount',
        'Order Item Quantity', 'Sales', 'Order Item Total'
    ]
    
    try:
        # Read a sample of data
        df = pd.read_csv(CSV_PATH, usecols=columns_to_read, nrows=BATCH_SIZE, encoding='latin-1')
        print(f"Loaded {len(df)} orders from CSV")
        
        # Show distribution of actual late deliveries
        late_count = df['Late_delivery_risk'].sum()
        print(f"\nActual late deliveries in sample: {late_count}/{len(df)} ({late_count/len(df)*100:.1f}%)")
        print(f"Delivery Status distribution:\n{df['Delivery Status'].value_counts()}\n")
        
        # Connect to bridge
        print(f"Connecting to bridge at {BRIDGE_HOST}:{BRIDGE_PORT}...")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((BRIDGE_HOST, BRIDGE_PORT))
        print("Connected!\n")
        
        # Prepare validation results storage
        validation_results = []
        
        # Send each order
        for idx, row in df.iterrows():
            # Create order message in the expected format
            order = {
                'Days for shipment (scheduled)': float(row['Days for shipment (scheduled)']),
                'Days for shipping (real)': float(row['Days for shipping (real)']),
                'Benefit per order': float(row['Benefit per order']),
                'Sales per customer': float(row['Sales per customer']),
                'Delivery Status': str(row['Delivery Status']),
                'Category Name': str(row['Category Name']),
                'Customer City': str(row['Customer City']),
                'Customer Country': str(row['Customer Country']),
                'Customer Segment': str(row['Customer Segment']),
                'Customer State': str(row['Customer State']),
                'Department Name': str(row['Department Name']),
                'Market': str(row['Market']),
                'Order City': str(row['Order City']),
                'Order Country': str(row['Order Country']),
                'Order Region': str(row['Order Region']),
                'Order State': str(row['Order State']),
                'Order Status': str(row['Order Status']),
                'Product Price': float(row['Product Price']),
                'Shipping Mode': str(row['Shipping Mode']),
                'Type': str(row['Type']),
                'Order Item Discount': float(row['Order Item Discount']),
                'Order Item Quantity': float(row['Order Item Quantity']),
                'Sales': float(row['Sales']),
                'Order Item Total': float(row['Order Item Total']),
                'event_ts': datetime.now().isoformat(),
                # Store ground truth for validation
                'actual_late_delivery_risk': int(row['Late_delivery_risk']),
                'order_id': str(row['Order Id'])
            }
            
            message = json.dumps(order) + "\n"
            sock.sendall(message.encode('utf-8'))
            
            # Store for validation
            validation_results.append({
                'order_id': order['order_id'],
                'actual_late': order['actual_late_delivery_risk'],
                'market': order['Market'],
                'delivery_status': order['Delivery Status']
            })
            
            if (idx + 1) % 10 == 0:
                print(f"Sent {idx + 1}/{len(df)} orders...")
            
            time.sleep(DELAY_BETWEEN_ORDERS)
        
        sock.close()
        print(f"\n✓ Successfully sent {len(df)} real orders for validation!")
        print("\nNow check:")
        print("1. PostgreSQL 'stream_predictions' table for model predictions")
        print("2. Compare predictions against actual 'Late_delivery_risk' values")
        print("\nTo validate, run the validation query script (see below)")
        
        # Save validation reference
        validation_df = pd.DataFrame(validation_results)
        validation_df.to_csv('/src/streaming/validation_reference.csv', index=False)
        print("\n✓ Saved validation reference to: streaming/validation_reference.csv")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    send_real_data_for_validation()
