"""
Validation Script - Compare Model Predictions vs Actual Outcomes
Run this after sending real data through the pipeline
"""
import psycopg2
import pandas as pd
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix, classification_report

# Database connection
POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_DB = "logistics"
POSTGRES_USER = "loguser"
POSTGRES_PASSWORD = "logpass"

def validate_model_performance():
    """
    Query predictions from PostgreSQL and compare against actual outcomes
    """
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    
    # Query predictions from the database
    query = """
    SELECT 
        "Market",
        late_prob,
        event_ts,
        ingest_batch_id
    FROM stream_predictions
    ORDER BY event_ts DESC
    LIMIT 100;
    """
    
    print("Fetching predictions from database...")
    predictions_df = pd.read_sql(query, conn)
    conn.close()
    
    print(f"\nFound {len(predictions_df)} predictions in database")
    
    if len(predictions_df) == 0:
        print("No predictions found! Make sure to run real_data_validator.py first.")
        return
    
    # Load actual outcomes
    try:
        actual_df = pd.read_csv('/src/streaming/validation_reference.csv')
        print(f"Loaded {len(actual_df)} actual outcomes from reference file")
    except FileNotFoundError:
        print("Error: validation_reference.csv not found!")
        print("Run real_data_validator.py first to generate it.")
        return
    
    # Convert probabilities to binary predictions (threshold = 0.5)
    predictions_df['predicted_late'] = (predictions_df['late_prob'] >= 0.5).astype(int)
    
    # Show prediction distribution
    print("\n" + "="*60)
    print("PREDICTION DISTRIBUTION")
    print("="*60)
    print(f"Total predictions: {len(predictions_df)}")
    print(f"Predicted LATE: {predictions_df['predicted_late'].sum()} ({predictions_df['predicted_late'].mean()*100:.1f}%)")
    print(f"Predicted ON-TIME: {(1-predictions_df['predicted_late']).sum()} ({(1-predictions_df['predicted_late']).mean()*100:.1f}%)")
    
    print("\n" + "="*60)
    print("ACTUAL OUTCOMES (from CSV)")
    print("="*60)
    print(f"Total actual orders: {len(actual_df)}")
    print(f"Actually LATE: {actual_df['actual_late'].sum()} ({actual_df['actual_late'].mean()*100:.1f}%)")
    print(f"Actually ON-TIME: {(1-actual_df['actual_late']).sum()} ({(1-actual_df['actual_late']).mean()*100:.1f}%)")
    
    # Since we can't perfectly match orders (due to async processing),
    # we'll compare distributions by market
    print("\n" + "="*60)
    print("PREDICTIONS BY MARKET")
    print("="*60)
    market_pred = predictions_df.groupby('Market').agg({
        'late_prob': ['mean', 'count'],
        'predicted_late': 'mean'
    }).round(3)
    print(market_pred)
    
    print("\n" + "="*60)
    print("ACTUAL LATE DELIVERIES BY MARKET")
    print("="*60)
    market_actual = actual_df.groupby('market').agg({
        'actual_late': ['mean', 'count']
    }).round(3)
    print(market_actual)
    
    # If we have the same number of records, we can do direct comparison
    if len(predictions_df) == len(actual_df):
        print("\n" + "="*60)
        print("MODEL PERFORMANCE METRICS")
        print("="*60)
        
        # Align by index (assumes same order)
        y_true = actual_df['actual_late'].values[:len(predictions_df)]
        y_pred = predictions_df['predicted_late'].values
        
        accuracy = accuracy_score(y_true, y_pred)
        precision = precision_score(y_true, y_pred, zero_division=0)
        recall = recall_score(y_true, y_pred, zero_division=0)
        f1 = f1_score(y_true, y_pred, zero_division=0)
        
        print(f"Accuracy:  {accuracy:.3f}")
        print(f"Precision: {precision:.3f} (of predicted late, how many were actually late)")
        print(f"Recall:    {recall:.3f} (of actually late, how many did we predict)")
        print(f"F1-Score:  {f1:.3f}")
        
        print("\nConfusion Matrix:")
        cm = confusion_matrix(y_true, y_pred)
        print(f"                Predicted")
        print(f"               ON-TIME  LATE")
        print(f"Actual ON-TIME   {cm[0,0]:4d}   {cm[0,1]:4d}")
        print(f"       LATE      {cm[1,0]:4d}   {cm[1,1]:4d}")
        
        print("\nDetailed Classification Report:")
        print(classification_report(y_true, y_pred, 
                                   target_names=['On-Time', 'Late'],
                                   zero_division=0))
    else:
        print("\n⚠ Warning: Number of predictions doesn't match actual outcomes.")
        print("This is expected with streaming data. Use market-level comparison above.")
    
    # Show sample predictions with probabilities
    print("\n" + "="*60)
    print("SAMPLE PREDICTIONS (Latest 10)")
    print("="*60)
    sample = predictions_df[['Market', 'late_prob', 'predicted_late', 'event_ts']].head(10)
    print(sample.to_string(index=False))

if __name__ == "__main__":
    validate_model_performance()
