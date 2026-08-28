import pandas as pd
import numpy as np
import multiprocessing as mp
import os
import glob
from pathlib import Path
import time
import argparse

def process_chunk(chunk_data):
    """
    CPU-bound vectorized data processing function that runs in a separate process.
    Bypasses GIL by using NumPy vectorization and Pandas C-level operations.
    """
    chunk_id, df = chunk_data
    
    # 1. Vectorized Datetime parsing (Pushing to C-level)
    # Using format='%Y-%m-%d %H:%M:%S.%f' speeds up parsing vs inferred
    df['ts'] = pd.to_datetime(df['ts'], errors='coerce', exact=False)
    
    # 2. Extract temporal features using vectorized dt accessor
    df['hour'] = df['ts'].dt.hour.astype(np.int8)
    df['day_of_week'] = df['ts'].dt.dayofweek.astype(np.int8)
    df['month'] = df['ts'].dt.month.astype(np.int8)
    
    # 3. Vectorized anomaly detection using NumPy (e.g., Z-score approximation per chunk)
    # This simulates a complex feature extraction
    values = df['value'].to_numpy()
    mean_val = np.nanmean(values)
    std_val = np.nanstd(values)
    
    # Avoid divide by zero
    if std_val > 0:
        df['value_zscore'] = (values - mean_val) / std_val
    else:
        df['value_zscore'] = 0.0
        
    # 4. NumPy vectorized log transformation to handle right-skewed data
    df['log_value'] = np.log1p(df['value'].clip(lower=0))
    
    # Save the processed chunk to Parquet
    out_dir = 'data/processed/meters/'
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f'part-{chunk_id:05d}.parquet')
    
    # Write to parquet
    df.to_parquet(out_path, engine='pyarrow', index=False)
    return chunk_id, len(df)

def main():
    parser = argparse.ArgumentParser(description="Multi-core Data Engineering Pipeline")
    parser.add_argument('--workers', type=int, default=mp.cpu_count(), help="Number of CPU cores to use")
    parser.add_argument('--chunksize', type=int, default=500000, help="Rows per chunk")
    args = parser.parse_args()

    print(f"Starting Multi-core Processing with {args.workers} workers.")
    print(f"Chunk size: {args.chunksize} rows.")
    
    start_time = time.time()
    
    # Create an iterator that reads CSV in chunks
    csv_path = 'data/meters.csv'
    
    # Ensure processed directory is clean
    processed_dir = 'data/processed/meters/'
    if os.path.exists(processed_dir):
        for f in glob.glob(os.path.join(processed_dir, '*.parquet')):
            os.remove(f)
    
    chunk_iterator = pd.read_csv(csv_path, chunksize=args.chunksize)
    
    # We use enumerate to give each chunk a unique ID
    def generate_chunks():
        for i, chunk in enumerate(chunk_iterator):
            yield (i, chunk)
            
    total_rows = 0
    chunks_processed = 0
    
    # Utilize multiprocessing Pool to bypass Python GIL and process chunks in parallel
    with mp.Pool(processes=args.workers) as pool:
        # imap_unordered is more memory efficient as it yields results as soon as they are ready
        for chunk_id, num_rows in pool.imap_unordered(process_chunk, generate_chunks()):
            total_rows += num_rows
            chunks_processed += 1
            if chunks_processed % 5 == 0:
                print(f"Processed {chunks_processed} chunks... ({total_rows:,} rows)")

    end_time = time.time()
    duration = end_time - start_time
    print(f"\n--- Processing Complete ---")
    print(f"Total Rows Processed: {total_rows:,}")
    print(f"Total Time: {duration:.2f} seconds")
    print(f"Throughput: {total_rows / duration:,.0f} rows/second")
    print(f"Processed output saved to: {processed_dir}")
    
    # Also process weather and building info quickly in single thread since they are small
    print("Processing smaller dimension tables...")
    weather_df = pd.read_csv('data/weather.csv')
    weather_df['timestamp'] = pd.to_datetime(weather_df['timestamp'], errors='coerce', exact=False)
    weather_df.to_parquet('data/processed/weather.parquet', engine='pyarrow', index=False)
    
    bldg_df = pd.read_csv('data/building_information.csv')
    bldg_df.to_parquet('data/processed/building_information.parquet', engine='pyarrow', index=False)
    print("Dimension tables saved as Parquet.")

if __name__ == '__main__':
    main()
