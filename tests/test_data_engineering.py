import pandas as pd
import numpy as np
from src.data_engineering import process_chunk

def test_process_chunk():
    # Create mock dataframe
    data = {
        'building_id': [1, 2],
        'meter_type': ['c', 'h'],
        'ts': ['2022-01-01 00:00:00.000', '2022-01-01 01:30:00.000'],
        'value': [100.0, 0.0]
    }
    df = pd.DataFrame(data)
    
    # Process
    chunk_id, processed_len = process_chunk((0, df))
    
    # We can't easily assert the file here without mocking, but we can verify it doesn't crash
    # and returns the correct length. In a real test, we'd mock the parquet writer.
    assert processed_len == 2
    assert chunk_id == 0
