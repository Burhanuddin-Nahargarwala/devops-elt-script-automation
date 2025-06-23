import pytest
import pandas as pd


import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.transformations import transform_data

# This is "mock" data that simulates the real API response.
@pytest.fixture
def sample_api_data():
    """Provides a sample of raw data for testing."""
    return [
        {
            "id": 1,
            "name": "Leanne Graham",
            "username": "Bret",
            "email": "Sincere@april.biz",
            "address": { "street": "Kulas Light", "city": "Gwenborough" },
            "phone": "1-770-736-8031 x56442",
            "website": "hildegard.org",
            "company": { "name": "Romaguera-Crona", "catchPhrase": "Multi-layered client-server neural-net" }
        }
    ]

def test_transform_data_structure_and_columns(sample_api_data):
    """
    Tests that the transform_data function produces a DataFrame
    with the correct columns and structure.
    """
    # Arrange: Get the mock data
    
    # Act: Run the function we are testing
    result_df = transform_data(sample_api_data)
    
    # Assert: Check if the output is correct
    assert isinstance(result_df, pd.DataFrame)
    
    expected_columns = ['id', 'name', 'username', 'email', 'city', 'company']
    assert list(result_df.columns) == expected_columns
    
    assert result_df.iloc[0]['city'] == 'Gwenborough'
    assert result_df.iloc[0]['company'] == 'Romaguera-Crona'
    assert len(result_df) == 1