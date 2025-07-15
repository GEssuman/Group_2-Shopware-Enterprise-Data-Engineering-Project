import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from io import StringIO

import batch.pos.services.validator.validate as validate_mode

@pytest.fixture
def sample_df():
    data = {
        "transaction_id": ["T1001", "T1002"],
        "store_id": [101, 102],
        "product_id": [2001, 2002],
        "quantity": [2, 3],
        "revenue": [50.5, 75.0],
        "discount_applied": [0.1, 0.0],
        "timestamp": [1721033100.0, 1721034000.0]
    }
    return pd.DataFrame(data)


def test_list_files_success():
    mock_response = {
        "Contents": [
            {"Key": "POS/pos_data1.csv"},
            {"Key": "POS/pos_data2.csv"},
            {"Key": "POS/readme.txt"},
        ]
    }

    with patch.object(validate_mode.s3, "list_objects_v2", return_value=mock_response):
        files = validate_mode.list_files("test-bucket", "POS")
        assert len(files) == 2
        assert all(file.endswith(".csv") for file in files)

def test_list_files_no_contents():
    with patch.object(validate_mode.s3, "list_objects_v2", return_value={}):
        files = validate_mode.list_files("test-bucket", "POS")
        assert files == []