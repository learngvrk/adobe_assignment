"""Tests for src/lambda/handler.py — Lambda entry point with mocked S3."""

import importlib
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

SAMPLE_DATA = Path(__file__).parent.parent / "requirements" / "data[98].sql"

# Ensure src/ is on path before importing the handler module
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# 'lambda' is a Python keyword — use importlib to import
handler_module = importlib.import_module("lambda.handler")


@pytest.fixture
def sample_tsv():
    with open(SAMPLE_DATA) as f:
        return f.read()


@pytest.fixture
def s3_event():
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "test-input-bucket"},
                    "object": {"key": "data/sample.tsv"},
                }
            }
        ]
    }


@patch.object(handler_module, "s3")
@patch.object(handler_module, "OUTPUT_BUCKET", "test-output-bucket")
@patch.object(handler_module, "OUTPUT_PREFIX", "results/")
def test_handler_success(mock_s3, s3_event, sample_tsv):
    """Handler reads TSV, processes it, and writes output to S3."""
    mock_body = MagicMock()
    mock_body.read.return_value = sample_tsv.encode("utf-8")
    mock_s3.get_object.return_value = {"Body": mock_body}

    result = handler_module.handler(s3_event, None)

    # Verify read from correct bucket/key
    mock_s3.get_object.assert_called_once_with(
        Bucket="test-input-bucket", Key="data/sample.tsv"
    )

    # Verify write to output bucket
    mock_s3.put_object.assert_called_once()
    call_kwargs = mock_s3.put_object.call_args[1]
    assert call_kwargs["Bucket"] == "test-output-bucket"
    assert call_kwargs["Key"].startswith("results/")
    assert call_kwargs["Key"].endswith("_SearchKeywordPerformance.tab")
    assert call_kwargs["ContentType"] == "text/tab-separated-values"

    # Verify response
    assert result["statusCode"] == 200
    assert result["keyword_groups"] == 2


@patch.object(handler_module, "s3")
@patch.object(handler_module, "OUTPUT_BUCKET", "test-output-bucket")
@patch.object(handler_module, "OUTPUT_PREFIX", "results/")
def test_handler_output_content(mock_s3, s3_event, sample_tsv):
    """Verify the actual content written to S3 contains expected data."""
    mock_body = MagicMock()
    mock_body.read.return_value = sample_tsv.encode("utf-8")
    mock_s3.get_object.return_value = {"Body": mock_body}

    handler_module.handler(s3_event, None)

    written_body = mock_s3.put_object.call_args[1]["Body"]
    content = written_body.decode("utf-8")
    assert "google.com" in content
    assert "ipod" in content
    assert "480.00" in content
    assert "bing.com" in content
    assert "zune" in content
    assert "250.00" in content


@patch.object(handler_module, "s3")
@patch.object(handler_module, "OUTPUT_BUCKET", "")
@patch.object(handler_module, "OUTPUT_PREFIX", "output/")
def test_handler_fallback_to_input_bucket(mock_s3, s3_event, sample_tsv):
    """When OUTPUT_BUCKET is empty, handler writes to input bucket."""
    mock_body = MagicMock()
    mock_body.read.return_value = sample_tsv.encode("utf-8")
    mock_s3.get_object.return_value = {"Body": mock_body}

    handler_module.handler(s3_event, None)

    call_kwargs = mock_s3.put_object.call_args[1]
    assert call_kwargs["Bucket"] == "test-input-bucket"
