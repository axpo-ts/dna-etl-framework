"""Tests for the DNA ETL Framework package."""
import pytest
from dna_etl import __version__, __author__, __email__
from dna_etl.core import Pipeline, ETLBase, get_version


def test_package_metadata():
    """Test that package metadata is correctly set."""
    assert __author__ == "Axpo Trading & Sales Ltd"
    assert __email__ == "support@axpo.com"
    assert isinstance(__version__, str)
    assert len(__version__) > 0


def test_get_version():
    """Test the get_version function."""
    version = get_version()
    assert isinstance(version, str)
    assert len(version) > 0
    assert version == __version__


class MockETLStep(ETLBase):
    """Mock ETL step for testing."""
    
    def execute(self):
        return f"Executed {self.__class__.__name__}"


def test_etl_base():
    """Test ETLBase functionality."""
    step = MockETLStep({"test": "config"})
    assert step.config == {"test": "config"}
    assert step.execute() == "Executed MockETLStep"


def test_pipeline():
    """Test Pipeline functionality."""
    pipeline = Pipeline("test_pipeline")
    assert pipeline.name == "test_pipeline"
    assert len(pipeline.steps) == 0
    
    # Add steps
    step1 = MockETLStep()
    step2 = MockETLStep()
    
    pipeline.add_step(step1)
    pipeline.add_step(step2)
    
    assert len(pipeline.steps) == 2
    
    # Execute pipeline
    results = pipeline.execute()
    assert len(results) == 2
    assert all("Executed MockETLStep" in result for result in results)


def test_pipeline_with_config():
    """Test Pipeline with configuration."""
    config = {"batch_size": 100, "timeout": 30}
    pipeline = Pipeline("configured_pipeline", config)
    
    assert pipeline.config == config
    assert pipeline.name == "configured_pipeline"


if __name__ == "__main__":
    pytest.main([__file__])