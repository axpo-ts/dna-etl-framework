#!/usr/bin/env python3
"""
Example script demonstrating DNA ETL Framework usage.
This script shows how to use the framework to create and execute ETL pipelines.
"""

import logging
import sys
import os

# Add the package to Python path for demonstration
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

from dna_etl import Pipeline, ETLBase, get_version


class DataExtractor(ETLBase):
    """Example data extractor."""
    
    def __init__(self, config=None):
        super().__init__(config)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def execute(self):
        """Extract sample data."""
        # Simulate data extraction
        sample_data = [
            {"id": 1, "name": "Alice", "value": 100},
            {"id": 2, "name": "Bob", "value": 200},
            {"id": 3, "name": "Charlie", "value": 150},
        ]
        self.logger.info(f"Extracted {len(sample_data)} records")
        return sample_data


class DataTransformer(ETLBase):
    """Example data transformer."""
    
    def __init__(self, config=None):
        super().__init__(config)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def execute(self):
        """Transform data (mock implementation)."""
        # In a real scenario, this would receive data from the previous step
        # For demo purposes, we'll just simulate transformation
        transformation_rules = self.config.get('rules', ['uppercase_names', 'double_values'])
        
        self.logger.info(f"Applying transformation rules: {transformation_rules}")
        
        # Simulate transformed data
        transformed_data = [
            {"id": 1, "name": "ALICE", "value": 200, "processed": True},
            {"id": 2, "name": "BOB", "value": 400, "processed": True},
            {"id": 3, "name": "CHARLIE", "value": 300, "processed": True},
        ]
        
        self.logger.info(f"Transformed {len(transformed_data)} records")
        return transformed_data


class DataLoader(ETLBase):
    """Example data loader."""
    
    def __init__(self, config=None):
        super().__init__(config)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def execute(self):
        """Load data to destination."""
        destination = self.config.get('destination', 'default_warehouse')
        
        self.logger.info(f"Loading data to destination: {destination}")
        
        # Simulate loading process
        loaded_count = 3
        self.logger.info(f"Successfully loaded {loaded_count} records")
        
        return {"status": "success", "loaded_count": loaded_count}


def main():
    """Run the example ETL pipeline."""
    print("DNA ETL Framework - Example Usage")
    print("=" * 50)
    print(f"Framework Version: {get_version()}")
    print()
    
    # Create pipeline
    pipeline = Pipeline(
        name="example_etl_pipeline",
        config={
            "description": "Demonstration of DNA ETL Framework",
            "batch_size": 1000,
            "timeout": 300
        }
    )
    
    # Create ETL steps
    extractor = DataExtractor(config={
        "source": "sample_database",
        "table": "raw_data"
    })
    
    transformer = DataTransformer(config={
        "rules": ["uppercase_names", "double_values", "add_processed_flag"]
    })
    
    loader = DataLoader(config={
        "destination": "data_warehouse",
        "table": "processed_data",
        "mode": "append"
    })
    
    # Add steps to pipeline
    pipeline.add_step(extractor)
    pipeline.add_step(transformer)
    pipeline.add_step(loader)
    
    print(f"Pipeline '{pipeline.name}' created with {len(pipeline.steps)} steps")
    print()
    
    try:
        # Execute pipeline
        print("Starting pipeline execution...")
        results = pipeline.execute()
        
        print()
        print("Pipeline execution completed successfully!")
        print(f"Results: {len(results)} steps executed")
        
        # Display results summary
        for i, result in enumerate(results, 1):
            step_name = pipeline.steps[i-1].__class__.__name__
            print(f"  Step {i} ({step_name}): {result}")
        
        return 0
        
    except Exception as e:
        print(f"Pipeline execution failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())