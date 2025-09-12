"""
Core functionality for the DNA ETL Framework.
"""

from typing import Any, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class ETLBase:
    """Base class for all ETL components."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        logger.info(f"Initialized {self.__class__.__name__}")
    
    def execute(self) -> Any:
        """Execute the ETL operation."""
        raise NotImplementedError("Subclasses must implement execute method")


class Pipeline(ETLBase):
    """ETL Pipeline orchestrator."""
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.name = name
        self.steps = []
    
    def add_step(self, step: ETLBase):
        """Add a step to the pipeline."""
        self.steps.append(step)
        logger.info(f"Added step {step.__class__.__name__} to pipeline {self.name}")
    
    def execute(self) -> Any:
        """Execute all pipeline steps in order."""
        logger.info(f"Starting pipeline execution: {self.name}")
        results = []
        
        for step in self.steps:
            try:
                result = step.execute()
                results.append(result)
                logger.info(f"Step {step.__class__.__name__} completed successfully")
            except Exception as e:
                logger.error(f"Step {step.__class__.__name__} failed: {e}")
                raise
        
        logger.info(f"Pipeline {self.name} completed successfully")
        return results


def get_version() -> str:
    """Get the current version of the DNA ETL Framework."""
    from . import __version__
    return __version__