# DNA ETL Framework

DNA ETL Framework providing standardized patterns, components, and tooling to build reliable, observable, and scalable ETL pipelines at Axpo.

## Features

- **Standardized ETL Components**: Base classes and patterns for extractors, transformers, and loaders
- **Pipeline Orchestration**: Simple yet powerful pipeline management
- **Automatic Versioning**: Git-based versioning with setuptools_scm
- **CI/CD Ready**: GitHub Actions workflow for automated testing and packaging
- **Type Safe**: Full type annotations and mypy support
- **Comprehensive Testing**: Unit tests with pytest and coverage reporting

## Installation

### From PyPI (when published)
```bash
pip install dna-etl
```

### Development Installation
```bash
git clone https://github.com/axpo-ts/dna-etl-framework.git
cd dna-etl-framework
pip install -e .[dev]
```

## Quick Start

```python
from dna_etl import Pipeline, ETLBase

# Create a custom ETL step
class MyExtractor(ETLBase):
    def execute(self):
        # Your extraction logic here
        return "extracted_data"

class MyTransformer(ETLBase):
    def execute(self):
        # Your transformation logic here
        return "transformed_data"

class MyLoader(ETLBase):
    def execute(self):
        # Your loading logic here
        return "loaded_data"

# Create and run a pipeline
pipeline = Pipeline("my_etl_pipeline")
pipeline.add_step(MyExtractor())
pipeline.add_step(MyTransformer())
pipeline.add_step(MyLoader())

results = pipeline.execute()
print(f"Pipeline completed with {len(results)} steps")
```

## Versioning Strategy

This package uses automatic versioning based on git tags and branches:

### Main Branch (Production Releases)
- Pushes to `main` branch trigger automatic version increments
- Creates git tags (e.g., `v1.0.0`, `v1.1.0`) 
- Builds release wheels
- Creates GitHub releases

### Feature Branches (Snapshot Builds)
- Pushes to any other branch create snapshot versions
- Version format: `X.Y.Z.devN+gHASH.dYYYYMMDD` (e.g., `1.0.0.dev0+g1234567.d20240101`)
- Builds snapshot wheels for testing
- No git tags created

## Development

### Setup Development Environment
```bash
# Clone the repository
git clone https://github.com/axpo-ts/dna-etl-framework.git
cd dna-etl-framework

# Install in development mode with all dependencies
pip install -e .[dev]

# Or install from requirements file
pip install -r requirements-dev.txt
pip install -e .
```

### Running Tests
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=dna_etl --cov-report=html

# Run specific test file
pytest tests/test_core.py
```

### Code Quality
```bash
# Format code
black dna_etl tests

# Sort imports
isort dna_etl tests

# Lint code
flake8 dna_etl

# Type checking
mypy dna_etl
```

### Building the Package
```bash
# Build wheel and source distribution
python -m build

# Check the built package
ls dist/
```

### Release Process

The release process is fully automated through GitHub Actions:

1. **Feature Development**: Work on feature branches
   - Automatic snapshot builds on every push
   - Version: `X.Y.Z.devN+gHASH.dYYYYMMDD`

2. **Release**: Merge to main branch
   - Automatic version increment based on conventional commits
   - Creates git tag (e.g., `v1.2.3`)
   - Builds and uploads release wheel
   - Creates GitHub release

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests for your changes
5. Ensure all tests pass (`pytest`)
6. Ensure code quality (`black`, `isort`, `flake8`, `mypy`)
7. Commit your changes (`git commit -m 'Add amazing feature'`)
8. Push to the branch (`git push origin feature/amazing-feature`)
9. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contact

- **Maintainer**: Axpo Trading & Sales Ltd
- **Email**: support@axpo.com
- **Repository**: https://github.com/axpo-ts/dna-etl-framework
