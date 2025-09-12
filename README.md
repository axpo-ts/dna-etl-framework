# DNA ETL Framework

DNA ETL Framework providing standardized patterns, components, and tooling to build reliable, observable, and scalable ETL pipelines at Axpo.

## Features

- **TypeScript Support**: Built with TypeScript for type safety and better developer experience
- **Base ETL Pipeline**: Abstract base class for implementing ETL pipelines
- **Error Handling**: Built-in error handling and logging
- **Testing**: Comprehensive test suite with Jest
- **Azure Artifacts Publishing**: Automated publishing to Azure Artifacts npm registry

## Installation

```bash
npm install @axpo-ts/dna-etl-framework
```

## Usage

### Basic ETL Pipeline

```typescript
import { BaseETLPipeline, ETLConfig } from '@axpo-ts/dna-etl-framework';

class MyETLPipeline extends BaseETLPipeline {
  constructor(config: ETLConfig) {
    super(config);
  }

  async extract(): Promise<any> {
    // Implement your data extraction logic
    return await this.fetchDataFromSource();
  }

  async transform(data: any): Promise<any> {
    // Implement your data transformation logic
    return this.processData(data);
  }

  async load(data: any): Promise<void> {
    // Implement your data loading logic
    await this.saveDataToDestination(data);
  }

  private async fetchDataFromSource(): Promise<any> {
    // Your source-specific implementation
  }

  private processData(data: any): any {
    // Your transformation logic
  }

  private async saveDataToDestination(data: any): Promise<void> {
    // Your destination-specific implementation
  }
}

// Usage
const config: ETLConfig = {
  name: 'my-etl-pipeline',
  version: '1.0.0',
  source: 'database-connection',
  destination: 'data-warehouse'
};

const pipeline = new MyETLPipeline(config);
await pipeline.execute();
```

## Development

### Prerequisites

- Node.js 18+
- npm 7+
- Access to Axpo Azure Artifacts registry

### Setup

1. Clone the repository
2. Install dependencies:
   ```bash
   npm install
   ```

3. Build the project:
   ```bash
   npm run build
   ```

4. Run tests:
   ```bash
   npm test
   ```

5. Run linting:
   ```bash
   npm run lint
   ```

### Publishing to Azure Artifacts

This package is automatically published to Azure Artifacts npm registry when changes are pushed to the `main` branch.

#### Manual Publishing

If you need to publish manually:

1. Ensure you're authenticated with Azure CLI:
   ```bash
   az login
   ```

2. Set up npm authentication:
   ```bash
   npx vsts-npm-auth -config .npmrc
   ```

3. Build and publish:
   ```bash
   npm run build
   npm publish
   ```

#### CI/CD Pipeline

The project includes both Azure DevOps Pipeline and GitHub Actions workflows for automated publishing:

- **Azure DevOps Pipeline**: `.azure-pipelines/azure-pipelines.yml`
- **GitHub Actions**: `.github/workflows/publish.yml`

The pipeline will:
1. Install dependencies
2. Run linting and tests
3. Build the TypeScript code
4. Publish to Azure Artifacts (only on main branch)

#### Required Secrets

For the CI/CD pipeline to work, you need to set up the following:

**For Azure DevOps:**
- Service connection named `axpo-ts-artifacts` for Azure Artifacts

**For GitHub Actions:**
- Secret `AZURE_ARTIFACTS_TOKEN` with your Azure Artifacts personal access token

### Azure Artifacts Configuration

The package is configured to publish to:
- Registry: `https://pkgs.dev.azure.com/axpo/_packaging/axpo-ts/npm/registry/`
- Scope: `@axpo-ts`

Authentication is handled through the `.npmrc` file which references the Azure Artifacts registry.

## API Documentation

### ETLConfig

Configuration interface for ETL pipelines:

```typescript
interface ETLConfig {
  name: string;           // Pipeline name
  version: string;        // Pipeline version
  source: string;         // Data source identifier
  destination: string;    // Data destination identifier
  transforms?: string[];  // Optional transformation steps
}
```

### BaseETLPipeline

Abstract base class for ETL pipelines:

```typescript
abstract class BaseETLPipeline {
  public readonly name: string;
  public readonly version: string;

  constructor(config: ETLConfig);
  
  abstract extract(): Promise<any>;
  abstract transform(data: any): Promise<any>;
  abstract load(data: any): Promise<void>;
  
  async execute(): Promise<void>;
}
```

## License

MIT
