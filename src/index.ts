/**
 * DNA ETL Framework
 * 
 * Provides standardized patterns, components, and tooling 
 * to build reliable, observable, and scalable ETL pipelines at Axpo.
 */

export interface ETLPipeline {
  name: string;
  version: string;
  extract(): Promise<any>;
  transform(data: any): Promise<any>;
  load(data: any): Promise<void>;
}

export interface ETLConfig {
  name: string;
  version: string;
  source: string;
  destination: string;
  transforms?: string[];
}

/**
 * Base ETL Pipeline class providing common functionality
 */
export abstract class BaseETLPipeline implements ETLPipeline {
  public readonly name: string;
  public readonly version: string;

  constructor(config: ETLConfig) {
    this.name = config.name;
    this.version = config.version;
  }

  abstract extract(): Promise<any>;
  abstract transform(data: any): Promise<any>;
  abstract load(data: any): Promise<void>;

  /**
   * Execute the complete ETL pipeline
   */
  async execute(): Promise<void> {
    console.log(`Starting ETL pipeline: ${this.name} v${this.version}`);
    
    try {
      const extractedData = await this.extract();
      const transformedData = await this.transform(extractedData);
      await this.load(transformedData);
      
      console.log(`ETL pipeline completed successfully: ${this.name}`);
    } catch (error) {
      console.error(`ETL pipeline failed: ${this.name}`, error);
      throw error;
    }
  }
}

/**
 * Framework version
 */
export const version = '1.0.0';

/**
 * Default export
 */
export default {
  BaseETLPipeline,
  version
};