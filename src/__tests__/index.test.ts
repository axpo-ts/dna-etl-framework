import { BaseETLPipeline, ETLConfig, version } from '../index';

// Mock ETL Pipeline implementation for testing
class TestETLPipeline extends BaseETLPipeline {
  async extract(): Promise<any> {
    return { data: 'test-data' };
  }

  async transform(data: any): Promise<any> {
    return { ...data, transformed: true };
  }

  async load(data: any): Promise<void> {
    console.log('Loading data:', data);
  }
}

describe('DNA ETL Framework', () => {
  let config: ETLConfig;
  let pipeline: TestETLPipeline;

  beforeEach(() => {
    config = {
      name: 'test-pipeline',
      version: '1.0.0',
      source: 'test-source',
      destination: 'test-destination'
    };
    pipeline = new TestETLPipeline(config);
  });

  describe('BaseETLPipeline', () => {
    test('should create pipeline with correct name and version', () => {
      expect(pipeline.name).toBe('test-pipeline');
      expect(pipeline.version).toBe('1.0.0');
    });

    test('should execute complete ETL pipeline', async () => {
      const consoleSpy = jest.spyOn(console, 'log').mockImplementation();
      
      await pipeline.execute();
      
      expect(consoleSpy).toHaveBeenCalledWith('Starting ETL pipeline: test-pipeline v1.0.0');
      expect(consoleSpy).toHaveBeenCalledWith('ETL pipeline completed successfully: test-pipeline');
      
      consoleSpy.mockRestore();
    });

    test('should handle pipeline errors', async () => {
      const errorPipeline = new class extends BaseETLPipeline {
        async extract(): Promise<any> {
          throw new Error('Extract failed');
        }
        async transform(): Promise<any> {
          return {};
        }
        async load(): Promise<void> {}
      }(config);

      const consoleSpy = jest.spyOn(console, 'error').mockImplementation();
      
      await expect(errorPipeline.execute()).rejects.toThrow('Extract failed');
      expect(consoleSpy).toHaveBeenCalledWith('ETL pipeline failed: test-pipeline', expect.any(Error));
      
      consoleSpy.mockRestore();
    });
  });

  describe('Framework', () => {
    test('should export correct version', () => {
      expect(version).toBe('1.0.0');
    });

    test('should export BaseETLPipeline class', () => {
      expect(BaseETLPipeline).toBeDefined();
      expect(typeof BaseETLPipeline).toBe('function');
    });
  });
});