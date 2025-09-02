import { ElasticsearchClient, ElasticsearchClientConfig } from './lib/connect';
import * as OpenSearchTypes from '@opensearch-project/opensearch/api/types';

/**
 * Configuration for checksum operations
 */
export interface ChecksumSettings {
  /** The Elasticsearch index to operate on */
  index: string;
  /** The document type (optional, deprecated in newer ES versions) */
  type?: string;
  /** The column/field to use as the primary ID for range operations */
  id_column: string;
  /** Optional separate column for document _id (if different from id_column) */
  _id_column?: string;
  /** Array of field names to include in checksum calculations */
  fields: string[];
  /** Optional additional query to filter documents */
  query?: OpenSearchTypes.QueryDslQueryContainer;
  /** Whether to track total hits accurately */
  track_total_hits?: boolean;
}

/**
 * Checksum data for batch operations
 */
export interface ChecksumData {
  /** Starting value for range operations */
  start: string | number;
  /** Ending value for range operations */
  end: string | number;
  /** Array of specific IDs to process */
  ids?: (string | number)[];
}

/**
 * Nibble operation data
 */
export interface NibbleData extends ChecksumData {
  /** Current position in the nibble operation */
  current?: string | number | null;
  /** Next position in the nibble operation */
  next?: string | number | null;
  /** Number of records to limit */
  limit: number;
  /** Whether to reverse the sort order */
  reverse?: boolean;
}

/**
 * Event structure for checksum operations
 */
export interface ChecksumEvent<T = ChecksumData | NibbleData> {
  /** The checksum data payload */
  data: T;
  /** Settings for the checksum operation */
  settings: ChecksumSettings;
  /** Optional session information */
  session?: any;
}

/**
 * Batch checksum result
 */
export interface BatchChecksumResult {
  /** Array of IDs processed */
  ids?: (string | number)[];
  /** Starting value of the range */
  start: string | number;
  /** Ending value of the range */
  end: string | number;
  /** Number of documents processed */
  qty: number;
  /** Calculated hash values as array of 4 numbers */
  hash: [number, number, number, number];
}

/**
 * Individual checksum item
 */
export interface IndividualChecksumItem {
  /** The document ID */
  id: string | number;
  /** The document _id (if different from id) */
  _id?: string | number;
  /** The calculated hash for this document */
  hash: string;
}

/**
 * Individual checksum result
 */
export interface IndividualChecksumResult {
  /** Array of IDs processed */
  ids: (string | number)[];
  /** Starting value of the range */
  start: string | number;
  /** Ending value of the range */
  end: string | number;
  /** Number of documents processed */
  qty: number;
  /** Array of individual checksums */
  checksums: IndividualChecksumItem[];
}

/**
 * Sample checksum result
 */
export interface SampleChecksumResult {
  /** Array of IDs processed */
  ids: (string | number)[];
  /** Starting value of the range */
  start: string | number;
  /** Ending value of the range */
  end: string | number;
  /** Number of documents processed */
  qty: number;
  /** Array of field values for each document */
  checksums: any[][];
}

/**
 * Range result for min/max operations
 */
export interface RangeResult {
  /** Minimum value found */
  min: string | number;
  /** Maximum value found */
  max: string | number;
  /** Total number of documents */
  total: number;
}

/**
 * Standard callback function for checksum operations
 */
export type ChecksumCallback<T = any> = (error?: Error | string | null, result?: T) => void;

/**
 * Checksum handler interface
 * Provides methods for calculating and comparing checksums across Elasticsearch documents
 */
export interface ChecksumHandler {
  /**
   * Calculate batch checksums for a range of documents
   * 
   * Processes documents within a specified range and calculates aggregate hash values.
   * Useful for comparing large datasets efficiently.
   * 
   * @param event Event containing checksum data and settings
   * @param callback Callback function to handle the result
   * @example
   * ```typescript
   * checksumHandler.batch({
   *   data: { start: 1, end: 1000 },
   *   settings: {
   *     index: 'my-index',
   *     id_column: 'timestamp',
   *     fields: ['name', 'email', 'status']
   *   }
   * }, (err, result) => {
   *   if (err) console.error(err);
   *   else console.log(`Processed ${result.qty} documents with hash:`, result.hash);
   * });
   * ```
   */
  batch(event: ChecksumEvent<ChecksumData>, callback: ChecksumCallback<BatchChecksumResult>): void;

  /**
   * Calculate individual checksums for documents
   * 
   * Processes documents and returns individual hash values for each document.
   * Useful for detailed comparison and identifying specific changed documents.
   * 
   * @param event Event containing checksum data and settings
   * @param callback Callback function to handle the result
   * @example
   * ```typescript
   * checksumHandler.individual({
   *   data: { start: 1, end: 100 },
   *   settings: {
   *     index: 'my-index',
   *     id_column: 'id',
   *     fields: ['name', 'email']
   *   }
   * }, (err, result) => {
   *   result.checksums.forEach(item => {
   *     console.log(`Document ${item.id}: ${item.hash}`);
   *   });
   * });
   * ```
   */
  individual(event: ChecksumEvent<ChecksumData>, callback: ChecksumCallback<IndividualChecksumResult>): void;

  /**
   * Get sample data for specific document IDs
   * 
   * Retrieves the actual field values for specified documents.
   * Useful for debugging checksum differences and examining data.
   * 
   * @param event Event containing IDs and settings
   * @param callback Callback function to handle the result
   * @example
   * ```typescript
   * checksumHandler.sample({
   *   data: { ids: [1, 2, 3], start: 1, end: 3 },
   *   settings: {
   *     index: 'my-index',
   *     id_column: 'id',
   *     fields: ['name', 'email', 'created_at']
   *   }
   * }, (err, result) => {
   *   result.checksums.forEach((fields, index) => {
   *     console.log(`Document ${result.ids[index]}:`, fields);
   *   });
   * });
   * ```
   */
  sample(event: ChecksumEvent<ChecksumData>, callback: ChecksumCallback<SampleChecksumResult>): void;

  /**
   * Find the range (min/max) values for the ID column
   * 
   * Determines the minimum and maximum values in the specified ID column.
   * Useful for establishing ranges for batch operations.
   * 
   * @param event Event containing settings
   * @param callback Callback function to handle the result
   * @example
   * ```typescript
   * checksumHandler.range({
   *   data: {},
   *   settings: {
   *     index: 'my-index',
   *     id_column: 'timestamp',
   *     fields: []
   *   }
   * }, (err, result) => {
   *   console.log(`Range: ${result.min} to ${result.max}, Total: ${result.total}`);
   * });
   * ```
   */
  range(event: ChecksumEvent<any>, callback: ChecksumCallback<RangeResult>): void;

  /**
   * Perform nibble operation to find next batch boundaries
   * 
   * Helps determine appropriate batch sizes and boundaries for processing.
   * Useful for pagination and incremental processing.
   * 
   * @param event Event containing nibble data and settings
   * @param callback Callback function to handle the result
   * @example
   * ```typescript
   * checksumHandler.nibble({
   *   data: { start: 1, limit: 1000 },
   *   settings: {
   *     index: 'my-index',
   *     id_column: 'id',
   *     fields: []
   *   }
   * }, (err, result) => {
   *   console.log(`Current: ${result.current}, Next: ${result.next}`);
   * });
   * ```
   */
  nibble(event: ChecksumEvent<NibbleData>, callback: ChecksumCallback<NibbleData>): void;

  /**
   * Initialize the checksum handler
   * 
   * Performs any necessary initialization. Currently a no-op.
   * 
   * @param event Event data
   * @param callback Callback function
   */
  initialize(event: any, callback: ChecksumCallback<{}>): void;

  /**
   * Clean up and destroy the checksum handler
   * 
   * Performs any necessary cleanup. Currently a no-op.
   * 
   * @param event Event data
   * @param callback Callback function
   */
  destroy(event: any, callback: ChecksumCallback<void>): void;

  /**
   * Handler function for AWS Lambda or similar environments
   * 
   * Processes checksum requests based on the method specified in query parameters.
   * 
   * @param event Lambda event containing method and data
   * @param context Lambda context
   * @param callback Lambda callback function
   */
  handler(event: any, context: any, callback: ChecksumCallback<any>): void;
}

/**
 * Main module interface for the Elasticsearch connector
 */
export interface ElasticsearchConnector {
  /**
   * Create a direct connection to Elasticsearch/OpenSearch
   * 
   * Returns a client instance with all the search, indexing, and streaming capabilities.
   * 
   * @param config Configuration for the Elasticsearch client
   * @returns Configured Elasticsearch client
   * @example
   * ```typescript
   * const client = connector.connect('https://my-cluster.es.amazonaws.com');
   * 
   * // Or with full configuration
   * const client = connector.connect({
   *   host: 'https://my-cluster.es.amazonaws.com',
   *   awsConfig: { region: 'us-east-1' },
   *   returnFullResponse: false
   * });
   * ```
   */
  connect(config?: ElasticsearchClient | string | ElasticsearchClientConfig): ElasticsearchClient;

  /**
   * Create a checksum handler for data integrity operations
   * 
   * Returns a handler that can calculate and compare checksums across documents.
   * Useful for data synchronization and integrity checking.
   * 
   * @param config Configuration for the Elasticsearch client
   * @returns Checksum handler with batch, individual, and other operations
   * @example
   * ```typescript
   * const checksumHandler = connector.checksum({
   *   host: 'https://my-cluster.es.amazonaws.com'
   * });
   * 
   * // Calculate batch checksums
   * checksumHandler.batch({
   *   data: { start: 1, end: 1000 },
   *   settings: {
   *     index: 'my-index',
   *     id_column: 'id',
   *     fields: ['name', 'email', 'status']
   *   }
   * }, (err, result) => {
   *   console.log(`Checksum for ${result.qty} documents:`, result.hash);
   * });
   * ```
   */
  checksum(config?: ElasticsearchClient | string | ElasticsearchClientConfig): ChecksumHandler;
}

/**
 * Leo Elasticsearch Connector
 * 
 * A comprehensive Elasticsearch/OpenSearch connector for the Leo platform that provides:
 * - Direct client connections with AWS authentication
 * - Streaming capabilities for high-throughput operations  
 * - Checksum operations for data integrity and synchronization
 * - Full TypeScript support with OpenSearch type definitions
 * 
 * @example
 * ```typescript
 * import elasticsearchConnector from 'leo-connector-elasticsearch';
 * 
 * // Create a client connection
 * const client = elasticsearchConnector.connect('https://my-cluster.es.amazonaws.com');
 * 
 * // Search for documents
 * const results = await client.queryWithScroll({
 *   index: 'my-index',
 *   body: { query: { match_all: {} } },
 *   scroll: '15s'
 * });
 * 
 * // Create checksum handler
 * const checksumHandler = elasticsearchConnector.checksum({
 *   host: 'https://my-cluster.es.amazonaws.com'
 * });
 * ```
 */
declare const elasticsearchConnector: ElasticsearchConnector;

export default elasticsearchConnector;

// Re-export types from connect module for convenience
export * from './lib/connect';
