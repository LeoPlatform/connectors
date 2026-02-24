import { 
	Client as OpenSearchClient,
	ApiResponse
} from '@opensearch-project/opensearch';
import { TransportRequestCallback, TransportRequestPromise } from '@opensearch-project/opensearch/lib/Transport';
import * as OpenSearchAPI from '@opensearch-project/opensearch/api';
import * as OpenSearchTypes from '@opensearch-project/opensearch/api/_types';
import { Writable, Transform } from 'stream';

/**
 * Advanced TypeScript type system for handling returnFullResponse behavior
 * 
 * The Leo Elasticsearch connector wraps the OpenSearch client transport layer to modify
 * response handling. When returnFullResponse is false (default), ALL client methods
 * return response.body instead of the full ApiResponse object. This type system
 * ensures TypeScript types accurately reflect this runtime behavior.
 */

/**
 * Transforms Promise-based method return types based on returnFullResponse configuration
 * 
 * In OpenSearch 3.5.1, methods return TransportRequestPromise<SpecificResponse> where
 * SpecificResponse extends ApiResponse and has a body property with the actual data.
 * 
 * @template T - The original method return type
 * @template TReturnFullResponse - Whether to return full response or just response.body
 * 
 * @example
 * ```typescript
 * // When returnFullResponse is false (default):
 * // search() returns Promise<SearchResponseBody> instead of Promise<SearchResponse>
 * 
 * // When returnFullResponse is true:
 * // search() returns Promise<SearchResponse> (original behavior)
 * ```
 */
type TransformReturnType<T, TReturnFullResponse extends boolean | undefined> = 
  T extends TransportRequestPromise<infer TResponse>
    ? TResponse extends { body: infer TBody }
      ? TReturnFullResponse extends true
        ? TransportRequestPromise<TResponse>  // Return full response (SearchResponse, BulkResponse, etc.)
        : Promise<TBody>                      // Return response.body only (SearchResponseBody, BulkResponseBody, etc.)
      : T  // If response doesn't have body property, return as-is
    : T extends TransportRequestCallback
      ? TransportRequestCallback
      : T;

/**
 * Transforms callback-based method signatures to match returnFullResponse behavior
 * 
 * In OpenSearch 3.5.1, callback functions receive SpecificResponse objects that extend ApiResponse.
 * 
 * @template T - The original callback method signature
 * @template TReturnFullResponse - Whether to return full response or just response.body
 * 
 * @example
 * ```typescript
 * // When returnFullResponse is false (default):
 * // search(params, (err, result) => {}) - result is SearchResponseBody
 * 
 * // When returnFullResponse is true:
 * // search(params, (err, result) => {}) - result is SearchResponse
 * ```
 */
type TransformCallbackType<T, TReturnFullResponse extends boolean | undefined> = 
  T extends (callback: (err: any, result: infer TResponse) => void) => any
    ? TResponse extends { body: infer TBody }
      ? TReturnFullResponse extends true
        ? T  // Keep original callback signature
        : (callback: (err: any, result: TBody) => void) => TransportRequestCallback
      : T  // If response doesn't have body property, return as-is
    : T extends (params: infer P, callback: (err: any, result: infer TResponse) => void) => any
      ? TResponse extends { body: infer TBody }
        ? TReturnFullResponse extends true
          ? T  // Keep original callback signature
          : (params: P, callback: (err: any, result: TBody) => void) => TransportRequestCallback
        : T  // If response doesn't have body property, return as-is
      : T extends (params: infer P, options: infer O, callback: (err: any, result: infer TResponse) => void) => any
        ? TResponse extends { body: infer TBody }
          ? TReturnFullResponse extends true
            ? T  // Keep original callback signature
            : (params: P, options: O, callback: (err: any, result: TBody) => void) => TransportRequestCallback
          : T  // If response doesn't have body property, return as-is
        : T;

/**
 * Recursively transforms ALL methods of the OpenSearch client to handle returnFullResponse behavior
 * 
 * This type performs a deep transformation of the entire OpenSearch client interface,
 * including nested objects like client.indices, client.cat, client.cluster, etc.
 * Every method call will have the correct TypeScript types that match runtime behavior.
 * 
 * @template TClient - The OpenSearch client type to transform
 * @template TReturnFullResponse - Whether to return full response or just response.body
 * 
 * @example
 * ```typescript
 * // All these methods are properly typed based on returnFullResponse:
 * client.search()           // Promise<SearchResponse> or Promise<ApiResponse<SearchResponse>>
 * client.bulk()             // Promise<BulkResponse> or Promise<ApiResponse<BulkResponse>>
 * client.indices.create()   // Promise<IndicesCreateResponse> or Promise<ApiResponse<...>>
 * client.cat.health()       // Promise<CatHealthResponse> or Promise<ApiResponse<...>>
 * client.cluster.health()   // Promise<ClusterHealthResponse> or Promise<ApiResponse<...>>
 * ```
 */
type TransformOpenSearchClient<
  TClient extends Record<string, any>,
  TReturnFullResponse extends boolean | undefined
> = {
  [K in keyof TClient]: TClient[K] extends (...args: any[]) => any
    ? TransformReturnType<TransformCallbackType<TClient[K], TReturnFullResponse>, TReturnFullResponse>
    : TClient[K] extends Record<string, any>
      ? TransformOpenSearchClient<TClient[K], TReturnFullResponse>  // Recursively transform nested objects
      : TClient[K];
};

/**
 * Configuration options for the Elasticsearch/OpenSearch client
 */
export interface ElasticsearchClientConfig {
  /** The host URL for the Elasticsearch/OpenSearch cluster */
  host?: string;
  /** The node URL for the Elasticsearch/OpenSearch cluster (alias for host) */
  node?: string;
  /** Whether to suggest compression for requests */
  suggestCompression?: boolean;
  /** AWS configuration for authentication */
  awsConfig?: {
    /** AWS region for the cluster */
    region?: string;
  };
  /** 
   * Whether to return the full response object or just response.body
   * @default false - returns response.body only
   */
  returnFullResponse?: boolean;
}

/**
 * Query parameters for Elasticsearch search operations
 * Based on OpenSearch SearchRequest interface for type safety
 */
export interface ElasticsearchQuery extends Omit<OpenSearchAPI.Search_Request, 'index'> {
  /** The index or indices to search */
  index?: string | string[];
  /** The document type (deprecated in newer ES versions but still supported) */
  type?: string;
}

/**
 * Configuration for scroll-based queries
 * Combines OpenSearch SearchRequest and ScrollRequest interfaces with custom extensions
 */
export interface ScrollQueryData extends Omit<OpenSearchAPI.Search_Request, 'index' | 'source'> {
  /** The index or indices to search */
  index?: string | string[];
  /** The document type (deprecated in newer ES versions but still supported) */
  type?: string;
  /** Fields to include in the response (alias for _source) */
  source?: OpenSearchTypes.Common.Fields | boolean;
  /** Maximum number of results to return (default: 100000) */
  max?: number;
  /** Transform function or predefined transform type */
  return?: 'full' | 'source' | ((item: OpenSearchTypes.Core_Search.Hit) => any);
  /** Existing scroll ID to continue from */
  scrollid?: string;
}

/**
 * Results from a scroll query operation
 * Based on OpenSearch response types with custom aggregation
 */
export interface ScrollQueryResults<TDocument = unknown> {
  /** Array of transformed result items */
  items: TDocument[];
  /** Total number of items retrieved */
  qty: number;
  /** Information about each scroll batch */
  scrolls: Array<{
    qty: number;
    total: OpenSearchTypes.Core_Search.TotalHits;
  }>;
  /** Total time taken for all scroll operations */
  took: number;
  /** Total number of matching documents */
  total?: OpenSearchTypes.Core_Search.TotalHits;
  /** Aggregation results if requested */
  aggregations?: Record<string, OpenSearchTypes.Common_Aggregations.Aggregate>;
  /** Current scroll ID for continuation */
  scrollid?: string;
}

/**
 * Query configuration for getIds operation
 * Based on OpenSearch query structure
 */
export interface GetIdsQuery {
  /** The index to search */
  index: string;
  /** The document type */
  type?: string;
  /** The query DSL object */
  query: OpenSearchTypes.Common_QueryDsl.QueryContainer;
}

/**
 * Stream configuration options
 */
export interface StreamSettings {
  /** System identifier for the stream */
  system?: string;
  /** Whether document type is required */
  requireType?: boolean;
  /** Starting total count */
  startTotal?: number;
  /** Whether to log summary information */
  logSummary?: boolean;
  /** Whether fields should be undefined instead of false */
  fieldsUndefined?: boolean;
  /** Whether to save results to S3 */
  dontSaveResults?: boolean;
  /** Retry on conflict, default is 3 */
  retryOnConflict?: number;
}

/**
 * Parallel stream configuration options
 */
export interface ParallelStreamSettings extends StreamSettings {
  /** Number of parallel operations during warmup */
  warmParallelLimit?: number;
  /** Number of parallel operations */
  parallelLimit?: number;
  /** Buffer configuration */
  buffer?: number | {
    /** Maximum number of records to buffer */
    records?: number;
    /** Maximum bytes to buffer */
    bytes?: number;
    /** Time-based buffer options */
    time?: {
      /** Milliseconds to wait before flushing buffer */
      milliseconds?: number;
    };
  };
  /** Number of records during warmup phase */
  warmup?: number;
}

/**
 * Batch operation configuration
 */
export interface BatchOptions {
  /** Number of records per batch */
  records?: number;
}

/**
 * Document data for stream operations
 * Based on OpenSearch bulk operation structure
 */
export interface StreamDocumentData<TDocument = unknown> {
  /** The target index */
  index: string;
  /** The document type */
  type?: string;
  /** The document ID */
  id: string | string[];
  /** The document data */
  doc?: TDocument;
  /** Whether this is a delete operation */
  delete?: boolean;
  /** Field to use for bulk delete operations */
  field?: string;
}

/**
 * Event structure for stream operations
 */
export interface StreamEvent<TDocument = unknown> {
  /** Event payload containing document data */
  payload?: StreamDocumentData<TDocument>;
  /** Additional event metadata */
  [key: string]: any;
}

/**
 * Standard callback function type
 */
export type Callback<T = any> = (error?: Error | string | null, result?: T) => void;

/**
 * Conditional response type helper - returns body-only or full response based on config
 */
type ConditionalResponse<
  TBody, 
  TFull = ApiResponse, 
  TConfig extends ElasticsearchClientConfig = ElasticsearchClientConfig
> = TConfig['returnFullResponse'] extends true ? TFull : TBody;

/**
 * Core OpenSearch method overrides with conditional response types
 */
interface ElasticsearchCoreMethodOverrides<TConfig extends ElasticsearchClientConfig = ElasticsearchClientConfig> {
  search<TDocument = unknown>(
    params?: ElasticsearchQuery,
    options?: any
  ): Promise<ConditionalResponse<OpenSearchTypes.Core_Search.ResponseBody, OpenSearchAPI.Search_Response, TConfig>>;
  
  search<TDocument = unknown>(
    params: ElasticsearchQuery,
    callback: (err: any, result: ConditionalResponse<OpenSearchTypes.Core_Search.ResponseBody, OpenSearchAPI.Search_Response, TConfig>) => void
  ): void;
  
  search<TDocument = unknown>(
    params: ElasticsearchQuery,
    options: any,
    callback: (err: any, result: ConditionalResponse<OpenSearchTypes.Core_Search.ResponseBody, OpenSearchAPI.Search_Response, TConfig>) => void
  ): void;
  
  bulk(
    params: { body: any[] },
    options?: any
  ): Promise<ConditionalResponse<OpenSearchAPI.Bulk_ResponseBody, OpenSearchAPI.Bulk_Response, TConfig>>;
  
  bulk(
    params: { body: any[] },
    callback: (err: any, result: ConditionalResponse<OpenSearchAPI.Bulk_ResponseBody, OpenSearchAPI.Bulk_Response, TConfig>) => void
  ): void;
  
  bulk(
    params: { body: any[] },
    options: any,
    callback: (err: any, result: ConditionalResponse<OpenSearchAPI.Bulk_ResponseBody, OpenSearchAPI.Bulk_Response, TConfig>) => void
  ): void;
  
  scroll(
    params?: { scroll_id?: string; scroll?: string },
    options?: any
  ): Promise<ConditionalResponse<OpenSearchTypes.Core_Search.ResponseBody, OpenSearchAPI.Scroll_Response, TConfig>>;
  
  scroll(
    params: { scroll_id?: string; scroll?: string },
    callback: (err: any, result: ConditionalResponse<OpenSearchTypes.Core_Search.ResponseBody, OpenSearchAPI.Scroll_Response, TConfig>) => void
  ): void;
}

/**
 * Base interface for Elasticsearch client methods that are not part of the standard OpenSearch client
 */
interface ElasticsearchClientExtensions<TConfig extends ElasticsearchClientConfig = ElasticsearchClientConfig> {
  /**
   * Retrieve document IDs matching the specified queries
   * 
   * @param queries Array of query configurations to execute
   * @param callback Callback function to handle results
   * @example
   * ```typescript
   * client.getIds([{
   *   index: 'my-index',
   *   query: { match: { status: 'active' } }
   * }], (err, ids) => {
   *   if (err) console.error(err);
   *   else console.log('Found IDs:', ids);
   * });
   * ```
   */
  getIds(queries: GetIdsQuery[], callback: Callback<string[]>): void;

  /**
   * Execute a search query with optional parameters
   * 
   * This method has multiple signatures:
   * - query(queryObj, callback) - Execute query without additional parameters
   * - query(queryObj, params, callback) - Execute query with additional parameters
   * 
   * @param query The Elasticsearch query object
   * @param params Optional parameters for the query
   * @param callback Callback function to handle results
   * @example
   * ```typescript
   * // Without parameters
   * client.query({
   *   index: 'my-index',
   *   body: { query: { match_all: {} } }
   * }, (err, result) => {
   *   console.log(result);
   * });
   * 
   * // With parameters
   * client.query({
   *   index: 'my-index',
   *   body: { query: { match_all: {} } }
   * }, { timeout: '30s' }, (err, result) => {
   *   console.log(result);
   * });
   * ```
   */
  query<TDocument = unknown>(
    query: ElasticsearchQuery, 
    callback: (err: any, result: ConditionalResponse<OpenSearchTypes.Core_Search.ResponseBody, OpenSearchAPI.Search_Response, TConfig>, fields?: any) => void
  ): void;
  query<TDocument = unknown>(
    query: ElasticsearchQuery, 
    params: any, 
    callback: (err: any, result: ConditionalResponse<OpenSearchTypes.Core_Search.ResponseBody, OpenSearchAPI.Search_Response, TConfig>, fields?: any) => void
  ): void;

  /**
   * Execute a query with scroll functionality to retrieve large result sets
   * 
   * This method supports both callback and Promise-based usage.
   * It automatically handles scrolling through large result sets and provides
   * various transformation options for the returned data.
   * 
   * @param data Configuration for the scroll query
   * @param callback Optional callback function (if not provided, returns a Promise)
   * @returns Promise if no callback is provided
   * @example
   * ```typescript
   * // Using callback
   * client.queryWithScroll({
   *   index: 'my-index',
   *   body: { query: { match_all: {} } },
   *   scroll: '15s',
   *   size: 1000,
   *   return: 'source'
   * }, (err, results) => {
   *   console.log(`Found ${results.qty} documents`);
   * });
   * 
   * // Using Promise
   * const results = await client.queryWithScroll({
   *   index: 'my-index',
   *   body: { query: { match_all: {} } },
   *   scroll: '15s'
   * });
   * ```
   */
  queryWithScroll<TDocument = unknown>(
    data: ScrollQueryData, 
    callback: Callback<ScrollQueryResults<TDocument>>
  ): void;
  queryWithScroll<TDocument = unknown>(
    data: ScrollQueryData
  ): Promise<ScrollQueryResults<TDocument>>;

  /**
   * Disconnect from the Elasticsearch cluster
   * 
   * Currently a no-op method for compatibility
   */
  disconnect(): void;

  /**
   * Describe a table structure
   * 
   * @param table Table name to describe
   * @param callback Callback function
   * @throws {Error} Not implemented
   */
  describeTable(table: string, callback: Callback): never;

  /**
   * Describe all available tables
   * 
   * @param callback Callback function
   * @throws {Error} Not implemented
   */
  describeTables(callback: Callback): never;

  /**
   * Create a stream for processing Elasticsearch documents
   * 
   * This method creates a transform stream that processes documents and sends them
   * to Elasticsearch using bulk operations. The stream handles batching, error handling,
   * and optional S3 result storage.
   * 
   * @param settings Configuration options for the stream
   * @returns A transform stream for processing documents
   * @example
   * ```typescript
   * const stream = client.stream({
   *   system: 'my-system',
   *   logSummary: true
   * });
   * 
   * // Pipe data through the stream
   * sourceStream
   *   .pipe(stream)
   *   .pipe(destinationStream);
   * ```
   */
  stream(settings: StreamSettings): Transform;

  /**
   * Create a parallel processing stream for high-throughput Elasticsearch operations
   * 
   * This method creates a stream optimized for parallel processing of large volumes
   * of documents. It provides better performance than the regular stream method
   * for high-throughput scenarios.
   * 
   * @param settings Configuration options including parallelism settings
   * @returns A transform stream optimized for parallel processing
   * @example
   * ```typescript
   * const parallelStream = client.streamParallel({
   *   system: 'my-system',
   *   parallelLimit: 5,
   *   buffer: {
   *     records: 1000,
   *     bytes: 1024 * 1024 // 1MB
   *   }
   * });
   * ```
   */
  streamParallel(settings: ParallelStreamSettings): Transform;

  /**
   * Stream data to a table from S3
   * 
   * @param table Target table name
   * @param opts Configuration options
   * @throws {Error} Not implemented
   */
  streamToTableFromS3(table: string, opts: any): never;

  /**
   * Create a batched stream for table operations
   * 
   * This method creates a stream that batches documents for efficient bulk operations.
   * It handles both update and delete operations based on the document configuration.
   * 
   * @param opts Batch configuration options
   * @returns A writable stream that processes batched documents
   * @example
   * ```typescript
   * const batchStream = client.streamToTableBatch({
   *   records: 500
   * });
   * 
   * // Write documents to the stream
   * batchStream.write({
   *   index: 'my-index',
   *   id: 'doc1',
   *   doc: { field: 'value' }
   * });
   * ```
   */
  streamToTableBatch<TDocument = unknown>(opts?: BatchOptions): Writable;

  /**
   * Create a stream for table operations
   * 
   * This is an alias for streamToTableBatch with the same functionality.
   * 
   * @param opts Configuration options
   * @returns A writable stream for table operations
   */
  streamToTable<TDocument = unknown>(opts?: BatchOptions): Writable;
}

/**
 * Elasticsearch client interface that combines OpenSearch client with custom extensions
 * and handles returnFullResponse behavior for ALL methods
 * 
 * This type uses a hybrid approach: explicit method overrides for core methods combined with
 * generic transformation for all other OpenSearch client methods to ensure comprehensive coverage.
 * 
 * @template TReturnFullResponse - Controls return type behavior for all methods
 *   - `false` or `undefined` (default): All methods return response.body
 *   - `true`: All methods return full response objects
 * 
 * @example
 * ```typescript
 * // Default behavior - all methods return response.body
 * const client: ElasticsearchClient = connect('https://cluster.com');
 * const searchResult = await client.search({ index: 'test' });
 * // searchResult is SearchResponseBody, not SearchResponse
 * 
 * // Full response mode - all methods return full response
 * const fullClient: ElasticsearchClient<true> = connect({ 
 *   host: 'https://cluster.com', 
 *   returnFullResponse: true 
 * });
 * const fullResult = await fullClient.search({ index: 'test' });
 * // fullResult is SearchResponse with statusCode, headers, body, etc.
 * ```
 */
/**
 * Main Elasticsearch client type with conditional response behavior
 * 
 * This client extends the OpenSearch client with custom methods and conditional
 * response types based on the `returnFullResponse` configuration.
 * 
 * @template TConfig - The configuration type that determines response behavior
 * 
 * @example
 * ```typescript
 * // Default behavior - returns response.body only
 * const client = createElasticsearchClient('https://localhost:9200');
 * const result = await client.search({ index: 'test' });
 * // result is SearchResponseBody with hits, took, etc.
 * 
 * // Full response behavior - returns complete response
 * const fullClient = createElasticsearchClient({
 *   host: 'https://localhost:9200',
 *   returnFullResponse: true
 * });
 * const fullResult = await fullClient.search({ index: 'test' });
 * // fullResult is SearchResponse with statusCode, headers, body, etc.
 * ```
 */
export type ElasticsearchClient<TConfig extends ElasticsearchClientConfig = ElasticsearchClientConfig> = 
  Omit<OpenSearchClient, 'search' | 'bulk' | 'scroll'>
  & ElasticsearchCoreMethodOverrides<TConfig>
  & ElasticsearchClientExtensions<TConfig>;

/**
 * Factory function that creates an Elasticsearch client
 * 
 * This function can accept either a pre-configured client object or configuration
 * parameters to create a new client. It supports both string URLs and configuration objects.
 * 
 * @param clientConfigHost Either a pre-configured client, a host URL string, or a configuration object
 * @param region AWS region for authentication (optional)
 * @returns Configured Elasticsearch client with extended functionality
 * 
 * @example
 * ```typescript
 * // Using a host URL
 * const client = createElasticsearchClient('https://my-cluster.es.amazonaws.com');
 * 
 * // Using a configuration object
 * const client = createElasticsearchClient({
 *   host: 'https://my-cluster.es.amazonaws.com',
 *   awsConfig: { region: 'us-east-1' },
 *   returnFullResponse: false
 * });
 * 
 * // Using a pre-configured client
 * const existingClient = new Client({ ... });
 * const client = createElasticsearchClient(existingClient);
 * ```
 */
declare function createElasticsearchClient<TConfig extends ElasticsearchClientConfig = ElasticsearchClientConfig>(
  clientConfigHost?: ElasticsearchClient<TConfig> | string | TConfig,
  region?: string
): ElasticsearchClient<TConfig>;

export default createElasticsearchClient;
