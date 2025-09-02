# Leo Elasticsearch Connector

A comprehensive Elasticsearch/OpenSearch connector for the RStreams (formerly Leo) platform that provides high-performance data operations, streaming capabilities, and data integrity features with full TypeScript support.

## Features

- 🔍 **Full-featured Elasticsearch/OpenSearch client** with AWS authentication
- 🚀 **High-throughput streaming** for bulk operations and real-time data processing
- 🔐 **AWS IAM integration** with automatic credential discovery
- 📊 **Data integrity tools** including checksum operations for synchronization
- 🎯 **TypeScript support** with comprehensive type definitions based on OpenSearch API
- 🔄 **Scroll queries** for processing large datasets efficiently
- 📦 **Batch operations** optimized for performance

## Installation

```bash
npm install leo-connector-elasticsearch
```

## Quick Start

### Basic Connection

```typescript
import elasticsearchConnector from 'leo-connector-elasticsearch';

// Simple connection with URL
const client = elasticsearchConnector.connect('https://my-cluster.es.amazonaws.com');

// Advanced configuration
const client = elasticsearchConnector.connect({
  host: 'https://my-cluster.es.amazonaws.com',
  awsConfig: { region: 'us-east-1' },
  returnFullResponse: false // Returns response.body only (default)
});
```

### Search Operations

```typescript
// Basic search
client.query({
  index: 'my-index',
  body: {
    query: { match: { title: 'search term' } },
    size: 100
  }
}, (err, results) => {
  console.log('Found documents:', results.hits.hits);
});

// Scroll through large datasets
const results = await client.queryWithScroll({
  index: 'my-index',
  body: { query: { match_all: {} } },
  scroll: '15s',
  size: 1000,
  return: 'source' // Returns only _source field
});

console.log(`Retrieved ${results.qty} documents`);
results.items.forEach(doc => {
  console.log('Document:', doc);
});
```

### Streaming Operations

```typescript
import { pipeline } from 'stream';

// Create a streaming pipeline for bulk operations
const sourceStream = /* your data source */;
const elasticsearchStream = client.stream({
  system: 'my-system',
  logSummary: true
});

pipeline(
  sourceStream,
  elasticsearchStream,
  (err) => {
    if (err) console.error('Pipeline failed:', err);
    else console.log('Pipeline completed successfully');
  }
);

// High-throughput parallel streaming
const parallelStream = client.streamParallel({
  system: 'my-system',
  parallelLimit: 5,
  buffer: {
    records: 1000,
    bytes: 1024 * 1024 // 1MB
  }
});
```

### Data Integrity with Checksums

```typescript
// Create checksum handler
const checksumHandler = elasticsearchConnector.checksum({
  host: 'https://my-cluster.es.amazonaws.com'
});

// Calculate batch checksums for data comparison
checksumHandler.batch({
  data: { start: 1, end: 1000 },
  settings: {
    index: 'my-index',
    id_column: 'timestamp',
    fields: ['name', 'email', 'status']
  }
}, (err, result) => {
  console.log(`Processed ${result.qty} documents`);
  console.log('Batch hash:', result.hash);
});

// Get individual checksums for detailed comparison
checksumHandler.individual({
  data: { start: 1, end: 100 },
  settings: {
    index: 'my-index',
    id_column: 'id',
    fields: ['name', 'email']
  }
}, (err, result) => {
  result.checksums.forEach(item => {
    console.log(`Document ${item.id}: ${item.hash}`);
  });
});

// Find data range for batch processing
checksumHandler.range({
  data: {},
  settings: {
    index: 'my-index',
    id_column: 'timestamp',
    fields: []
  }
}, (err, result) => {
  console.log(`Data range: ${result.min} to ${result.max}`);
  console.log(`Total documents: ${result.total}`);
});
```

## API Reference

### ElasticsearchClient

The main client interface extending OpenSearch client with additional Leo-specific functionality.

#### Methods

##### `query<TDocument>(query, [params], callback)`

Execute a search query with optional parameters.

**Parameters:**
- `query: ElasticsearchQuery` - The search query configuration
- `params?: any` - Optional additional parameters
- `callback: Callback<SearchResponse<TDocument>>` - Result callback

**Example:**
```typescript
client.query({
  index: 'products',
  body: {
    query: {
      bool: {
        must: [{ match: { category: 'electronics' } }],
        filter: [{ range: { price: { gte: 100 } } }]
      }
    },
    aggs: {
      avg_price: { avg: { field: 'price' } }
    }
  }
}, (err, result) => {
  console.log('Average price:', result.aggregations.avg_price.value);
});
```

##### `queryWithScroll<TDocument>(data, [callback])`

Execute a scroll query for processing large result sets.

**Parameters:**
- `data: ScrollQueryData` - Scroll configuration
- `callback?: Callback<ScrollQueryResults<TDocument>>` - Optional callback (returns Promise if omitted)

**Returns:** `Promise<ScrollQueryResults<TDocument>>` if no callback provided

**Example:**
```typescript
// With Promise
const results = await client.queryWithScroll({
  index: 'logs',
  body: { query: { range: { timestamp: { gte: 'now-1d' } } } },
  scroll: '15s',
  size: 5000,
  max: 50000 // Maximum total results
});

// With callback
client.queryWithScroll({
  index: 'logs',
  body: { query: { match_all: {} } },
  scroll: '15s',
  return: (hit) => ({ id: hit._id, ...hit._source }) // Custom transform
}, (err, results) => {
  console.log(`Processed ${results.qty} documents`);
});
```

##### `getIds(queries, callback)`

Retrieve document IDs matching specified queries.

**Parameters:**
- `queries: GetIdsQuery[]` - Array of query configurations
- `callback: Callback<string[]>` - Callback with array of document IDs

##### `stream(settings)`

Create a transform stream for bulk document operations.

**Parameters:**
- `settings: StreamSettings` - Stream configuration

**Returns:** `TransformStream`

##### `streamParallel(settings)`

Create a parallel processing stream for high-throughput operations.

**Parameters:**
- `settings: ParallelStreamSettings` - Parallel stream configuration

**Returns:** `TransformStream`

##### `streamToTableBatch<TDocument>(opts?)`

Create a batched stream for table operations.

**Parameters:**
- `opts?: BatchOptions` - Batch configuration

**Returns:** `WritableStream`

### ChecksumHandler

Interface for data integrity operations.

#### Methods

##### `batch(event, callback)`

Calculate aggregate checksums for document ranges.

##### `individual(event, callback)`

Calculate individual checksums for each document.

##### `sample(event, callback)`

Retrieve sample data for specified document IDs.

##### `range(event, callback)`

Find min/max values in the ID column.

##### `nibble(event, callback)`

Determine batch boundaries for incremental processing.

## Configuration

### ElasticsearchClientConfig

```typescript
interface ElasticsearchClientConfig {
  host?: string;                    // Cluster URL
  node?: string;                    // Alias for host
  suggestCompression?: boolean;     // Enable compression
  awsConfig?: {
    region?: string;                // AWS region
  };
  returnFullResponse?: boolean;     // Return full response vs response.body
}
```

#### **returnFullResponse Configuration**

The `returnFullResponse` option is a critical configuration that affects **every single method** in the OpenSearch client:

- **`false` or `undefined` (default)**: All methods return `response.body` directly
  - Simpler API, direct access to data
  - Matches behavior of most Elasticsearch libraries
  - Recommended for most use cases

- **`true`**: All methods return the full `ApiResponse` object
  - Access to response metadata (status codes, headers, timing)
  - Useful for debugging, monitoring, and advanced error handling
  - Required when you need response metadata

```typescript
// Default behavior - cleaner API
const client = connector.connect({ host: 'https://cluster.com' });
const hits = await client.search({ index: 'test' });
console.log(hits.hits.hits); // Direct access

// Full response - access to metadata
const fullClient = connector.connect({ 
  host: 'https://cluster.com',
  returnFullResponse: true 
});
const response = await fullClient.search({ index: 'test' });
console.log('Status:', response.statusCode);
console.log('Took:', response.body.took);
console.log('Hits:', response.body.hits.hits);
```

### StreamSettings

```typescript
interface StreamSettings {
  system?: string;                  // System identifier
  requireType?: boolean;            // Require document type
  startTotal?: number;              // Starting count
  logSummary?: boolean;             // Enable summary logging
  fieldsUndefined?: boolean;        // Use undefined vs false for fields
  dontSaveResults?: boolean;        // Skip S3 result storage
}
```

### ParallelStreamSettings

Extends `StreamSettings` with:

```typescript
interface ParallelStreamSettings extends StreamSettings {
  parallelLimit?: number;           // Number of parallel operations
  warmParallelLimit?: number;       // Parallel limit during warmup
  buffer?: number | {               // Buffer configuration
    records?: number;               // Max records to buffer
    bytes?: number;                 // Max bytes to buffer
    time?: {
      milliseconds?: number;        // Time-based flush interval
    };
  };
  warmup?: number;                  // Records during warmup phase
}
```

## TypeScript Support

This package provides **advanced TypeScript support** with comprehensive type definitions based on the official OpenSearch API types. The connector features an intelligent type system that automatically adjusts return types based on the `returnFullResponse` configuration.

### Advanced Type System Features

#### **Automatic Return Type Transformation**

The connector wraps the OpenSearch client transport layer to modify response handling. When `returnFullResponse` is `false` (default), **ALL** client methods return `response.body` instead of the full `ApiResponse` object. The TypeScript types automatically reflect this behavior:

```typescript
import { 
  ElasticsearchClient, 
  ElasticsearchQuery, 
  ScrollQueryData,
  ChecksumHandler 
} from 'leo-connector-elasticsearch';

// Default behavior: All methods return response.body
const client = connector.connect('https://my-cluster.es.amazonaws.com');

// Type: Promise<SearchResponse<TDocument>>
const searchResult = await client.search({ 
  index: 'my-index',
  body: { query: { match_all: {} } }
});
console.log('Total hits:', searchResult.hits.total); // Direct access to response.body

// Type: Promise<BulkResponse>
const bulkResult = await client.bulk({
  body: [/* bulk operations */]
});
console.log('Errors:', bulkResult.errors); // Direct access to response.body
```

#### **Full Response Mode**

When `returnFullResponse` is `true`, all methods return the complete `ApiResponse` object with metadata:

```typescript
// Full response mode: All methods return ApiResponse objects
const fullClient = connector.connect({
  host: 'https://my-cluster.es.amazonaws.com',
  returnFullResponse: true
});

// Type: Promise<ApiResponse<SearchResponse<TDocument>, Context>>
const fullResult = await fullClient.search({ 
  index: 'my-index',
  body: { query: { match_all: {} } }
});

// Access response metadata
console.log('Status Code:', fullResult.statusCode);
console.log('Response Headers:', fullResult.headers);
console.log('Request Meta:', fullResult.meta);
console.log('Search Results:', fullResult.body.hits.hits);
```

#### **Automatic Type Inference**

TypeScript automatically infers the correct client type based on your configuration:

```typescript
// TypeScript infers ElasticsearchClient<false> - returns response.body
const defaultClient = connector.connect('https://cluster.com');

// TypeScript infers ElasticsearchClient<true> - returns full ApiResponse
const fullResponseClient = connector.connect({
  host: 'https://cluster.com',
  returnFullResponse: true
});

// TypeScript infers ElasticsearchClient<undefined> - returns response.body
const configClient = connector.connect({
  host: 'https://cluster.com'
  // returnFullResponse is undefined, defaults to false behavior
});
```

#### **Comprehensive Method Coverage**

The type transformation applies to **every single method** in the OpenSearch client, including nested objects:

```typescript
// All these methods have correct return types based on returnFullResponse:
client.search()                    // SearchResponse or ApiResponse<SearchResponse>
client.bulk()                      // BulkResponse or ApiResponse<BulkResponse>
client.scroll()                    // ScrollResponse or ApiResponse<ScrollResponse>
client.indices.create()            // IndicesCreateResponse or ApiResponse<...>
client.indices.delete()            // IndicesDeleteResponse or ApiResponse<...>
client.cat.health()               // CatHealthResponse or ApiResponse<...>
client.cat.indices()              // CatIndicesResponse or ApiResponse<...>
client.cluster.health()           // ClusterHealthResponse or ApiResponse<...>
client.cluster.stats()            // ClusterStatsResponse or ApiResponse<...>
client.snapshot.create()          // SnapshotCreateResponse or ApiResponse<...>
// ... and hundreds more methods
```

#### **Callback Support**

Both callback and Promise-based usage have correct typing:

```typescript
// Default mode: callback receives response.body
client.search({ index: 'my-index' }, (err, result) => {
  // result is SearchResponse<TDocument>, not ApiResponse
  if (err) console.error(err);
  else console.log('Hits:', result.hits.hits);
});

// Full response mode: callback receives ApiResponse
fullClient.search({ index: 'my-index' }, (err, result) => {
  // result is ApiResponse<SearchResponse<TDocument>, Context>
  if (err) console.error(err);
  else {
    console.log('Status:', result.statusCode);
    console.log('Hits:', result.body.hits.hits);
  }
});
```

### Document Type Safety

Generic document typing provides end-to-end type safety:

```typescript
// Define your document structure
interface MyDocument {
  id: string;
  name: string;
  timestamp: Date;
  tags: string[];
}

// Fully typed queries with OpenSearch API types
const query: ElasticsearchQuery = {
  index: 'my-index',
  body: {
    query: { 
      bool: {
        must: [{ match: { name: 'search term' } }],
        filter: [{ range: { timestamp: { gte: '2023-01-01' } } }]
      }
    },
    aggs: { 
      tag_counts: { 
        terms: { field: 'tags.keyword' } 
      } 
    }
  }
};

// Type-safe results
const results = await client.queryWithScroll<MyDocument>({
  index: 'documents',
  body: { query: { match_all: {} } }
});

// results.items is MyDocument[]
results.items.forEach(doc => {
  console.log(`Document: ${doc.name} (${doc.id})`);
  console.log(`Tags: ${doc.tags.join(', ')}`);
});
```

### Advanced Usage Examples

#### **Conditional Client Types**

```typescript
function createClient<T extends boolean>(useFullResponse: T) {
  return connector.connect({
    host: 'https://cluster.com',
    returnFullResponse: useFullResponse
  });
}

const bodyClient = createClient(false);  // ElasticsearchClient<false>
const fullClient = createClient(true);   // ElasticsearchClient<true>

// TypeScript knows the return types at compile time
const bodyResult = await bodyClient.search({ index: 'test' });  // SearchResponse
const fullResult = await fullClient.search({ index: 'test' });  // ApiResponse<SearchResponse>
```

#### **Type Guards and Utilities**

```typescript
import { ApiResponse } from '@opensearch-project/opensearch';

function isFullResponse<T>(result: T | ApiResponse<T>): result is ApiResponse<T> {
  return result && typeof result === 'object' && 'statusCode' in result;
}

// Use with dynamic client configuration
const client = connector.connect(config); // config.returnFullResponse may be true or false
const result = await client.search({ index: 'test' });

if (isFullResponse(result)) {
  console.log('Status:', result.statusCode);
  console.log('Data:', result.body);
} else {
  console.log('Data:', result);
}
```

## Error Handling

The connector provides comprehensive error handling:

```typescript
client.query(query, (err, result) => {
  if (err) {
    console.error('Search failed:', err);
    // Handle different error types
    if (err.statusCode === 404) {
      console.log('Index not found');
    } else if (err.statusCode === 400) {
      console.log('Invalid query');
    }
    return;
  }
  
  // Process successful result
  console.log('Search completed:', result.hits.total);
});
```

## Performance Tips

1. **Use scroll queries** ONLY for large datasets that require a frozen data set. For normal user-initiated queries, avoid a scroll query.
2. **Configure parallel streams** for high-throughput bulk operations
3. **Batch operations** using appropriate buffer sizes (1-10MB recommended)
4. **Enable compression** for network efficiency
5. **Use field filtering** to reduce response size
6. **Monitor S3 storage** when using result persistence

## AWS Authentication

The connector automatically discovers AWS credentials using the standard AWS SDK credential chain:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. AWS credentials file (`~/.aws/credentials`)
3. IAM roles (when running on EC2/Lambda)
4. AWS SSO credentials

No manual credential configuration is typically required.

## Examples

### Complete Data Synchronization

```typescript
import elasticsearchConnector from 'leo-connector-elasticsearch';

async function syncData() {
  const client = elasticsearchConnector.connect('https://source-cluster.es.amazonaws.com');
  const checksumHandler = elasticsearchConnector.checksum(client);
  
  // Find data range
  const range = await new Promise((resolve, reject) => {
    checksumHandler.range({
      data: {},
      settings: {
        index: 'products',
        id_column: 'updated_at',
        fields: ['name', 'price', 'category']
      }
    }, (err, result) => {
      if (err) reject(err);
      else resolve(result);
    });
  });
  
  console.log(`Syncing ${range.total} products from ${range.min} to ${range.max}`);
  
  // Process in batches
  const batchSize = 1000;
  let current = range.min;
  
  while (current <= range.max) {
    const end = Math.min(current + batchSize, range.max);
    
    const checksum = await new Promise((resolve, reject) => {
      checksumHandler.batch({
        data: { start: current, end },
        settings: {
          index: 'products',
          id_column: 'updated_at',
          fields: ['name', 'price', 'category']
        }
      }, (err, result) => {
        if (err) reject(err);
        else resolve(result);
      });
    });
    
    console.log(`Batch ${current}-${end}: ${checksum.qty} documents, hash: ${checksum.hash}`);
    current = end + 1;
  }
}
```

### Real-time Data Streaming

```typescript
import { Readable } from 'stream';
import elasticsearchConnector from 'leo-connector-elasticsearch';

// Create data source
const dataSource = new Readable({
  objectMode: true,
  read() {
    // Generate or fetch data
    this.push({
      index: 'events',
      id: Date.now(),
      doc: {
        timestamp: new Date(),
        event_type: 'user_action',
        user_id: Math.floor(Math.random() * 1000)
      }
    });
  }
});

// Create Elasticsearch stream
const client = elasticsearchConnector.connect('https://my-cluster.es.amazonaws.com');
const esStream = client.streamParallel({
  system: 'event-processor',
  parallelLimit: 10,
  buffer: { records: 500, milliseconds: 1000 }
});

// Process stream
dataSource
  .pipe(esStream)
  .on('data', (result) => {
    if (result.payload.error) {
      console.error('Indexing error:', result.payload.error);
    } else {
      console.log('Batch processed successfully');
    }
  })
  .on('end', () => {
    console.log('Stream processing completed');
  });
```

## License

MIT

## Contributing

Please read our contributing guidelines and submit pull requests to help improve this connector.

## Support

For issues and questions:
- Create an issue in the repository
- Check existing documentation
- Review the OpenSearch client documentation for advanced usage
