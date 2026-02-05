/**
 * Connectome gRPC Package
 * Protocol definitions and client/server infrastructure for Connectome microservices
 */

// Core types
export type { Facet, Attachment, ComponentRef, SpaceEvent } from './types';

// Core client/server
export { ConnectomeClient, type ConnectomeClientConfig, type SubscriptionOptions, type FacetDelta, type FrameResult, type HealthStatus } from './client';
export { ConnectomeServer, type ConnectomeServerConfig, type ConnectomeServiceHandlers } from './server';

// Serialization helpers
export {
  facetToProto,
  protoToFacet,
  facetsToProto,
  protoToFacets,
  type ProtoFacet,
  type ProtoAttachment
} from './serialization/facet-serializer';

export {
  eventToProto,
  protoToEvent,
  createProtoEvent,
  componentRefToProto,
  protoToComponentRef,
  eventsToProto,
  protoToEvents,
  EventPriority,
  type ProtoSpaceEvent,
  type ProtoComponentRef
} from './serialization/event-serializer';

export {
  MAX_INLINE_SIZE,
  CHUNK_SIZE,
  DEFAULT_JPEG_QUALITY,
  MAX_IMAGE_DIMENSION,
  shouldInlineAttachment,
  generateAttachmentId,
  createAttachment,
  createUrlAttachment,
  chunkData,
  assembleChunks,
  calculateChecksum,
  verifyChecksum,
  compressImageBasic,
  getContentTypeFromFilename,
  isImageContentType,
  supportsCompression,
  estimateCompressedSize,
  type AttachmentMetadata
} from './serialization/attachment-handler';

// MCP (Model Context Protocol) integration
export {
  MCPManager,
  type MCPServerConfig,
  type MCPServerConfigStdio,
  type MCPServerConfigSSE,
  type MCPTool,
  type MCPToolResult,
  type MCPServerState,
  type MCPManagerConfig,
  type ToolHandler
} from './mcp/index.js';
