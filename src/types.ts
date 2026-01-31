/**
 * Core Connectome types for gRPC serialization
 * These mirror the types from connectome-ts to make this package self-contained
 */

/**
 * A Facet represents a unit of state in the VEIL system
 */
export interface Facet {
  id: string;
  type: string;
  content?: string;
  state?: Record<string, any>;
  tags?: string[];
  agentId?: string;
  agentName?: string;
  streamId?: string;
  streamType?: string;
  scopes?: string[];
  ephemeral?: boolean;
  children?: Facet[];
  attributes?: Record<string, any>;
  attachments?: Attachment[];
}

/**
 * Attachment for binary data (images, files, etc.)
 */
export interface Attachment {
  id: string;
  contentType: string;
  data?: Uint8Array | string;
  url?: string;
  sizeBytes?: number;
  filename?: string;
  mimeType?: string;
  metadata?: Record<string, any>;
}

/**
 * Reference to a component in the Space
 */
export interface ComponentRef {
  componentId: string;
  componentPath?: string[];
  componentType?: string;
}

/**
 * An event in the Space system
 */
export interface SpaceEvent {
  topic: string;
  source: ComponentRef;
  payload?: any;
  timestamp: number;
  priority?: 'immediate' | 'high' | 'normal' | 'low';
  sync?: boolean;
  metadata?: Record<string, any>;
}
