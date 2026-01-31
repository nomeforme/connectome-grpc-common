/**
 * Facet serialization helpers for gRPC transport
 * Converts between Connectome Facet types and Proto messages
 */

import type { Facet as ConnectomeFacet, Attachment } from '../types';

/**
 * Proto Facet structure (matches connectome.proto)
 */
export interface ProtoFacet {
  id: string;
  type: string;
  stateJson: Uint8Array;
  content: string;
  tags: string[];
  agentId: string;
  agentName: string;
  streamId: string;
  streamType: string;
  scopes: string[];
  ephemeral: boolean;
  children: ProtoFacet[];
  attributes: Record<string, string>;
  attachments: ProtoAttachment[];
}

export interface ProtoAttachment {
  id: string;
  contentType: string;
  inlineData?: Uint8Array;
  url?: string;
  sizeBytes: number;
  filename: string;
  metadata: Record<string, string>;
}

/**
 * Convert a Connectome Facet to Proto format
 */
export function facetToProto(facet: ConnectomeFacet): ProtoFacet {
  const proto: ProtoFacet = {
    id: facet.id,
    type: facet.type,
    stateJson: new Uint8Array(0),
    content: '',
    tags: facet.tags || [],
    agentId: '',
    agentName: '',
    streamId: '',
    streamType: '',
    scopes: [],
    ephemeral: false,
    children: [],
    attributes: {},
    attachments: []
  };

  // Extract content if present
  if ('content' in facet && typeof facet.content === 'string') {
    proto.content = facet.content;
  }

  // Serialize state to JSON bytes
  if ('state' in facet && facet.state !== undefined) {
    const stateJson = JSON.stringify(facet.state);
    proto.stateJson = new TextEncoder().encode(stateJson);
  }

  // Extract agent aspects
  if ('agentId' in facet && typeof facet.agentId === 'string') {
    proto.agentId = facet.agentId;
  }
  if ('agentName' in facet && typeof facet.agentName === 'string') {
    proto.agentName = facet.agentName;
  }

  // Extract stream aspects
  if ('streamId' in facet && typeof facet.streamId === 'string') {
    proto.streamId = facet.streamId;
  }
  if ('streamType' in facet && typeof facet.streamType === 'string') {
    proto.streamType = facet.streamType;
  }

  // Extract scopes
  if ('scopes' in facet && Array.isArray(facet.scopes)) {
    proto.scopes = facet.scopes;
  }

  // Extract ephemeral flag
  if ('ephemeral' in facet && facet.ephemeral === true) {
    proto.ephemeral = true;
  }

  // Convert children recursively
  if ('children' in facet && Array.isArray(facet.children)) {
    proto.children = facet.children.map((child: any) => facetToProto(child));
  }

  // Convert attributes
  if ('attributes' in facet && facet.attributes) {
    for (const [key, value] of Object.entries(facet.attributes)) {
      proto.attributes[key] = typeof value === 'string' ? value : JSON.stringify(value);
    }
  }

  // Handle attachments (images, files, etc.)
  if ('attachments' in facet && Array.isArray((facet as any).attachments)) {
    proto.attachments = (facet as any).attachments.map((att: any) => attachmentToProto(att));
  }

  return proto;
}

/**
 * Convert a Proto Facet back to Connectome format
 */
export function protoToFacet(proto: ProtoFacet): ConnectomeFacet {
  const facet: any = {
    id: proto.id,
    type: proto.type
  };

  // Parse state from JSON bytes
  if (proto.stateJson && proto.stateJson.length > 0) {
    const stateStr = new TextDecoder().decode(proto.stateJson);
    try {
      facet.state = JSON.parse(stateStr);
    } catch {
      console.warn(`[FacetSerializer] Failed to parse state JSON for facet ${proto.id}`);
      facet.state = {};
    }
  }

  // Set content if present
  if (proto.content) {
    facet.content = proto.content;
  }

  // Set tags
  if (proto.tags && proto.tags.length > 0) {
    facet.tags = proto.tags;
  }

  // Set agent aspects
  if (proto.agentId) {
    facet.agentId = proto.agentId;
  }
  if (proto.agentName) {
    facet.agentName = proto.agentName;
  }

  // Set stream aspects
  if (proto.streamId) {
    facet.streamId = proto.streamId;
  }
  if (proto.streamType) {
    facet.streamType = proto.streamType;
  }

  // Set scopes
  if (proto.scopes && proto.scopes.length > 0) {
    facet.scopes = proto.scopes;
  }

  // Set ephemeral flag
  if (proto.ephemeral) {
    facet.ephemeral = true;
  }

  // Convert children recursively
  if (proto.children && proto.children.length > 0) {
    facet.children = proto.children.map(child => protoToFacet(child));
  }

  // Convert attributes
  if (proto.attributes && Object.keys(proto.attributes).length > 0) {
    facet.attributes = {};
    for (const [key, value] of Object.entries(proto.attributes)) {
      try {
        facet.attributes[key] = JSON.parse(value);
      } catch {
        facet.attributes[key] = value;
      }
    }
  }

  // Convert attachments
  if (proto.attachments && proto.attachments.length > 0) {
    facet.attachments = proto.attachments.map(att => protoToAttachment(att));
  }

  return facet as ConnectomeFacet;
}

/**
 * Convert attachment to proto format
 */
function attachmentToProto(attachment: any): ProtoAttachment {
  const proto: ProtoAttachment = {
    id: attachment.id || '',
    contentType: attachment.contentType || attachment.mimeType || 'application/octet-stream',
    sizeBytes: 0,
    filename: attachment.filename || '',
    metadata: {}
  };

  // Handle inline data vs URL
  if (attachment.data) {
    if (typeof attachment.data === 'string') {
      // Base64 encoded
      proto.inlineData = Uint8Array.from(atob(attachment.data), c => c.charCodeAt(0));
    } else if (attachment.data instanceof Uint8Array) {
      proto.inlineData = attachment.data;
    } else if (Buffer.isBuffer(attachment.data)) {
      proto.inlineData = new Uint8Array(attachment.data);
    }
    proto.sizeBytes = proto.inlineData?.length || 0;
  } else if (attachment.url) {
    proto.url = attachment.url;
    proto.sizeBytes = attachment.sizeBytes || 0;
  }

  // Copy metadata
  if (attachment.metadata) {
    for (const [key, value] of Object.entries(attachment.metadata)) {
      proto.metadata[key] = typeof value === 'string' ? value : JSON.stringify(value);
    }
  }

  return proto;
}

/**
 * Convert proto attachment back to Connectome format
 */
function protoToAttachment(proto: ProtoAttachment): any {
  const attachment: any = {
    id: proto.id,
    contentType: proto.contentType,
    filename: proto.filename
  };

  if (proto.inlineData && proto.inlineData.length > 0) {
    attachment.data = proto.inlineData;
  } else if (proto.url) {
    attachment.url = proto.url;
  }

  attachment.sizeBytes = proto.sizeBytes;

  if (proto.metadata && Object.keys(proto.metadata).length > 0) {
    attachment.metadata = {};
    for (const [key, value] of Object.entries(proto.metadata)) {
      try {
        attachment.metadata[key] = JSON.parse(value);
      } catch {
        attachment.metadata[key] = value;
      }
    }
  }

  return attachment;
}

/**
 * Serialize multiple facets efficiently
 */
export function facetsToProto(facets: ConnectomeFacet[]): ProtoFacet[] {
  return facets.map(facetToProto);
}

/**
 * Deserialize multiple proto facets
 */
export function protoToFacets(protos: ProtoFacet[]): ConnectomeFacet[] {
  return protos.map(protoToFacet);
}
