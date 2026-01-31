/**
 * SpaceEvent serialization helpers for gRPC transport
 * Converts between Connectome SpaceEvent and Proto messages
 */

import type { SpaceEvent as ConnectomeSpaceEvent, ComponentRef as ConnectomeComponentRef } from '../types';

/**
 * Event priority enum matching proto
 */
export enum EventPriority {
  PRIORITY_NORMAL = 0,
  PRIORITY_LOW = 1,
  PRIORITY_HIGH = 2,
  PRIORITY_IMMEDIATE = 3
}

/**
 * Proto SpaceEvent structure
 */
export interface ProtoSpaceEvent {
  topic: string;
  source: ProtoComponentRef;
  payloadJson: Uint8Array;
  timestamp: number;
  priority: EventPriority;
  metadata: Record<string, string>;
  sync: boolean;
}

/**
 * Proto ComponentRef structure
 */
export interface ProtoComponentRef {
  componentId: string;
  componentPath: string[];
  componentType: string;
}

/**
 * Convert Connectome priority string to Proto enum
 */
function priorityToProto(priority?: string): EventPriority {
  switch (priority) {
    case 'immediate':
      return EventPriority.PRIORITY_IMMEDIATE;
    case 'high':
      return EventPriority.PRIORITY_HIGH;
    case 'low':
      return EventPriority.PRIORITY_LOW;
    default:
      return EventPriority.PRIORITY_NORMAL;
  }
}

/**
 * Convert Proto priority enum to Connectome string
 */
function protoToPriority(priority: EventPriority): 'immediate' | 'high' | 'normal' | 'low' {
  switch (priority) {
    case EventPriority.PRIORITY_IMMEDIATE:
      return 'immediate';
    case EventPriority.PRIORITY_HIGH:
      return 'high';
    case EventPriority.PRIORITY_LOW:
      return 'low';
    default:
      return 'normal';
  }
}

/**
 * Convert ComponentRef to Proto format
 */
export function componentRefToProto(ref: ConnectomeComponentRef): ProtoComponentRef {
  return {
    componentId: ref.componentId,
    componentPath: ref.componentPath || [],
    componentType: ref.componentType || ''
  };
}

/**
 * Convert Proto ComponentRef to Connectome format
 */
export function protoToComponentRef(proto: ProtoComponentRef): ConnectomeComponentRef {
  const ref: ConnectomeComponentRef = {
    componentId: proto.componentId,
    componentPath: proto.componentPath
  };

  if (proto.componentType) {
    ref.componentType = proto.componentType;
  }

  return ref;
}

/**
 * Convert a Connectome SpaceEvent to Proto format
 */
export function eventToProto(event: ConnectomeSpaceEvent): ProtoSpaceEvent {
  // Serialize payload to JSON bytes
  let payloadJson = new Uint8Array(0);
  if (event.payload !== undefined) {
    const jsonStr = JSON.stringify(event.payload);
    payloadJson = new TextEncoder().encode(jsonStr);
  }

  // Convert metadata
  const metadata: Record<string, string> = {};
  if (event.metadata) {
    for (const [key, value] of Object.entries(event.metadata)) {
      metadata[key] = typeof value === 'string' ? value : JSON.stringify(value);
    }
  }

  return {
    topic: event.topic,
    source: componentRefToProto(event.source),
    payloadJson,
    timestamp: event.timestamp,
    priority: priorityToProto(event.priority),
    metadata,
    sync: event.sync || false
  };
}

/**
 * Convert a Proto SpaceEvent to Connectome format
 */
export function protoToEvent(proto: ProtoSpaceEvent): ConnectomeSpaceEvent {
  // Parse payload from JSON bytes
  let payload: unknown;
  if (proto.payloadJson && proto.payloadJson.length > 0) {
    const jsonStr = new TextDecoder().decode(proto.payloadJson);
    try {
      payload = JSON.parse(jsonStr);
    } catch {
      console.warn('[EventSerializer] Failed to parse payload JSON');
      payload = {};
    }
  }

  // Convert metadata
  const metadata: Record<string, any> = {};
  if (proto.metadata) {
    for (const [key, value] of Object.entries(proto.metadata)) {
      try {
        metadata[key] = JSON.parse(value);
      } catch {
        metadata[key] = value;
      }
    }
  }

  const event: ConnectomeSpaceEvent = {
    topic: proto.topic,
    source: protoToComponentRef(proto.source),
    payload,
    timestamp: proto.timestamp,
    priority: protoToPriority(proto.priority)
  };

  if (Object.keys(metadata).length > 0) {
    event.metadata = metadata;
  }

  if (proto.sync) {
    event.sync = true;
  }

  return event;
}

/**
 * Create a SpaceEvent for gRPC client
 */
export function createProtoEvent(
  topic: string,
  sourceId: string,
  payload: any,
  options?: {
    priority?: 'immediate' | 'high' | 'normal' | 'low';
    sync?: boolean;
    metadata?: Record<string, any>;
  }
): ProtoSpaceEvent {
  return eventToProto({
    topic,
    source: {
      componentId: sourceId,
      componentPath: [sourceId]
    },
    payload,
    timestamp: Date.now(),
    priority: options?.priority,
    sync: options?.sync,
    metadata: options?.metadata
  });
}

/**
 * Serialize multiple events efficiently
 */
export function eventsToProto(events: ConnectomeSpaceEvent[]): ProtoSpaceEvent[] {
  return events.map(eventToProto);
}

/**
 * Deserialize multiple proto events
 */
export function protoToEvents(protos: ProtoSpaceEvent[]): ConnectomeSpaceEvent[] {
  return protos.map(protoToEvent);
}
