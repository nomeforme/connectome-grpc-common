/**
 * Connectome gRPC Client
 * Base client for axon services to communicate with the Connectome server
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { join } from 'path';
import { readFileSync } from 'fs';
import { EventEmitter } from 'events';

import {
  facetToProto,
  protoToFacet,
  type ProtoFacet
} from './serialization/facet-serializer';
import {
  eventToProto,
  protoToEvent,
  createProtoEvent,
  type ProtoSpaceEvent
} from './serialization/event-serializer';

/**
 * Client configuration
 */
export interface ConnectomeClientConfig {
  host: string;
  port?: number;
  clientId: string;
  reconnectInterval?: number;
  maxReconnectAttempts?: number;
  /** Keepalive ping interval in ms (default: 30000) */
  keepaliveTimeMs?: number;
  /** Keepalive ping timeout in ms (default: 5000) */
  keepaliveTimeoutMs?: number;
  /** Default request deadline in ms (default: 30000) */
  defaultDeadlineMs?: number;
  /** TLS configuration. If provided, enables mTLS. */
  tls?: { caCertPath?: string; certPath?: string; keyPath?: string };
}

/**
 * Subscription options
 */
export interface SubscriptionOptions {
  filters?: Array<{
    types?: string[];
    aspectMatch?: Record<string, string>;
    attributeMatch?: Record<string, string>;
  }>;
  includeExisting?: boolean;
  fromSequence?: number;
  streamIds?: string[];
}

/**
 * Facet delta from subscription
 */
export interface FacetDelta {
  type: 'added' | 'changed' | 'removed';
  facet: any;
  oldFacet?: any;
  sequence: number;
  frameUuid: string;
}

/**
 * Frame result from event emission
 */
export interface FrameResult {
  success: boolean;
  sequence: number;
  frameUuid: string;
  deltas: FacetDelta[];
  error?: string;
}

/**
 * Health status
 */
export interface HealthStatus {
  healthy: boolean;
  currentSequence: number;
  activeStreams: number;
  activeAgents: number;
  uptimeMs: number;
}

/**
 * Connectome gRPC Client
 */
export class ConnectomeClient extends EventEmitter {
  private config: Required<Omit<ConnectomeClientConfig, 'tls'>> & Pick<ConnectomeClientConfig, 'tls'>;
  private client: any;
  private connected: boolean = false;
  private reconnecting: boolean = false;
  private reconnectAttempts: number = 0;
  private subscriptionStreams: Map<string, any> = new Map();
  private subscriptionCounter: number = 0;
  private protoDescriptor: any;

  constructor(config: ConnectomeClientConfig) {
    super();

    this.config = {
      host: config.host,
      port: config.port || 50051,
      clientId: config.clientId,
      reconnectInterval: config.reconnectInterval || 5000,
      maxReconnectAttempts: config.maxReconnectAttempts || -1, // -1 = infinite
      keepaliveTimeMs: config.keepaliveTimeMs || 30000,
      keepaliveTimeoutMs: config.keepaliveTimeoutMs || 5000,
      defaultDeadlineMs: config.defaultDeadlineMs || 30000
    };
  }

  /**
   * Connect to the Connectome server
   */
  async connect(): Promise<void> {
    const protoPath = join(import.meta.dirname, '..', 'proto', 'connectome.proto');

    const packageDefinition = await protoLoader.load(protoPath, {
      keepCase: false,
      longs: Number,
      enums: String,
      defaults: true,
      oneofs: true
    });

    this.protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
    const ConnectomeService = (this.protoDescriptor as any).connectome.ConnectomeService;

    const address = `${this.config.host}:${this.config.port}`;

    // gRPC channel options with keepalive for connection health
    const channelOptions = {
      'grpc.keepalive_time_ms': this.config.keepaliveTimeMs,
      'grpc.keepalive_timeout_ms': this.config.keepaliveTimeoutMs,
      'grpc.keepalive_permit_without_calls': 1,
      'grpc.http2.min_time_between_pings_ms': Math.floor(this.config.keepaliveTimeMs / 2),
      'grpc.http2.max_pings_without_data': 0,
      // Allow large context responses (default 4MB is too small)
      'grpc.max_send_message_length': 64 * 1024 * 1024,   // 64MB
      'grpc.max_receive_message_length': 64 * 1024 * 1024, // 64MB
    };

    // Build client credentials (mTLS or insecure)
    let creds: grpc.ChannelCredentials;
    if (this.config.tls?.caCertPath && this.config.tls?.certPath && this.config.tls?.keyPath) {
      const caCert = readFileSync(this.config.tls.caCertPath);
      const cert = readFileSync(this.config.tls.certPath);
      const key = readFileSync(this.config.tls.keyPath);
      creds = grpc.credentials.createSsl(caCert, key, cert);
    } else {
      creds = grpc.credentials.createInsecure();
    }

    this.client = new ConnectomeService(
      address,
      creds,
      channelOptions
    );

    // Wait for connection
    return new Promise((resolve, reject) => {
      const deadline = new Date();
      deadline.setSeconds(deadline.getSeconds() + 10);

      this.client.waitForReady(deadline, (error: Error | null) => {
        if (error) {
          reject(new Error(`Failed to connect to ${address}: ${error.message}`));
        } else {
          this.connected = true;
          this.reconnectAttempts = 0;
          this.emit('connected');
          console.log(`[ConnectomeClient] Connected to ${address}`);
          resolve();
        }
      });
    });
  }

  /**
   * Disconnect from the server
   */
  disconnect(): void {
    for (const [subId, stream] of this.subscriptionStreams) {
      stream.cancel();
    }
    this.subscriptionStreams.clear();

    if (this.client) {
      this.client.close();
      this.client = null;
    }

    this.connected = false;
    this.emit('disconnected');
  }

  /**
   * Check if connected
   */
  isConnected(): boolean {
    return this.connected;
  }

  /**
   * Health check
   */
  async health(timeoutMs?: number): Promise<HealthStatus> {
    return this.retryUnary(() => {
      const deadline = this.createDeadline(timeoutMs || 5000);
      return new Promise<HealthStatus>((resolve, reject) => {
        this.client.Health(
          { clientId: this.config.clientId },
          { deadline },
          (error: any, response: any) => {
            if (error) {
              reject(error);
            } else {
              resolve({
                healthy: response.healthy,
                currentSequence: response.currentSequence,
                activeStreams: response.activeStreams,
                activeAgents: response.activeAgents,
                uptimeMs: response.uptimeMs
              });
            }
          }
        );
      });
    }, { maxRetries: 1 });
  }

  /**
   * Emit an event to the Space
   */
  async emitEvent(
    topic: string,
    payload: any,
    options?: {
      priority?: 'immediate' | 'high' | 'normal' | 'low';
      sync?: boolean;
      metadata?: Record<string, any>;
      waitForFrame?: boolean;
      timeoutMs?: number;
    }
  ): Promise<FrameResult> {
    return this.retryUnary(() => {
      const protoEvent = createProtoEvent(
        topic,
        this.config.clientId,
        payload,
        options
      );

      const deadline = this.createDeadline(options?.timeoutMs || 60000);

      return new Promise<FrameResult>((resolve, reject) => {
        this.client.EmitEvent(
          {
            event: protoEvent,
            waitForFrame: options?.waitForFrame ?? true
          },
          { deadline },
          (error: any, response: any) => {
            if (error) {
              if (error.code === grpc.status.DEADLINE_EXCEEDED) {
                console.error(`[ConnectomeClient] EmitEvent timed out for topic ${topic}`);
              }
              reject(error);
            } else {
              resolve({
                success: response.success,
                sequence: response.sequence,
                frameUuid: response.frameUuid,
                deltas: (response.deltas || []).map((d: any) => ({
                  type: d.type === 'DELTA_ADDED' ? 'added' :
                        d.type === 'DELTA_CHANGED' ? 'changed' : 'removed',
                  facet: d.facet ? protoToFacet(d.facet) : null,
                  oldFacet: d.oldFacet ? protoToFacet(d.oldFacet) : null,
                  sequence: d.sequence,
                  frameUuid: d.frameUuid
                })),
                error: response.error
              });
            }
          }
        );
      });
    });
  }

  /**
   * Subscribe to facet changes with auto-reconnect and sequence tracking
   */
  subscribe(
    options: SubscriptionOptions,
    callback: (delta: FacetDelta) => void
  ): () => void {
    const subId = `${this.config.clientId}-sub${++this.subscriptionCounter}`;
    let lastSequence = options.fromSequence || 0;
    let cancelled = false;
    let reconnectTimeout: ReturnType<typeof setTimeout> | null = null;
    let reconnectAttempt = 0;
    /** Track active stream so stale end/error handlers from replaced streams are ignored */
    let activeStream: any = null;

    const createStream = () => {
      if (cancelled || !this.client) return;

      const request = {
        clientId: subId,
        filters: (options.filters || []).map(f => ({
          types: f.types || [],
          aspectMatch: f.aspectMatch || {},
          attributeMatch: f.attributeMatch || {}
        })),
        // Only include existing on first connect (not reconnect)
        includeExisting: (options.includeExisting || false) && lastSequence === 0,
        fromSequence: lastSequence,
        streamIds: options.streamIds || []
      };

      const stream = this.client.SubscribeToFacets(request);
      activeStream = stream;
      this.subscriptionStreams.set(subId, stream);

      stream.on('data', (data: any) => {
        reconnectAttempt = 0; // Reset on successful data
        const delta: FacetDelta = {
          type: data.type === 'DELTA_ADDED' ? 'added' :
                data.type === 'DELTA_CHANGED' ? 'changed' : 'removed',
          facet: data.facet ? protoToFacet(data.facet) : null,
          oldFacet: data.oldFacet ? protoToFacet(data.oldFacet) : null,
          sequence: data.sequence,
          frameUuid: data.frameUuid
        };
        if (delta.sequence > lastSequence) {
          lastSequence = delta.sequence;
        }
        callback(delta);
      });

      stream.on('error', (error: any) => {
        if (error.code === grpc.status.CANCELLED || cancelled) return;
        if (stream !== activeStream) return; // Stale stream — already replaced
        console.error(`[ConnectomeClient] Subscription ${subId} error: ${error.message}, reconnecting...`);
        this.subscriptionStreams.delete(subId);
        scheduleReconnect();
      });

      stream.on('end', () => {
        if (cancelled) return;
        if (stream !== activeStream) return; // Stale stream — already replaced
        console.log(`[ConnectomeClient] Subscription ${subId} ended, reconnecting...`);
        this.subscriptionStreams.delete(subId);
        scheduleReconnect();
      });
    };

    const scheduleReconnect = () => {
      if (cancelled) return;
      // Clear any pending reconnect to prevent stacking
      if (reconnectTimeout) clearTimeout(reconnectTimeout);
      reconnectAttempt++;
      const delay = Math.min(1000 * Math.pow(2, Math.min(reconnectAttempt, 5)) + Math.random() * 500, 30000);
      console.log(`[ConnectomeClient] Subscription ${subId} reconnect attempt ${reconnectAttempt} in ${Math.round(delay)}ms`);
      reconnectTimeout = setTimeout(() => createStream(), delay);
    };

    createStream();

    // Return unsubscribe function
    return () => {
      cancelled = true;
      if (reconnectTimeout) clearTimeout(reconnectTimeout);
      const stream = this.subscriptionStreams.get(subId);
      if (stream) stream.cancel();
      this.subscriptionStreams.delete(subId);
    };
  }

  /**
   * Register an agent
   */
  async registerAgent(
    agentId: string,
    agentName: string,
    options?: {
      agentType?: string;
      capabilities?: string[];
      metadata?: Record<string, string>;
      timeoutMs?: number;
    }
  ): Promise<{ agentId: string; sessionToken: string; success: boolean; error?: string }> {
    return this.retryUnary(() => {
      const deadline = this.createDeadline(options?.timeoutMs || 10000);
      return new Promise<{ agentId: string; sessionToken: string; success: boolean; error?: string }>((resolve, reject) => {
        this.client.RegisterAgent(
          {
            agentId,
            agentName,
            agentType: options?.agentType || 'assistant',
            capabilities: options?.capabilities || [],
            metadata: options?.metadata || {}
          },
          { deadline },
          (error: any, response: any) => {
            if (error) {
              if (error.code === grpc.status.DEADLINE_EXCEEDED) {
                console.error(`[ConnectomeClient] RegisterAgent timed out for ${agentName}`);
              }
              reject(error);
            } else {
              resolve({
                agentId: response.agentId,
                sessionToken: response.sessionToken,
                success: response.success,
                error: response.error
              });
            }
          }
        );
      });
    });
  }

  /**
   * Get rendered context for an agent
   */
  async getContext(
    agentId: string,
    streamId: string,
    options?: {
      maxFrames?: number;
      maxTokens?: number;
      facetTypes?: string[];
      includeUnfocused?: boolean;
      timeoutMs?: number;
    }
  ): Promise<{ context: any; tokenCount: number; frameCount: number }> {
    return this.retryUnary(() => {
      const deadline = this.createDeadline(options?.timeoutMs || 60000);
      return new Promise<{ context: any; tokenCount: number; frameCount: number }>((resolve, reject) => {
        this.client.GetContext(
          {
            agentId,
            streamId,
            maxFrames: options?.maxFrames || 100,
            maxTokens: options?.maxTokens || 100000,
            facetTypes: options?.facetTypes || [],
            includeUnfocused: options?.includeUnfocused || false
          },
          { deadline },
          (error: any, response: any) => {
            if (error) {
              if (error.code === grpc.status.DEADLINE_EXCEEDED) {
                console.error(`[ConnectomeClient] GetContext timed out for stream ${streamId}`);
              }
              reject(error);
            } else {
              let context = {};
              if (response.contextJson && response.contextJson.length > 0) {
                const jsonStr = new TextDecoder().decode(response.contextJson);
                try {
                  context = JSON.parse(jsonStr);
                } catch {
                  console.warn('[ConnectomeClient] Failed to parse context JSON');
                }
              }

              resolve({
                context,
                tokenCount: response.tokenCount,
                frameCount: response.frameCount
              });
            }
          }
        );
      });
    });
  }

  /**
   * Create a new stream
   */
  async createStream(
    streamId: string,
    streamType: string,
    metadata?: Record<string, string>,
    parentStreamId?: string,
    timeoutMs?: number
  ): Promise<{ streamId: string; streamType: string; success: boolean; error?: string }> {
    return this.retryUnary(() => {
      const deadline = this.createDeadline(timeoutMs || 15000);
      return new Promise<{ streamId: string; streamType: string; success: boolean; error?: string }>((resolve, reject) => {
        this.client.CreateStream(
          {
            streamId,
            streamType,
            metadata: metadata || {},
            parentStreamId: parentStreamId || '',
          },
          { deadline },
          (error: any, response: any) => {
            if (error) {
              if (error.code === grpc.status.DEADLINE_EXCEEDED) {
                console.error(`[ConnectomeClient] CreateStream timed out for ${streamId}`);
              }
              reject(error);
            } else {
              resolve({
                streamId: response.streamId,
                streamType: response.streamType,
                success: response.success,
                error: response.error
              });
            }
          }
        );
      });
    });
  }

  /**
   * Get current state snapshot
   */
  async getStateSnapshot(options?: {
    sequence?: number;
    facetTypes?: string[];
    streamIds?: string[];
    timeoutMs?: number;
  }): Promise<{ sequence: number; facets: any[]; streams: any[]; agents: any[] }> {
    return this.retryUnary(() => {
      const deadline = this.createDeadline(options?.timeoutMs || 60000);
      return new Promise<{ sequence: number; facets: any[]; streams: any[]; agents: any[] }>((resolve, reject) => {
        this.client.GetStateSnapshot(
          {
            sequence: options?.sequence || 0,
            facetTypes: options?.facetTypes || [],
            streamIds: options?.streamIds || []
          },
          { deadline },
          (error: any, response: any) => {
            if (error) {
              if (error.code === grpc.status.DEADLINE_EXCEEDED) {
                console.error('[ConnectomeClient] GetStateSnapshot timed out');
              }
              reject(error);
            } else {
              resolve({
                sequence: response.sequence,
                facets: (response.facets || []).map((f: any) => protoToFacet(f)),
                streams: response.streams || [],
                agents: response.agents || []
              });
            }
          }
        );
      });
    });
  }

  /**
   * Activate an agent for a stream
   */
  async activateAgent(
    agentId: string,
    streamId: string,
    options?: {
      reason?: string;
      priority?: 'low' | 'normal' | 'high' | 'critical';
      metadata?: Record<string, string>;
      timeoutMs?: number;
    }
  ): Promise<{ success: boolean; activationId: string; error?: string }> {
    return this.retryUnary(() => {
      const deadline = this.createDeadline(options?.timeoutMs || 45000);

      const priorityMap: Record<string, string> = {
        'low': 'ACTIVATION_LOW',
        'normal': 'ACTIVATION_NORMAL',
        'high': 'ACTIVATION_HIGH',
        'critical': 'ACTIVATION_CRITICAL'
      };

      return new Promise<{ success: boolean; activationId: string; error?: string }>((resolve, reject) => {
        this.client.ActivateAgent(
          {
            agentId,
            streamId,
            reason: options?.reason || 'external activation',
            priority: priorityMap[options?.priority || 'normal'],
            metadata: options?.metadata || {}
          },
          { deadline },
          (error: any, response: any) => {
            if (error) {
              if (error.code === grpc.status.DEADLINE_EXCEEDED) {
                console.error(`[ConnectomeClient] ActivateAgent timed out for ${agentId} on ${streamId}`);
              }
              reject(error);
            } else {
              resolve({
                success: response.success,
                activationId: response.activationId,
                error: response.error
              });
            }
          }
        );
      });
    });
  }

  /**
   * Retry a unary RPC with exponential backoff on transient errors
   */
  private async retryUnary<T>(
    fn: () => Promise<T>,
    options: {
      maxRetries?: number;
      baseDelayMs?: number;
      maxDelayMs?: number;
    } = {}
  ): Promise<T> {
    const {
      maxRetries = 3,
      baseDelayMs = 500,
      maxDelayMs = 10000,
    } = options;

    const retryableStatuses = new Set([
      grpc.status.DEADLINE_EXCEEDED,
      grpc.status.UNAVAILABLE,
      grpc.status.RESOURCE_EXHAUSTED,
    ]);

    let lastError: any;
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        return await fn();
      } catch (error: any) {
        lastError = error;
        if (attempt === maxRetries || !retryableStatuses.has(error.code)) {
          throw error;
        }
        const delay = Math.min(
          baseDelayMs * Math.pow(2, attempt) + Math.random() * 100,
          maxDelayMs
        );
        console.warn(
          `[ConnectomeClient] Retry ${attempt + 1}/${maxRetries} after ${Math.round(delay)}ms: ${error.message}`
        );
        await this.sleep(delay);
      }
    }
    throw lastError;
  }

  /**
   * Handle disconnection and attempt reconnect
   */
  private handleDisconnect(): void {
    if (this.reconnecting) return;

    this.connected = false;
    this.reconnecting = true;
    this.emit('disconnected');

    this.attemptReconnect();
  }

  /**
   * Attempt to reconnect
   */
  private async attemptReconnect(): Promise<void> {
    const maxAttempts = this.config.maxReconnectAttempts;

    while (this.reconnecting) {
      if (maxAttempts >= 0 && this.reconnectAttempts >= maxAttempts) {
        console.error(`[ConnectomeClient] Max reconnect attempts (${maxAttempts}) exceeded`);
        this.reconnecting = false;
        this.emit('reconnect_failed');
        return;
      }

      this.reconnectAttempts++;
      console.log(`[ConnectomeClient] Reconnect attempt ${this.reconnectAttempts}...`);

      try {
        await this.connect();
        this.reconnecting = false;
        this.emit('reconnected');
        return;
      } catch (error: any) {
        console.warn(`[ConnectomeClient] Reconnect failed: ${error.message}`);
        await this.sleep(this.config.reconnectInterval);
      }
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Create a deadline for RPC calls
   */
  private createDeadline(timeoutMs?: number): Date {
    const deadline = new Date();
    deadline.setMilliseconds(deadline.getMilliseconds() + (timeoutMs || this.config.defaultDeadlineMs));
    return deadline;
  }
}
