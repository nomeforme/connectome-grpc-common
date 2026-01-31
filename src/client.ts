/**
 * Connectome gRPC Client
 * Base client for axon services to communicate with the Connectome server
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { join } from 'path';
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
  private config: Required<ConnectomeClientConfig>;
  private client: any;
  private connected: boolean = false;
  private reconnecting: boolean = false;
  private reconnectAttempts: number = 0;
  private subscriptionStream: any = null;
  private protoDescriptor: any;

  constructor(config: ConnectomeClientConfig) {
    super();

    this.config = {
      host: config.host,
      port: config.port || 50051,
      clientId: config.clientId,
      reconnectInterval: config.reconnectInterval || 5000,
      maxReconnectAttempts: config.maxReconnectAttempts || -1 // -1 = infinite
    };
  }

  /**
   * Connect to the Connectome server
   */
  async connect(): Promise<void> {
    const protoPath = join(__dirname, '..', 'proto', 'connectome.proto');

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

    this.client = new ConnectomeService(
      address,
      grpc.credentials.createInsecure()
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
    if (this.subscriptionStream) {
      this.subscriptionStream.cancel();
      this.subscriptionStream = null;
    }

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
  async health(): Promise<HealthStatus> {
    return new Promise((resolve, reject) => {
      this.client.Health({ clientId: this.config.clientId }, (error: any, response: any) => {
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
      });
    });
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
    }
  ): Promise<FrameResult> {
    const protoEvent = createProtoEvent(
      topic,
      this.config.clientId,
      payload,
      options
    );

    return new Promise((resolve, reject) => {
      this.client.EmitEvent(
        {
          event: protoEvent,
          waitForFrame: options?.waitForFrame ?? true
        },
        (error: any, response: any) => {
          if (error) {
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
  }

  /**
   * Subscribe to facet changes
   */
  subscribe(
    options: SubscriptionOptions,
    callback: (delta: FacetDelta) => void
  ): () => void {
    const request = {
      clientId: this.config.clientId,
      filters: (options.filters || []).map(f => ({
        types: f.types || [],
        aspectMatch: f.aspectMatch || {},
        attributeMatch: f.attributeMatch || {}
      })),
      includeExisting: options.includeExisting || false,
      fromSequence: options.fromSequence || 0,
      streamIds: options.streamIds || []
    };

    const stream = this.client.SubscribeToFacets(request);
    this.subscriptionStream = stream;

    stream.on('data', (data: any) => {
      const delta: FacetDelta = {
        type: data.type === 'DELTA_ADDED' ? 'added' :
              data.type === 'DELTA_CHANGED' ? 'changed' : 'removed',
        facet: data.facet ? protoToFacet(data.facet) : null,
        oldFacet: data.oldFacet ? protoToFacet(data.oldFacet) : null,
        sequence: data.sequence,
        frameUuid: data.frameUuid
      };
      callback(delta);
    });

    stream.on('error', (error: any) => {
      if (error.code !== grpc.status.CANCELLED) {
        console.error('[ConnectomeClient] Subscription error:', error.message);
        this.emit('error', error);
        this.handleDisconnect();
      }
    });

    stream.on('end', () => {
      console.log('[ConnectomeClient] Subscription stream ended');
      this.subscriptionStream = null;
    });

    // Return unsubscribe function
    return () => {
      stream.cancel();
      this.subscriptionStream = null;
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
    }
  ): Promise<{ agentId: string; sessionToken: string; success: boolean; error?: string }> {
    return new Promise((resolve, reject) => {
      this.client.RegisterAgent(
        {
          agentId,
          agentName,
          agentType: options?.agentType || 'assistant',
          capabilities: options?.capabilities || [],
          metadata: options?.metadata || {}
        },
        (error: any, response: any) => {
          if (error) {
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
    }
  ): Promise<{ context: any; tokenCount: number; frameCount: number }> {
    return new Promise((resolve, reject) => {
      this.client.GetContext(
        {
          agentId,
          streamId,
          maxFrames: options?.maxFrames || 100,
          maxTokens: options?.maxTokens || 100000,
          facetTypes: options?.facetTypes || []
        },
        (error: any, response: any) => {
          if (error) {
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
  }

  /**
   * Create a new stream
   */
  async createStream(
    streamId: string,
    streamType: string,
    metadata?: Record<string, string>
  ): Promise<{ streamId: string; streamType: string; success: boolean; error?: string }> {
    return new Promise((resolve, reject) => {
      this.client.CreateStream(
        {
          streamId,
          streamType,
          metadata: metadata || {}
        },
        (error: any, response: any) => {
          if (error) {
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
  }

  /**
   * Get current state snapshot
   */
  async getStateSnapshot(options?: {
    sequence?: number;
    facetTypes?: string[];
    streamIds?: string[];
  }): Promise<{ sequence: number; facets: any[]; streams: any[]; agents: any[] }> {
    return new Promise((resolve, reject) => {
      this.client.GetStateSnapshot(
        {
          sequence: options?.sequence || 0,
          facetTypes: options?.facetTypes || [],
          streamIds: options?.streamIds || []
        },
        (error: any, response: any) => {
          if (error) {
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
    }
  ): Promise<{ success: boolean; activationId: string; error?: string }> {
    return new Promise((resolve, reject) => {
      const priorityMap: Record<string, string> = {
        'low': 'ACTIVATION_LOW',
        'normal': 'ACTIVATION_NORMAL',
        'high': 'ACTIVATION_HIGH',
        'critical': 'ACTIVATION_CRITICAL'
      };

      this.client.ActivateAgent(
        {
          agentId,
          streamId,
          reason: options?.reason || 'external activation',
          priority: priorityMap[options?.priority || 'normal'],
          metadata: options?.metadata || {}
        },
        (error: any, response: any) => {
          if (error) {
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
}
