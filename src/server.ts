/**
 * Connectome gRPC Server
 * Base server infrastructure for hosting the Connectome service
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
  type ProtoSpaceEvent
} from './serialization/event-serializer';

/**
 * Server configuration
 */
export interface ConnectomeServerConfig {
  port?: number;
  host?: string;
  maxConcurrentStreams?: number;
}

/**
 * Service handlers interface
 */
export interface ConnectomeServiceHandlers {
  health: () => Promise<{
    healthy: boolean;
    currentSequence: number;
    activeStreams: number;
    activeAgents: number;
    uptimeMs: number;
  }>;

  emitEvent: (event: any, waitForFrame: boolean) => Promise<{
    success: boolean;
    sequence: number;
    frameUuid: string;
    deltas: any[];
    error?: string;
  }>;

  subscribeToFacets: (
    request: any,
    callback: (delta: any) => void,
    onEnd: () => void
  ) => () => void;

  registerAgent: (request: any) => Promise<{
    agentId: string;
    sessionToken: string;
    success: boolean;
    error?: string;
  }>;

  getContext: (request: any) => Promise<{
    agentId: string;
    streamId: string;
    contextJson: Uint8Array;
    tokenCount: number;
    frameCount: number;
    compressionRatio: number;
  }>;

  createStream: (request: any) => Promise<{
    streamId: string;
    streamType: string;
    success: boolean;
    error?: string;
  }>;

  getStateSnapshot: (request: any) => Promise<{
    sequence: number;
    timestamp: string;
    facets: any[];
    streams: any[];
    agents: any[];
    currentStream?: any;
  }>;

  getFrames: (request: any) => Promise<{
    frames: any[];
    currentSequence: number;
  }>;

  activateAgent: (request: any) => Promise<{
    success: boolean;
    activationId: string;
    error?: string;
  }>;
}

/**
 * Connectome gRPC Server
 */
export class ConnectomeServer extends EventEmitter {
  private config: Required<ConnectomeServerConfig>;
  private server: grpc.Server | null = null;
  private handlers: ConnectomeServiceHandlers | null = null;
  private activeSubscriptions: Map<string, { cancel: () => void }> = new Map();

  constructor(config: ConnectomeServerConfig = {}) {
    super();

    this.config = {
      port: config.port || 50051,
      host: config.host || '0.0.0.0',
      maxConcurrentStreams: config.maxConcurrentStreams || 100
    };
  }

  /**
   * Set the service handlers
   */
  setHandlers(handlers: ConnectomeServiceHandlers): void {
    this.handlers = handlers;
  }

  /**
   * Start the gRPC server
   */
  async start(): Promise<void> {
    if (!this.handlers) {
      throw new Error('Service handlers must be set before starting server');
    }

    const protoPath = join(__dirname, '..', 'proto', 'connectome.proto');

    const packageDefinition = await protoLoader.load(protoPath, {
      keepCase: false,
      longs: Number,
      enums: String,
      defaults: true,
      oneofs: true
    });

    const protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
    const ConnectomeService = (protoDescriptor as any).connectome.ConnectomeService;

    this.server = new grpc.Server({
      'grpc.max_concurrent_streams': this.config.maxConcurrentStreams
    });

    // Add service implementation
    this.server.addService(ConnectomeService.service, {
      Health: this.handleHealth.bind(this),
      EmitEvent: this.handleEmitEvent.bind(this),
      SubscribeToFacets: this.handleSubscribeToFacets.bind(this),
      RegisterAgent: this.handleRegisterAgent.bind(this),
      GetContext: this.handleGetContext.bind(this),
      CreateStream: this.handleCreateStream.bind(this),
      GetStateSnapshot: this.handleGetStateSnapshot.bind(this),
      GetFrames: this.handleGetFrames.bind(this),
      ActivateAgent: this.handleActivateAgent.bind(this)
    });

    const address = `${this.config.host}:${this.config.port}`;

    return new Promise((resolve, reject) => {
      this.server!.bindAsync(
        address,
        grpc.ServerCredentials.createInsecure(),
        (error, port) => {
          if (error) {
            reject(new Error(`Failed to bind server: ${error.message}`));
          } else {
            console.log(`[ConnectomeServer] gRPC server listening on ${address}`);
            this.emit('started', { port });
            resolve();
          }
        }
      );
    });
  }

  /**
   * Stop the gRPC server
   */
  async stop(): Promise<void> {
    // Cancel all active subscriptions
    for (const [clientId, sub] of this.activeSubscriptions) {
      sub.cancel();
    }
    this.activeSubscriptions.clear();

    if (this.server) {
      return new Promise((resolve) => {
        this.server!.tryShutdown(() => {
          console.log('[ConnectomeServer] Server stopped');
          this.server = null;
          this.emit('stopped');
          resolve();
        });
      });
    }
  }

  /**
   * Handle Health RPC
   */
  private async handleHealth(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ): Promise<void> {
    try {
      const result = await this.handlers!.health();
      callback(null, {
        healthy: result.healthy,
        currentSequence: result.currentSequence,
        activeStreams: result.activeStreams,
        activeAgents: result.activeAgents,
        uptimeMs: result.uptimeMs
      });
    } catch (error: any) {
      callback({
        code: grpc.status.INTERNAL,
        message: error.message
      });
    }
  }

  /**
   * Handle EmitEvent RPC
   */
  private async handleEmitEvent(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ): Promise<void> {
    try {
      const request = call.request;
      const event = protoToEvent(request.event);
      const result = await this.handlers!.emitEvent(event, request.waitForFrame);

      callback(null, {
        success: result.success,
        sequence: result.sequence,
        frameUuid: result.frameUuid,
        deltas: (result.deltas || []).map((d: any) => ({
          type: d.type === 'added' ? 'DELTA_ADDED' :
                d.type === 'changed' ? 'DELTA_CHANGED' : 'DELTA_REMOVED',
          facet: d.facet ? facetToProto(d.facet) : null,
          oldFacet: d.oldFacet ? facetToProto(d.oldFacet) : null,
          sequence: d.sequence,
          frameUuid: d.frameUuid
        })),
        error: result.error || ''
      });
    } catch (error: any) {
      callback({
        code: grpc.status.INTERNAL,
        message: error.message
      });
    }
  }

  /**
   * Handle SubscribeToFacets RPC (streaming)
   */
  private handleSubscribeToFacets(
    call: grpc.ServerWritableStream<any, any>
  ): void {
    const request = call.request;
    const clientId = request.clientId || `client-${Date.now()}`;

    console.log(`[ConnectomeServer] New subscription from ${clientId}`);

    const sendDelta = (delta: any) => {
      if (!call.writable) return;

      const protoData = {
        type: delta.type === 'added' ? 'DELTA_ADDED' :
              delta.type === 'changed' ? 'DELTA_CHANGED' : 'DELTA_REMOVED',
        facet: delta.facet ? facetToProto(delta.facet) : null,
        oldFacet: delta.oldFacet ? facetToProto(delta.oldFacet) : null,
        sequence: delta.sequence,
        frameUuid: delta.frameUuid
      };

      try {
        call.write(protoData);
      } catch (error: any) {
        console.error(`[ConnectomeServer] Error writing to stream: ${error.message}`);
      }
    };

    const onEnd = () => {
      if (call.writable) {
        call.end();
      }
      this.activeSubscriptions.delete(clientId);
    };

    // Register subscription with handlers
    const unsubscribe = this.handlers!.subscribeToFacets(request, sendDelta, onEnd);

    this.activeSubscriptions.set(clientId, { cancel: unsubscribe });

    // Handle client disconnect
    call.on('cancelled', () => {
      console.log(`[ConnectomeServer] Subscription cancelled for ${clientId}`);
      unsubscribe();
      this.activeSubscriptions.delete(clientId);
    });

    call.on('error', (error: any) => {
      if (error.code !== grpc.status.CANCELLED) {
        console.error(`[ConnectomeServer] Subscription error for ${clientId}:`, error.message);
      }
      unsubscribe();
      this.activeSubscriptions.delete(clientId);
    });
  }

  /**
   * Handle RegisterAgent RPC
   */
  private async handleRegisterAgent(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ): Promise<void> {
    try {
      const result = await this.handlers!.registerAgent(call.request);
      callback(null, result);
    } catch (error: any) {
      callback({
        code: grpc.status.INTERNAL,
        message: error.message
      });
    }
  }

  /**
   * Handle GetContext RPC
   */
  private async handleGetContext(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ): Promise<void> {
    try {
      const result = await this.handlers!.getContext(call.request);
      callback(null, result);
    } catch (error: any) {
      callback({
        code: grpc.status.INTERNAL,
        message: error.message
      });
    }
  }

  /**
   * Handle CreateStream RPC
   */
  private async handleCreateStream(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ): Promise<void> {
    try {
      const result = await this.handlers!.createStream(call.request);
      callback(null, result);
    } catch (error: any) {
      callback({
        code: grpc.status.INTERNAL,
        message: error.message
      });
    }
  }

  /**
   * Handle GetStateSnapshot RPC
   */
  private async handleGetStateSnapshot(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ): Promise<void> {
    try {
      const result = await this.handlers!.getStateSnapshot(call.request);

      callback(null, {
        sequence: result.sequence,
        timestamp: result.timestamp,
        facets: (result.facets || []).map((f: any) => facetToProto(f)),
        streams: result.streams || [],
        agents: result.agents || [],
        currentStream: result.currentStream
      });
    } catch (error: any) {
      callback({
        code: grpc.status.INTERNAL,
        message: error.message
      });
    }
  }

  /**
   * Handle GetFrames RPC
   */
  private async handleGetFrames(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ): Promise<void> {
    try {
      const result = await this.handlers!.getFrames(call.request);
      callback(null, result);
    } catch (error: any) {
      callback({
        code: grpc.status.INTERNAL,
        message: error.message
      });
    }
  }

  /**
   * Handle ActivateAgent RPC
   */
  private async handleActivateAgent(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ): Promise<void> {
    try {
      const result = await this.handlers!.activateAgent(call.request);
      callback(null, result);
    } catch (error: any) {
      callback({
        code: grpc.status.INTERNAL,
        message: error.message
      });
    }
  }

  /**
   * Get server stats
   */
  getStats(): { activeSubscriptions: number } {
    return {
      activeSubscriptions: this.activeSubscriptions.size
    };
  }
}
