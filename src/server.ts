/**
 * Connectome gRPC Server
 * Base server infrastructure for hosting the Connectome service
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
  type ProtoSpaceEvent
} from './serialization/event-serializer';

/**
 * Backpressure-aware buffered writer for gRPC streaming subscriptions.
 * Prevents slow clients from blocking the event loop / other subscribers.
 */
class BufferedDeltaWriter {
  private queue: any[] = [];
  private draining = false;
  private dropped = 0;
  private readonly MAX_QUEUE_SIZE = 1000;

  constructor(
    private call: grpc.ServerWritableStream<any, any>,
    private clientId: string
  ) {}

  write(protoData: any): void {
    if (!this.call.writable) return;

    if (this.queue.length >= this.MAX_QUEUE_SIZE) {
      this.queue.shift();
      this.dropped++;
      if (this.dropped % 100 === 1) {
        console.warn(
          `[ConnectomeServer] Client ${this.clientId}: dropped ${this.dropped} deltas (slow consumer)`
        );
      }
    }

    this.queue.push(protoData);
    this.flush();
  }

  private flush(): void {
    if (this.draining) return;

    while (this.queue.length > 0 && this.call.writable) {
      const data = this.queue.shift()!;
      const canContinue = this.call.write(data);
      if (!canContinue) {
        this.draining = true;
        this.call.once('drain', () => {
          this.draining = false;
          this.flush();
        });
        return;
      }
    }
  }
}

/**
 * Server configuration
 */
export interface TlsConfig {
  /** Path to CA certificate (PEM) */
  caCertPath?: string;
  /** Path to server/client certificate (PEM) */
  certPath?: string;
  /** Path to server/client private key (PEM) */
  keyPath?: string;
}

export interface ConnectomeServerConfig {
  port?: number;
  host?: string;
  maxConcurrentStreams?: number;
  /** TLS configuration. If provided, enables mTLS. */
  tls?: TlsConfig;
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
  private config: Required<Omit<ConnectomeServerConfig, 'tls'>> & Pick<ConnectomeServerConfig, 'tls'>;
  private server: grpc.Server | null = null;
  private handlers: ConnectomeServiceHandlers | null = null;
  private activeSubscriptions: Map<string, { cancel: () => void }> = new Map();

  constructor(config: ConnectomeServerConfig = {}) {
    super();

    this.config = {
      port: config.port || 50051,
      host: config.host || '0.0.0.0',
      maxConcurrentStreams: config.maxConcurrentStreams || 500,
      tls: config.tls,
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

    const protoPath = join(import.meta.dirname, '..', 'proto', 'connectome.proto');

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
      'grpc.max_concurrent_streams': this.config.maxConcurrentStreams,
      // Allow large context responses (default 4MB is too small)
      'grpc.max_send_message_length': 64 * 1024 * 1024,   // 64MB
      'grpc.max_receive_message_length': 64 * 1024 * 1024, // 64MB
      // Allow client keepalive pings
      'grpc.keepalive_permit_without_calls': 1,
      'grpc.http2.min_ping_interval_without_data_ms': 10000,
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

    // Build server credentials (mTLS or insecure)
    let credentials: grpc.ServerCredentials;
    if (this.config.tls?.caCertPath && this.config.tls?.certPath && this.config.tls?.keyPath) {
      const caCert = readFileSync(this.config.tls.caCertPath);
      const cert = readFileSync(this.config.tls.certPath);
      const key = readFileSync(this.config.tls.keyPath);
      credentials = grpc.ServerCredentials.createSsl(caCert, [{ cert_chain: cert, private_key: key }], true);
      console.log('[ConnectomeServer] mTLS enabled');
    } else {
      credentials = grpc.ServerCredentials.createInsecure();
    }

    return new Promise((resolve, reject) => {
      this.server!.bindAsync(
        address,
        credentials,
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

    const writer = new BufferedDeltaWriter(call, clientId);
    const sendDelta = (delta: any) => {
      const protoData = {
        type: delta.type === 'added' ? 'DELTA_ADDED' :
              delta.type === 'changed' ? 'DELTA_CHANGED' : 'DELTA_REMOVED',
        facet: delta.facet ? facetToProto(delta.facet) : null,
        oldFacet: delta.oldFacet ? facetToProto(delta.oldFacet) : null,
        sequence: delta.sequence,
        frameUuid: delta.frameUuid
      };
      writer.write(protoData);
    };

    // Dedup: cancel old subscription before creating new one
    const existingSubscription = this.activeSubscriptions.get(clientId);
    if (existingSubscription) {
      console.log(`[ConnectomeServer] Cancelling existing subscription for ${clientId}`);
      existingSubscription.cancel();
    }

    // Register subscription with handlers
    // Note: onEnd references unsubscribe via closure, so declare it first as mutable
    let cancelFn: (() => void) | null = null;

    const onEnd = () => {
      if (call.writable) {
        call.end();
      }
      // Guard: only delete if we're still the active subscription
      const current = this.activeSubscriptions.get(clientId);
      if (current && current.cancel === cancelFn) {
        this.activeSubscriptions.delete(clientId);
      }
    };

    const unsubscribe = this.handlers!.subscribeToFacets(request, sendDelta, onEnd);
    cancelFn = unsubscribe;
    this.activeSubscriptions.set(clientId, { cancel: unsubscribe });

    // Handle client disconnect (guarded against stale events from replaced subscriptions)
    call.on('cancelled', () => {
      const current = this.activeSubscriptions.get(clientId);
      if (current?.cancel !== unsubscribe) return; // Already replaced
      console.log(`[ConnectomeServer] Subscription cancelled for ${clientId}`);
      unsubscribe();
      this.activeSubscriptions.delete(clientId);
    });

    call.on('error', (error: any) => {
      const current = this.activeSubscriptions.get(clientId);
      if (current?.cancel !== unsubscribe) return; // Already replaced
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
