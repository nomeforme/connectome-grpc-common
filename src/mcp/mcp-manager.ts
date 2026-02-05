/**
 * MCP Manager
 * Manages connections to multiple MCP servers and provides unified tool access
 */

import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { SSEClientTransport } from '@modelcontextprotocol/sdk/client/sse.js';
import { EventEmitter } from 'events';
import type {
  MCPServerConfig,
  MCPServerConfigStdio,
  MCPServerConfigSSE,
  MCPTool,
  MCPToolResult,
  MCPServerState,
  MCPManagerConfig,
  ToolHandler
} from './types.js';

/**
 * Internal state for a connected MCP server
 */
interface ConnectedServer {
  config: MCPServerConfig;
  client: Client;
  transport: StdioClientTransport | SSEClientTransport;
  tools: MCPTool[];
}

/**
 * MCP Manager - Global pool of MCP server connections
 *
 * Usage:
 * ```typescript
 * const manager = new MCPManager();
 * await manager.connectAll([
 *   { name: 'filesystem', transport: 'stdio', command: 'npx', args: ['-y', '@modelcontextprotocol/server-filesystem', '/tmp'] }
 * ]);
 *
 * // Get tools for a specific server
 * const tools = manager.getToolHandlers('filesystem');
 *
 * // Or get all tools from all servers
 * const allTools = manager.getAllToolHandlers();
 * ```
 */
export class MCPManager extends EventEmitter {
  private servers: Map<string, ConnectedServer> = new Map();
  private config: Required<MCPManagerConfig>;

  constructor(config?: MCPManagerConfig) {
    super();
    this.config = {
      connectionTimeoutMs: config?.connectionTimeoutMs ?? 30000,
      toolTimeoutMs: config?.toolTimeoutMs ?? 60000,
      autoReconnect: config?.autoReconnect ?? true,
      reconnectIntervalMs: config?.reconnectIntervalMs ?? 5000
    };

    // Prevent unhandled error crashes - errors are logged in connect()
    this.on('error', () => {});
  }

  /**
   * Connect to multiple MCP servers
   */
  async connectAll(serverConfigs: MCPServerConfig[]): Promise<void> {
    const results = await Promise.allSettled(
      serverConfigs.map(config => this.connect(config))
    );

    // Log any failures
    results.forEach((result, index) => {
      if (result.status === 'rejected') {
        console.error(`[MCPManager] Failed to connect to ${serverConfigs[index].name}:`, result.reason);
      }
    });
  }

  /**
   * Connect to a single MCP server
   */
  async connect(serverConfig: MCPServerConfig): Promise<void> {
    const { name } = serverConfig;

    // Disconnect existing connection if any
    if (this.servers.has(name)) {
      await this.disconnect(name);
    }

    console.log(`[MCPManager] Connecting to MCP server: ${name}`);

    try {
      const client = new Client({
        name: `connectome-${name}`,
        version: '1.0.0'
      });

      let transport: StdioClientTransport | SSEClientTransport;

      if (serverConfig.transport === 'stdio') {
        transport = this.createStdioTransport(serverConfig);
      } else {
        transport = this.createSSETransport(serverConfig);
      }

      // Connect with timeout
      await Promise.race([
        client.connect(transport),
        this.timeout(this.config.connectionTimeoutMs, `Connection to ${name} timed out`)
      ]);

      // List available tools
      const toolsResult = await client.listTools();
      const tools: MCPTool[] = toolsResult.tools.map(tool => ({
        name: tool.name,
        description: tool.description,
        inputSchema: tool.inputSchema as MCPTool['inputSchema']
      }));

      console.log(`[MCPManager] Connected to ${name}, tools: ${tools.map(t => t.name).join(', ')}`);

      this.servers.set(name, {
        config: serverConfig,
        client,
        transport,
        tools
      });

      this.emit('connected', { name, tools });
    } catch (error: any) {
      console.error(`[MCPManager] Failed to connect to ${name}:`, error.message);
      this.emit('error', { name, error });
      throw error;
    }
  }

  /**
   * Create stdio transport
   */
  private createStdioTransport(config: MCPServerConfigStdio): StdioClientTransport {
    // Merge process.env with config.env, filtering out undefined values
    let env: Record<string, string> | undefined;
    if (config.env) {
      env = {};
      for (const [key, value] of Object.entries(process.env)) {
        if (value !== undefined) {
          env[key] = value;
        }
      }
      Object.assign(env, config.env);
    }

    return new StdioClientTransport({
      command: config.command,
      args: config.args || [],
      env,
      cwd: config.cwd
    });
  }

  /**
   * Create SSE transport
   */
  private createSSETransport(config: MCPServerConfigSSE): SSEClientTransport {
    const url = new URL(config.url);
    return new SSEClientTransport(url);
  }

  /**
   * Disconnect from a specific MCP server
   */
  async disconnect(name: string): Promise<void> {
    const server = this.servers.get(name);
    if (!server) return;

    try {
      await server.client.close();
    } catch (error: any) {
      console.warn(`[MCPManager] Error closing ${name}:`, error.message);
    }

    this.servers.delete(name);
    this.emit('disconnected', { name });
    console.log(`[MCPManager] Disconnected from ${name}`);
  }

  /**
   * Disconnect from all MCP servers
   */
  async disconnectAll(): Promise<void> {
    const names = Array.from(this.servers.keys());
    await Promise.all(names.map(name => this.disconnect(name)));
  }

  /**
   * Call a tool on a specific server
   */
  async callTool(
    serverName: string,
    toolName: string,
    args: Record<string, unknown>
  ): Promise<MCPToolResult> {
    const server = this.servers.get(serverName);
    if (!server) {
      throw new Error(`MCP server not connected: ${serverName}`);
    }

    console.log(`[MCPManager] Calling ${serverName}/${toolName}`, args);

    try {
      const result = await Promise.race([
        server.client.callTool({ name: toolName, arguments: args }),
        this.timeout(this.config.toolTimeoutMs, `Tool call ${toolName} timed out`)
      ]) as MCPToolResult;

      return result;
    } catch (error: any) {
      console.error(`[MCPManager] Tool call failed:`, error.message);
      return {
        content: [{ type: 'text', text: `Error: ${error.message}` }],
        isError: true
      };
    }
  }

  /**
   * Get tools from a specific server
   */
  getTools(serverName: string): MCPTool[] {
    return this.servers.get(serverName)?.tools || [];
  }

  /**
   * Get all tools from all connected servers
   * Returns a map of serverName -> tools
   */
  getAllTools(): Map<string, MCPTool[]> {
    const result = new Map<string, MCPTool[]>();
    for (const [name, server] of this.servers) {
      result.set(name, server.tools);
    }
    return result;
  }

  /**
   * Get ToolHandler instances for a specific server
   * These can be directly used with ToolLoopAgent
   */
  getToolHandlers(serverName: string): ToolHandler[] {
    const server = this.servers.get(serverName);
    if (!server) return [];

    return server.tools.map(tool => this.createToolHandler(serverName, tool));
  }

  /**
   * Get ToolHandler instances for specific servers
   */
  getToolHandlersForServers(serverNames: string[]): ToolHandler[] {
    const handlers: ToolHandler[] = [];
    for (const name of serverNames) {
      handlers.push(...this.getToolHandlers(name));
    }
    return handlers;
  }

  /**
   * Get all ToolHandler instances from all connected servers
   */
  getAllToolHandlers(): ToolHandler[] {
    const handlers: ToolHandler[] = [];
    for (const name of this.servers.keys()) {
      handlers.push(...this.getToolHandlers(name));
    }
    return handlers;
  }

  /**
   * Create a ToolHandler for an MCP tool
   */
  private createToolHandler(serverName: string, tool: MCPTool): ToolHandler {
    // Prefix tool name with server name to avoid collisions
    const prefixedName = `${serverName}__${tool.name}`;

    return {
      name: prefixedName,
      description: tool.description || `Tool from MCP server: ${serverName}`,
      parameters: tool.inputSchema.properties || {},
      required: tool.inputSchema.required,
      handler: async (input: Record<string, any>): Promise<string> => {
        const result = await this.callTool(serverName, tool.name, input);
        return this.formatToolResult(result);
      }
    };
  }

  /**
   * Format MCP tool result as string for LLM
   */
  private formatToolResult(result: MCPToolResult): string {
    const parts: string[] = [];

    for (const content of result.content) {
      if (content.type === 'text' && content.text) {
        parts.push(content.text);
      } else if (content.type === 'image' && content.data) {
        parts.push(`[Image: ${content.mimeType || 'unknown type'}]`);
      } else if (content.type === 'resource') {
        parts.push(`[Resource: ${content.mimeType || 'unknown'}]`);
      }
    }

    if (result.isError) {
      return `Error: ${parts.join('\n')}`;
    }

    return parts.join('\n') || '(no content returned)';
  }

  /**
   * Get connection status for all servers
   */
  getStatus(): MCPServerState[] {
    const states: MCPServerState[] = [];
    for (const [name, server] of this.servers) {
      states.push({
        config: server.config,
        connected: true,
        tools: server.tools
      });
    }
    return states;
  }

  /**
   * Check if a server is connected
   */
  isConnected(serverName: string): boolean {
    return this.servers.has(serverName);
  }

  /**
   * Get list of connected server names
   */
  getConnectedServers(): string[] {
    return Array.from(this.servers.keys());
  }

  /**
   * Helper for timeout promises
   */
  private timeout(ms: number, message: string): Promise<never> {
    return new Promise((_, reject) => {
      setTimeout(() => reject(new Error(message)), ms);
    });
  }
}
