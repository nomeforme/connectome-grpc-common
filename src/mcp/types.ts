/**
 * MCP (Model Context Protocol) types
 * Configuration and interface types for MCP server integration
 */

/**
 * MCP server configuration for stdio transport
 */
export interface MCPServerConfigStdio {
  /** Unique name for this MCP server */
  name: string;
  /** Transport type */
  transport: 'stdio';
  /** Command to execute (e.g., 'npx', 'node', 'python') */
  command: string;
  /** Arguments for the command */
  args?: string[];
  /** Environment variables for the process */
  env?: Record<string, string>;
  /** Working directory for the process */
  cwd?: string;
}

/**
 * MCP server configuration for SSE transport
 */
export interface MCPServerConfigSSE {
  /** Unique name for this MCP server */
  name: string;
  /** Transport type */
  transport: 'sse';
  /** URL of the SSE endpoint */
  url: string;
  /** Optional headers for the connection */
  headers?: Record<string, string>;
}

/**
 * Union type for all MCP server configurations
 */
export type MCPServerConfig = MCPServerConfigStdio | MCPServerConfigSSE;

/**
 * MCP tool definition (from server)
 */
export interface MCPTool {
  name: string;
  description?: string;
  inputSchema: {
    type: string;
    properties?: Record<string, any>;
    required?: string[];
  };
}

/**
 * MCP tool call result
 */
export interface MCPToolResult {
  content: Array<{
    type: string;
    text?: string;
    data?: string;
    mimeType?: string;
  }>;
  isError?: boolean;
}

/**
 * Connected MCP server state
 */
export interface MCPServerState {
  config: MCPServerConfig;
  connected: boolean;
  tools: MCPTool[];
  error?: string;
}

/**
 * ToolHandler interface (matches axon tool-loop-agent)
 * This allows MCP tools to be used directly with ToolLoopAgent
 */
export interface ToolHandler {
  name: string;
  description: string;
  parameters: Record<string, any>;
  /** Optional list of required parameter names (if not set, all are assumed required) */
  required?: string[];
  handler: (input: Record<string, any>) => Promise<string>;
}

/**
 * MCP Manager configuration
 */
export interface MCPManagerConfig {
  /** Connection timeout in ms (default: 30000) */
  connectionTimeoutMs?: number;
  /** Tool call timeout in ms (default: 60000) */
  toolTimeoutMs?: number;
  /** Reconnect on disconnect (default: true) */
  autoReconnect?: boolean;
  /** Reconnect interval in ms (default: 5000) */
  reconnectIntervalMs?: number;
}
