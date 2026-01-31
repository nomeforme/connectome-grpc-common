/**
 * Attachment handling for large payloads
 * Handles image compression, chunking, and streaming
 */

import type { ProtoAttachment } from './facet-serializer';

/**
 * Maximum inline attachment size (bytes)
 * Attachments larger than this should use URL references
 */
export const MAX_INLINE_SIZE = 1024 * 1024; // 1MB

/**
 * Chunk size for streaming large attachments
 */
export const CHUNK_SIZE = 64 * 1024; // 64KB

/**
 * Image compression quality (0-100)
 */
export const DEFAULT_JPEG_QUALITY = 80;

/**
 * Maximum image dimension for compression
 */
export const MAX_IMAGE_DIMENSION = 1920;

/**
 * Attachment metadata for tracking
 */
export interface AttachmentMetadata {
  id: string;
  contentType: string;
  originalSize: number;
  compressedSize?: number;
  compressionRatio?: number;
  width?: number;
  height?: number;
  url?: string;
  checksum?: string;
}

/**
 * Check if attachment should be inlined or use URL reference
 */
export function shouldInlineAttachment(size: number): boolean {
  return size <= MAX_INLINE_SIZE;
}

/**
 * Generate a unique attachment ID
 */
export function generateAttachmentId(): string {
  return `att-${Date.now()}-${Math.random().toString(36).substring(2, 11)}`;
}

/**
 * Create an attachment proto from raw data
 */
export function createAttachment(
  data: Uint8Array | Buffer,
  contentType: string,
  filename?: string,
  metadata?: Record<string, string>
): ProtoAttachment {
  const bytes = data instanceof Uint8Array ? data : new Uint8Array(data);

  const attachment: ProtoAttachment = {
    id: generateAttachmentId(),
    contentType,
    sizeBytes: bytes.length,
    filename: filename || '',
    metadata: metadata || {}
  };

  if (shouldInlineAttachment(bytes.length)) {
    attachment.inlineData = bytes;
  } else {
    // For large attachments, the caller should store the data
    // and provide a URL reference
    attachment.metadata['requiresUpload'] = 'true';
    attachment.metadata['originalSize'] = String(bytes.length);
  }

  return attachment;
}

/**
 * Create an attachment from a URL reference
 */
export function createUrlAttachment(
  url: string,
  contentType: string,
  sizeBytes: number,
  filename?: string,
  metadata?: Record<string, string>
): ProtoAttachment {
  return {
    id: generateAttachmentId(),
    contentType,
    url,
    sizeBytes,
    filename: filename || '',
    metadata: metadata || {}
  };
}

/**
 * Chunk large data for streaming
 */
export function* chunkData(data: Uint8Array, chunkSize: number = CHUNK_SIZE): Generator<Uint8Array> {
  for (let offset = 0; offset < data.length; offset += chunkSize) {
    yield data.slice(offset, Math.min(offset + chunkSize, data.length));
  }
}

/**
 * Reassemble chunked data
 */
export function assembleChunks(chunks: Uint8Array[]): Uint8Array {
  const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;

  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.length;
  }

  return result;
}

/**
 * Calculate checksum for data integrity
 * Uses simple FNV-1a hash for speed
 */
export function calculateChecksum(data: Uint8Array): string {
  let hash = 2166136261; // FNV offset basis

  for (let i = 0; i < data.length; i++) {
    hash ^= data[i];
    hash = (hash * 16777619) >>> 0; // FNV prime, keep as 32-bit
  }

  return hash.toString(16).padStart(8, '0');
}

/**
 * Verify data integrity using checksum
 */
export function verifyChecksum(data: Uint8Array, expectedChecksum: string): boolean {
  return calculateChecksum(data) === expectedChecksum;
}

/**
 * Compress image data (basic implementation without sharp dependency)
 * For full compression, use sharp in the server-side code
 */
export async function compressImageBasic(
  data: Uint8Array,
  contentType: string
): Promise<{ data: Uint8Array; contentType: string; metadata: AttachmentMetadata }> {
  // This is a basic implementation that just passes through
  // Full compression with sharp should be done on the server side
  const metadata: AttachmentMetadata = {
    id: generateAttachmentId(),
    contentType,
    originalSize: data.length,
    compressedSize: data.length,
    compressionRatio: 1.0
  };

  return { data, contentType, metadata };
}

/**
 * Get content type from filename
 */
export function getContentTypeFromFilename(filename: string): string {
  const ext = filename.split('.').pop()?.toLowerCase();

  const contentTypes: Record<string, string> = {
    'jpg': 'image/jpeg',
    'jpeg': 'image/jpeg',
    'png': 'image/png',
    'gif': 'image/gif',
    'webp': 'image/webp',
    'svg': 'image/svg+xml',
    'pdf': 'application/pdf',
    'json': 'application/json',
    'txt': 'text/plain',
    'html': 'text/html',
    'css': 'text/css',
    'js': 'application/javascript',
    'ts': 'application/typescript',
    'mp3': 'audio/mpeg',
    'mp4': 'video/mp4',
    'webm': 'video/webm',
    'ogg': 'audio/ogg',
    'wav': 'audio/wav'
  };

  return contentTypes[ext || ''] || 'application/octet-stream';
}

/**
 * Check if content type is an image
 */
export function isImageContentType(contentType: string): boolean {
  return contentType.startsWith('image/');
}

/**
 * Check if content type supports compression
 */
export function supportsCompression(contentType: string): boolean {
  const compressible = [
    'image/jpeg',
    'image/png',
    'image/webp'
  ];

  return compressible.includes(contentType);
}

/**
 * Estimate compressed size (rough heuristic)
 */
export function estimateCompressedSize(originalSize: number, contentType: string): number {
  if (!supportsCompression(contentType)) {
    return originalSize;
  }

  // Rough compression ratios for different formats
  const ratios: Record<string, number> = {
    'image/png': 0.3,   // PNG compression
    'image/jpeg': 0.8,  // Already compressed
    'image/webp': 0.5   // Good compression
  };

  const ratio = ratios[contentType] || 1.0;
  return Math.ceil(originalSize * ratio);
}
