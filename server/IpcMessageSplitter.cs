using System;
using System.IO;
using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace DuckArrowServer
{
    /// <summary>
    /// Splits Arrow IPC stream output into individual IPC messages
    /// (metadata + body) for the standard Flight wire format.
    ///
    /// Why this is needed:
    ///   ArrowStreamWriter produces a complete IPC stream (schema + batches + EOS).
    ///   The Flight protocol needs each message split into:
    ///     - data_header = IPC metadata (flatbuffer)
    ///     - data_body   = IPC body (column buffers)
    ///   Standard Flight clients (Python, C++, Go) expect this format.
    ///
    /// IPC stream message format:
    ///   [0xFFFFFFFF]              continuation token (4 bytes)
    ///   [metadata_size]           int32 little-endian (4 bytes)
    ///   [metadata]                flatbuffer bytes, padded to 8-byte boundary
    ///   [body]                    raw column buffer bytes
    /// </summary>
    public static class IpcMessageSplitter
    {
        /// <summary>
        /// An individual IPC message split into metadata and body.
        /// </summary>
        public struct IpcMessage
        {
            /// <summary>The IPC metadata (flatbuffer Message).</summary>
            public byte[] Metadata;

            /// <summary>The IPC body (column buffers). May be empty for schema messages.</summary>
            public byte[] Body;
        }

        /// <summary>
        /// Serialize a schema into an IPC metadata message.
        /// The schema message has metadata but no body.
        /// </summary>
        public static IpcMessage SerializeSchema(Schema schema)
        {
            byte[] ipcBytes = WriteSchemaToIpcStream(schema);
            return ExtractFirstMessage(ipcBytes);
        }

        /// <summary>
        /// Serialize a RecordBatch into an IPC metadata + body message.
        /// </summary>
        public static IpcMessage SerializeBatch(RecordBatch batch, Schema schema)
        {
            byte[] ipcBytes = WriteBatchToIpcStream(batch, schema);

            // The IPC stream contains: [schema message] [batch message] [EOS]
            // We want the batch message (second one), not the schema.
            int pos = 0;
            SkipOneMessage(ipcBytes, ref pos); // skip schema
            return ExtractMessageAt(ipcBytes, pos);
        }

        // ── IPC stream generation ────────────────────────────────────────────

        private static byte[] WriteSchemaToIpcStream(Schema schema)
        {
            using (var ms = new MemoryStream())
            {
                var writer = new ArrowStreamWriter(ms, schema, leaveOpen: true);
                writer.WriteStartAsync().Wait();
                writer.WriteEndAsync().Wait();
                writer.Dispose();
                return ms.ToArray();
            }
        }

        private static byte[] WriteBatchToIpcStream(RecordBatch batch, Schema schema)
        {
            using (var ms = new MemoryStream())
            {
                var writer = new ArrowStreamWriter(ms, schema, leaveOpen: true);
                writer.WriteRecordBatchAsync(batch).Wait();
                writer.WriteEndAsync().Wait();
                writer.Dispose();
                return ms.ToArray();
            }
        }

        // ── IPC message parsing ──────────────────────────────────────────────

        /// <summary>
        /// Extract the first IPC message from an IPC stream byte array.
        /// </summary>
        private static IpcMessage ExtractFirstMessage(byte[] ipcBytes)
        {
            int pos = 0;
            return ExtractMessageAt(ipcBytes, pos);
        }

        /// <summary>
        /// Extract an IPC message at the given position in the byte array.
        /// </summary>
        private static IpcMessage ExtractMessageAt(byte[] ipcBytes, int pos)
        {
            var result = new IpcMessage();

            // Read continuation token (must be 0xFFFFFFFF).
            if (pos + 8 > ipcBytes.Length) return result;
            int continuation = BitConverter.ToInt32(ipcBytes, pos);
            pos += 4;

            // Read metadata size.
            int metadataSize = BitConverter.ToInt32(ipcBytes, pos);
            pos += 4;

            if (metadataSize <= 0) return result;

            // Read metadata bytes.
            result.Metadata = new byte[metadataSize];
            Buffer.BlockCopy(ipcBytes, pos, result.Metadata, 0, metadataSize);

            // Advance past metadata (padded to 8-byte boundary).
            int paddedMetadataSize = PadTo8(metadataSize);
            pos += paddedMetadataSize;

            // Read body size from the flatbuffer metadata.
            // The body size is encoded in the Message flatbuffer at a known offset.
            long bodySize = ReadBodySizeFromMetadata(result.Metadata);

            if (bodySize > 0 && pos + bodySize <= ipcBytes.Length)
            {
                result.Body = new byte[bodySize];
                Buffer.BlockCopy(ipcBytes, pos, result.Body, 0, (int)bodySize);
            }
            else
            {
                result.Body = new byte[0];
            }

            return result;
        }

        /// <summary>
        /// Skip one IPC message in the byte array.
        /// </summary>
        private static void SkipOneMessage(byte[] ipcBytes, ref int pos)
        {
            if (pos + 8 > ipcBytes.Length) return;

            pos += 4; // continuation
            int metadataSize = BitConverter.ToInt32(ipcBytes, pos);
            pos += 4;

            if (metadataSize <= 0) return;

            // Read metadata to get body size.
            byte[] metadata = new byte[metadataSize];
            Buffer.BlockCopy(ipcBytes, pos, metadata, 0, metadataSize);

            int paddedMetadataSize = PadTo8(metadataSize);
            pos += paddedMetadataSize;

            long bodySize = ReadBodySizeFromMetadata(metadata);
            pos += (int)bodySize;
        }

        /// <summary>
        /// Read the body length from a Message flatbuffer.
        /// The Message flatbuffer has bodyLength at offset 8 from the table start.
        /// Flatbuffer layout: [vtable_offset (int32)] ... [bodyLength at field index 2]
        /// </summary>
        private static long ReadBodySizeFromMetadata(byte[] metadata)
        {
            // The Arrow IPC Message flatbuffer has this structure:
            //   Root table offset at position 0 (int32)
            //   vtable at (root - vtable_offset)
            //   bodyLength is field index 2 (0-based) in the vtable
            //
            // For simplicity, we scan for the body length by looking at the
            // known flatbuffer structure. The bodyLength field is an int64.
            //
            // A simpler approach: calculate the remaining bytes in the IPC stream
            // after the metadata. But we don't have that info here.
            //
            // The most reliable approach: use the flatbuffer structure.
            // Root table is at offset stored at position 0.
            if (metadata.Length < 8) return 0;

            int rootTableOffset = BitConverter.ToInt32(metadata, 0);
            if (rootTableOffset < 0 || rootTableOffset >= metadata.Length) return 0;

            // vtable offset is a negative-relative int32 at the root table position
            int vtableRelOffset = BitConverter.ToInt32(metadata, rootTableOffset);
            int vtablePos = rootTableOffset - vtableRelOffset;

            if (vtablePos < 0 || vtablePos + 4 >= metadata.Length) return 0;

            // vtable: [vtable_size (uint16)] [table_size (uint16)] [field_offsets...]
            int vtableSize = BitConverter.ToUInt16(metadata, vtablePos);

            // bodyLength is field index 2 (after version=0, header=1)
            // Each field offset is 2 bytes, starting at vtablePos + 4
            int fieldIndex = 2; // bodyLength
            int fieldOffsetPos = vtablePos + 4 + fieldIndex * 2;
            if (fieldOffsetPos + 2 > vtablePos + vtableSize) return 0;

            int fieldOffset = BitConverter.ToUInt16(metadata, fieldOffsetPos);
            if (fieldOffset == 0) return 0; // field not present

            int bodyLengthPos = rootTableOffset + fieldOffset;
            if (bodyLengthPos + 8 > metadata.Length) return 0;

            return BitConverter.ToInt64(metadata, bodyLengthPos);
        }

        private static int PadTo8(int size)
        {
            return (size + 7) & ~7;
        }
    }
}
