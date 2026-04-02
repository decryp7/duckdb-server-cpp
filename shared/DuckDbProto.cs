using System;
using System.IO;
using Google.Protobuf;
using Grpc.Core;

namespace DuckDbProto
{
    /// <summary>
    /// Hand-written protobuf message types and gRPC method definitions
    /// matching proto/duckdb_service.proto.
    ///
    /// Centralized here so server, client, and benchmark all share the
    /// same protocol definition. Changes here affect all projects.
    ///
    /// Why hand-written instead of Grpc.Tools codegen?
    ///   Grpc.Tools integration with classic .csproj (VS2017) is unreliable.
    ///   The proto messages are simple enough to encode/decode manually.
    ///   The proto file serves as the canonical cross-language definition
    ///   (C++ uses protoc codegen from the same .proto file).
    /// </summary>
    public static class DuckDbService
    {
        public const string ServiceName = "duckdb.v1.DuckDbService";

        // ── gRPC method definitions ──────────────────────────────────────────

        public static readonly Method<QueryRequest, QueryResponse> QueryMethod =
            new Method<QueryRequest, QueryResponse>(
                MethodType.ServerStreaming,
                ServiceName, "Query",
                QueryRequest.Marshaller, QueryResponse.Marshaller);

        public static readonly Method<ExecuteRequest, ExecuteResponse> ExecuteMethod =
            new Method<ExecuteRequest, ExecuteResponse>(
                MethodType.Unary,
                ServiceName, "Execute",
                ExecuteRequest.Marshaller, ExecuteResponse.Marshaller);

        public static readonly Method<PingRequest, PingResponse> PingMethod =
            new Method<PingRequest, PingResponse>(
                MethodType.Unary,
                ServiceName, "Ping",
                PingRequest.Marshaller, PingResponse.Marshaller);

        public static readonly Method<StatsRequest, StatsResponse> StatsMethod =
            new Method<StatsRequest, StatsResponse>(
                MethodType.Unary,
                ServiceName, "GetStats",
                StatsRequest.Marshaller, StatsResponse.Marshaller);
    }

    // ── Messages ─────────────────────────────────────────────────────────────
    // Each message has Serialize/Deserialize + a Marshaller for gRPC.

    public sealed class QueryRequest
    {
        public string Sql { get; set; }

        public byte[] Serialize()
        {
            using (var ms = new MemoryStream())
            {
                var cos = new CodedOutputStream(ms);
                if (!string.IsNullOrEmpty(Sql))
                {
                    cos.WriteTag(1, WireFormat.WireType.LengthDelimited);
                    cos.WriteString(Sql);
                }
                cos.Flush();
                return ms.ToArray();
            }
        }

        public static QueryRequest Deserialize(byte[] data)
        {
            var r = new QueryRequest();
            var input = new CodedInputStream(data);
            uint tag;
            while ((tag = input.ReadTag()) != 0)
            {
                if (tag == 10) r.Sql = input.ReadString();
                else input.SkipLastField();
            }
            return r;
        }

        public static readonly Marshaller<QueryRequest> Marshaller =
            new Marshaller<QueryRequest>(r => r.Serialize(), QueryRequest.Deserialize);
    }

    public sealed class QueryResponse
    {
        public byte[] IpcData { get; set; }

        public byte[] Serialize()
        {
            using (var ms = new MemoryStream())
            {
                var cos = new CodedOutputStream(ms);
                if (IpcData != null && IpcData.Length > 0)
                {
                    cos.WriteTag(1, WireFormat.WireType.LengthDelimited);
                    cos.WriteBytes(ByteString.CopyFrom(IpcData));
                }
                cos.Flush();
                return ms.ToArray();
            }
        }

        public static QueryResponse Deserialize(byte[] data)
        {
            var r = new QueryResponse();
            var input = new CodedInputStream(data);
            uint tag;
            while ((tag = input.ReadTag()) != 0)
            {
                if (tag == 10) r.IpcData = input.ReadBytes().ToByteArray();
                else input.SkipLastField();
            }
            return r;
        }

        public static readonly Marshaller<QueryResponse> Marshaller =
            new Marshaller<QueryResponse>(r => r.Serialize(), QueryResponse.Deserialize);
    }

    public sealed class ExecuteRequest
    {
        public string Sql { get; set; }

        public byte[] Serialize()
        {
            using (var ms = new MemoryStream())
            {
                var cos = new CodedOutputStream(ms);
                if (!string.IsNullOrEmpty(Sql))
                {
                    cos.WriteTag(1, WireFormat.WireType.LengthDelimited);
                    cos.WriteString(Sql);
                }
                cos.Flush();
                return ms.ToArray();
            }
        }

        public static ExecuteRequest Deserialize(byte[] data)
        {
            var r = new ExecuteRequest();
            var input = new CodedInputStream(data);
            uint tag;
            while ((tag = input.ReadTag()) != 0)
            {
                if (tag == 10) r.Sql = input.ReadString();
                else input.SkipLastField();
            }
            return r;
        }

        public static readonly Marshaller<ExecuteRequest> Marshaller =
            new Marshaller<ExecuteRequest>(r => r.Serialize(), ExecuteRequest.Deserialize);
    }

    public sealed class ExecuteResponse
    {
        public bool Success { get; set; }
        public string Error { get; set; }

        public byte[] Serialize()
        {
            using (var ms = new MemoryStream())
            {
                var cos = new CodedOutputStream(ms);
                if (Success)
                {
                    cos.WriteTag(1, WireFormat.WireType.Varint);
                    cos.WriteBool(true);
                }
                if (!string.IsNullOrEmpty(Error))
                {
                    cos.WriteTag(2, WireFormat.WireType.LengthDelimited);
                    cos.WriteString(Error);
                }
                cos.Flush();
                return ms.ToArray();
            }
        }

        public static ExecuteResponse Deserialize(byte[] data)
        {
            var r = new ExecuteResponse();
            var input = new CodedInputStream(data);
            uint tag;
            while ((tag = input.ReadTag()) != 0)
            {
                switch (tag)
                {
                    case 8: r.Success = input.ReadBool(); break;
                    case 18: r.Error = input.ReadString(); break;
                    default: input.SkipLastField(); break;
                }
            }
            return r;
        }

        public static readonly Marshaller<ExecuteResponse> Marshaller =
            new Marshaller<ExecuteResponse>(r => r.Serialize(), ExecuteResponse.Deserialize);
    }

    public sealed class PingRequest
    {
        public byte[] Serialize() { return new byte[0]; }

        public static PingRequest Deserialize(byte[] data) { return new PingRequest(); }

        public static readonly Marshaller<PingRequest> Marshaller =
            new Marshaller<PingRequest>(r => r.Serialize(), PingRequest.Deserialize);
    }

    public sealed class PingResponse
    {
        public string Message { get; set; }

        public byte[] Serialize()
        {
            using (var ms = new MemoryStream())
            {
                var cos = new CodedOutputStream(ms);
                if (!string.IsNullOrEmpty(Message))
                {
                    cos.WriteTag(1, WireFormat.WireType.LengthDelimited);
                    cos.WriteString(Message);
                }
                cos.Flush();
                return ms.ToArray();
            }
        }

        public static PingResponse Deserialize(byte[] data)
        {
            var r = new PingResponse();
            var input = new CodedInputStream(data);
            uint tag;
            while ((tag = input.ReadTag()) != 0)
            {
                if (tag == 10) r.Message = input.ReadString();
                else input.SkipLastField();
            }
            return r;
        }

        public static readonly Marshaller<PingResponse> Marshaller =
            new Marshaller<PingResponse>(r => r.Serialize(), PingResponse.Deserialize);
    }

    public sealed class StatsRequest
    {
        public byte[] Serialize() { return new byte[0]; }

        public static StatsRequest Deserialize(byte[] data) { return new StatsRequest(); }

        public static readonly Marshaller<StatsRequest> Marshaller =
            new Marshaller<StatsRequest>(r => r.Serialize(), StatsRequest.Deserialize);
    }

    public sealed class StatsResponse
    {
        public long QueriesRead { get; set; }
        public long QueriesWrite { get; set; }
        public long Errors { get; set; }
        public int ReaderPoolSize { get; set; }
        public int Port { get; set; }

        public byte[] Serialize()
        {
            using (var ms = new MemoryStream())
            {
                var cos = new CodedOutputStream(ms);
                if (QueriesRead != 0) { cos.WriteTag(1, WireFormat.WireType.Varint); cos.WriteInt64(QueriesRead); }
                if (QueriesWrite != 0) { cos.WriteTag(2, WireFormat.WireType.Varint); cos.WriteInt64(QueriesWrite); }
                if (Errors != 0) { cos.WriteTag(3, WireFormat.WireType.Varint); cos.WriteInt64(Errors); }
                if (ReaderPoolSize != 0) { cos.WriteTag(4, WireFormat.WireType.Varint); cos.WriteInt32(ReaderPoolSize); }
                if (Port != 0) { cos.WriteTag(5, WireFormat.WireType.Varint); cos.WriteInt32(Port); }
                cos.Flush();
                return ms.ToArray();
            }
        }

        public static StatsResponse Deserialize(byte[] data)
        {
            var r = new StatsResponse();
            var input = new CodedInputStream(data);
            uint tag;
            while ((tag = input.ReadTag()) != 0)
            {
                switch (tag)
                {
                    case 8: r.QueriesRead = input.ReadInt64(); break;
                    case 16: r.QueriesWrite = input.ReadInt64(); break;
                    case 24: r.Errors = input.ReadInt64(); break;
                    case 32: r.ReaderPoolSize = input.ReadInt32(); break;
                    case 40: r.Port = input.ReadInt32(); break;
                    default: input.SkipLastField(); break;
                }
            }
            return r;
        }

        public static readonly Marshaller<StatsResponse> Marshaller =
            new Marshaller<StatsResponse>(r => r.Serialize(), StatsResponse.Deserialize);

        public string ToJson()
        {
            return string.Format(
                "{{\"queries_read\":{0},\"queries_write\":{1},\"errors\":{2},\"reader_pool_size\":{3},\"port\":{4}}}",
                QueriesRead, QueriesWrite, Errors, ReaderPoolSize, Port);
        }
    }
}
