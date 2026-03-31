using Google.Protobuf;

namespace DuckArrowServer
{
    /// <summary>
    /// Lightweight serializer/deserializer for Arrow Flight protocol buffers.
    ///
    /// The Apache.Arrow.Flight.Protocol types (Ticket, Action, Result, etc.)
    /// are internal in the NuGet package and cannot be used from external code.
    /// This class manually encodes/decodes the proto wire format for the
    /// subset of Flight messages we need.
    ///
    /// Proto definitions (from flight.proto):
    ///   message Ticket      { bytes ticket = 1; }
    ///   message Action      { string type = 1; bytes body = 2; }
    ///   message Result      { bytes body = 1; }
    ///   message ActionType  { string type = 1; string description = 2; }
    ///   message FlightData  { bytes data_header = 2; bytes data_body = 1000; }
    /// </summary>
    public static class FlightProto
    {
        // ── Parsed message types ─────────────────────────────────────────────

        public struct TicketMsg
        {
            public byte[] TicketBytes;
        }

        public struct ActionMsg
        {
            public string Type;
            public byte[] Body;
        }

        // ── Deserializers ────────────────────────────────────────────────────

        /// <summary>Parse a Ticket proto message. Field 1 = bytes ticket.</summary>
        public static TicketMsg ParseTicket(byte[] data)
        {
            var result = new TicketMsg();
            var input = new CodedInputStream(data);
            uint tag;
            while ((tag = input.ReadTag()) != 0)
            {
                if (tag == 10) // field 1, wire type 2 (length-delimited)
                    result.TicketBytes = input.ReadBytes().ToByteArray();
                else
                    input.SkipLastField();
            }
            return result;
        }

        /// <summary>Parse an Action proto message. Field 1 = string type, Field 2 = bytes body.</summary>
        public static ActionMsg ParseAction(byte[] data)
        {
            var result = new ActionMsg();
            var input = new CodedInputStream(data);
            uint tag;
            while ((tag = input.ReadTag()) != 0)
            {
                switch (tag)
                {
                    case 10: // field 1, wire type 2
                        result.Type = input.ReadString();
                        break;
                    case 18: // field 2, wire type 2
                        result.Body = input.ReadBytes().ToByteArray();
                        break;
                    default:
                        input.SkipLastField();
                        break;
                }
            }
            return result;
        }

        // ── Serializers ──────────────────────────────────────────────────────

        /// <summary>Serialize a Result proto message. Field 1 = bytes body.</summary>
        public static byte[] WriteResult(byte[] body)
        {
            var output = new CodedOutputStream(new byte[body.Length + 10]);

            // Use MemoryStream for dynamic sizing.
            using (var ms = new System.IO.MemoryStream())
            {
                var cos = new CodedOutputStream(ms);
                cos.WriteTag(1, WireFormat.WireType.LengthDelimited);
                cos.WriteBytes(ByteString.CopyFrom(body));
                cos.Flush();
                return ms.ToArray();
            }
        }

        /// <summary>Serialize an ActionType proto message. Field 1 = string type, Field 2 = string description.</summary>
        public static byte[] WriteActionType(string type, string description)
        {
            using (var ms = new System.IO.MemoryStream())
            {
                var cos = new CodedOutputStream(ms);
                cos.WriteTag(1, WireFormat.WireType.LengthDelimited);
                cos.WriteString(type);
                cos.WriteTag(2, WireFormat.WireType.LengthDelimited);
                cos.WriteString(description);
                cos.Flush();
                return ms.ToArray();
            }
        }

        /// <summary>
        /// Serialize a FlightData proto message containing IPC data.
        /// Field 2 = bytes data_header, Field 1000 = bytes data_body.
        /// </summary>
        public static byte[] WriteFlightData(byte[] ipcMessage)
        {
            using (var ms = new System.IO.MemoryStream())
            {
                var cos = new CodedOutputStream(ms);
                // data_header = field 2
                cos.WriteTag(2, WireFormat.WireType.LengthDelimited);
                cos.WriteBytes(ByteString.CopyFrom(ipcMessage));
                cos.Flush();
                return ms.ToArray();
            }
        }
    }
}
