using System;
using System.Runtime.Serialization;

namespace DuckArrowClient
{
    /// <summary>
    /// Exception thrown by <see cref="DasFlightClient"/> when the server returns
    /// an error, the gRPC call fails, or a protocol violation is detected.
    ///
    /// <para>
    /// Wraps the underlying <see cref="Grpc.Core.RpcException"/> (available as
    /// <see cref="Exception.InnerException"/>) for callers that need the raw
    /// gRPC status code or trailing metadata.
    /// </para>
    ///
    /// <para><b>Retrying</b></para>
    /// <para>
    /// A <see cref="DasException"/> from a write operation indicates the
    /// statement failed on the server — retrying the same SQL is unlikely to
    /// succeed without fixing the underlying data problem.  A
    /// <see cref="DasException"/> from a read operation caused by a transient
    /// network fault (gRPC <c>UNAVAILABLE</c>) may be retried after a delay.
    /// </para>
    ///
    /// <para><b>Serializability</b></para>
    /// <para>
    /// This exception is marked <c>[Serializable]</c> and provides the protected
    /// deserialization constructor required by .NET Framework for correct behaviour
    /// when exceptions cross AppDomain boundaries (e.g. in WCF, remoting, or
    /// distributed tracing frameworks that serialize exception graphs).
    /// </para>
    /// </summary>
    [Serializable]
    public sealed class DasException : Exception
    {
        /// <summary>Construct with a message only.</summary>
        public DasException(string message) : base(message) { }

        /// <summary>Construct with a message and an inner exception.</summary>
        public DasException(string message, Exception inner) : base(message, inner) { }

        /// <summary>
        /// Deserialization constructor required by <see cref="ISerializable"/>.
        /// Called by the .NET runtime when deserializing an exception that has
        /// been serialized across an AppDomain boundary.
        /// </summary>
        protected DasException(SerializationInfo info, StreamingContext context)
            : base(info, context) { }
    }
}
