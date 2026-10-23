package net.imadz.infra.saga.strict

import akka.serialization.SerializerWithStringManifest
import net.imadz.infra.saga.strict.proto.saga_strict.{StrictEventPO, StrictStatePO}

/** Independent, narrow durable-envelope serializer. No legacy, Java, business-class or
  * generic-object fallback. Commands and port payloads are deliberately not bound here.
  */
final class StrictSagaSerializer extends SerializerWithStringManifest {
  override val identifier: Int = 77102231
  // Unpinned strict v1 envelopes cannot be authenticated or silently upgraded.
  private val EventManifest = "event-v2"
  private val StateManifest = "state-v2"

  override def manifest(value: AnyRef): String = value match {
    case _: StrictEvent => EventManifest
    case _: StrictState => StateManifest
    case _ => throw new IllegalArgumentException("not a strict durable envelope")
  }
  override def toBinary(value: AnyRef): Array[Byte] = value match {
    case event: StrictEvent => StrictSagaProtoConverters.eventToProto(event).toByteArray
    case state: StrictState => StrictSagaProtoConverters.stateToProto(state).toByteArray
    case _ => throw new IllegalArgumentException("not a strict durable envelope")
  }
  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    require(bytes != null && bytes.nonEmpty, "strict wire envelope required")
    manifest match {
      case EventManifest => StrictSagaProtoConverters.eventFromProto(StrictEventPO.parseFrom(bytes))
      case StateManifest => StrictSagaProtoConverters.stateFromProto(StrictStatePO.parseFrom(bytes))
      case _ => throw new IllegalArgumentException(s"unknown strict manifest: $manifest")
    }
  }
}
