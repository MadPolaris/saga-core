package net.imadz.infra.saga.strict

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser, JsonToken}
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonSerializer, KeyDeserializer, SerializerProvider}
import com.google.protobuf.ByteString
import java.util.Base64

/** Jackson wiring for the strict coordinator wire family (CborSerializable → jackson-cbor).
  *
  * akka-serialization-jackson 2.6.20 does not register com.google.protobuf.ByteString, and
  * jackson cannot deserialize Map[OperationIdentity, Evidence] (case-class map keys) or the
  * nested sealed traits (Phase/StrictStatus/Evidence) without help. This module supplies
  * explicit wire shapes for exactly those types; everything else (case classes of primitives,
  * Option/Vector/Map plumbing) rides DefaultScalaModule unchanged.
  *
  * Wire rules, fail-closed: phases/statuses/evidence kinds are transmitted as names and
  * looked up against the known set (unknown name = exception, never a default); ByteStrings
  * travel as base64 strings; null and unknown fields are rejected. Registered from
  * reference.conf via akka.actor.serialization.jackson.jackson-modules.
  */
class StrictJacksonModule extends SimpleModule("StrictJacksonModule") {

  private val b64 = Base64.getEncoder
  private val b64d = Base64.getDecoder

  private def bytesOf(value: ByteString): String = b64.encodeToString(value.toByteArray)
  private def bytesIn(text: String): ByteString = ByteString.copyFrom(b64d.decode(text))

  private def phaseNamed(name: String): Phase =
    Phase.all.find(_.toString == name)
      .getOrElse(throw new IllegalArgumentException(s"unknown phase: $name"))

  private def statusNamed(name: String): StrictStatus = name match {
    case "Empty" => StrictStatus.Empty
    case "Preparing" => StrictStatus.Preparing
    case "Committing" => StrictStatus.Committing
    case "Aborting" => StrictStatus.Aborting
    case "RecoveryRequired" => StrictStatus.RecoveryRequired
    case "Succeeded" => StrictStatus.Succeeded
    case "Aborted" => StrictStatus.Aborted
    case other => throw new IllegalArgumentException(s"unknown strict status: $other")
  }

  addSerializer(classOf[ByteString], new JsonSerializer[ByteString] {
    override def serialize(value: ByteString, gen: JsonGenerator, provider: SerializerProvider): Unit =
      gen.writeString(bytesOf(value))
  })
  addDeserializer(classOf[ByteString], new JsonDeserializer[ByteString] {
    override def getNullValue(ctxt: DeserializationContext): ByteString =
      throw new IllegalArgumentException("null ByteString is not part of the strict wire contract")
    override def deserialize(p: JsonParser, ctxt: DeserializationContext): ByteString =
      bytesIn(p.getValueAsString)
  })

  addSerializer(classOf[Phase], new JsonSerializer[Phase] {
    override def serialize(value: Phase, gen: JsonGenerator, provider: SerializerProvider): Unit =
      gen.writeString(value.toString)
  })
  addDeserializer(classOf[Phase], new JsonDeserializer[Phase] {
    override def getNullValue(ctxt: DeserializationContext): Phase =
      throw new IllegalArgumentException("null phase is not part of the strict wire contract")
    override def deserialize(p: JsonParser, ctxt: DeserializationContext): Phase =
      phaseNamed(p.getValueAsString)
  })

  addSerializer(classOf[StrictStatus], new JsonSerializer[StrictStatus] {
    override def serialize(value: StrictStatus, gen: JsonGenerator, provider: SerializerProvider): Unit =
      gen.writeString(value.toString)
  })
  addDeserializer(classOf[StrictStatus], new JsonDeserializer[StrictStatus] {
    override def getNullValue(ctxt: DeserializationContext): StrictStatus =
      throw new IllegalArgumentException("null status is not part of the strict wire contract")
    override def deserialize(p: JsonParser, ctxt: DeserializationContext): StrictStatus =
      statusNamed(p.getValueAsString)
  })

  // Evidence: polymorphic tag field, exhaustive match, unknown kind/field fails closed.
  addSerializer(classOf[Evidence], new JsonSerializer[Evidence] {
    override def serialize(value: Evidence, gen: JsonGenerator, provider: SerializerProvider): Unit = {
      gen.writeStartObject()
      value match {
        case Evidence.Confirmed(identity, receiptBytes) =>
          gen.writeStringField("kind", "Confirmed")
          gen.writeObjectField("identity", identity)
          gen.writeStringField("receiptBytes", bytesOf(receiptBytes))
        case Evidence.Absent(identity, proof, safeToRetry) =>
          gen.writeStringField("kind", "Absent")
          gen.writeObjectField("identity", identity)
          gen.writeStringField("proof", proof)
          gen.writeBooleanField("safeToRetry", safeToRetry)
        case Evidence.Uncertain(identity, reason) =>
          gen.writeStringField("kind", "Uncertain")
          gen.writeObjectField("identity", identity)
          gen.writeStringField("reason", reason)
        case Evidence.Refused(identity, reason) =>
          gen.writeStringField("kind", "Refused")
          gen.writeObjectField("identity", identity)
          gen.writeStringField("reason", reason)
      }
      gen.writeEndObject()
    }
  })
  addDeserializer(classOf[Evidence], new JsonDeserializer[Evidence] {
    override def getNullValue(ctxt: DeserializationContext): Evidence =
      throw new IllegalArgumentException("null evidence is not part of the strict wire contract")
    override def deserialize(p: JsonParser, ctxt: DeserializationContext): Evidence = {
      require(p.currentToken() == JsonToken.START_OBJECT, "expected evidence object")
      var kind: String = null
      var identity: OperationIdentity = null
      var receiptBytes: ByteString = null
      var proof: String = null
      var safeToRetry: Boolean = false
      var reason: String = null
      while (p.nextToken() != JsonToken.END_OBJECT) {
        val field = p.currentName()
        p.nextToken()
        field match {
          case "kind" => kind = p.getValueAsString
          case "identity" => identity = ctxt.readValue(p, classOf[OperationIdentity])
          case "receiptBytes" => receiptBytes = bytesIn(p.getValueAsString)
          case "proof" => proof = p.getValueAsString
          case "safeToRetry" => safeToRetry = p.getValueAsBoolean
          case "reason" => reason = p.getValueAsString
          case other => throw new IllegalArgumentException(s"unknown evidence field: $other")
        }
      }
      kind match {
        case "Confirmed" => Evidence.Confirmed(identity, receiptBytes)
        case "Absent" => Evidence.Absent(identity, proof, safeToRetry)
        case "Uncertain" => Evidence.Uncertain(identity, reason)
        case "Refused" => Evidence.Refused(identity, reason)
        case other => throw new IllegalArgumentException(s"unknown evidence kind: $other")
      }
    }
  })

  // OperationIdentity as a nested value object...
  addSerializer(classOf[OperationIdentity], new JsonSerializer[OperationIdentity] {
    override def serialize(value: OperationIdentity, gen: JsonGenerator, provider: SerializerProvider): Unit = {
      gen.writeStartObject()
      gen.writeObjectField("transaction", value.transaction)
      gen.writeStringField("participantId", value.participantId)
      gen.writeStringField("resource", value.resource)
      gen.writeStringField("stepId", value.stepId)
      gen.writeStringField("phase", value.phase.toString)
      gen.writeEndObject()
    }
  })
  addDeserializer(classOf[OperationIdentity], new JsonDeserializer[OperationIdentity] {
    override def getNullValue(ctxt: DeserializationContext): OperationIdentity =
      throw new IllegalArgumentException("null operation identity is not part of the strict wire contract")
    override def deserialize(p: JsonParser, ctxt: DeserializationContext): OperationIdentity = {
      require(p.currentToken() == JsonToken.START_OBJECT, "expected operation identity object")
      var transaction: TransactionIdentity = null
      var participantId: String = null
      var resource: String = null
      var stepId: String = null
      var phase: Phase = null
      while (p.nextToken() != JsonToken.END_OBJECT) {
        val field = p.currentName()
        p.nextToken()
        field match {
          case "transaction" => transaction = ctxt.readValue(p, classOf[TransactionIdentity])
          case "participantId" => participantId = p.getValueAsString
          case "resource" => resource = p.getValueAsString
          case "stepId" => stepId = p.getValueAsString
          case "phase" => phase = phaseNamed(p.getValueAsString)
          case other => throw new IllegalArgumentException(s"unknown operation identity field: $other")
        }
      }
      require(transaction != null && participantId != null && resource != null && stepId != null && phase != null,
        "operation identity is missing required fields")
      OperationIdentity(transaction, participantId, resource, stepId, phase)
    }
  })

  // ...and as a Map key: each field is individually base64 (no delimiter can appear), joined
  // with '|'. 12 fields: transactionId, definitionName, definitionVersion, argsDigest,
  // definitionDigest, argsCodecId, argsCodecVersion, planDigest, participantId, resource,
  // stepId, phase.
  addKeySerializer(classOf[OperationIdentity], new JsonSerializer[OperationIdentity] {
    override def serialize(value: OperationIdentity, gen: JsonGenerator, provider: SerializerProvider): Unit = {
      val t = value.transaction
      val fields = Seq(t.transactionId, t.definitionName, t.definitionVersion.toString, t.argsDigest,
        t.definitionDigest, t.argsCodec.id, t.argsCodec.version.toString, t.planDigest,
        value.participantId, value.resource, value.stepId, value.phase.toString)
        .map(f => b64.encodeToString(f.getBytes("UTF-8")))
      gen.writeFieldName(fields.mkString("|"))
    }
  })
  addKeyDeserializer(classOf[OperationIdentity], new KeyDeserializer {
    override def deserializeKey(key: String, ctxt: DeserializationContext): AnyRef = {
      val f = key.split("\\|", -1)
      require(f.length == 12, s"malformed operation identity key (${f.length} fields)")
      val dec = (i: Int) => new String(b64d.decode(f(i)), "UTF-8")
      val transaction = TransactionIdentity(dec(0), dec(1), dec(2).toInt, dec(3), dec(4),
        CodecPin(dec(5), dec(6).toInt), dec(7))
      OperationIdentity(transaction, dec(8), dec(9), dec(10), phaseNamed(dec(11)))
    }
  })
}
