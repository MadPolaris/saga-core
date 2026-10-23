package net.imadz.infra.saga.strict

import com.google.protobuf.ByteString
import scala.util.Try

/** Consumer-bound, read-only receipt inputs. No public state/bytes constructor or evidence dump.
  * The snapshot retains only identities, codec pins and immutable confirmed bytes, never
  * actor state, bindings, caller args or decoded business receipts. Each lookup decodes anew.
  */
final class StrictReceiptContext private (
    private val consumer: Option[OperationIdentity],
    private val receipts: Map[OperationIdentity, (CodecPin, ByteString)]) {

  def lookup[R](consumerIdentity: OperationIdentity, producer: OperationIdentity,
                expectedCodec: CodecPin, codec: PayloadCodec[R]): Try[R] = Try {
    require(consumerIdentity != null && consumer.contains(consumerIdentity), "receipt consumer mismatch/unavailable")
    require(producer != null && producer.transaction == consumerIdentity.transaction, "receipt transaction mismatch")
    require(expectedCodec != null && codec != null, "receipt codec required")
    val (durablePin, bytes) = receipts.getOrElse(producer,
      throw new IllegalArgumentException("receipt unavailable for this consumer"))
    require(expectedCodec == durablePin, "receipt expected pin mismatch")
    require(codec.pin == expectedCodec, "receipt codec drift")
    val decoded = codec.decode(bytes).get
    require(decoded != null, "decoded receipt required")
    require(codec.pin == expectedCodec, "receipt codec drift")
    // PayloadCodec receipts are decode-validated, not required to re-encode canonically.
    decoded
  }
}

object StrictReceiptContext {
  /** Safe default for existing caller-created OperationRequest(identity, args). */
  val unavailable: StrictReceiptContext = new StrictReceiptContext(None, Map.empty)

  /** Runtime-internal trust boundary: call only with the state delivered by persist.thenRun.
    * No public factory accepts a caller's state, JSON, bytes, flags or copyable token.
    */
  private[strict] def fromPersistedIntent(state: StrictState, consumer: OperationIdentity): StrictReceiptContext = {
    require(state.identity.contains(consumer.transaction), "receipt consumer transaction mismatch")
    StrictSagaProtoConverters.validateStarted(state.identity.get, state.argsBytes, state.participants)
    val intent = state.pending.getOrElse(throw new IllegalArgumentException("durable receipt intent required"))
    require(intent.identity == consumer, "receipt consumer intent mismatch")
    val index = state.participants.indexWhere(p => state.operation(p, consumer.phase) == consumer)
    require(index >= 0, "receipt consumer descriptor mismatch")
    consumer.phase match {
      case Phase.Prepare =>
        require(!state.commitDecision && (!state.abortRequested || intent.query), "receipt prepare after decision")
      case Phase.Commit =>
        require(state.commitDecision && !state.abortRequested, "receipt commit decision required")
      case Phase.Compensate =>
        require(state.abortRequested && !state.commitDecision, "receipt abort decision required")
        require(!state.evidence.values.exists {
          case Evidence.Confirmed(id, _) => id.phase == Phase.Commit
          case _ => false
        }, "receipt compensation after commit evidence")
    }
    val allowed = consumer.phase match {
      case Phase.Prepare => state.participants.take(index).map(p => state.operation(p, Phase.Prepare))
      case Phase.Commit =>
        state.participants.map(p => state.operation(p, Phase.Prepare)) ++
          state.participants.take(index).map(p => state.operation(p, Phase.Commit))
      case Phase.Compensate => Vector(consumer.copy(phase = Phase.Prepare))
    }
    val confirmed = allowed.flatMap { id =>
      val descriptor = state.participants.find(p => state.operation(p, id.phase) == id).get
      state.evidence.get(id) match {
        case Some(Evidence.Confirmed(evidenceId, bytes)) if evidenceId == id && bytes != null =>
          Some(id -> (descriptor.receiptCodec(id.phase), bytes))
        case _ => None // Missing, unknown, absence, rejection and mismatched key/identity are not inputs.
      }
    }.toMap
    new StrictReceiptContext(Some(consumer), confirmed)
  }
}
