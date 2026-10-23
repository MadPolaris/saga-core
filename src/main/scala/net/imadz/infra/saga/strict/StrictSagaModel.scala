package net.imadz.infra.saga.strict

import com.google.protobuf.ByteString

/** Only these outer journal/snapshot types have strict Protobuf serializer bindings.
  * Commands are local typed messages in this batch, not advertised as remoting APIs.
  */
sealed trait StrictDurable
final case class ReceiptCodecPin(phase: Phase, codec: CodecPin)
final case class ParticipantDescriptor(stepId: String, participantId: String, resource: String,
                                        phases: Vector[Phase], receiptCodecs: Vector[ReceiptCodecPin]) {
  def receiptCodec(phase: Phase): CodecPin = receiptCodecs.find(_.phase == phase).get.codec
}
sealed trait Evidence { def identity: OperationIdentity }
object Evidence {
  final case class Confirmed(identity: OperationIdentity, receiptBytes: ByteString) extends Evidence
  final case class Absent(identity: OperationIdentity, proof: String, safeToRetry: Boolean) extends Evidence
  final case class Uncertain(identity: OperationIdentity, reason: String) extends Evidence
  final case class Refused(identity: OperationIdentity, reason: String) extends Evidence
}

sealed trait StrictStatus
object StrictStatus {
  case object Empty extends StrictStatus
  case object Preparing extends StrictStatus
  case object Committing extends StrictStatus
  case object Aborting extends StrictStatus
  case object RecoveryRequired extends StrictStatus
  case object Succeeded extends StrictStatus
  case object Aborted extends StrictStatus
}

final case class Intent(identity: OperationIdentity, ordinal: Long, query: Boolean)
sealed trait StrictEvent extends StrictDurable
object StrictEvent {
  final case class Started(identity: TransactionIdentity, argsBytes: ByteString,
                            participants: Vector[ParticipantDescriptor]) extends StrictEvent
  final case class IntentPersisted(intent: Intent) extends StrictEvent
  final case class EvidenceRecorded(ordinal: Long, evidence: Evidence) extends StrictEvent
  case object CommitDecision extends StrictEvent
  final case class AbortRequested(reason: String) extends StrictEvent
  final case class RecoveryRequired(reason: String) extends StrictEvent
  final case class ReconcileAuthorized(principal: String, maxInteractions: Int) extends StrictEvent
  case object Succeeded extends StrictEvent
  case object Aborted extends StrictEvent
}

final case class StrictState(identity: Option[TransactionIdentity] = None,
                              argsBytes: ByteString = ByteString.EMPTY,
                              participants: Vector[ParticipantDescriptor] = Vector.empty,
                              status: StrictStatus = StrictStatus.Empty,
                              commitDecision: Boolean = false,
                              abortRequested: Boolean = false,
                              lastOrdinal: Long = 0L,
                              pending: Option[Intent] = None,
                              evidence: Map[OperationIdentity, Evidence] = Map.empty,
                              reason: String = "") extends StrictDurable {
  def terminal: Boolean = status == StrictStatus.Succeeded || status == StrictStatus.Aborted
  def operation(p: ParticipantDescriptor, phase: Phase): OperationIdentity =
    OperationIdentity(identity.get, p.participantId, p.resource, p.stepId, phase)
  def confirmed(id: OperationIdentity): Boolean = evidence.get(id).exists(_.isInstanceOf[Evidence.Confirmed])
  def safelyAbsent(id: OperationIdentity): Boolean = evidence.get(id).exists {
    case Evidence.Absent(_, proof, true) => proof.nonEmpty
    case _ => false
  }
}

object StrictState {
  /** Pure journal application: no ports, codecs, actor refs, registry or I/O. */
  def evolve(state: StrictState, event: StrictEvent): StrictState = event match {
    case StrictEvent.Started(id, args, participants) =>
      require(state.identity.isEmpty, "transaction already started")
      StrictSagaProtoConverters.validateStarted(id, args, participants)
      StrictState(Some(id), args, participants, StrictStatus.Preparing)
    case StrictEvent.IntentPersisted(intent) =>
      require(!state.terminal && state.pending.isEmpty && intent.ordinal == state.lastOrdinal + 1,
        "invalid intent ordinal or concurrent intent")
      require(state.identity.contains(intent.identity.transaction), "intent transaction mismatch")
      require(state.participants.exists(p => state.operation(p, intent.identity.phase) == intent.identity),
        "intent participant mismatch")
      if (!intent.query) intent.identity.phase match {
        case Phase.Prepare => require(!state.commitDecision && !state.abortRequested, "prepare after decision")
        case Phase.Commit => require(state.commitDecision && !state.abortRequested, "commit before decision")
        case Phase.Compensate =>
          require(state.abortRequested && !state.commitDecision, "compensate after commit decision")
          require(state.confirmed(intent.identity.copy(phase = Phase.Prepare)), "no confirmed reservation")
      }
      state.copy(lastOrdinal = intent.ordinal, pending = Some(intent))
    case StrictEvent.EvidenceRecorded(ordinal, proof) =>
      require(state.pending.exists(i => i.ordinal == ordinal && i.identity == proof.identity),
        "evidence does not match intent")
      state.copy(pending = None, evidence = state.evidence.updated(proof.identity, proof))
    case StrictEvent.CommitDecision =>
      require(!state.commitDecision && !state.abortRequested && state.pending.isEmpty &&
        state.participants.forall(p => state.confirmed(state.operation(p, Phase.Prepare))),
        "commit decision requires every confirmed prepare")
      state.copy(commitDecision = true, status = StrictStatus.Committing, reason = "")
    case StrictEvent.AbortRequested(reason) =>
      require(!state.commitDecision && !state.terminal, "abort after commit decision/terminal")
      state.copy(abortRequested = true, status = StrictStatus.Aborting, reason = reason)
    case StrictEvent.RecoveryRequired(reason) =>
      require(!state.terminal, "terminal recovery")
      state.copy(status = StrictStatus.RecoveryRequired, reason = reason)
    case StrictEvent.ReconcileAuthorized(principal, budget) =>
      require(!state.terminal && principal.nonEmpty && budget > 0, "invalid reconciliation")
      state.copy(status = if (state.commitDecision) StrictStatus.Committing
        else if (state.abortRequested) StrictStatus.Aborting else StrictStatus.Preparing, reason = "")
    case StrictEvent.Succeeded =>
      require(state.commitDecision && state.pending.isEmpty &&
        state.participants.forall(p => state.confirmed(state.operation(p, Phase.Commit))), "unconfirmed commit")
      state.copy(status = StrictStatus.Succeeded, reason = "")
    case StrictEvent.Aborted =>
      require(state.abortRequested && !state.commitDecision && state.pending.isEmpty &&
        state.participants.forall { p =>
          val prepare = state.operation(p, Phase.Prepare)
          if (state.confirmed(prepare)) state.confirmed(state.operation(p, Phase.Compensate))
          else !state.evidence.contains(prepare) || state.safelyAbsent(prepare)
        }, "unresolved reservation or compensation")
      state.copy(status = StrictStatus.Aborted, reason = "")
  }
}
