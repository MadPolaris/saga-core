package net.imadz.infra.saga.strict

import com.google.protobuf.ByteString
import net.imadz.infra.saga.strict.proto.saga_strict._

/** Version-2 outer wire only. Args and receipts stay caller-codec ByteStrings: this
  * converter neither deserializes business values nor consults a runtime registry.
  * Missing oneofs/messages, unspecified/unknown enums and invalid identities fail closed.
  * An explicitly selected empty receipt is valid (e.g. a caller's Unit codec); a missing
  * outcome is not. Empty args likewise remain subject to the definition's codec.
  */
object StrictSagaProtoConverters {
  private val Version = 2
  private def text(value: String, field: String): String = {
    require(value != null && value.nonEmpty, s"$field required")
    value
  }
  private def bytes(value: ByteString, field: String): ByteString = {
    require(value != null, s"$field required")
    value
  }
  private def required[A](value: Option[A], field: String): A =
    value.getOrElse(throw new IllegalArgumentException(s"$field required"))
  private def clean(fields: scalapb.UnknownFieldSet): Unit =
    require(fields == scalapb.UnknownFieldSet.empty, "unknown strict v2 fields")
  private def ordinal(value: Long): Long = {
    require(value > 0, "positive intent/evidence ordinal required")
    value
  }

  def phaseToProto(value: Phase): StrictPhasePO = value match {
    case Phase.Prepare => StrictPhasePO.PREPARE
    case Phase.Commit => StrictPhasePO.COMMIT
    case Phase.Compensate => StrictPhasePO.COMPENSATE
    case _ => throw new IllegalArgumentException("strict phase required")
  }
  def phaseFromProto(value: StrictPhasePO): Phase = value match {
    case StrictPhasePO.PREPARE => Phase.Prepare
    case StrictPhasePO.COMMIT => Phase.Commit
    case StrictPhasePO.COMPENSATE => Phase.Compensate
    case _ => throw new IllegalArgumentException("unspecified/unknown strict phase")
  }
  def statusToProto(value: StrictStatus): StrictStatusPO = value match {
    case StrictStatus.Empty => StrictStatusPO.EMPTY
    case StrictStatus.Preparing => StrictStatusPO.PREPARING
    case StrictStatus.Committing => StrictStatusPO.COMMITTING
    case StrictStatus.Aborting => StrictStatusPO.ABORTING
    case StrictStatus.RecoveryRequired => StrictStatusPO.RECOVERY_REQUIRED
    case StrictStatus.Succeeded => StrictStatusPO.SUCCEEDED
    case StrictStatus.Aborted => StrictStatusPO.ABORTED
    case _ => throw new IllegalArgumentException("strict status required")
  }
  def statusFromProto(value: StrictStatusPO): StrictStatus = value match {
    case StrictStatusPO.EMPTY => StrictStatus.Empty
    case StrictStatusPO.PREPARING => StrictStatus.Preparing
    case StrictStatusPO.COMMITTING => StrictStatus.Committing
    case StrictStatusPO.ABORTING => StrictStatus.Aborting
    case StrictStatusPO.RECOVERY_REQUIRED => StrictStatus.RecoveryRequired
    case StrictStatusPO.SUCCEEDED => StrictStatus.Succeeded
    case StrictStatusPO.ABORTED => StrictStatus.Aborted
    case _ => throw new IllegalArgumentException("unspecified/unknown strict status")
  }

  def codecToProto(value: CodecPin): CodecPinPO = {
    require(value != null, "codec pin required")
    CodecPinPO(value.id, value.version)
  }
  def codecFromProto(value: CodecPinPO): CodecPin = {
    clean(value.unknownFields)
    CodecPin(value.id, value.version)
  }

  private def validateIdentity(value: TransactionIdentity): Unit = {
    require(value != null, "transaction identity required")
    text(value.transactionId, "transaction id")
    text(value.definitionName, "definition name")
    require(value.definitionVersion > 0, "positive definition version required")
    require(value.argsDigest != null && value.argsDigest.matches("[0-9a-f]{64}"), "canonical SHA-256 args digest required")
    Digests.requireSha256(value.definitionDigest, "definition semantics digest")
    codecToProto(value.argsCodec)
    Digests.requireSha256(value.planDigest, "participant plan digest")
  }
  def identityToProto(value: TransactionIdentity): TransactionIdentityPO = {
    validateIdentity(value)
    TransactionIdentityPO(value.transactionId, value.definitionName, value.definitionVersion, value.argsDigest,
      value.definitionDigest, Some(codecToProto(value.argsCodec)), value.planDigest)
  }
  def identityFromProto(value: TransactionIdentityPO): TransactionIdentity = {
    clean(value.unknownFields)
    val result = TransactionIdentity(value.transactionId, value.definitionName, value.definitionVersion, value.argsDigest,
      value.definitionDigest, codecFromProto(required(value.argsCodec, "args codec pin")), value.planDigest)
    validateIdentity(result)
    result
  }
  def operationToProto(value: OperationIdentity): OperationIdentityPO = {
    require(value != null, "operation identity required")
    OperationIdentityPO(Some(identityToProto(value.transaction)), text(value.participantId, "participant id"),
      text(value.resource, "resource"), text(value.stepId, "step id"), phaseToProto(value.phase))
  }
  def operationFromProto(value: OperationIdentityPO): OperationIdentity = {
    clean(value.unknownFields)
    OperationIdentity(identityFromProto(required(value.transaction, "operation transaction")),
      text(value.participantId, "participant id"), text(value.resource, "resource"), text(value.stepId, "step id"),
      phaseFromProto(value.phase))
  }
  private def validateParticipant(value: ParticipantDescriptor): Unit = {
    require(value != null, "participant required")
    text(value.stepId, "step id")
    text(value.participantId, "participant id")
    text(value.resource, "resource")
    require(value.phases == Phase.all, "strict v2 requires canonical three-phase bindings")
    require(value.receiptCodecs != null && value.receiptCodecs.map(_.phase) == Phase.all,
      "canonical per-phase receipt codec pins required")
    value.receiptCodecs.foreach(pin => codecToProto(pin.codec))
  }
  def participantToProto(value: ParticipantDescriptor): ParticipantPO = {
    validateParticipant(value)
    ParticipantPO(value.stepId, value.participantId, value.resource, value.phases.map(phaseToProto),
      value.receiptCodecs.map(pin => ReceiptCodecPinPO(phaseToProto(pin.phase), Some(codecToProto(pin.codec)))))
  }
  def participantFromProto(value: ParticipantPO): ParticipantDescriptor = {
    clean(value.unknownFields)
    val pins = value.receiptCodecs.map { pin =>
      clean(pin.unknownFields)
      ReceiptCodecPin(phaseFromProto(pin.phase), codecFromProto(required(pin.codec, "receipt codec pin")))
    }.toVector
    val result = ParticipantDescriptor(value.stepId, value.participantId, value.resource,
      value.phases.map(phaseFromProto).toVector, pins)
    validateParticipant(result)
    result
  }
  private def validatePlan(value: Vector[ParticipantDescriptor]): Unit = {
    require(value != null && value.nonEmpty, "nonempty participant plan required")
    value.foreach(validateParticipant)
    require(value.map(_.stepId).distinct.size == value.size, "duplicate step ids")
  }
  private[strict] def validateStarted(id: TransactionIdentity, args: ByteString, plan: Vector[ParticipantDescriptor]): Unit = {
    validateIdentity(id)
    bytes(args, "args bytes")
    require(Digests.sha256(args) == id.argsDigest, "args digest mismatch")
    validatePlan(plan)
    require(Digests.plan(plan) == id.planDigest, "participant/receipt codec plan digest mismatch")
  }
  def intentToProto(value: Intent): IntentPO = {
    require(value != null, "intent required")
    IntentPO(Some(operationToProto(value.identity)), ordinal(value.ordinal), value.query)
  }
  def intentFromProto(value: IntentPO): Intent = {
    clean(value.unknownFields)
    Intent(operationFromProto(required(value.identity, "intent identity")), ordinal(value.ordinal), value.query)
  }
  def evidenceToProto(value: Evidence): EvidencePO = {
    require(value != null, "evidence required")
    val outcome = value match {
      case Evidence.Confirmed(_, receipt) => EvidencePO.Outcome.ConfirmedReceipt(bytes(receipt, "receipt bytes"))
      case Evidence.Absent(_, proof, safe) => EvidencePO.Outcome.Absent(AbsentPO(text(proof, "absence proof"), safe))
      case Evidence.Uncertain(_, reason) => EvidencePO.Outcome.Unknown(text(reason, "unknown reason"))
      case Evidence.Refused(_, reason) => EvidencePO.Outcome.Rejected(text(reason, "rejection reason"))
    }
    EvidencePO(Some(operationToProto(value.identity)), outcome)
  }
  def evidenceFromProto(value: EvidencePO): Evidence = {
    clean(value.unknownFields)
    val id = operationFromProto(required(value.identity, "evidence identity"))
    value.outcome match {
      case EvidencePO.Outcome.ConfirmedReceipt(receipt) => Evidence.Confirmed(id, receipt)
      case EvidencePO.Outcome.Absent(absent) =>
        clean(absent.unknownFields)
        Evidence.Absent(id, text(absent.proof, "absence proof"), absent.safeToRetry)
      case EvidencePO.Outcome.Unknown(reason) => Evidence.Uncertain(id, text(reason, "unknown reason"))
      case EvidencePO.Outcome.Rejected(reason) => Evidence.Refused(id, text(reason, "rejection reason"))
      case _ => throw new IllegalArgumentException("strict evidence outcome required")
    }
  }

  def eventToProto(value: StrictEvent): StrictEventPO = {
    val event = value match {
      case StrictEvent.Started(id, args, participants) =>
        validateStarted(id, args, participants)
        StrictEventPO.Event.Started(StartedPO(Some(identityToProto(id)), args, participants.map(participantToProto)))
      case StrictEvent.IntentPersisted(intent) => StrictEventPO.Event.Intent(intentToProto(intent))
      case StrictEvent.EvidenceRecorded(index, evidence) =>
        StrictEventPO.Event.Evidence(EvidenceRecordedPO(ordinal(index), Some(evidenceToProto(evidence))))
      case StrictEvent.CommitDecision => StrictEventPO.Event.CommitDecision(MarkerPO())
      case StrictEvent.AbortRequested(reason) => StrictEventPO.Event.AbortRequested(text(reason, "abort reason"))
      case StrictEvent.RecoveryRequired(reason) => StrictEventPO.Event.RecoveryRequired(text(reason, "recovery reason"))
      case StrictEvent.ReconcileAuthorized(principal, maximum) =>
        require(maximum > 0, "positive reconciliation budget required")
        StrictEventPO.Event.ReconcileAuthorized(ReconcileAuthorizedPO(text(principal, "principal"), maximum))
      case StrictEvent.Succeeded => StrictEventPO.Event.Succeeded(MarkerPO())
      case StrictEvent.Aborted => StrictEventPO.Event.Aborted(MarkerPO())
      case _ => throw new IllegalArgumentException("strict event required")
    }
    StrictEventPO(Version, event)
  }
  def eventFromProto(value: StrictEventPO): StrictEvent = {
    clean(value.unknownFields)
    require(value.schemaVersion == Version, "unsupported strict event schema version")
    value.event match {
      case StrictEventPO.Event.Started(started) =>
        clean(started.unknownFields)
        val id = identityFromProto(required(started.identity, "started identity"))
        val plan = started.participants.map(participantFromProto).toVector
        validateStarted(id, started.args, plan)
        StrictEvent.Started(id, started.args, plan)
      case StrictEventPO.Event.Intent(intent) => StrictEvent.IntentPersisted(intentFromProto(intent))
      case StrictEventPO.Event.Evidence(recorded) =>
        clean(recorded.unknownFields)
        StrictEvent.EvidenceRecorded(ordinal(recorded.ordinal), evidenceFromProto(required(recorded.evidence, "recorded evidence")))
      case StrictEventPO.Event.CommitDecision(marker) => clean(marker.unknownFields); StrictEvent.CommitDecision
      case StrictEventPO.Event.AbortRequested(reason) => StrictEvent.AbortRequested(text(reason, "abort reason"))
      case StrictEventPO.Event.RecoveryRequired(reason) => StrictEvent.RecoveryRequired(text(reason, "recovery reason"))
      case StrictEventPO.Event.ReconcileAuthorized(auth) =>
        clean(auth.unknownFields)
        require(auth.maxInteractions > 0, "positive reconciliation budget required")
        StrictEvent.ReconcileAuthorized(text(auth.principal, "principal"), auth.maxInteractions)
      case StrictEventPO.Event.Succeeded(marker) => clean(marker.unknownFields); StrictEvent.Succeeded
      case StrictEventPO.Event.Aborted(marker) => clean(marker.unknownFields); StrictEvent.Aborted
      case _ => throw new IllegalArgumentException("strict event oneof required")
    }
  }

  private def validateState(value: StrictState): Unit = {
    require(value != null && value.identity != null && value.pending != null && value.evidence != null && value.reason != null,
      "strict state fields required")
    bytes(value.argsBytes, "args bytes")
    statusToProto(value.status)
    require(value.lastOrdinal >= 0, "negative last ordinal")
    value.identity match {
      case None => require(value == StrictState(), "identity-less state must be canonical empty state")
      case Some(id) =>
        validateStarted(id, value.argsBytes, value.participants)
        require(value.status != StrictStatus.Empty, "started state cannot be empty")
        require(!(value.commitDecision && value.abortRequested), "conflicting decisions")
        def operationInPlan(operation: OperationIdentity): Boolean =
          operation.transaction == id && value.participants.exists(p =>
            operation.participantId == p.participantId && operation.resource == p.resource &&
              operation.stepId == p.stepId && p.phases.contains(operation.phase))
        value.evidence.foreach { case (operation, evidence) =>
          evidenceToProto(evidence)
          require(operation == evidence.identity && operationInPlan(operation), "evidence identity outside state plan")
          operation.phase match {
            case Phase.Prepare => ()
            case Phase.Commit => require(value.commitDecision, "commit evidence before durable decision")
            case Phase.Compensate => require(value.abortRequested && !value.commitDecision &&
              value.confirmed(operation.copy(phase = Phase.Prepare)), "compensation evidence without confirmed reservation/abort")
          }
        }
        value.pending.foreach { intent =>
          intentToProto(intent)
          require(intent.ordinal == value.lastOrdinal && operationInPlan(intent.identity), "pending intent outside state/ordinal")
          if (!intent.query) intent.identity.phase match {
            case Phase.Prepare => require(!value.commitDecision && !value.abortRequested, "prepare after decision")
            case Phase.Commit => require(value.commitDecision && !value.abortRequested, "commit before decision")
            case Phase.Compensate => require(value.abortRequested && !value.commitDecision &&
              value.confirmed(intent.identity.copy(phase = Phase.Prepare)), "compensate without confirmed prepare/abort")
          }
        }
        require(value.evidence.isEmpty || value.lastOrdinal > 0, "evidence without interaction ordinal")
        if (value.commitDecision) require(value.participants.forall(p => value.confirmed(value.operation(p, Phase.Prepare))),
          "commit decision without all prepare receipts")
        value.status match {
          case StrictStatus.Preparing => require(!value.commitDecision && !value.abortRequested, "invalid preparing state")
          case StrictStatus.Committing => require(value.commitDecision, "committing without decision")
          case StrictStatus.Aborting => require(value.abortRequested, "aborting without decision")
          case StrictStatus.Succeeded =>
            require(value.commitDecision && value.pending.isEmpty &&
              value.participants.forall(p => value.confirmed(value.operation(p, Phase.Commit))), "unconfirmed terminal commit")
          case StrictStatus.Aborted =>
            require(value.abortRequested && value.pending.isEmpty && value.participants.forall { p =>
              val prepare = value.operation(p, Phase.Prepare)
              if (value.confirmed(prepare)) value.confirmed(value.operation(p, Phase.Compensate))
              else !value.evidence.contains(prepare) || value.safelyAbsent(prepare)
            }, "unresolved terminal abort")
          case StrictStatus.RecoveryRequired => text(value.reason, "recovery reason")
          case _ => throw new IllegalArgumentException("invalid started status")
        }
    }
  }
  def stateToProto(value: StrictState): StrictStatePO = {
    validateState(value)
    // Map iteration order must not leak into snapshot wire bytes.
    val evidence = value.evidence.values.toVector.sortBy(e =>
      (e.identity.stepId, e.identity.participantId, e.identity.resource, phaseToProto(e.identity.phase).value))
    StrictStatePO(Version, value.identity.map(identityToProto), value.argsBytes,
      value.participants.map(participantToProto), statusToProto(value.status), value.commitDecision,
      value.abortRequested, value.lastOrdinal, value.pending.map(intentToProto), evidence.map(evidenceToProto), value.reason)
  }
  def stateFromProto(value: StrictStatePO): StrictState = {
    clean(value.unknownFields)
    require(value.schemaVersion == Version, "unsupported strict state schema version")
    val evidence = value.evidence.map(evidenceFromProto).toVector
    require(evidence.map(_.identity).distinct.size == evidence.size, "duplicate evidence identities")
    val result = StrictState(value.identity.map(identityFromProto), value.args, value.participants.map(participantFromProto).toVector,
      statusFromProto(value.status), value.commitDecision, value.abortRequested, value.lastOrdinal,
      value.pending.map(intentFromProto), evidence.map(e => e.identity -> e).toMap, value.reason)
    validateState(result)
    result
  }
}
