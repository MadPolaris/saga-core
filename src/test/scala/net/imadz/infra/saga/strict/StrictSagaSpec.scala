package net.imadz.infra.saga.strict

import akka.actor.testkit.typed.scaladsl.{ManualTime, ScalaTestWithActorTestKit}
import akka.actor.typed.ActorRef
import akka.persistence.testkit.{PersistenceTestKitPlugin, PersistenceTestKitSnapshotPlugin, SnapshotMeta}
import akka.persistence.testkit.scaladsl.{PersistenceTestKit, SnapshotTestKit}
import akka.persistence.typed.PersistenceId
import akka.serialization.{SerializationExtension, Serializers}
import com.google.protobuf.ByteString
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.Eventually
import org.scalatest.wordspec.AnyWordSpecLike

import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Try}

/** Real journal/snapshot serialization is enabled. Commands and replies are local typed
  * messages: spawning actors directly intentionally avoids command serialization checks.
  * These tests concern the Int-version strict API, not the unimplemented SagaIR bridge.
  */
class StrictSagaSpec extends ScalaTestWithActorTestKit(
  ConfigFactory.parseString("""
    akka.actor.allow-java-serialization = off
    akka.actor.serialize-messages = off
    akka.actor.serialize-creators = off
    akka.persistence.testkit.events.serialize = on
    akka.persistence.testkit.snapshots.serialize = on
    akka.actor.testkit.typed.single-expect-default = 5s
  """).withFallback(ManualTime.config)
    .withFallback(PersistenceTestKitPlugin.config)
    .withFallback(PersistenceTestKitSnapshotPlugin.config)
) with AnyWordSpecLike with Eventually {
  import StrictSagaCoordinator._

  implicit override val patience: PatienceConfig = PatienceConfig(5.seconds, 20.millis)

  private val journal = PersistenceTestKit(system)
  private val snapshots = SnapshotTestKit(system)
  private val clock = ManualTime()(system)
  private val serialization = SerializationExtension(system.classicSystem)

  private val semanticsDigest = "sha256:" + "a" * 64
  private def pinnedStringCodec(codecPin: CodecPin): PayloadCodec[String] = new PayloadCodec[String] {
    override val pin: CodecPin = codecPin
    override def encode(value: String): ByteString = ByteString.copyFromUtf8(value)
    override def decode(bytes: ByteString): Try[String] = Try(bytes.toStringUtf8)
  }
  private val stringCodec: PayloadCodec[String] = pinnedStringCodec(CodecPin("spec/string", 1))
  private final class MutablePinnedCodec(initial: CodecPin) extends PayloadCodec[String] {
    @volatile var currentPin: CodecPin = initial
    @volatile var onEncode: () => Unit = () => ()
    @volatile var onDecode: () => Unit = () => ()
    override def pin: CodecPin = currentPin
    override def encode(value: String): ByteString = { onEncode(); stringCodec.encode(value) }
    override def decode(bytes: ByteString): Try[String] = { onDecode(); stringCodec.decode(bytes) }
  }
  private def descriptor(step: String, participant: String, resource: String): ParticipantDescriptor =
    ParticipantDescriptor(step, participant, resource, Phase.all,
      Phase.all.map(phase => ReceiptCodecPin(phase, stringCodec.pin)))
  private def transaction(tx: String, name: String, bytes: ByteString,
                          plan: Vector[ParticipantDescriptor]): TransactionIdentity =
    TransactionIdentity(tx, name, 1, Digests.sha256(bytes), semanticsDigest, stringCodec.pin, Digests.plan(plan))
  private val authorizer = new ReconcileAuthorizer {
    override def authorize(identity: TransactionIdentity, token: String): Either[String, String] =
      if (token == "operator-token") Right("operator/alice") else Left("not authorized")
  }

  private def stored(pid: PersistenceId): Vector[StrictEvent] =
    journal.persistedInStorage(pid.id).map {
      case event: StrictEvent => event
      case other => fail(s"Unexpected strict journal payload: $other")
    }.toVector

  private def evolve(events: Seq[StrictEvent]): StrictState =
    events.foldLeft(StrictState())(StrictState.evolve)

  private final case class Call(request: OperationRequest[String], query: Boolean,
                                completion: Promise[PhaseOutcome[String]],
                                durableAtInvocation: Vector[StrictEvent]) {
    def confirm(receipt: String = "durable-receipt"): Unit =
      completion.success(ConfirmedReceipt(request.identity, receipt))
    def unknown(): Unit = completion.success(Unknown(request.identity, "participant outcome unavailable"))
  }

  private final class Rig(participantCount: Int = 1,
                          interactionTimeout: FiniteDuration = 30.seconds,
                          prepareCodec: PayloadCodec[String] = stringCodec,
                          prepareResult: Option[OperationRequest[String] => Future[PhaseOutcome[String]]] = None,
                          querySafety: QuerySafety = QuerySafety.DeclaredReadOnly) {
    val pid: PersistenceId = PersistenceId.ofUniqueId(s"strict-spec-${UUID.randomUUID()}")
    val transactionId: String = s"tx-${UUID.randomUUID()}"
    val args: String = "from=account-a;to=account-b;amount=10"
    val argsBytes: ByteString = stringCodec.encode(args)
    val calls = createTestProbe[Call]()
    private val invocations = new ConcurrentLinkedQueue[Call]()
    val materializations = new AtomicInteger()
    val authorizationChecks = new AtomicInteger()
    private val trackedAuthorizer = new ReconcileAuthorizer {
      override def authorize(id: TransactionIdentity, token: String): Either[String, String] = {
        authorizationChecks.incrementAndGet()
        authorizer.authorize(id, token)
      }
    }

    private def binding(phase: Phase, replacement: Option[PayloadCodec[String]] = None): PhaseBinding[String] =
      PhaseBinding(new PhasePort[String, String] {
      override val receiptCodec: PayloadCodec[String] =
        replacement.getOrElse(if (phase == Phase.Prepare) prepareCodec else stringCodec)
      override val querySafety: QuerySafety = Rig.this.querySafety
      private def invoke(request: OperationRequest[String], isQuery: Boolean): Future[PhaseOutcome[String]] = {
        val call = Call(request, isQuery, Promise[PhaseOutcome[String]](), stored(pid))
        invocations.add(call)
        calls.ref ! call
        if (phase == Phase.Prepare && !isQuery && prepareResult.nonEmpty) prepareResult.get(request)
        else call.completion.future
      }
      override def execute(request: OperationRequest[String]): Future[PhaseOutcome[String]] = invoke(request, isQuery = false)
      override def query(request: OperationRequest[String]): Future[PhaseOutcome[String]] = invoke(request, isQuery = true)
    })

    private val participants = (1 to participantCount).map { index =>
      StrictParticipant[String](s"step-$index", s"participant-$index", s"account-$index",
        Phase.all.map(phase => phase -> binding(phase)).toMap)
    }.toVector
    private def definitionFor(digest: String, argsCodec: PayloadCodec[String],
                              replacements: Map[Phase, PayloadCodec[String]],
                              transform: Vector[StrictParticipant[String]] => Vector[StrictParticipant[String]]) =
      StrictSagaDefinition[String]("strict-spec", 1, digest, argsCodec, _ => {
        materializations.incrementAndGet()
        transform(participants.map { p => p.copy(phases = p.phases.map { case (phase, original) =>
          phase -> replacements.get(phase).map(codec => binding(phase, Some(codec))).getOrElse(original)
        }) })
      })
    val definition: StrictSagaDefinition[String] =
      definitionFor(semanticsDigest, stringCodec, Map.empty, plan => plan)
    val identity: TransactionIdentity = definition.identity(transactionId, argsBytes, participants.map(_.descriptor))
    private def behavior(selected: StrictSagaDefinition[String]) = StrictSagaCoordinator(pid, selected, trackedAuthorizer,
      Settings(interactionTimeout, maxReconcileInteractions = 8))
    var ref: ActorRef[Command] = spawn(behavior(definition))

    def allCalls: Vector[Call] = invocations.iterator().asScala.toVector
    def events: Vector[StrictEvent] = stored(pid)
    def reply(command: ActorRef[Reply] => Command): Reply = {
      val probe = createTestProbe[Reply]()
      ref ! command(probe.ref)
      probe.receiveMessage()
    }
    def status(): StrictState = reply(GetStatus.apply) match {
      case Status(state) => state
      case other => fail(s"Expected status, got $other")
    }
    def start(): StrictState = reply(to => Start(transactionId, argsBytes, to)) match {
      case Status(state) => state
      case other => fail(s"Expected accepted start, got $other")
    }
    def awaitStatus(expected: StrictStatus): StrictState = {
      var state = StrictState()
      eventually { state = status(); state.status shouldBe expected }
      state
    }
    def reconcile(budget: Int): StrictState = reply(to => Reconcile(identity, "operator-token", budget, to)) match {
      case Status(state) => state
      case other => fail(s"Expected authorized reconciliation, got $other")
    }
    def next(step: Int, phase: Phase, query: Boolean = false): Call = {
      val call = calls.receiveMessage()
      call.query shouldBe query
      call.request.identity shouldBe
        OperationIdentity(identity, s"participant-$step", s"account-$step", s"step-$step", phase)
      call.request.args shouldBe args
      val durable = evolve(call.durableAtInvocation)
      val intent = durable.pending.getOrElse(fail("Port invoked without a durable pending intent"))
      intent.identity shouldBe call.request.identity
      intent.query shouldBe query
      call.durableAtInvocation.last shouldBe StrictEvent.IntentPersisted(intent)
      call
    }
    def noFurtherIO(): Unit = calls.expectNoMessage(100.millis)
    def restart(snapshot: Option[StrictState] = None,
                selected: StrictSagaDefinition[String] = definition): Unit = {
      testKit.stop(ref)
      snapshot.foreach { state =>
        snapshots.persistForRecovery(pid.id, (SnapshotMeta(events.size.toLong), state))
      }
      ref = spawn(behavior(selected))
    }
    def changedDefinition(digest: String = semanticsDigest, argsCodec: PayloadCodec[String] = stringCodec,
                          receipts: Map[Phase, PayloadCodec[String]] = Map.empty,
                          transform: Vector[StrictParticipant[String]] => Vector[StrictParticipant[String]] = plan => plan)
        : StrictSagaDefinition[String] = definitionFor(digest, argsCodec, receipts, transform)
    def close(): Unit = testKit.stop(ref)
  }

  private def withRig(rig: Rig)(test: Rig => Unit): Unit = try test(rig) finally rig.close()

  "StrictSagaCoordinator with serialized persistence and Java serialization disabled" should {
    "bind every strict durable envelope to event-v2/state-v2 and round-trip complete state" in {
      system.settings.config.getBoolean("akka.actor.allow-java-serialization") shouldBe false
      system.settings.config.getBoolean("akka.persistence.testkit.events.serialize") shouldBe true
      system.settings.config.getBoolean("akka.persistence.testkit.snapshots.serialize") shouldBe true

      val bytes = stringCodec.encode("canonical-args")
      val participant = descriptor("reserve", "bank", "account-a")
      val tx = transaction("serializer-tx", "serializer-definition", bytes, Vector(participant))
      val prepare = OperationIdentity(tx, "bank", "account-a", "reserve", Phase.Prepare)
      val commit = prepare.copy(phase = Phase.Commit)
      val compensate = prepare.copy(phase = Phase.Compensate)
      val started = StrictEvent.Started(tx, bytes, Vector(participant))
      val pending = Vector[StrictEvent](started, StrictEvent.IntentPersisted(Intent(prepare, 1L, query = false)))
      val confirmedPrepare = pending :+ StrictEvent.EvidenceRecorded(1L, Evidence.Confirmed(prepare, bytes))
      val committing = confirmedPrepare :+ StrictEvent.CommitDecision
      val succeeded = committing ++ Vector[StrictEvent](
        StrictEvent.IntentPersisted(Intent(commit, 2L, query = false)),
        StrictEvent.EvidenceRecorded(2L, Evidence.Confirmed(commit, bytes)), StrictEvent.Succeeded)
      val aborting = confirmedPrepare :+ StrictEvent.AbortRequested("caller cancelled")
      val aborted = aborting ++ Vector[StrictEvent](
        StrictEvent.IntentPersisted(Intent(compensate, 2L, query = false)),
        StrictEvent.EvidenceRecorded(2L, Evidence.Confirmed(compensate, ByteString.EMPTY)), StrictEvent.Aborted)
      val evidence: Vector[Evidence] = Vector(
        Evidence.Confirmed(prepare, ByteString.EMPTY),
        Evidence.Absent(prepare, "authoritative fenced absence", safeToRetry = true),
        Evidence.Uncertain(prepare, "unknown"), Evidence.Refused(prepare, "business rejection"))
      val evidenceEvents = evidence.map(StrictEvent.EvidenceRecorded(1L, _))
      val recovery = pending ++ Vector[StrictEvent](evidenceEvents(2), StrictEvent.RecoveryRequired("operator required"))
      val reconciling = recovery ++ Vector[StrictEvent](StrictEvent.ReconcileAuthorized("operator/alice", 2),
        StrictEvent.IntentPersisted(Intent(prepare, 2L, query = true)))
      val events: Vector[StrictEvent] = (succeeded ++ aborted ++ recovery ++ reconciling ++ evidenceEvents).distinct
      val states = Vector(StrictState(), evolve(pending), evolve(committing), evolve(aborting),
        evolve(recovery), evolve(reconciling), evolve(succeeded), evolve(aborted)) ++
        evidenceEvents.map(event => evolve(pending :+ event))

      (events ++ states).foreach { value =>
        val serializer = serialization.findSerializerFor(value)
        serializer.getClass.getSimpleName shouldBe "StrictSagaSerializer"
        serializer.identifier shouldBe 77102231
        val manifest = Serializers.manifestFor(serializer, value)
        manifest shouldBe (if (value.isInstanceOf[StrictEvent]) "event-v2" else "state-v2")
        val encoded = serialization.serialize(value).get
        serialization.deserialize(encoded, serializer.identifier, manifest).get shouldBe value
      }
    }

    "fail closed on missing, unknown and malformed outer wire without any generic fallback" in {
      import net.imadz.infra.saga.strict.proto.saga_strict._
      val serializer = new StrictSagaSerializer
      def refused(wire: Array[Byte], manifest: String = "event-v2"): Unit =
        Try(serializer.fromBinary(wire, manifest)).isFailure shouldBe true
      refused(Array.emptyByteArray)
      refused(null)
      refused(Array[Byte](0x12, 0x7f)) // Truncated length-delimited field.
      refused(StrictEventPO(schemaVersion = 2).toByteArray) // Missing event oneof.
      refused(StrictEventPO(schemaVersion = 1, event = StrictEventPO.Event.Succeeded(MarkerPO())).toByteArray)
      refused(StrictEventPO(schemaVersion = 2, event = StrictEventPO.Event.Intent(IntentPO())).toByteArray)
      refused(StrictEventPO(schemaVersion = 2, event = StrictEventPO.Event.AbortRequested("")).toByteArray)
      refused(StrictStatePO(schemaVersion = 2).toByteArray, "state-v2") // Unspecified status.
      refused(StrictStatePO(schemaVersion = 2, status = StrictStatusPO.Unrecognized(99)).toByteArray, "state-v2")
      val validState = serializer.toBinary(StrictState())
      refused(validState, "unknown-manifest")
      refused(validState, "event-v2")
      refused(validState ++ Array[Byte](0x78, 0x01), "state-v2") // Unknown v2 field.
      intercept[IllegalArgumentException] { serializer.toBinary("not-a-strict-envelope") }
      intercept[IllegalArgumentException] { serializer.manifest(ByteString.EMPTY) }
      // Local messages and raw payloads have no strict binding and cannot use Java fallback.
      serialization.serialize(ByteString.EMPTY).isFailure shouldBe true
      serialization.serialize(GetStatus(createTestProbe[Reply]().ref)).isFailure shouldBe true

      val args = ByteString.copyFromUtf8("args")
      val participant = descriptor("step", "participant", "resource")
      val id = transaction("wire-tx", "wire-definition", args, Vector(participant))
      val operation = OperationIdentity(id, "participant", "resource", "step", Phase.Prepare)
      val intent = StrictSagaProtoConverters.intentToProto(Intent(operation, 1, query = false))
      val operationPO = intent.identity.get
      Vector(StrictPhasePO.PHASE_UNSPECIFIED, StrictPhasePO.Unrecognized(99)).foreach { phase =>
        refused(StrictEventPO(2, StrictEventPO.Event.Intent(intent.copy(identity = Some(operationPO.copy(phase = phase))))).toByteArray)
      }
      val missingOutcome = EvidencePO(identity = Some(operationPO))
      val absentWithoutProof = missingOutcome.copy(outcome = EvidencePO.Outcome.Absent(AbsentPO()))
      Vector(missingOutcome, absentWithoutProof).foreach { proof =>
        refused(StrictEventPO(2, StrictEventPO.Event.Evidence(EvidenceRecordedPO(1, Some(proof)))).toByteArray)
      }
      val started = StrictEvent.Started(id, args, Vector(participant))
      val wireStarted = StrictSagaProtoConverters.eventToProto(started).event.started.get
      refused(StrictEventPO(2, StrictEventPO.Event.Started(wireStarted.copy(args = ByteString.EMPTY))).toByteArray)
      val state = evolve(Vector(started, StrictEvent.IntentPersisted(Intent(operation, 1, query = false)),
        StrictEvent.EvidenceRecorded(1, Evidence.Confirmed(operation, ByteString.EMPTY))))
      val wireState = StrictSagaProtoConverters.stateToProto(state)
      refused(wireState.copy(evidence = wireState.evidence ++ wireState.evidence).toByteArray, "state-v2")
      refused(wireState.copy(commitDecision = true, abortRequested = true).toByteArray, "state-v2")
      // Selected empty receipt and codec-owned empty args are distinct from missing wire data.
      val emptyId = id.copy(argsDigest = Digests.sha256(ByteString.EMPTY))
      val emptyStarted = StrictEvent.Started(emptyId, ByteString.EMPTY, Vector(participant))
      serializer.fromBinary(serializer.toBinary(emptyStarted), "event-v2") shouldBe emptyStarted
    }

    "journal Started and the exact intent before execute, and all prepares before CommitDecision and commit I/O" in {
      withRig(new Rig(participantCount = 2)) { rig =>
        rig.start().identity shouldBe Some(rig.identity)
        val prepare1 = rig.next(1, Phase.Prepare)
        prepare1.durableAtInvocation shouldBe Vector(
          StrictEvent.Started(rig.identity, rig.argsBytes, rig.status().participants),
          StrictEvent.IntentPersisted(Intent(prepare1.request.identity, 1L, query = false)))
        prepare1.confirm("reserved-1")
        val prepare2 = rig.next(2, Phase.Prepare)
        prepare2.durableAtInvocation should not contain StrictEvent.CommitDecision
        prepare2.durableAtInvocation.collect {
          case StrictEvent.EvidenceRecorded(_, proof: Evidence.Confirmed) => proof.identity
        } shouldBe Vector(prepare1.request.identity)
        rig.status().commitDecision shouldBe false

        prepare2.confirm("reserved-2")
        val commit1 = rig.next(1, Phase.Commit)
        val atCommit = evolve(commit1.durableAtInvocation)
        atCommit.commitDecision shouldBe true
        atCommit.participants.forall(p => atCommit.confirmed(atCommit.operation(p, Phase.Prepare))) shouldBe true
        val decisionIndex = commit1.durableAtInvocation.indexOf(StrictEvent.CommitDecision)
        decisionIndex should be > 0
        commit1.durableAtInvocation.take(decisionIndex).collect {
          case StrictEvent.EvidenceRecorded(_, proof: Evidence.Confirmed) => proof.identity
        } shouldBe Vector(prepare1.request.identity, prepare2.request.identity)
        commit1.confirm("committed-1")
        rig.next(2, Phase.Commit).confirm("committed-2")
        val state = rig.awaitStatus(StrictStatus.Succeeded)
        state.pending shouldBe None
        rig.events.last shouldBe StrictEvent.Succeeded
        rig.allCalls.map(_.request.identity.phase) shouldBe Vector(Phase.Prepare, Phase.Prepare, Phase.Commit, Phase.Commit)
        rig.noFurtherIO()
      }
    }

    "deny Abort as a recovery backdoor and preserve the authorized interaction budget" in {
      withRig(new Rig(participantCount = 2)) { rig =>
        rig.start()
        rig.next(1, Phase.Prepare).confirm()
        rig.next(2, Phase.Prepare).unknown()
        val before = rig.awaitStatus(StrictStatus.RecoveryRequired)
        rig.restart()
        rig.reply(to => Abort(rig.identity, "cancel after restart", to)) shouldBe
          Denied("abort cannot resume recovery or bypass an active reconciliation budget")
        rig.noFurtherIO()
        rig.status() shouldBe before
        rig.reconcile(1)
        val query = rig.next(2, Phase.Prepare, query = true)
        rig.reply(to => Abort(rig.identity, "remove the budget", to)) shouldBe
          Denied("abort cannot resume recovery or bypass an active reconciliation budget")
        query.confirm()
        rig.awaitStatus(StrictStatus.RecoveryRequired)
        rig.allCalls.exists(_.request.identity.phase == Phase.Commit) shouldBe false
        rig.allCalls.exists(_.request.identity.phase == Phase.Compensate) shouldBe false
        rig.noFurtherIO()
      }
    }

    "reject snapshot evidence claiming a commit before its durable decision" in {
      val bytes = stringCodec.encode("snapshot-input")
      val p = descriptor("step", "participant", "account")
      val id = transaction("snapshot-tx", "snapshot-definition", bytes, Vector(p))
      val commit = OperationIdentity(id, p.participantId, p.resource, p.stepId, Phase.Commit)
      val impossible = StrictState(Some(id), bytes, Vector(p), StrictStatus.Preparing,
        lastOrdinal = 1L, evidence = Map(commit -> Evidence.Confirmed(commit, bytes)))
      intercept[IllegalArgumentException](StrictSagaProtoConverters.stateToProto(impossible))
    }

    "decode fresh canonical arguments for each phase even if an earlier port mutates its input" in {
      final case class MutableArgs(var amount: Int)
      val codec = new PayloadCodec[MutableArgs] {
        val pin: CodecPin = CodecPin("spec/mutable-args", 1)
        def encode(value: MutableArgs): ByteString = ByteString.copyFromUtf8(value.amount.toString)
        def decode(bytes: ByteString): Try[MutableArgs] = Try(MutableArgs(bytes.toStringUtf8.toInt))
      }
      val seen = createTestProbe[(Phase, Int)]()
      def binding(phase: Phase) = PhaseBinding(new PhasePort[MutableArgs, String] {
        val receiptCodec: PayloadCodec[String] = stringCodec
        val querySafety: QuerySafety = QuerySafety.DeclaredReadOnly
        def execute(request: OperationRequest[MutableArgs]): Future[PhaseOutcome[String]] = {
          seen.ref ! (phase -> request.args.amount)
          request.args.amount = 20
          Future.successful(ConfirmedReceipt(request.identity, "confirmed"))
        }
        def query(request: OperationRequest[MutableArgs]): Future[PhaseOutcome[String]] = execute(request)
      })
      val definition = StrictSagaDefinition("mutable-input", 1, semanticsDigest, codec, (_: MutableArgs) => Vector(
        StrictParticipant("step", "participant", "account", Phase.all.map(phase => phase -> binding(phase)).toMap)))
      val pid = PersistenceId.ofUniqueId(s"mutable-${UUID.randomUUID()}")
      val ref = spawn(StrictSagaCoordinator(pid, definition, authorizer))
      try {
        val replies = createTestProbe[Reply]()
        ref ! Start("mutable-tx", codec.encode(MutableArgs(10)), replies.ref)
        replies.expectMessageType[Status]
        seen.expectMessage(Phase.Prepare -> 10)
        seen.expectMessage(Phase.Commit -> 10)
        eventually { stored(pid).last shouldBe StrictEvent.Succeeded }
      } finally testKit.stop(ref)
    }

    "recover an interrupted journal inertly and retire its pending intent only on authorized reconciliation" in {
      withRig(new Rig()) { rig =>
        rig.start()
        val original = rig.next(1, Phase.Prepare)
        val before = rig.status()
        val history = rig.events
        val beforeRecoveryMaterializations = rig.materializations.get()
        rig.restart()
        rig.status() shouldBe before
        rig.materializations.get() shouldBe beforeRecoveryMaterializations
        rig.reply(to => Start(rig.transactionId, rig.argsBytes, to)) shouldBe Status(before)
        rig.reply(to => Attach(rig.identity, to)) shouldBe Status(before)
        rig.reply(to => Reconcile(rig.identity, "invalid-token", 2, to)) shouldBe Denied("not authorized")
        rig.noFurtherIO()
        rig.events shouldBe history

        rig.reconcile(2)
        val query = rig.next(1, Phase.Prepare, query = true)
        query.request.identity shouldBe original.request.identity
        query.durableAtInvocation should contain(StrictEvent.ReconcileAuthorized("operator/alice", 2))
        query.durableAtInvocation should contain(StrictEvent.EvidenceRecorded(1L,
          Evidence.Uncertain(original.request.identity, "interrupted intent; outcome unknown")))
        evolve(query.durableAtInvocation).lastOrdinal shouldBe 2L
        val pending = rig.status()
        val beforeLateCallback = rig.events
        original.confirm("stopped-incarnation-late-receipt")
        rig.noFurtherIO()
        rig.status() shouldBe pending
        rig.events shouldBe beforeLateCallback
        query.confirm("reservation-found")
        rig.next(1, Phase.Commit).confirm()
        rig.awaitStatus(StrictStatus.Succeeded)
        rig.allCalls.count(call => !call.query && call.request.identity.phase == Phase.Prepare) shouldBe 1
        rig.noFurtherIO()
      }
    }

    "recover a serialized snapshot containing receipts and an interrupted intent without materializing or invoking ports" in {
      withRig(new Rig(participantCount = 2)) { rig =>
        rig.start()
        rig.next(1, Phase.Prepare).confirm("reservation-1")
        rig.next(2, Phase.Prepare)
        val before = rig.status()
        val history = rig.events
        val beforeRecoveryMaterializations = rig.materializations.get()
        rig.restart(snapshot = Some(before))
        snapshots.persistedInStorage(rig.pid.id).map(_._2) shouldBe Vector(before)
        rig.status() shouldBe before
        rig.materializations.get() shouldBe beforeRecoveryMaterializations
        rig.reply(to => Attach(rig.identity, to)) shouldBe Status(before)
        rig.reply(to => Start(rig.transactionId, rig.argsBytes, to)) shouldBe Status(before)
        clock.timePasses(1.minute)
        rig.noFurtherIO()
        rig.status() shouldBe before
        rig.events shouldBe history
      }
    }

    "retain an irreversible commit decision after partial commit failure and recover by query without compensation" in {
      withRig(new Rig(participantCount = 2)) { rig =>
        rig.start()
        rig.next(1, Phase.Prepare).confirm()
        rig.next(2, Phase.Prepare).confirm()
        rig.next(1, Phase.Commit).confirm("committed-1")
        val failedCommit = rig.next(2, Phase.Commit)
        failedCommit.completion.failure(new IllegalStateException("commit acknowledgement lost"))
        val recovery = rig.awaitStatus(StrictStatus.RecoveryRequired)
        recovery.commitDecision shouldBe true
        recovery.abortRequested shouldBe false
        recovery.evidence(failedCommit.request.identity) shouldBe a[Evidence.Uncertain]
        rig.reply(to => Abort(rig.identity, "roll back", to)) shouldBe
          Denied("abort is only allowed before durable commit decision")
        val history = rig.events
        rig.restart()
        rig.status() shouldBe recovery
        rig.events shouldBe history
        rig.noFurtherIO()

        rig.reconcile(1)
        val query = rig.next(2, Phase.Commit, query = true)
        query.request.identity shouldBe failedCommit.request.identity
        query.durableAtInvocation.takeRight(2) shouldBe Vector(
          StrictEvent.ReconcileAuthorized("operator/alice", 1),
          StrictEvent.IntentPersisted(Intent(failedCommit.request.identity, 5L, query = true)))
        query.confirm("committed-2-from-durable-ledger")
        val completed = rig.awaitStatus(StrictStatus.Succeeded)
        completed.commitDecision shouldBe true
        rig.events.count(_ == StrictEvent.CommitDecision) shouldBe 1
        rig.events.exists(_.isInstanceOf[StrictEvent.AbortRequested]) shouldBe false
        rig.allCalls.exists(_.request.identity.phase == Phase.Compensate) shouldBe false
        rig.allCalls.count(call => !call.query && call.request.identity.phase == Phase.Commit) shouldBe 2
        rig.noFurtherIO()
      }
    }

    "leave unknown prepare unresolved, deny unauthorized reconciliation without I/O, and make Start/Attach/status observational" in {
      withRig(new Rig(participantCount = 2)) { rig =>
        rig.start()
        rig.next(1, Phase.Prepare).confirm("existing-reservation")
        val unknown = rig.next(2, Phase.Prepare)
        unknown.unknown()
        val recovery = rig.awaitStatus(StrictStatus.RecoveryRequired)
        recovery.abortRequested shouldBe false
        recovery.commitDecision shouldBe false
        recovery.safelyAbsent(unknown.request.identity) shouldBe false
        val history = rig.events
        rig.reply(to => Start(rig.transactionId, rig.argsBytes, to)) shouldBe Status(recovery)
        rig.reply(to => Attach(rig.identity, to)) shouldBe Status(recovery)
        rig.status() shouldBe recovery
        rig.reply(to => Start(rig.transactionId, stringCodec.encode("different-args"), to)) shouldBe ConflictingIdentity
        rig.reply(to => Attach(rig.identity.copy(definitionVersion = 2), to)) shouldBe ConflictingIdentity
        rig.reply(to => Reconcile(rig.identity, "invalid-token", 2, to)) shouldBe Denied("not authorized")
        rig.reply(to => Reconcile(rig.identity, "operator-token", 0, to)) shouldBe Denied("invalid reconciliation interaction budget")
        rig.reply(to => Reconcile(rig.identity, "operator-token", 9, to)) shouldBe Denied("invalid reconciliation interaction budget")
        rig.noFurtherIO()
        rig.events shouldBe history

        rig.reconcile(3)
        val query = rig.next(2, Phase.Prepare, query = true)
        query.request.identity shouldBe unknown.request.identity
        rig.status().pending.map(_.query) shouldBe Some(true)
        rig.events.exists(_.isInstanceOf[StrictEvent.AbortRequested]) shouldBe false
        rig.events should not contain StrictEvent.CommitDecision
        rig.noFurtherIO() // No abort/compensation/commit while the evidence query is unanswered.
        query.confirm("authoritative-reservation-2")
        rig.next(1, Phase.Commit).confirm()
        rig.next(2, Phase.Commit).confirm()
        rig.awaitStatus(StrictStatus.Succeeded)
        rig.events.exists(_.isInstanceOf[StrictEvent.AbortRequested]) shouldBe false
        rig.allCalls.exists(_.request.identity.phase == Phase.Compensate) shouldBe false
        rig.noFurtherIO()
      }
    }

    "abort only on authoritative safe prepare absence and compensate only confirmed reservations" in {
      withRig(new Rig(participantCount = 2)) { rig =>
        rig.start()
        val reserved = rig.next(1, Phase.Prepare)
        reserved.confirm()
        val absent = rig.next(2, Phase.Prepare)
        absent.completion.success(NotExecuted(absent.request.identity, "durable fenced absence", safeToRetry = true))
        val compensate = rig.next(1, Phase.Compensate)
        val state = evolve(compensate.durableAtInvocation)
        state.abortRequested shouldBe true
        state.commitDecision shouldBe false
        state.confirmed(reserved.request.identity) shouldBe true
        state.safelyAbsent(absent.request.identity) shouldBe true
        compensate.confirm("reservation-released")
        rig.awaitStatus(StrictStatus.Aborted)
        rig.events.last shouldBe StrictEvent.Aborted
        rig.allCalls.map(_.request.identity.phase) shouldBe Vector(Phase.Prepare, Phase.Prepare, Phase.Compensate)
        rig.noFurtherIO()
      }
    }

    "ignore a timed-out prepare's late callback while a higher ordinal query is pending" in {
      withRig(new Rig(interactionTimeout = 10.seconds)) { rig =>
        rig.start()
        val late = rig.next(1, Phase.Prepare)
        clock.timePasses(11.seconds)
        val recovery = rig.awaitStatus(StrictStatus.RecoveryRequired)
        recovery.lastOrdinal shouldBe 1L
        recovery.evidence(late.request.identity) shouldBe Evidence.Uncertain(late.request.identity, "interaction timed out")
        rig.reconcile(2)
        val query = rig.next(1, Phase.Prepare, query = true)
        val pending = rig.status()
        pending.pending shouldBe Some(Intent(late.request.identity, 2L, query = true))
        val beforeLate = rig.events
        late.confirm("late-reservation-receipt")
        rig.noFurtherIO()
        rig.status() shouldBe pending
        rig.events shouldBe beforeLate
        query.confirm("reservation-from-ledger")
        rig.next(1, Phase.Commit).confirm()
        rig.awaitStatus(StrictStatus.Succeeded)
        rig.events.count(_.isInstanceOf[StrictEvent.RecoveryRequired]) shouldBe 1
        rig.allCalls.map(call => (call.request.identity.phase, call.query)) shouldBe
          Vector((Phase.Prepare, false), (Phase.Prepare, true), (Phase.Commit, false))
        rig.noFurtherIO()
      }
    }

    "cancel an old ordinal deadline and ignore a late commit callback after the current deadline" in {
      withRig(new Rig(interactionTimeout = 10.seconds)) { rig =>
        rig.start()
        val prepare = rig.next(1, Phase.Prepare)
        clock.timePasses(5.seconds)
        prepare.confirm()
        val commit = rig.next(1, Phase.Commit)
        clock.timePasses(6.seconds) // The cancelled prepare deadline is due, not the commit deadline.
        val committing = rig.status()
        committing.status shouldBe StrictStatus.Committing
        committing.pending shouldBe Some(Intent(commit.request.identity, 2L, query = false))
        rig.events.exists(_.isInstanceOf[StrictEvent.RecoveryRequired]) shouldBe false
        rig.noFurtherIO()
        clock.timePasses(5.seconds)
        val recovery = rig.awaitStatus(StrictStatus.RecoveryRequired)
        val history = rig.events
        commit.confirm("late-commit-receipt")
        rig.noFurtherIO()
        rig.status() shouldBe recovery
        rig.events shouldBe history
        rig.reconcile(1)
        rig.next(1, Phase.Commit, query = true).confirm("commit-from-ledger")
        rig.awaitStatus(StrictStatus.Succeeded)
        rig.allCalls.count(call => !call.query && call.request.identity.phase == Phase.Commit) shouldBe 1
        rig.allCalls.exists(_.request.identity.phase == Phase.Compensate) shouldBe false
        rig.noFurtherIO()
      }
    }

    "halt at the authorized interaction budget instead of issuing another effect" in {
      withRig(new Rig()) { rig =>
        rig.start()
        rig.next(1, Phase.Prepare).unknown()
        rig.awaitStatus(StrictStatus.RecoveryRequired)
        rig.reconcile(1)
        rig.next(1, Phase.Prepare, query = true).confirm("reservation-found")
        val exhausted = rig.awaitStatus(StrictStatus.RecoveryRequired)
        exhausted.commitDecision shouldBe true
        exhausted.reason shouldBe "reconcile interaction budget exhausted"
        rig.allCalls.map(call => (call.request.identity.phase, call.query)) shouldBe
          Vector((Phase.Prepare, false), (Phase.Prepare, true))
        rig.noFurtherIO()
        rig.reconcile(1)
        rig.next(1, Phase.Commit).confirm()
        rig.awaitStatus(StrictStatus.Succeeded)
        rig.noFurtherIO()
      }
    }

    "refuse recovery queries on ports that do not declare read-only query safety" in {
      withRig(new Rig(querySafety = QuerySafety.Unsupported)) { rig =>
        rig.start()
        rig.next(1, Phase.Prepare).unknown()
        rig.awaitStatus(StrictStatus.RecoveryRequired)
        rig.reconcile(2)
        val recovery = rig.awaitStatus(StrictStatus.RecoveryRequired)
        recovery.reason shouldBe "port does not declare a read-only evidence query"
        rig.allCalls.size shouldBe 1
        rig.events.collect { case event: StrictEvent.IntentPersisted => event.intent.query } shouldBe Vector(false)
        rig.noFurtherIO()
      }
    }

    val interactionFailures: Vector[(String, OperationRequest[String] => Future[PhaseOutcome[String]])] = Vector(
      "a synchronous NonFatal exception" -> ((_: OperationRequest[String]) => throw new IllegalArgumentException("sync port failure")),
      "a failed Future" -> ((_: OperationRequest[String]) => Future.failed(new IllegalStateException("async port failure"))),
      "a mismatched receipt identity" -> ((request: OperationRequest[String]) =>
        Future.successful(ConfirmedReceipt(request.identity.copy(resource = "different-account"), "receipt"))))
    interactionFailures.foreach { case (label, result) =>
      s"record $label as unknown, never safe absence or an automatic abort" in {
        withRig(new Rig(prepareResult = Some(result))) { rig =>
          rig.start()
          val call = rig.next(1, Phase.Prepare)
          val state = rig.awaitStatus(StrictStatus.RecoveryRequired)
          state.evidence(call.request.identity) shouldBe a[Evidence.Uncertain]
          state.safelyAbsent(call.request.identity) shouldBe false
          state.abortRequested shouldBe false
          rig.events.exists(_.isInstanceOf[StrictEvent.AbortRequested]) shouldBe false
          rig.noFurtherIO()
        }
      }
    }

    val codecFailures: Vector[(String, PayloadCodec[String])] = Vector(
      "receipt encode failure" -> new PayloadCodec[String] {
        override val pin: CodecPin = CodecPin("spec/failing-encode", 1)
        override def encode(value: String): ByteString = throw new IllegalArgumentException("cannot encode receipt")
        override def decode(bytes: ByteString): Try[String] = stringCodec.decode(bytes)
      },
      "receipt decode failure" -> new PayloadCodec[String] {
        override val pin: CodecPin = CodecPin("spec/failing-decode", 1)
        override def encode(value: String): ByteString = stringCodec.encode(value)
        override def decode(bytes: ByteString): Try[String] = Failure(new IllegalArgumentException("invalid durable receipt"))
      })
    codecFailures.foreach { case (label, codec) =>
      s"record $label as unknown rather than confirmation or absence" in {
        withRig(new Rig(prepareCodec = codec)) { rig =>
          rig.start()
          val call = rig.next(1, Phase.Prepare)
          call.confirm()
          val state = rig.awaitStatus(StrictStatus.RecoveryRequired)
          state.evidence(call.request.identity) shouldBe a[Evidence.Uncertain]
          state.confirmed(call.request.identity) shouldBe false
          state.safelyAbsent(call.request.identity) shouldBe false
          state.abortRequested shouldBe false
          rig.noFurtherIO()
        }
      }
    }

    "preserve a rejection as refusal, not proof that prepare was never executed" in {
      withRig(new Rig()) { rig =>
        rig.start()
        val prepare = rig.next(1, Phase.Prepare)
        prepare.completion.success(Rejected(prepare.request.identity, "business rejection"))
        val recovery = rig.awaitStatus(StrictStatus.RecoveryRequired)
        recovery.evidence(prepare.request.identity) shouldBe Evidence.Refused(prepare.request.identity, "business rejection")
        recovery.safelyAbsent(prepare.request.identity) shouldBe false
        recovery.abortRequested shouldBe false
        rig.noFurtherIO()
        rig.reconcile(2)
        rig.next(1, Phase.Prepare, query = true).confirm("earlier-invocation-reservation")
        rig.next(1, Phase.Commit).confirm()
        rig.awaitStatus(StrictStatus.Succeeded)
        rig.allCalls.exists(_.request.identity.phase == Phase.Compensate) shouldBe false
        rig.noFurtherIO()
      }
    }

    val pinDrifts: Vector[(String, Rig => StrictSagaDefinition[String])] = Vector(
      "same-version semantics digest" -> ((rig: Rig) => rig.changedDefinition(digest = "sha256:" + "b" * 64)),
      "same-version args codec id" -> ((rig: Rig) => rig.changedDefinition(
        argsCodec = pinnedStringCodec(CodecPin("spec/other-args", 1)))),
      "same-version args codec version" -> ((rig: Rig) => rig.changedDefinition(
        argsCodec = pinnedStringCodec(CodecPin("spec/string", 2))))) ++
      Phase.all.flatMap { phase => Vector(
        s"same-version $phase receipt codec id" -> ((rig: Rig) => rig.changedDefinition(
          receipts = Map(phase -> pinnedStringCodec(CodecPin("spec/other-receipt", 1))))),
        s"same-version $phase receipt codec version" -> ((rig: Rig) => rig.changedDefinition(
          receipts = Map(phase -> pinnedStringCodec(CodecPin("spec/string", 2)))))) }

    pinDrifts.foreach { case (label, change) =>
      s"reject $label on interrupted/recovery and both terminal histories before any port I/O" in {
        Vector("interrupted", "recovery", "succeeded", "aborted").zipWithIndex.foreach { case (history, index) =>
          withRig(new Rig()) { rig =>
            rig.start()
            val prepare = rig.next(1, Phase.Prepare)
            history match {
              case "interrupted" => ()
              case "recovery" => prepare.unknown(); rig.awaitStatus(StrictStatus.RecoveryRequired)
              case "succeeded" =>
                prepare.confirm(); rig.next(1, Phase.Commit).confirm(); rig.awaitStatus(StrictStatus.Succeeded)
              case "aborted" =>
                prepare.completion.success(NotExecuted(prepare.request.identity, "fenced ledger absence", safeToRetry = true))
                rig.awaitStatus(StrictStatus.Aborted)
            }
            val before = rig.status()
            val events = rig.events
            val calls = rig.allCalls
            val materializations = rig.materializations.get()
            val changed = change(rig)
            changed.name shouldBe rig.definition.name
            changed.version shouldBe rig.definition.version
            rig.restart(snapshot = if (index % 2 == 1) Some(before) else None, selected = changed)
            rig.status() shouldBe before // GetStatus is historical, not identity admission.
            rig.materializations.get() shouldBe materializations
            rig.noFurtherIO()
            rig.reply(to => Start(rig.transactionId, rig.argsBytes, to)) shouldBe ConflictingIdentity
            rig.reply(to => Attach(rig.identity, to)) shouldBe ConflictingIdentity
            rig.reply(to => Abort(rig.identity, "do not resume stale semantics", to)) shouldBe ConflictingIdentity
            rig.reply(to => Reconcile(rig.identity, "operator-token", 2, to)) shouldBe ConflictingIdentity
            rig.authorizationChecks.get() shouldBe 0
            clock.timePasses(1.minute)
            rig.noFurtherIO()
            rig.allCalls shouldBe calls
            rig.events shouldBe events
            rig.status() shouldBe before
          }
        }
      }
    }

    "reject caller-supplied semantics, args codec or plan pin conflicts even after terminal success" in {
      withRig(new Rig()) { rig =>
        rig.start()
        rig.next(1, Phase.Prepare).confirm()
        rig.next(1, Phase.Commit).confirm()
        val terminal = rig.awaitStatus(StrictStatus.Succeeded)
        val events = rig.events
        Vector(rig.identity.copy(definitionDigest = "sha256:" + "c" * 64),
          rig.identity.copy(argsCodec = CodecPin("spec/string", 2)),
          rig.identity.copy(planDigest = "sha256:" + "d" * 64)).foreach { stale =>
          rig.reply(to => Attach(stale, to)) shouldBe ConflictingIdentity
          rig.reply(to => Reconcile(stale, "operator-token", 2, to)) shouldBe ConflictingIdentity
          rig.reply(to => Abort(stale, "stale identity", to)) shouldBe ConflictingIdentity
        }
        rig.status() shouldBe terminal
        rig.events shouldBe events
        rig.authorizationChecks.get() shouldBe 0
        rig.noFurtherIO()
      }
    }

    "return the original same-pin state without repeatedly materializing observational Start/Attach" in {
      withRig(new Rig()) { rig =>
        rig.start()
        rig.next(1, Phase.Prepare).unknown()
        val before = rig.awaitStatus(StrictStatus.RecoveryRequired)
        rig.restart(snapshot = Some(before), selected = rig.changedDefinition())
        rig.status() shouldBe before
        val beforeAdmission = rig.materializations.get()
        rig.reply(to => Attach(rig.identity, to)) shouldBe Status(before)
        rig.materializations.get() shouldBe beforeAdmission + 1
        val admitted = rig.materializations.get()
        val events = rig.events
        (1 to 4).foreach { _ =>
          rig.reply(to => Start(rig.transactionId, rig.argsBytes, to)) shouldBe Status(before)
          rig.reply(to => Attach(rig.identity, to)) shouldBe Status(before)
          rig.status() shouldBe before
        }
        rig.materializations.get() shouldBe admitted
        rig.events shouldBe events
        rig.noFurtherIO()
        rig.reconcile(2)
        val query = rig.next(1, Phase.Prepare, query = true)
        query.request.identity.transaction shouldBe rig.identity
        query.confirm("test-port-declared-confirmation")
        rig.next(1, Phase.Commit).confirm()
        val terminal = rig.awaitStatus(StrictStatus.Succeeded)
        val terminalEvents = rig.events
        val terminalMaterializations = rig.materializations.get()
        rig.reply(to => Start(rig.transactionId, rig.argsBytes, to)) shouldBe Status(terminal)
        rig.reply(to => Attach(rig.identity, to)) shouldBe Status(terminal)
        rig.reply(to => Reconcile(rig.identity, "operator-token", 2, to)) shouldBe Status(terminal)
        rig.materializations.get() shouldBe terminalMaterializations
        rig.events shouldBe terminalEvents
        rig.allCalls.count(c => !c.query && c.request.identity.phase == Phase.Prepare) shouldBe 1
        rig.noFurtherIO()
      }
    }

    "reject mutable args pin drift before decoding or authorizing a recovered transaction" in {
      withRig(new Rig()) { rig =>
        rig.start(); rig.next(1, Phase.Prepare).unknown()
        val before = rig.awaitStatus(StrictStatus.RecoveryRequired)
        val codec = new MutablePinnedCodec(stringCodec.pin)
        val decodes = new AtomicInteger()
        codec.onDecode = () => { decodes.incrementAndGet(); () }
        rig.restart(selected = rig.changedDefinition(argsCodec = codec))
        decodes.get() shouldBe 0
        rig.reply(to => Attach(rig.identity, to)) shouldBe Status(before)
        val admittedDecodes = decodes.get()
        codec.currentPin = CodecPin("spec/string", 2)
        rig.reply(to => Start(rig.transactionId, rig.argsBytes, to)) shouldBe ConflictingIdentity
        rig.reply(to => Attach(rig.identity, to)) shouldBe ConflictingIdentity
        rig.reply(to => Reconcile(rig.identity, "operator-token", 2, to)) shouldBe ConflictingIdentity
        decodes.get() shouldBe admittedDecodes
        rig.authorizationChecks.get() shouldBe 0
        rig.status() shouldBe before
        rig.noFurtherIO()
      }
    }

    Vector("before completion", "inside encode", "inside decode").foreach { timing =>
      s"record late receipt codec drift $timing as unknown and resume only under the original authorized pin" in {
        val codec = new MutablePinnedCodec(stringCodec.pin)
        withRig(new Rig(prepareCodec = codec)) { rig =>
          rig.start()
          val prepare = rig.next(1, Phase.Prepare)
          val drift = () => { codec.currentPin = CodecPin("spec/string", 2); () }
          timing match {
            case "before completion" => drift()
            case "inside encode" => codec.onEncode = drift
            case "inside decode" => codec.onDecode = drift
          }
          prepare.confirm("test-port-declared-receipt")
          val recovery = rig.awaitStatus(StrictStatus.RecoveryRequired)
          recovery.evidence(prepare.request.identity) shouldBe a[Evidence.Uncertain]
          recovery.confirmed(prepare.request.identity) shouldBe false
          recovery.commitDecision shouldBe false
          recovery.abortRequested shouldBe false
          val events = rig.events
          rig.reply(to => Attach(rig.identity, to)) shouldBe ConflictingIdentity
          rig.reply(to => Reconcile(rig.identity, "operator-token", 2, to)) shouldBe ConflictingIdentity
          rig.events shouldBe events
          rig.noFurtherIO()
          codec.onEncode = () => ()
          codec.onDecode = () => ()
          codec.currentPin = stringCodec.pin
          rig.restart(snapshot = Some(recovery))
          rig.status() shouldBe recovery
          rig.noFurtherIO()
          rig.reconcile(2)
          rig.next(1, Phase.Prepare, query = true).confirm()
          rig.next(1, Phase.Commit).confirm()
          rig.awaitStatus(StrictStatus.Succeeded)
          rig.allCalls.count(c => !c.query && c.request.identity.phase == Phase.Prepare) shouldBe 1
          rig.noFurtherIO()
        }
      }
    }

    "reject receipt codec drift at the invocation boundary without calling the port" in {
      implicit val ec: scala.concurrent.ExecutionContext = system.executionContext
      val codec = new MutablePinnedCodec(stringCodec.pin)
      val io = new AtomicInteger()
      val binding = PhaseBinding(new PhasePort[String, String] {
        val receiptCodec: PayloadCodec[String] = codec
        val querySafety: QuerySafety = QuerySafety.DeclaredReadOnly
        def execute(request: OperationRequest[String]): Future[PhaseOutcome[String]] = {
          io.incrementAndGet(); Future.successful(ConfirmedReceipt(request.identity, "test-receipt"))
        }
        def query(request: OperationRequest[String]): Future[PhaseOutcome[String]] = execute(request)
      })
      val p = descriptor("step", "participant", "resource")
      val tx = transaction("invoke-tx", "invoke-definition", ByteString.EMPTY, Vector(p))
      val request = OperationRequest(OperationIdentity(tx, p.participantId, p.resource, p.stepId, Phase.Prepare), "")
      codec.currentPin = CodecPin("spec/string", 2)
      Vector(false, true).foreach { query =>
        intercept[IllegalArgumentException] { binding.invoke(request, query, stringCodec.pin) }
      }
      io.get() shouldBe 0
    }

    "digest the actual ordered descriptors and every phase receipt codec into operation identity" in {
      val plan = Vector(descriptor("first", "bank", "account-a"), descriptor("second", "bank", "account-b"))
      val digest = Digests.plan(plan)
      digest should fullyMatch regex "sha256:[0-9a-f]{64}"
      Digests.plan(plan) shouldBe digest
      val variants = Vector(plan.reverse,
        plan.updated(0, plan.head.copy(stepId = "changed-step")),
        plan.updated(0, plan.head.copy(participantId = "changed-participant")),
        plan.updated(0, plan.head.copy(resource = "changed-resource")),
        plan.updated(0, plan.head.copy(phases = plan.head.phases.reverse))) ++
        Phase.all.indices.map { index =>
          val pins = plan.head.receiptCodecs.updated(index,
            ReceiptCodecPin(Phase.all(index), CodecPin("spec/string", 2)))
          plan.updated(0, plan.head.copy(receiptCodecs = pins))
        }
      val original = transaction("plan-tx", "plan-definition", ByteString.EMPTY, plan)
      val operation = OperationIdentity(original, "bank", "account-a", "first", Phase.Prepare)
      variants.foreach { variant =>
        Digests.plan(variant) should not be digest
        operation.copy(transaction = original.copy(planDigest = Digests.plan(variant))) should not be operation
      }
      withRig(new Rig(participantCount = 2)) { rig =>
        rig.start(); rig.next(1, Phase.Prepare).unknown()
        val before = rig.awaitStatus(StrictStatus.RecoveryRequired)
        rig.restart(selected = rig.changedDefinition(transform = _.reverse))
        rig.reply(to => Start(rig.transactionId, rig.argsBytes, to)) shouldBe ConflictingIdentity
        rig.reply(to => Attach(rig.identity, to)) shouldBe ConflictingIdentity
        rig.reply(to => Reconcile(rig.identity, "operator-token", 2, to)) shouldBe ConflictingIdentity
        rig.status() shouldBe before
        rig.noFurtherIO()
      }
    }

    "carry complete explicit pins in v2 Started, intent, evidence and snapshot wire independently of business versions" in {
      import net.imadz.infra.saga.strict.proto.saga_strict._
      val bytes = stringCodec.encode("pin-wire-input")
      val pins = Phase.all.zipWithIndex.map { case (phase, index) =>
        ReceiptCodecPin(phase, CodecPin(s"business/receipt-$index", index + 11))
      }
      val p = ParticipantDescriptor("step", "participant", "resource", Phase.all, pins)
      val id = transaction("pin-wire-tx", "name-has-no-schema-v99", bytes, Vector(p)).copy(
        definitionVersion = 17, argsCodec = CodecPin("business/args", 23))
      val started = StrictEvent.Started(id, bytes, Vector(p))
      val operation = OperationIdentity(id, p.participantId, p.resource, p.stepId, Phase.Prepare)
      val intent = StrictEvent.IntentPersisted(Intent(operation, 1, query = false))
      val recorded = StrictEvent.EvidenceRecorded(1, Evidence.Confirmed(operation, ByteString.EMPTY))
      val state = evolve(Vector(started, intent, recorded))
      val serializer = new StrictSagaSerializer
      def checkIdentity(wire: TransactionIdentityPO): Unit = {
        wire.definitionVersion shouldBe 17
        wire.definitionDigest shouldBe semanticsDigest
        wire.argsCodec shouldBe Some(CodecPinPO("business/args", 23))
        wire.planDigest shouldBe Digests.plan(Vector(p))
        StrictSagaProtoConverters.identityFromProto(wire) shouldBe id
      }
      Vector(started, intent, recorded).foreach { event =>
        val encoded = serializer.toBinary(event)
        val wire = StrictEventPO.parseFrom(encoded)
        wire.schemaVersion shouldBe 2
        serializer.manifest(event) shouldBe "event-v2"
        serializer.fromBinary(encoded, "event-v2") shouldBe event
        wire.event match {
          case StrictEventPO.Event.Started(value) =>
            checkIdentity(value.identity.get)
            value.participants.head.receiptCodecs.map(pin =>
              ReceiptCodecPin(StrictSagaProtoConverters.phaseFromProto(pin.phase),
                StrictSagaProtoConverters.codecFromProto(pin.codec.get))).toVector shouldBe pins
          case StrictEventPO.Event.Intent(value) => checkIdentity(value.identity.get.transaction.get)
          case StrictEventPO.Event.Evidence(value) => checkIdentity(value.evidence.get.identity.get.transaction.get)
          case other => fail(s"Unexpected wire event: $other")
        }
      }
      val wireState = StrictStatePO.parseFrom(serializer.toBinary(state))
      wireState.schemaVersion shouldBe 2
      checkIdentity(wireState.identity.get)
      checkIdentity(wireState.evidence.head.identity.get.transaction.get)
      wireState.participants.head shouldBe StrictSagaProtoConverters.participantToProto(p)
      serializer.fromBinary(wireState.toByteArray, "state-v2") shouldBe state
    }

    "fail closed on absent or malformed v2 pins and on unpinned strict v1 without manufacturing declarations" in {
      import net.imadz.infra.saga.strict.proto.saga_strict._
      val serializer = new StrictSagaSerializer
      val args = stringCodec.encode("pin-failclosed-args")
      val p = descriptor("step", "participant", "resource")
      val id = transaction("pin-failclosed-tx", "pin-definition", args, Vector(p))
      val started = StrictEvent.Started(id, args, Vector(p))
      val wire = StrictSagaProtoConverters.eventToProto(started).event.started.get
      val wireId = wire.identity.get
      val state = StrictSagaProtoConverters.stateToProto(evolve(Vector(started)))
      def refuse(start: StartedPO): Unit = {
        Try(serializer.fromBinary(StrictEventPO(2, StrictEventPO.Event.Started(start)).toByteArray,
          "event-v2")).isFailure shouldBe true
        Try(serializer.fromBinary(state.copy(identity = start.identity, participants = start.participants).toByteArray,
          "state-v2")).isFailure shouldBe true
      }
      def refuseIdentity(bad: TransactionIdentityPO): Unit = {
        refuse(wire.copy(identity = Some(bad)))
        val operation = StrictSagaProtoConverters.operationToProto(
          OperationIdentity(id, p.participantId, p.resource, p.stepId, Phase.Prepare)).copy(transaction = Some(bad))
        val invalidEvents = Vector(
          StrictEventPO(2, StrictEventPO.Event.Intent(IntentPO(Some(operation), 1, query = false))),
          StrictEventPO(2, StrictEventPO.Event.Evidence(EvidenceRecordedPO(1,
            Some(EvidencePO(Some(operation), EvidencePO.Outcome.ConfirmedReceipt(ByteString.EMPTY)))))))
        invalidEvents.foreach { event =>
          Try(serializer.fromBinary(event.toByteArray, "event-v2")).isFailure shouldBe true
        }
      }
      Vector("", "sha256:abc", "sha256:" + "g" * 64, "sha256:" + "A" * 64, "a" * 64).foreach { digest =>
        refuseIdentity(wireId.copy(definitionDigest = digest))
        refuseIdentity(wireId.copy(planDigest = digest))
      }
      val badCodecs: Vector[Option[CodecPinPO]] = Vector(None, Some(CodecPinPO()),
        Some(CodecPinPO("business/args", 0)), Some(CodecPinPO("business/args", -1)),
        Some(CodecPinPO("invalid codec id", 1)))
      badCodecs.foreach { bad =>
        refuseIdentity(wireId.copy(argsCodec = bad))
        Phase.all.indices.foreach { index =>
          val participant = wire.participants.head
          val pin = participant.receiptCodecs(index).copy(codec = bad)
          refuse(wire.copy(participants = Vector(participant.copy(receiptCodecs =
            participant.receiptCodecs.updated(index, pin)))))
        }
      }
      val participant = wire.participants.head
      Vector(participant.copy(receiptCodecs = Vector.empty),
        participant.copy(receiptCodecs = participant.receiptCodecs.dropRight(1)),
        participant.copy(receiptCodecs = participant.receiptCodecs.reverse),
        participant.copy(receiptCodecs = participant.receiptCodecs.updated(1, participant.receiptCodecs.head)),
        participant.copy(receiptCodecs = participant.receiptCodecs.updated(0,
          participant.receiptCodecs.head.copy(phase = StrictPhasePO.PHASE_UNSPECIFIED)))).foreach { bad =>
        refuse(wire.copy(participants = Vector(bad)))
      }
      val drifted = participant.copy(receiptCodecs = participant.receiptCodecs.updated(2,
        participant.receiptCodecs(2).copy(codec = Some(CodecPinPO("spec/string", 2)))))
      refuse(wire.copy(participants = Vector(drifted))) // Valid pin, inconsistent plan digest.
      val legacy = wire.copy(identity = Some(wireId.copy(definitionDigest = "", argsCodec = None, planDigest = "")),
        participants = Vector(participant.copy(receiptCodecs = Vector.empty)))
      refuse(legacy) // Claiming v2 does not supply the absent pins.
      val legacyEvent = StrictEventPO(1, StrictEventPO.Event.Started(legacy)).toByteArray
      Vector("event-v1", "event-v2").foreach { manifest =>
        Try(serializer.fromBinary(legacyEvent, manifest)).isFailure shouldBe true
      }
      val legacyState = state.copy(schemaVersion = 1, identity = legacy.identity, participants = legacy.participants).toByteArray
      Vector("state-v1", "state-v2").foreach { manifest =>
        Try(serializer.fromBinary(legacyState, manifest)).isFailure shouldBe true
      }
      Try(serializer.fromBinary(serializer.toBinary(started), "event-v1")).isFailure shouldBe true
      Try(serializer.fromBinary(serializer.toBinary(evolve(Vector(started))), "state-v1")).isFailure shouldBe true
    }

    "require canonical semantics SHA-256 and positive explicitly declared codec versions without name inference" in {
      Vector(null, "", "not-a-digest", "a" * 64, "sha256:" + "A" * 64).foreach { digest =>
        intercept[IllegalArgumentException] {
          StrictSagaDefinition[String]("definition-v900", 1, digest, stringCodec, _ => Vector.empty)
        }
      }
      Vector(null, "", "id with spaces").foreach { id =>
        intercept[IllegalArgumentException] { CodecPin(id, 1) }
      }
      Vector(0, -1).foreach { version =>
        intercept[IllegalArgumentException] { CodecPin("business/args", version) }
      }
      CodecPin("business/args-v900", 3).version shouldBe 3
      val noPin = new PayloadCodec[String] {
        def pin: CodecPin = null
        def encode(value: String): ByteString = stringCodec.encode(value)
        def decode(bytes: ByteString): Try[String] = stringCodec.decode(bytes)
      }
      intercept[IllegalArgumentException] {
        StrictSagaDefinition[String]("definition-v900", 7, semanticsDigest, noPin, _ => Vector.empty)
      }
    }

    "reject null or empty start inputs without journaling or invoking participants" in {
      withRig(new Rig()) { rig =>
        rig.reply(to => Start(null, rig.argsBytes, to)) shouldBe a[Denied]
        rig.reply(to => Start(rig.transactionId, null, to)) shouldBe a[Denied]
        rig.reply(to => Start("", rig.argsBytes, to)) shouldBe a[Denied]
        rig.status() shouldBe StrictState()
        rig.events shouldBe empty
        rig.materializations.get() shouldBe 0
        rig.noFurtherIO()
      }
    }
  }
}
