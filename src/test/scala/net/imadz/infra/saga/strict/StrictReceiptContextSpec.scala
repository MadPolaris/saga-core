package net.imadz.infra.saga.strict

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.testkit.scaladsl.PersistenceTestKit
import akka.persistence.typed.PersistenceId
import com.google.protobuf.ByteString
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.Eventually
import org.scalatest.wordspec.AnyWordSpecLike

import java.util.UUID
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/** Engineering tests with real typed PhasePorts and serialized durable evidence.
  * Not a public application, database-side receipt certification or cross-JVM test.
  */
class StrictReceiptContextSpec extends ScalaTestWithActorTestKit(
  ConfigFactory.parseString("""
    akka.actor.allow-java-serialization = off
    akka.actor.serialize-messages = off
    akka.persistence.testkit.events.serialize = on
    akka.actor.testkit.typed.single-expect-default = 5s
  """).withFallback(PersistenceTestKitPlugin.config)
) with AnyWordSpecLike with Eventually {
  import StrictSagaCoordinator._
  implicit override val patience: PatienceConfig = PatienceConfig(5.seconds, 20.millis)
  private val journal = PersistenceTestKit(system)
  private val digest = "sha256:" + "b" * 64
  private val argsCodec = new PayloadCodec[String] {
    override val pin: CodecPin = CodecPin("receipt-spec/args", 1)
    override def encode(value: String): ByteString = ByteString.copyFromUtf8(value)
    override def decode(bytes: ByteString): Try[String] = Try(bytes.toStringUtf8)
  }
  private final case class Reservation(token: String, upstream: String)
  private final case class Settlement(token: String, reservation: String, upstream: String)
  private val reservationCodec = new PayloadCodec[Reservation] {
    override val pin: CodecPin = CodecPin("receipt-spec/reservation", 1)
    override def encode(value: Reservation): ByteString = ByteString.copyFromUtf8(s"${value.token}|${value.upstream}")
    override def decode(bytes: ByteString): Try[Reservation] = Try {
      val fields = bytes.toStringUtf8.split("\\|", -1)
      require(fields.length == 2 && fields(0).nonEmpty, "malformed reservation")
      Reservation(fields(0), fields(1))
    }
  }
  private val settlementCodec = new PayloadCodec[Settlement] {
    override val pin: CodecPin = CodecPin("receipt-spec/settlement", 1)
    override def encode(value: Settlement): ByteString =
      ByteString.copyFromUtf8(s"${value.token}|${value.reservation}|${value.upstream}")
    override def decode(bytes: ByteString): Try[Settlement] = Try {
      val fields = bytes.toStringUtf8.split("\\|", -1)
      require(fields.length == 3 && fields(0).nonEmpty && fields(1).nonEmpty, "malformed settlement")
      Settlement(fields(0), fields(1), fields(2))
    }
  }
  private def descriptor(step: String): ParticipantDescriptor =
    ParticipantDescriptor(step, s"$step-port", s"$step-account", Phase.all,
      Phase.all.map(p => ReceiptCodecPin(p, if (p == Phase.Commit) settlementCodec.pin else reservationCodec.pin)))
  private val plan = Vector(descriptor("source"), descriptor("target"))
  private val bytes = argsCodec.encode("amount=10")
  private val tx = TransactionIdentity("unit-tx", "receipt-spec", 2, Digests.sha256(bytes), digest,
    argsCodec.pin, Digests.plan(plan))
  private def operation(transaction: TransactionIdentity, step: String, phase: Phase): OperationIdentity = {
    val p = plan.find(_.stepId == step).get
    OperationIdentity(transaction, p.participantId, p.resource, p.stepId, phase)
  }
  private def op(step: String, phase: Phase): OperationIdentity = operation(tx, step, phase)
  private val sourcePrepare = op("source", Phase.Prepare)
  private val targetPrepare = op("target", Phase.Prepare)
  private val sourceCommit = op("source", Phase.Commit)
  private val targetCommit = op("target", Phase.Commit)
  private val sourceCompensate = op("source", Phase.Compensate)
  private val targetCompensate = op("target", Phase.Compensate)
  private val confirmed: Map[OperationIdentity, Evidence] = Map(
    sourcePrepare -> Evidence.Confirmed(sourcePrepare, reservationCodec.encode(Reservation("source-reservation", ""))),
    targetPrepare -> Evidence.Confirmed(targetPrepare, reservationCodec.encode(Reservation("target-reservation", "source-reservation"))),
    sourceCommit -> Evidence.Confirmed(sourceCommit, settlementCodec.encode(Settlement("source-settled", "source-reservation", ""))),
    targetCommit -> Evidence.Confirmed(targetCommit, settlementCodec.encode(Settlement("target-settled", "target-reservation", "source-settled"))),
    sourceCompensate -> Evidence.Confirmed(sourceCompensate, reservationCodec.encode(Reservation("released", ""))))
  private def state(consumer: OperationIdentity, evidence: Map[OperationIdentity, Evidence] = confirmed,
                    commit: Boolean = false, abort: Boolean = false, query: Boolean = false): StrictState =
    StrictState(Some(tx), bytes, plan, commitDecision = commit, abortRequested = abort,
      lastOrdinal = 1L, pending = Some(Intent(consumer, 1L, query)), evidence = evidence)
  private def context(s: StrictState): StrictReceiptContext =
    StrictReceiptContext.fromPersistedIntent(s, s.pending.get.identity)
  private def reserve(c: StrictReceiptContext, consumer: OperationIdentity, producer: OperationIdentity): Try[Reservation] =
    c.lookup(consumer, producer, reservationCodec.pin, reservationCodec)

  "Consumer-bound strict receipt context" should {
    "keep old request construction, copy and two-field extraction source-compatible and default closed" in {
      val request = OperationRequest(targetPrepare, "args")
      val OperationRequest(id, args) = request
      id shouldBe targetPrepare
      args shouldBe "args"
      request.copy(args = "changed").receipt(sourcePrepare, reservationCodec.pin, reservationCodec).isFailure shouldBe true
      request.receipt(sourcePrepare, reservationCodec.pin, reservationCodec).isFailure shouldBe true
    }
    "allow target Prepare to read only prior source Prepare" in {
      val c = context(state(targetPrepare))
      reserve(c, targetPrepare, sourcePrepare).get.token shouldBe "source-reservation"
      Vector(targetPrepare, sourceCommit, targetCommit, sourceCompensate, targetCompensate).foreach { p =>
        reserve(c, targetPrepare, p).isFailure shouldBe true
      }
    }
    "deny future Prepare and every receipt to the first Prepare" in {
      val c = context(state(sourcePrepare))
      Vector(sourcePrepare, targetPrepare, sourceCommit, targetCommit).foreach { p =>
        reserve(c, sourcePrepare, p).isFailure shouldBe true
      }
    }
    "allow all Prepare and prior Commit only after durable CommitDecision" in {
      val c = context(state(targetCommit, commit = true))
      reserve(c, targetCommit, sourcePrepare).get.token shouldBe "source-reservation"
      reserve(c, targetCommit, targetPrepare).get.token shouldBe "target-reservation"
      c.lookup(targetCommit, sourceCommit, settlementCodec.pin, settlementCodec).get.token shouldBe "source-settled"
      Vector(targetCommit, sourceCompensate, targetCompensate).foreach { p =>
        c.lookup(targetCommit, p, settlementCodec.pin, settlementCodec).isFailure shouldBe true
      }
      val first = context(state(sourceCommit, commit = true))
      first.lookup(sourceCommit, targetCommit, settlementCodec.pin, settlementCodec).isFailure shouldBe true
      first.lookup(sourceCommit, sourceCommit, settlementCodec.pin, settlementCodec).isFailure shouldBe true
    }
    "require CommitDecision for Commit execute and query" in {
      Vector(false, true).foreach { query =>
        Try(context(state(targetCommit, query = query))).isFailure shouldBe true
        Try(context(state(targetCommit, commit = true, abort = true, query = query))).isFailure shouldBe true
      }
    }
    "limit Compensate to own Prepare under AbortDecision without committed evidence" in {
      val beforeCommit = confirmed.filterNot { case (id, _) => id.phase == Phase.Commit }
      val c = context(state(targetCompensate, beforeCommit, abort = true))
      reserve(c, targetCompensate, targetPrepare).get.token shouldBe "target-reservation"
      Vector(sourcePrepare, sourceCompensate, targetCompensate, sourceCommit, targetCommit).foreach { p =>
        reserve(c, targetCompensate, p).isFailure shouldBe true
      }
      Try(context(state(targetCompensate, beforeCommit))).isFailure shouldBe true
      Try(context(state(targetCompensate, beforeCommit, commit = true, abort = true))).isFailure shouldBe true
      Try(context(state(targetCompensate, abort = true))).isFailure shouldBe true
    }
    "allow unknown Prepare queries after abort without allowing new Prepare execution" in {
      val s = state(targetPrepare, confirmed.updated(targetPrepare, Evidence.Uncertain(targetPrepare, "unknown")),
        abort = true, query = true)
      reserve(context(s), targetPrepare, sourcePrepare).isSuccess shouldBe true
      Try(context(s.copy(pending = Some(Intent(targetPrepare, 1L, query = false))))).isFailure shouldBe true
      Try(context(s.copy(commitDecision = true))).isFailure shouldBe true
    }
    "require a matching persisted intent and complete consumer descriptor" in {
      val s = state(targetPrepare)
      Try(context(s.copy(pending = None))).isFailure shouldBe true
      Try(StrictReceiptContext.fromPersistedIntent(s, sourcePrepare)).isFailure shouldBe true
      val bad = targetPrepare.copy(resource = "wrong-account")
      Try(context(s.copy(pending = Some(Intent(bad, 1L, query = false))))).isFailure shouldBe true
      val otherTx = targetPrepare.copy(transaction = tx.copy(transactionId = "other-tx"))
      Try(context(s.copy(pending = Some(Intent(otherTx, 1L, query = true))))).isFailure shouldBe true
      Try(context(s.copy(participants = plan.reverse))).isFailure shouldBe true
    }
    "bind each lookup to the full consumer identity even when a request is copied" in {
      val c = context(state(targetPrepare))
      val changed = Vector(targetPrepare.copy(resource = "elsewhere"), targetPrepare.copy(participantId = "other"),
        targetPrepare.copy(stepId = "source"), targetPrepare.copy(phase = Phase.Commit),
        targetPrepare.copy(transaction = tx.copy(definitionDigest = "sha256:" + "c" * 64)))
      changed.foreach { consumer => reserve(c, consumer, sourcePrepare).isFailure shouldBe true }
      OperationRequest(targetPrepare, "args", c).copy(identity = sourcePrepare)
        .receipt(sourcePrepare, reservationCodec.pin, reservationCodec).isFailure shouldBe true
    }
    "reject producer transaction, resource, participant, step, phase and durable pin mismatches" in {
      val c = context(state(targetPrepare))
      val changed = Vector(sourcePrepare.copy(resource = "other"), sourcePrepare.copy(participantId = "other"),
        sourcePrepare.copy(stepId = "other"), sourcePrepare.copy(phase = Phase.Commit),
        sourcePrepare.copy(transaction = tx.copy(transactionId = "other")),
        sourcePrepare.copy(transaction = tx.copy(argsCodec = CodecPin("other/args", 1))))
      changed.foreach { producer => reserve(c, targetPrepare, producer).isFailure shouldBe true }
      c.lookup(targetPrepare, sourcePrepare, CodecPin(reservationCodec.pin.id, 2), reservationCodec).isFailure shouldBe true
      c.lookup(targetPrepare, sourcePrepare, CodecPin("wrong/receipt", 1), reservationCodec).isFailure shouldBe true
    }
    "reject missing, unknown, absent, refused, null bytes and mismatched evidence key/identity" in {
      val bad: Vector[Option[Evidence]] = Vector(None,
        Some(Evidence.Uncertain(sourcePrepare, "unknown")), Some(Evidence.Absent(sourcePrepare, "proof", true)),
        Some(Evidence.Refused(sourcePrepare, "no")), Some(Evidence.Confirmed(sourcePrepare, null)),
        Some(Evidence.Confirmed(targetPrepare, reservationCodec.encode(Reservation("wrong", "")))),
        Some(Evidence.Confirmed(sourcePrepare.copy(resource = "other"), bytes)),
        Some(Evidence.Confirmed(sourcePrepare.copy(transaction = tx.copy(transactionId = "other")), bytes)))
      bad.foreach { e =>
        val proofs = e.fold(confirmed - sourcePrepare)(v => confirmed.updated(sourcePrepare, v))
        reserve(context(state(targetPrepare, proofs)), targetPrepare, sourcePrepare).isFailure shouldBe true
      }
      val wrongKey = (confirmed - sourcePrepare).updated(sourcePrepare.copy(resource = "other"), confirmed(sourcePrepare))
      reserve(context(state(targetPrepare, wrongKey)), targetPrepare, sourcePrepare).isFailure shouldBe true
    }
    "reject malformed/null decoded receipts and null codecs without exposing bytes" in {
      val c = context(state(targetPrepare, confirmed.updated(sourcePrepare, Evidence.Confirmed(sourcePrepare, bytes))))
      reserve(c, targetPrepare, sourcePrepare).isFailure shouldBe true
      val valid = context(state(targetPrepare))
      val nullCodec = new PayloadCodec[Reservation] {
        override val pin: CodecPin = reservationCodec.pin
        override def encode(value: Reservation): ByteString = fail("lookup must not encode")
        override def decode(bytes: ByteString): Try[Reservation] = Success(null)
      }
      valid.lookup(targetPrepare, sourcePrepare, nullCodec.pin, nullCodec).isFailure shouldBe true
      valid.lookup[Reservation](targetPrepare, sourcePrepare, reservationCodec.pin, null).isFailure shouldBe true
      valid.lookup(targetPrepare, sourcePrepare, null, reservationCodec).isFailure shouldBe true
    }
    "check actual codec pin before and after fresh decode without imposing canonical re-encode" in {
      val c = context(state(targetPrepare))
      var currentPin = reservationCodec.pin
      var driftInside = false
      var decodes = 0
      val codec = new PayloadCodec[Reservation] {
        override def pin: CodecPin = StrictReceiptContextSpec.this.reservationCodec.pin.copy(version =
          if (driftInside && decodes > 0) 2 else currentPin.version)
        override def encode(value: Reservation): ByteString = fail("receipt lookup must not re-encode")
        override def decode(bytes: ByteString): Try[Reservation] = { decodes += 1; reservationCodec.decode(bytes) }
      }
      c.lookup(targetPrepare, sourcePrepare, reservationCodec.pin, codec).isSuccess shouldBe true
      currentPin = currentPin.copy(version = 2)
      c.lookup(targetPrepare, sourcePrepare, reservationCodec.pin, codec).isFailure shouldBe true
      decodes shouldBe 1
      currentPin = reservationCodec.pin; decodes = 0; driftInside = true
      c.lookup(targetPrepare, sourcePrepare, reservationCodec.pin, codec).isFailure shouldBe true
      decodes shouldBe 1
    }
    "fresh-decode mutable receipts on every lookup and retain immutable snapshot evidence only" in {
      var decodes = 0
      val codec = new PayloadCodec[ArrayBuffer[String]] {
        override val pin: CodecPin = reservationCodec.pin
        override def encode(value: ArrayBuffer[String]): ByteString = fail("no re-encode")
        override def decode(bytes: ByteString): Try[ArrayBuffer[String]] = Try {
          decodes += 1; ArrayBuffer(bytes.toStringUtf8)
        }
      }
      val s = state(targetPrepare)
      val c = context(s)
      val first = c.lookup(targetPrepare, sourcePrepare, codec.pin, codec).get
      first(0) = "mutated"
      val second = c.lookup(targetPrepare, sourcePrepare, codec.pin, codec).get
      (first eq second) shouldBe false
      second.toVector shouldBe Vector("source-reservation|")
      decodes shouldBe 2
      val replacement = s.copy(evidence = s.evidence.updated(sourcePrepare,
        Evidence.Confirmed(sourcePrepare, reservationCodec.encode(Reservation("replacement", "")))))
      reserve(c, targetPrepare, sourcePrepare).get.token shouldBe "source-reservation"
      reserve(context(replacement), targetPrepare, sourcePrepare).get.token shouldBe "replacement"
    }
  }

  private final case class Call(request: OperationRequest[String], query: Boolean, durable: Vector[StrictEvent],
                                own: Option[Reservation], upstreamPrepare: Option[Reservation],
                                upstreamCommit: Option[Settlement], confirm: () => Unit, unknown: () => Unit)
  private final class Rig {
    val pid: PersistenceId = PersistenceId.ofUniqueId(s"receipt-spec-${UUID.randomUUID()}")
    val transactionId: String = s"receipt-tx-${UUID.randomUUID()}"
    val calls = createTestProbe[Call]()
    private def stored: Vector[StrictEvent] = journal.persistedInStorage(pid.id).map(_.asInstanceOf[StrictEvent]).toVector
    private def port[R](step: String, phase: Phase, codec: PayloadCodec[R])(
        produce: OperationRequest[String] => (R, Option[Reservation], Option[Reservation], Option[Settlement])) =
      PhaseBinding(new PhasePort[String, R] {
        override val receiptCodec: PayloadCodec[R] = codec
        override val querySafety: QuerySafety = QuerySafety.DeclaredReadOnly
        private def invoke(request: OperationRequest[String], query: Boolean): Future[PhaseOutcome[R]] = {
          val completion = Promise[PhaseOutcome[R]]()
          Try(produce(request)) match {
            case Success((value, own, prepare, commit)) =>
              calls.ref ! Call(request, query, stored, own, prepare, commit,
                () => { completion.success(ConfirmedReceipt(request.identity, value)); () },
                () => { completion.success(Unknown(request.identity, "unknown remote receipt")); () })
              completion.future
            case Failure(ex) => Future.failed(ex)
          }
        }
        override def execute(request: OperationRequest[String]): Future[PhaseOutcome[R]] = invoke(request, query = false)
        override def query(request: OperationRequest[String]): Future[PhaseOutcome[R]] = invoke(request, query = true)
      })
    private def readPrepare(request: OperationRequest[String], step: String): Reservation =
      request.receipt(operation(request.identity.transaction, step, Phase.Prepare), reservationCodec.pin, reservationCodec).get
    private val participants = plan.map { p =>
      val step = p.stepId
      val prepare = port(step, Phase.Prepare, reservationCodec) { request =>
        val upstream = if (step == "target") Some(readPrepare(request, "source")) else None
        (Reservation(s"$step-reservation", upstream.fold("")(_.token)), None, upstream, None)
      }
      val commit = port(step, Phase.Commit, settlementCodec) { request =>
        val own = readPrepare(request, step)
        val upstream = if (step == "target") Some(request.receipt(
          operation(request.identity.transaction, "source", Phase.Commit), settlementCodec.pin, settlementCodec).get) else None
        (Settlement(s"$step-settled", own.token, upstream.fold("")(_.token)), Some(own), None, upstream)
      }
      val compensate = port(step, Phase.Compensate, reservationCodec) { request =>
        val own = readPrepare(request, step)
        val other = if (step == "source") "target" else "source"
        request.receipt(operation(request.identity.transaction, other, Phase.Prepare), reservationCodec.pin, reservationCodec)
          .isFailure shouldBe true
        request.receipt(operation(request.identity.transaction, step, Phase.Compensate), reservationCodec.pin, reservationCodec)
          .isFailure shouldBe true
        (Reservation(s"$step-released", own.token), Some(own), None, None)
      }
      StrictParticipant[String](p.stepId, p.participantId, p.resource,
        Map[Phase, PhaseBinding[String]](Phase.Prepare -> prepare, Phase.Commit -> commit, Phase.Compensate -> compensate))
    }
    val definition: StrictSagaDefinition[String] = StrictSagaDefinition("receipt-spec", 2, digest, argsCodec, _ => participants)
    val identity: TransactionIdentity = definition.identity(transactionId, bytes, participants.map(_.descriptor))
    private val authorizer = new ReconcileAuthorizer {
      override def authorize(id: TransactionIdentity, token: String): Either[String, String] =
        if (token == "operator") Right("receipt/operator") else Left("denied")
    }
    private def behavior = StrictSagaCoordinator(pid, definition, authorizer, Settings(30.seconds, 8))
    var ref: ActorRef[Command] = spawn(behavior)
    def reply(command: ActorRef[Reply] => Command): Reply = {
      val probe = createTestProbe[Reply](); ref ! command(probe.ref); probe.receiveMessage()
    }
    def start(): Unit = reply(to => Start(transactionId, bytes, to)) shouldBe a[Status]
    def status(): StrictState = reply(GetStatus.apply).asInstanceOf[Status].state
    def awaitStatus(expected: StrictStatus): Unit = eventually { status().status shouldBe expected }
    def reconcile(): Unit = reply(to => Reconcile(identity, "operator", 8, to)) shouldBe a[Status]
    def abort(): Unit = reply(to => Abort(identity, "cancel", to)) shouldBe a[Status]
    def next(step: String, phase: Phase, query: Boolean = false): Call = {
      val call = calls.receiveMessage()
      call.request.identity shouldBe operation(identity, step, phase)
      call.query shouldBe query
      call.request.args shouldBe "amount=10"
      val durable = call.durable.foldLeft(StrictState())(StrictState.evolve)
      durable.pending.get.identity shouldBe call.request.identity
      durable.pending.get.query shouldBe query
      call.durable.last shouldBe StrictEvent.IntentPersisted(durable.pending.get)
      if (phase == Phase.Commit) {
        durable.commitDecision shouldBe true
        call.durable should contain(StrictEvent.CommitDecision)
      }
      if (phase == Phase.Compensate) {
        durable.abortRequested shouldBe true
        durable.commitDecision shouldBe false
      }
      call
    }
    def restart(): Unit = { testKit.stop(ref); ref = spawn(behavior); calls.expectNoMessage(100.millis) }
    def close(): Unit = testKit.stop(ref)
  }
  private def withRig(test: Rig => Unit): Unit = {
    val rig = new Rig
    try test(rig) finally rig.close()
  }

  "Serial two-participant real typed ports" should {
    "feed source Prepare to target Prepare, own reservations to both Commit and source Commit to target Commit" in withRig { r =>
      r.start()
      r.next("source", Phase.Prepare).confirm()
      val target = r.next("target", Phase.Prepare)
      target.upstreamPrepare.get.token shouldBe "source-reservation"
      target.confirm()
      val sourceCommit = r.next("source", Phase.Commit)
      sourceCommit.own.get.token shouldBe "source-reservation"
      sourceCommit.confirm()
      val targetCommit = r.next("target", Phase.Commit)
      targetCommit.own.get.token shouldBe "target-reservation"
      targetCommit.upstreamCommit.get.token shouldBe "source-settled"
      targetCommit.confirm()
      r.awaitStatus(StrictStatus.Succeeded)
      r.calls.expectNoMessage(100.millis)
    }
    "never expose unknown source Prepare to target and use durable source query confirmation after recovery" in withRig { r =>
      r.start()
      r.next("source", Phase.Prepare).unknown()
      r.awaitStatus(StrictStatus.RecoveryRequired)
      r.calls.expectNoMessage(100.millis)
      r.restart(); r.reconcile()
      r.next("source", Phase.Prepare, query = true).confirm()
      val target = r.next("target", Phase.Prepare)
      target.upstreamPrepare.get.token shouldBe "source-reservation"
      target.durable.collect {
        case StrictEvent.EvidenceRecorded(_, Evidence.Confirmed(id, _)) => id
      } should contain(operation(r.identity, "source", Phase.Prepare))
      target.confirm()
      r.next("source", Phase.Commit).confirm(); r.next("target", Phase.Commit).confirm()
      r.awaitStatus(StrictStatus.Succeeded)
    }
    "rebuild target Prepare query context from serialized evidence after an interrupted intent and inert replay" in withRig { r =>
      r.start(); r.next("source", Phase.Prepare).confirm()
      val interrupted = r.next("target", Phase.Prepare)
      interrupted.upstreamPrepare.get.token shouldBe "source-reservation"
      r.restart()
      interrupted.confirm() // old-incarnation completion must not drive the recovered actor
      r.calls.expectNoMessage(100.millis)
      r.reconcile()
      val query = r.next("target", Phase.Prepare, query = true)
      (query.request.receiptContext eq interrupted.request.receiptContext) shouldBe false
      query.upstreamPrepare.get.token shouldBe "source-reservation"
      query.confirm()
      r.next("source", Phase.Commit).confirm(); r.next("target", Phase.Commit).confirm()
      r.awaitStatus(StrictStatus.Succeeded)
    }
    "supply own reservation and prior Commit to target Commit query after unknown and recovery" in withRig { r =>
      r.start(); r.next("source", Phase.Prepare).confirm(); r.next("target", Phase.Prepare).confirm()
      r.next("source", Phase.Commit).confirm()
      val target = r.next("target", Phase.Commit)
      target.unknown(); r.awaitStatus(StrictStatus.RecoveryRequired); r.restart(); r.reconcile()
      val query = r.next("target", Phase.Commit, query = true)
      query.own.get.token shouldBe "target-reservation"
      query.upstreamCommit.get.token shouldBe "source-settled"
      (query.request.receiptContext eq target.request.receiptContext) shouldBe false
      query.confirm(); r.awaitStatus(StrictStatus.Succeeded)
    }
    "query interrupted Prepare during abort then compensate in reverse order with only each own reservation" in withRig { r =>
      r.start(); r.next("source", Phase.Prepare).confirm()
      val pending = r.next("target", Phase.Prepare)
      r.abort()
      val query = r.next("target", Phase.Prepare, query = true)
      query.upstreamPrepare.get.token shouldBe "source-reservation"
      query.durable.foldLeft(StrictState())(StrictState.evolve).abortRequested shouldBe true
      pending.confirm() // retired ordinal cannot consume this query's receipt
      query.confirm()
      val target = r.next("target", Phase.Compensate)
      target.own.get.token shouldBe "target-reservation"
      target.unknown(); r.awaitStatus(StrictStatus.RecoveryRequired); r.restart(); r.reconcile()
      val compensationQuery = r.next("target", Phase.Compensate, query = true)
      compensationQuery.own.get.token shouldBe "target-reservation"
      compensationQuery.confirm()
      val source = r.next("source", Phase.Compensate)
      source.own.get.token shouldBe "source-reservation"
      source.confirm(); r.awaitStatus(StrictStatus.Aborted)
      r.calls.expectNoMessage(100.millis)
    }
  }
}
