package net.imadz.infra.saga.strict

import com.google.protobuf.ByteString
import java.security.MessageDigest
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/** Explicit caller-owned wire contract. No inferred identity or permissive default. */
final case class CodecPin(id: String, version: Int) {
  require(id != null && id.matches("[A-Za-z0-9][A-Za-z0-9._/-]*") && version > 0,
    "stable codec id and positive version required")
}

/** A declaration, not automatic certification of an arbitrary Scala implementation. */
trait PayloadCodec[A] {
  def pin: CodecPin
  def encode(value: A): ByteString
  def decode(bytes: ByteString): Try[A]
}

sealed trait Phase
object Phase {
  case object Prepare extends Phase
  case object Commit extends Phase
  case object Compensate extends Phase
  val all: Vector[Phase] = Vector(Prepare, Commit, Compensate)
}

final case class TransactionIdentity(transactionId: String, definitionName: String,
                                     definitionVersion: Int, argsDigest: String,
                                     definitionDigest: String, argsCodec: CodecPin, planDigest: String)
final case class OperationIdentity(transaction: TransactionIdentity, participantId: String,
                                   resource: String, stepId: String, phase: Phase)
/** Receipt inputs are separate from caller args. Caller-created requests fail closed. */
final case class OperationRequest[A](identity: OperationIdentity, args: A,
                                     receiptContext: StrictReceiptContext = StrictReceiptContext.unavailable) {
  def receipt[R](producer: OperationIdentity, expectedCodec: CodecPin, codec: PayloadCodec[R]): Try[R] =
    receiptContext.lookup(identity, producer, expectedCodec, codec)
}
object OperationRequest {
  // Preserve the original two-field extractor as well as apply(identity, args) and PhasePort signatures.
  def unapply[A](request: OperationRequest[A]): Option[(OperationIdentity, A)] =
    Option(request).map(r => (r.identity, r.args))
}

sealed trait PhaseOutcome[+R] { def identity: OperationIdentity }
final case class ConfirmedReceipt[R](identity: OperationIdentity, receipt: R) extends PhaseOutcome[R]
/** Authoritative absence, not a timeout or an eventually-consistent "not found".
  * safeToRetry additionally declares that earlier calls cannot subsequently take effect
  * (participant-side fencing/deduplication). The runtime cannot verify that declaration.
  */
final case class NotExecuted(identity: OperationIdentity, evidence: String, safeToRetry: Boolean)
    extends PhaseOutcome[Nothing]
final case class Unknown(identity: OperationIdentity, reason: String) extends PhaseOutcome[Nothing]
/** A rejection alone is NOT proof that an earlier invocation has not executed. */
final case class Rejected(identity: OperationIdentity, reason: String) extends PhaseOutcome[Nothing]

sealed trait QuerySafety
object QuerySafety {
  case object DeclaredReadOnly extends QuerySafety
  case object Unsupported extends QuerySafety
}

/** Typed execute AND evidence query. DeclaredReadOnly is a port contract, not database certification.
  * Both methods must return promptly with a Future; blocking synchronous I/O is unsupported.
  * Receipt payloads must validate the participant's actual durable operation, not fabricate success.
  */
trait PhasePort[A, R] {
  def receiptCodec: PayloadCodec[R]
  def querySafety: QuerySafety
  def execute(request: OperationRequest[A]): Future[PhaseOutcome[R]]
  def query(request: OperationRequest[A]): Future[PhaseOutcome[R]]
}

/** Existential receipt type is hidden only through its codec; the engine never casts business values. */
sealed trait PhaseBinding[A] {
  def querySafety: QuerySafety
  def receiptCodecPin: CodecPin
  private[strict] def invoke(request: OperationRequest[A], query: Boolean, expectedCodec: CodecPin)
                            (implicit ec: ExecutionContext): Future[Evidence]
}
object PhaseBinding {
  def apply[A, R](port: PhasePort[A, R]): PhaseBinding[A] = {
    require(port != null && port.receiptCodec != null, "port and receipt codec required")
    val codec = port.receiptCodec // Bind the actual codec, not a separate descriptor declaration.
    new PhaseBinding[A] {
      override def querySafety: QuerySafety = port.querySafety
      override def receiptCodecPin: CodecPin = {
        require(port.receiptCodec eq codec, "receipt codec replacement")
        require(codec.pin != null, "receipt codec pin required")
        codec.pin
      }
      override private[strict] def invoke(request: OperationRequest[A], query: Boolean, expectedCodec: CodecPin)
                                         (implicit ec: ExecutionContext): Future[Evidence] = {
        def checkCodec(): Unit = require(receiptCodecPin == expectedCodec, "receipt codec drift")
        checkCodec()
        val result = if (query) port.query(request) else port.execute(request)
        require(result != null, "port must return a Future")
        result.map { outcome =>
          checkCodec() // Even non-confirmation outcomes must belong to the pinned binding.
          val evidence = outcome match {
            case ConfirmedReceipt(id, receipt) =>
              val bytes = codec.encode(receipt)
              // Empty is codec-defined (e.g. Unit); null/malformed is never success.
              require(bytes != null, "receipt bytes required")
              checkCodec()
              codec.decode(bytes).get
              Evidence.Confirmed(id, bytes)
            case NotExecuted(id, proof, safe) => Evidence.Absent(id, proof, safe)
            case Unknown(id, reason) => Evidence.Uncertain(id, reason)
            case Rejected(id, reason) => Evidence.Refused(id, reason)
          }
          checkCodec() // Also fence drift occurring inside encode/decode.
          evidence
        }
      }
    }
  }
}

final case class StrictParticipant[A](stepId: String, participantId: String, resource: String,
                                      phases: Map[Phase, PhaseBinding[A]]) {
  private[strict] def descriptor: ParticipantDescriptor =
    ParticipantDescriptor(stepId, participantId, resource, Phase.all.filter(phases.contains),
      Phase.all.filter(phases.contains).map(phase => ReceiptCodecPin(phase, phases(phase).receiptCodecPin)))
}

/** Additive strict API. Use a NEW persistence namespace, never an existing legacy journal.
  * participants must be a pure, deterministic function of canonical args. This first batch
  * requires all three TCC bindings for every participant and executes them sequentially.
  * No production artifact version is selected by this API.
  */
final case class StrictSagaDefinition[A](name: String, version: Int, definitionDigest: String,
                                         argsCodec: PayloadCodec[A],
                                         participants: A => Vector[StrictParticipant[A]]) {
  require(name != null && name.nonEmpty && version > 0, "definition name/version required")
  Digests.requireSha256(definitionDigest, "definition semantics digest")
  require(argsCodec != null && participants != null, "args codec and participant function required")
  private[strict] val argsCodecPin: CodecPin = argsCodec.pin
  require(argsCodecPin != null, "args codec pin required")

  private[strict] def identity(transactionId: String, bytes: ByteString,
                                plan: Vector[ParticipantDescriptor]): TransactionIdentity =
    TransactionIdentity(transactionId, name, version, Digests.sha256(bytes), definitionDigest,
      argsCodecPin, Digests.plan(plan))

  private[strict] def requireConfigured(id: TransactionIdentity): Unit = {
    require(id.definitionName == name && id.definitionVersion == version &&
      id.definitionDigest == definitionDigest, "definition identity/semantics drift")
    require(argsCodec.pin == argsCodecPin && id.argsCodec == argsCodecPin, "args codec drift")
  }

  private[strict] def materialize(bytes: ByteString): Try[(A, Vector[StrictParticipant[A]])] = Try {
    require(bytes != null, "args bytes required")
    require(argsCodec.pin == argsCodecPin, "args codec drift")
    val args = argsCodec.decode(bytes).get
    require(argsCodec.encode(args) == bytes, "args must use the caller codec's canonical encoding")
    require(argsCodec.pin == argsCodecPin, "args codec drift")
    val plan = participants(args)
    require(plan != null && plan.nonEmpty, "empty strict saga is not supported")
    require(plan.forall(_ != null), "participant required")
    require(plan.map(_.stepId).distinct.size == plan.size, "step ids must be unique")
    plan.foreach { p =>
      require(p.stepId != null && p.stepId.nonEmpty && p.participantId != null &&
        p.participantId.nonEmpty && p.resource != null && p.resource.nonEmpty,
        "stable step, participant and resource identities required")
      require(p.phases != null && p.phases.keySet == Phase.all.toSet && p.phases.values.forall(_ != null),
        "this strict batch requires all three TCC phases")
      p.descriptor.receiptCodecs.foreach(pin => require(pin.codec != null, "receipt codec pin required"))
    }
    require(argsCodec.pin == argsCodecPin, "args codec drift")
    (args, plan)
  }
}

private[strict] object Digests {
  def requireSha256(value: String, field: String): Unit =
    require(value != null && value.matches("sha256:[0-9a-f]{64}"), s"canonical $field required")
  def sha256(bytes: ByteString): String =
    MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray).map(b => f"$b%02x").mkString

  /** Domain-separated, length-prefixed UTF-8 fields; plan/phase order is significant. */
  def plan(participants: Vector[ParticipantDescriptor]): String = {
    val buffer = new java.io.ByteArrayOutputStream()
    val out = new java.io.DataOutputStream(buffer)
    def text(value: String): Unit = {
      val bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8)
      out.writeInt(bytes.length)
      out.write(bytes)
    }
    text("strict-saga-plan-v2")
    out.writeInt(participants.size)
    participants.foreach { p =>
      text(p.stepId); text(p.participantId); text(p.resource)
      def phaseName(phase: Phase): String = phase match {
        case Phase.Prepare => "prepare"
        case Phase.Commit => "commit"
        case Phase.Compensate => "compensate"
      }
      out.writeInt(p.phases.size)
      p.phases.foreach(phase => text(phaseName(phase)))
      out.writeInt(p.receiptCodecs.size)
      p.receiptCodecs.foreach { pin =>
        text(phaseName(pin.phase))
        text(pin.codec.id); out.writeInt(pin.codec.version)
      }
    }
    out.flush()
    "sha256:" + sha256(ByteString.copyFrom(buffer.toByteArray))
  }
}

/** Supplied by the host. Authorization is synchronous and must not perform I/O.
  * Right contains the authenticated principal to journal; no permissive default exists.
  */
trait ReconcileAuthorizer {
  def authorize(identity: TransactionIdentity, token: String): Either[String, String]
}
