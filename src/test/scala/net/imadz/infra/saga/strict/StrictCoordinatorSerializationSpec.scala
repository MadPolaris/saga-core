package net.imadz.infra.saga.strict

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.serialization.{SerializationExtension, SerializerWithStringManifest}
import com.google.protobuf.ByteString
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

/** Wire-contract regression: every cross-node coordinator command/reply must round-trip
  * under jackson-cbor with allow-java-serialization off — the full StrictState graph
  * (case-class map keys, nested sealed traits, case objects, ByteString, live ActorRef)
  * included. Apps deployed from the code generators turn allow-java-serialization off.
  */
class StrictCoordinatorSerializationSpec extends ScalaTestWithActorTestKit(
  ConfigFactory.parseString("""
    akka.actor.allow-java-serialization = off
    akka.actor.serialize-messages = off
  """).withFallback(ConfigFactory.load())
) with AnyWordSpecLike {
  import StrictSagaCoordinator._

  private val identity = TransactionIdentity(
    transactionId = "tx-1",
    definitionName = "money-transfer",
    definitionVersion = 1,
    argsDigest = "sha256:" + "a" * 64,
    definitionDigest = "sha256:" + "3" * 64,
    argsCodec = CodecPin("args/codec", 1),
    planDigest = "sha256:" + "p" * 64)

  private def operation(phase: Phase): OperationIdentity =
    OperationIdentity(identity, "participant", "args:from", "step-1", phase)

  private val state = StrictState(
    identity = Some(identity),
    argsBytes = ByteString.copyFromUtf8("args-bytes"),
    participants = Vector(
      ParticipantDescriptor("step-1", "participant", "args:from",
        Vector(Phase.Prepare, Phase.Commit, Phase.Compensate),
        Vector(ReceiptCodecPin(Phase.Prepare, CodecPin("receipt/prepare", 1)),
          ReceiptCodecPin(Phase.Commit, CodecPin("receipt/commit", 1)))),
      ParticipantDescriptor("step-2", "participant-2", "args:to", Vector(Phase.Prepare),
        Vector(ReceiptCodecPin(Phase.Prepare, CodecPin("receipt/p2", 2))))),
    status = StrictStatus.RecoveryRequired,
    commitDecision = true,
    abortRequested = false,
    lastOrdinal = 3L,
    pending = Some(Intent(operation(Phase.Commit), 3, query = false)),
    evidence = Map(
      operation(Phase.Prepare) -> Evidence.Confirmed(operation(Phase.Prepare), ByteString.copyFromUtf8("receipt")),
      operation(Phase.Commit) -> Evidence.Absent(operation(Phase.Commit), "durable proof", safeToRetry = true),
      operation(Phase.Compensate) -> Evidence.Uncertain(operation(Phase.Compensate), "interrupted intent"),
    ),
    reason = "interrupted intent; outcome unknown")

  /** Exactly what remoting does: find the binding, serialize, deserialize by id+manifest. */
  private def roundtrip[A <: AnyRef](msg: A): A = {
    val serialization = SerializationExtension(system.classicSystem)
    val serializer = serialization.findSerializerFor(msg)
    val bytes = serialization.serialize(msg).get
    serializer match {
      case swm: SerializerWithStringManifest =>
        serialization.deserialize(bytes, serializer.identifier, swm.manifest(msg)).get.asInstanceOf[A]
      case other =>
        serialization.deserialize(bytes, serializer.identifier, "").get.asInstanceOf[A]
    }
  }

  "Strict coordinator wire messages" should {
    "ride jackson-cbor with every command/reply type round-tripping" in {
      val probe = createTestProbe[Reply]()
      val serialization = SerializationExtension(system.classicSystem)
      def assertCbor(msg: AnyRef): Unit =
        assert(serialization.findSerializerFor(msg).getClass.getSimpleName == "JacksonCborSerializer",
          s"expected jackson-cbor binding for ${msg.getClass.getSimpleName}")

      val commands = Seq[Command](
        Start("tx-1", ByteString.copyFromUtf8("args-bytes"), probe.ref),
        Attach(identity, probe.ref),
        GetStatus(probe.ref),
        Abort(identity, "caller abort", probe.ref),
        Reconcile(identity, "auth-token", 8, probe.ref))
      commands.foreach { cmd =>
        assertCbor(cmd)
        assert(roundtrip(cmd) == cmd)
      }

      val replies = Seq[Reply](Status(state), Denied("invalid definition/args: boom"), ConflictingIdentity)
      replies.foreach { rep =>
        assertCbor(rep)
        assert(roundtrip(rep) == rep)
      }
    }

    "deserialize the full StrictState graph losslessly" in {
      val roundtripped = roundtrip(Status(state): Reply).asInstanceOf[Status]
      assert(roundtripped.state == state)
      // Evidence subtype identity survives the sealed-trait boundary.
      assert(roundtripped.state.evidence(operation(Phase.Prepare))
        .isInstanceOf[Evidence.Confirmed])
      assert(roundtripped.state.evidence(operation(Phase.Commit))
        .asInstanceOf[Evidence.Absent].safeToRetry)
    }

    "deliver a live ActorRef through the round-trip" in {
      val probe = createTestProbe[Reply]()
      val deserialized = roundtrip(GetStatus(probe.ref): Command).asInstanceOf[GetStatus]
      assert(deserialized.replyTo.path == probe.ref.path)
      deserialized.replyTo ! Denied("live ref check")
      probe.expectMessage(Denied("live ref check"))
    }
  }
}
