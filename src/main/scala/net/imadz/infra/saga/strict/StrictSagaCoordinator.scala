package net.imadz.infra.saga.strict

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.typed.{PersistenceId, RecoveryCompleted}
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import com.google.protobuf.ByteString
import net.imadz.common.CborSerializable
import java.util.UUID
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/** Independent ESB entry point. RecoveryCompleted is inert; Start/Attach/GetStatus never
  * re-drive an existing transaction. Only authorized bounded Reconcile resumes recovery.
  * Wire-crossing commands/replies are CborSerializable (jackson-cbor); Drive/Returned/Deadline
  * stay private local-only (never serialized).
  */
object StrictSagaCoordinator {
  sealed trait Command extends CborSerializable
  final case class Start(transactionId: String, argsBytes: ByteString, replyTo: ActorRef[Reply]) extends Command
  final case class Attach(identity: TransactionIdentity, replyTo: ActorRef[Reply]) extends Command
  final case class GetStatus(replyTo: ActorRef[Reply]) extends Command
  final case class Abort(identity: TransactionIdentity, reason: String, replyTo: ActorRef[Reply]) extends Command
  final case class Reconcile(identity: TransactionIdentity, authorization: String, maxInteractions: Int,
                              replyTo: ActorRef[Reply]) extends Command
  sealed trait Reply extends CborSerializable
  final case class Status(state: StrictState) extends Reply
  final case class Denied(reason: String) extends Reply
  case object ConflictingIdentity extends Reply

  private final case class Drive(incarnation: String) extends Command
  private final case class Returned(incarnation: String, ordinal: Long, result: Try[Evidence]) extends Command
  private final case class Deadline(incarnation: String, ordinal: Long) extends Command

  final case class Settings(interactionTimeout: FiniteDuration = 5.seconds, maxReconcileInteractions: Int = 32) {
    require(interactionTimeout > Duration.Zero && maxReconcileInteractions > 0)
  }

  def apply[A](persistenceId: PersistenceId, definition: StrictSagaDefinition[A],
                authorizer: ReconcileAuthorizer, settings: Settings = Settings()): Behavior[Command] =
    Behaviors.setup { context =>
      Behaviors.withTimers { timers =>
        implicit val ec: scala.concurrent.ExecutionContext = context.executionContext
        val incarnation = UUID.randomUUID().toString
        var inFlight: Option[Long] = None
        var enabled = false
        var budget: Option[Int] = None
        // Cache only checked bindings for observational commands, never mutable decoded A.
        // Cleared on recovery; first explicit command validates the current pure plan.
        var checkedPlan: Option[(TransactionIdentity, Vector[StrictParticipant[A]])] = None
        val timerKey = "strict-interaction"

        def drive(): Unit = context.self ! Drive(incarnation)
        def halt(): Unit = {
          enabled = false
          budget = None
          inFlight = None
          timers.cancel(timerKey)
        }
        def recover(reason: String): Effect[StrictEvent, StrictState] = {
          halt()
          Effect.persist(StrictEvent.RecoveryRequired(reason))
        }
        def load(state: StrictState): Try[(A, Vector[StrictParticipant[A]])] = Try {
          val id = state.identity.get
          StrictSagaProtoConverters.validateStarted(id, state.argsBytes, state.participants)
          definition.requireConfigured(id) // Reject before decoding under a different args codec.
          // Decode fresh caller data for every interaction. A port can mutate a
          // generic A; never share that object with a later phase under old bytes.
          val value = definition.materialize(state.argsBytes).get
          require(value._2.map(_.descriptor) == state.participants, "participant/resource/phase/receipt codec drift")
          checkedPlan = Some(id -> value._2)
          value
        }
        def matches(state: StrictState, id: TransactionIdentity): Boolean =
          state.identity.contains(id) && Try {
            definition.requireConfigured(id)
            checkedPlan match {
              case Some((checked, plan)) if checked == id =>
                require(plan.map(_.descriptor) == state.participants, "receipt codec drift")
              case _ => load(state).get
            }
          }.isSuccess
        def dispatch(state: StrictState, id: OperationIdentity, query: Boolean): Effect[StrictEvent, StrictState] = {
          if (budget.contains(0)) recover("reconcile interaction budget exhausted")
          else load(state) match {
            case Failure(ex) => recover(s"materialize failed: ${ex.getMessage}")
            case Success((args, participants)) =>
              val binding = participants.find(_.stepId == id.stepId).get.phases(id.phase)
              if (query && binding.querySafety != QuerySafety.DeclaredReadOnly)
                recover("port does not declare a read-only evidence query")
              else {
                val intent = Intent(id, state.lastOrdinal + 1, query)
                Effect.persist(StrictEvent.IntentPersisted(intent)).thenRun { persisted =>
                  budget = budget.map(_ - 1)
                  inFlight = Some(intent.ordinal)
                  timers.startSingleTimer(timerKey, Deadline(incarnation, intent.ordinal), settings.interactionTimeout)
                  // Invoke only after persistence. Future callbacks capture immutable stamps,
                  // never actor context, mutable actor state, timers, or materialization cache.
                  val expectedCodec = persisted.participants.find(_.stepId == id.stepId).get.receiptCodec(id.phase)
                  val future = Try {
                    val receipts = StrictReceiptContext.fromPersistedIntent(persisted, id)
                    binding.invoke(OperationRequest(id, args, receipts), query, expectedCodec)
                  }.fold(Future.failed, identity)
                  val stamp = incarnation
                  val ordinal = intent.ordinal
                  context.pipeToSelf(future)(result => Returned(stamp, ordinal, result))
                }
              }
          }
        }
        def next(state: StrictState, id: OperationIdentity): Effect[StrictEvent, StrictState] =
          state.evidence.get(id) match {
            case None => dispatch(state, id, query = false)
            case Some(Evidence.Absent(_, proof, true)) if proof.nonEmpty => dispatch(state, id, query = false)
            case _ => dispatch(state, id, query = true)
          }

        def advance(state: StrictState): Effect[StrictEvent, StrictState] = {
          if (!enabled || inFlight.nonEmpty || state.terminal) Effect.none
          else state.pending match {
            // Interrupted intent is UNKNOWN, not failed/not-executed. Retire it durably
            // before issuing a new, ordinal-stamped query for the SAME operation identity.
            case Some(intent) =>
              Effect.persist(StrictEvent.EvidenceRecorded(intent.ordinal,
                Evidence.Uncertain(intent.identity, "interrupted intent; outcome unknown"))).thenRun(_ => drive())
            case None if state.abortRequested =>
              val unresolved = state.participants.map(p => state.operation(p, Phase.Prepare))
                .find(id => state.evidence.contains(id) && !state.confirmed(id) && !state.safelyAbsent(id))
              unresolved match {
                case Some(id) => dispatch(state, id, query = true)
                case None =>
                  val compensate = state.participants.reverse
                    .filter(p => state.confirmed(state.operation(p, Phase.Prepare)))
                    .map(p => state.operation(p, Phase.Compensate)).find(id => !state.confirmed(id))
                  compensate.map(next(state, _)).getOrElse {
                    halt()
                    Effect.persist(StrictEvent.Aborted)
                  }
              }
            case None if state.commitDecision =>
              state.participants.map(p => state.operation(p, Phase.Commit)).find(id => !state.confirmed(id))
                .map(next(state, _)).getOrElse {
                  halt()
                  Effect.persist(StrictEvent.Succeeded)
                }
            case None =>
              state.participants.map(p => state.operation(p, Phase.Prepare)).find(id => !state.confirmed(id))
                .map(next(state, _)).getOrElse {
                  Effect.persist(StrictEvent.CommitDecision).thenRun(_ => drive())
                }
          }
        }

        def record(state: StrictState, intent: Intent, result: Try[Evidence]): Effect[StrictEvent, StrictState] = {
          inFlight = None
          timers.cancel(timerKey)
          val evidence = result match {
            case Success(proof) if proof != null && proof.identity == intent.identity => proof match {
              case Evidence.Confirmed(id, null) => Evidence.Uncertain(id, "null receipt bytes")
              case Evidence.Absent(id, proof, _) if proof == null || proof.isEmpty =>
                Evidence.Uncertain(id, "empty absence evidence")
              case Evidence.Uncertain(id, reason) if reason == null || reason.isEmpty =>
                Evidence.Uncertain(id, "empty unknown reason")
              case Evidence.Refused(id, reason) if reason == null || reason.isEmpty =>
                Evidence.Uncertain(id, "empty rejection reason")
              case other => other
            }
            case Success(_) => Evidence.Uncertain(intent.identity, "receipt/evidence identity mismatch")
            case Failure(ex) => Evidence.Uncertain(intent.identity, s"interaction/codec failed: ${ex.getMessage}")
          }
          val recorded = StrictEvent.EvidenceRecorded(intent.ordinal, evidence)
          evidence match {
            case _: Evidence.Confirmed => Effect.persist(recorded).thenRun(_ => drive())
            case Evidence.Absent(_, proof, true) if proof.nonEmpty =>
              if (intent.query) Effect.persist(recorded).thenRun(_ => drive())
              else if (intent.identity.phase == Phase.Prepare && !state.abortRequested) {
                Effect.persist(Vector(recorded, StrictEvent.AbortRequested("prepare not executed")))
                  .thenRun(_ => drive())
              } else {
                halt()
                Effect.persist(Vector(recorded, StrictEvent.RecoveryRequired("operation not executed; explicit reconcile required")))
              }
            case _ =>
              halt()
              Effect.persist(Vector(recorded, StrictEvent.RecoveryRequired("unresolved operation evidence")))
          }
        }

        EventSourcedBehavior[Command, StrictEvent, StrictState](
          persistenceId, StrictState(), (state, command) => command match {
            case Start(txId, bytes, replyTo) if txId == null || txId.isEmpty || bytes == null =>
              replyTo ! Denied("transaction id and args bytes required")
              Effect.none
            case Start(txId, bytes, replyTo) =>
              if (state.identity.nonEmpty) {
                val id = state.identity.get
                val sameInput = id.transactionId == txId && id.argsDigest == Digests.sha256(bytes) &&
                  state.argsBytes == bytes
                replyTo ! (if (sameInput && matches(state, id)) Status(state) else ConflictingIdentity)
                Effect.none
              } else definition.materialize(bytes).flatMap { value => Try {
                val plan = value._2.map(_.descriptor)
                val id = definition.identity(txId, bytes, plan)
                StrictSagaProtoConverters.validateStarted(id, bytes, plan)
                (id, plan, value._2)
              }} match {
                case Failure(ex) =>
                  replyTo ! Denied(s"invalid definition/args: ${ex.getMessage}")
                  Effect.none
                case Success((id, plan, participants)) =>
                  checkedPlan = Some(id -> participants)
                  enabled = true
                  budget = None
                  Effect.persist(StrictEvent.Started(id, bytes, plan)).thenRun { s =>
                    replyTo ! Status(s)
                    drive()
                  }
              }
            case GetStatus(replyTo) => replyTo ! Status(state); Effect.none
            case Attach(id, replyTo) =>
              replyTo ! (if (matches(state, id)) Status(state) else ConflictingIdentity)
              Effect.none
            case Abort(_, reason, replyTo) if reason == null || reason.isEmpty =>
              replyTo ! Denied("abort reason required")
              Effect.none
            case Abort(id, reason, replyTo) =>
              if (!matches(state, id)) { replyTo ! ConflictingIdentity; Effect.none }
              else if (state.commitDecision || state.terminal) {
                replyTo ! Denied("abort is only allowed before durable commit decision")
                Effect.none
              } else if (!enabled || budget.nonEmpty || state.status == StrictStatus.RecoveryRequired) {
                replyTo ! Denied("abort cannot resume recovery or bypass an active reconciliation budget")
                Effect.none
              } else {
                // Fence the current local callback. The remote call may still execute: query
                // must supply durable confirmation or safe/fenced absence before compensation.
                halt()
                enabled = true
                val interrupted = state.pending.toVector.map(i => StrictEvent.EvidenceRecorded(i.ordinal,
                  Evidence.Uncertain(i.identity, "abort interrupted intent; query required")))
                Effect.persist(interrupted :+ StrictEvent.AbortRequested(reason)).thenRun { s =>
                  replyTo ! Status(s)
                  drive()
                }
              }
            case Reconcile(id, token, maximum, replyTo) =>
              if (!matches(state, id)) { replyTo ! ConflictingIdentity; Effect.none }
              else if (state.terminal) { replyTo ! Status(state); Effect.none }
              else if (enabled || inFlight.nonEmpty) { replyTo ! Denied("operation/reconciliation already active"); Effect.none }
              else if (maximum <= 0 || maximum > settings.maxReconcileInteractions) {
                replyTo ! Denied("invalid reconciliation interaction budget"); Effect.none
              } else Try(authorizer.authorize(id, token)) match {
                case Success(Right(principal)) if principal != null && principal.nonEmpty =>
                  enabled = true
                  budget = Some(maximum)
                  Effect.persist(StrictEvent.ReconcileAuthorized(principal, maximum)).thenRun { s =>
                    replyTo ! Status(s)
                    drive()
                  }
                case Success(Left(reason)) => replyTo ! Denied(reason); Effect.none
                case _ => replyTo ! Denied("reconciliation authorization failed"); Effect.none
              }
            case Drive(stamp) if stamp == incarnation => advance(state)
            case Returned(stamp, ordinal, result) if stamp == incarnation && inFlight.contains(ordinal) &&
                state.pending.exists(_.ordinal == ordinal) => record(state, state.pending.get, result)
            case Deadline(stamp, ordinal) if stamp == incarnation && inFlight.contains(ordinal) &&
                state.pending.exists(_.ordinal == ordinal) =>
              record(state, state.pending.get, Success(Evidence.Uncertain(state.pending.get.identity, "interaction timed out")))
            case _ => Effect.none
          }, StrictState.evolve
        ).receiveSignal {
          case (_, RecoveryCompleted) =>
            checkedPlan = None
            halt() // absolutely no codecs, materialization, port I/O or self re-drive on replay
        }
      }
    }
}
