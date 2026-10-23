> 📖 完整指南：[docs/SAGA_GUIDE-zh.md](../docs/SAGA_GUIDE-zh.md)（中文） / [docs/SAGA_GUIDE.md](../docs/SAGA_GUIDE.md)（EN）
> 本文件是该模块的中文速查表。

# Saga Core

## 文件真源与新增严格契约

[`truth/`](truth/) 保存本次新增的协调器领域知识，明确区分现有 legacy 行为和待验证的 strict 决策/结果核对契约；它不是银行或 Fab 的应用业务定义库。可使用工作区的 `metarch-truthsrc` 公共 CLI 对此树运行 `validate` 与 `roundtrip`。

转账和 Fab 拆合批的闭合文件示例位于 `metarch-truthsrc/example/saga-money-transfer` 与 `example/fab-split-merge`，目前仅为 **contract-only**：schema 校验通过不证明参与者 receipt、生成应用或跨 JVM 恢复已实现。Monarch 阶段引用独立 Saga，事务成功才允许成功 checkpoint；结果未知不得自动补偿或换事务号。Release/Cancel 撤销预留不等于反转已提交效果。

### 独立 strict API／持久化 schema v2（未选择发布版本）

`net.imadz.infra.saga.strict.StrictSagaCoordinator` 是 additive、本地 typed API，不替换下方 legacy 0.2 API／`saga_v3` wire。必须使用独立 persistence namespace；strict schema v2 使用 serializer `77102231` 与 `event-v2`／`state-v2`，缺 pin 的 strict v1 事件／快照或旧 manifest 一律 fail closed，不能从名称、注册表或当前部署补造声明，也不提供 generic／Java／业务类序列化 fallback。v2 是外层持久化 schema，**不是** definition 或业务 codec 的版本。

- 定义显式提供 `StrictSagaDefinition(name, version: Int, definitionDigest, argsCodec, participants)`；`definitionDigest` 必须为规范小写 `sha256:` + 64 hex。每个 `PayloadCodec` 显式声明 `CodecPin(id, version: Int)`（稳定非空 id、正整数版本），业务 schema 版本不从 name／id 猜测。args bytes 使用该 codec 的 canonical encoding。
- durable `TransactionIdentity` 固定 txId、definition name/version、args digest、definition semantics digest、args codec pin、ordered plan digest。plan digest 对实际有序 step／participant／resource／phase descriptors 及 **每个 Prepare／Commit／Compensate receipt codec id/version** 做 domain-separated、length-prefixed SHA-256；整个 transaction identity 纳入每个 operation identity。绑定记录的是实际 codec 的 pin，而不是另填一份可脱离 codec 的描述。
- 同 txId／args／全部 pin 重复 `Start`／`Attach` 返回原持久状态，不重驱业务；任一 semantics／args codec／任一 phase receipt codec／计划漂移，即使事务已 Succeeded／Aborted，也在 `Start`／`Attach`／`Abort`／`Reconcile` 的终态短路前拒绝，不执行 port I/O 或新增 journal。`GetStatus` **只读历史**，不表示当前 definition 可以恢复执行。recovery 回放不解码、不 materialize、不查询／执行 port；恢复后只有显式同 pin、鉴权通过且有正数上限的 `Reconcile` 能继续同一事务／operation identity。
- `participants` 与 codec 必须纯、确定、非阻塞，pin／实现应在 incarnation 内稳定。首个显式 admission 重新核验当前纯计划；观察性 Start／Attach 复用已核验 bindings、检查实时 codec pins，不重复 materialize。每次真实 interaction 则从持久 args bytes 重新解码／核验，避免共享被前一 port 修改的业务对象。调用前和 receipt completion／encode／decode 后发现 codec 漂移时，不接受 confirmation；已有调用的结果记为 unknown，停止并要求原 pin 下授权核对。声明同 pin 却偷偷改变代码语义不在此检测保证内。
- **digest、codec pin、DeclaredReadOnly、安全 absence／fencing 与 receipt 都是宿主／参与者声明，不是运行时对任意代码或数据库副作用的自动认证。** 参与者仍须按 durable operation identity 查询真实 durable receipt、去重／fencing；codec round-trip 只验证编码契约，不认证业务效果。unknown／timeout／rejection 不是 absence；不能伪造 receipt、换 txId 规避未知结果，或反转已作出的 commit decision。

工程门只验证协调器／Protobuf／persistence testkit 的边界（含 Java serialization off），不证明真实银行／Fab 业务 receipt、跨 JVM／远端数据库恢复或端到端业务已贯通。可用 JDK 11 执行 `sbt "testOnly net.imadz.infra.saga.strict.StrictSagaSpec"`；不据此发布候选或宣称兼容性／业务认证。

### strict receipt inputs（serial-plan 工程 API）

`OperationRequest(identity, args)` 与既有 `PhasePort.execute/query` 签名保持源码调用兼容，原两字段 extractor 保留；新增 `receiptContext` 默认不可用、fail closed。运行时在 `IntentPersisted.thenRun` 内绑定完整 consumer identity，传入只读 `StrictReceiptContext`，与 args 分离。port 使用 `request.receipt(producerOperationIdentity, expectedCodecPin, payloadCodec)` 得到 `Try[R]`；直接使用 context 时也必须传完整 consumer identity。没有公开 state／JSON／bytes 工厂或不受限 evidence dump，复制 request 改 identity 不能转移读取权限。

- **当前唯一执行模型是 serial-plan，不是 truth 中同组并发。** Prepare 只能读有序计划中先前步骤的 Prepare；Commit 必须已有 durable CommitDecision，可读全部 Prepare 与先前 Commit，不能读自己／未来 Commit；Compensate 必须有 AbortRequested（abort decision）且无 commit decision／confirmed Commit evidence，只能读自己的 Prepare，逆序补偿不授予跨参与者读取权限。abort 中既有未知 Prepare 的只读 query 仍合法，不允许新 Prepare execute。
- 每个 lookup 校验完整 transaction／participant／resource／step／phase、durable descriptor receipt pin 与调用者 expected pin，再检查实际 `codec.pin` 的 decode 前后值。只有 key 与完整 identity 一致的 `Evidence.Confirmed` 可读；missing／Unknown／Absent／Refused、错 identity／resource／pin、future／Compensate evidence 一律拒绝。
- context 仅保留 immutable confirmed ByteString、identity 与 pin，不保留 actor／StrictState／decoded A/R 或 codec。每次 lookup 从 durable bytes 新解码，拒绝 null／malformed／pin 漂移；不缓存可变 R，也不额外要求 receipt canonical re-encode。codec 仍必须履行其新解码契约；同 pin 的恶意 codec 或业务 port 效果不由此认证。
- context 是非持久运行时输入，没有新 durable 字段，不改 strict model／proto／converter／serializer。replay 不建 context／不调用 port，授权 Reconcile 后从恢复的 durable evidence 新建；Intent-before-I/O、CommitDecision-before-Commit、incarnation／ordinal／budget／unknown query 规则不变。

工程回归使用 `sbt "testOnly net.imadz.infra.saga.strict.StrictSagaSpec net.imadz.infra.saga.strict.StrictReceiptContextSpec"`，包括双 participant typed ports 的 receipt 数据流、execute/query 与故障恢复；它不是公共应用接线、真实账户效果／数据库耐久 receipt、跨 JVM、发布或二进制兼容认证。公共 runtime boundary 在本子批 **NOT RUN**，Node lowering／AST／生成器及生成应用留后续。

下方是 legacy v3 DSL 速查，其历史“工业级”“强一致性”“回滚”表述不能作为新增安全契约的验收结论。

`saga-core` 是一个基于 Akka Typed 和 Event Sourcing 构建的工业级分布式事务协调引擎。它实现了 TCC（Try-Confirm-Cancel）模式，支持高并发、强一致性、以及复杂的混合编排（串行+并行）逻辑。

## 核心特性

*   **TCC 模型支持**：完整实现 Prepare, Commit, Compensate 三阶段协议。
*   **声明式 DSL（v3）**：业务开发者只写一个 `SagaDefinition`（~40-60 行），不再需要 Transactor 影子聚合、步骤编排和参与者序列化。
*   **参与者零序列化**：journal 只记录 `(definition name + version, args, argsHash, StepDescriptor[])`；参与者是 `(definition, args)` 的纯函数，恢复时从注册表确定性重建。
*   **混合编排 (Execution Groups)**：通过 `stepGroup` 支持同一阶段内步骤的并行执行、组间串行。
*   **反向回滚 (Reverse Rollback)**：自动计算补偿顺序，确保"后执行先回滚"。
*   **单步调试模式**：持久化的"暂停-继续"机制，`runner.admin.proceed` 逐组推进。
*   **人工干预机制**：`ManualFix`、`RetryPhase`、`ResolveSuspended` 运维能力。
*   **崩溃恢复**：Coordinator 与 StepExecutor 均为事件溯源聚合，世代号（attempt generation）关闭迟到响应窗口。
*   **定义漂移防护**：结构漂移（stepId/phase/group 集合变化）→ 事务挂起；可调参数漂移（重试/超时）→ 告警沿用。
*   **事件溯源与分片**：状态完全持久化（saga_v3 proto journal），支持集群水平扩展。

## 快速上手（v3 DSL）

### 1. 定义步骤与参与者 (`SagaStep.builder` / `AskParticipant`)

推荐使用 **`SagaStep.builder[E, C](stepId)`**（或包函数 `stepBuilder[E, C](stepId)`）流式定义步骤。它直接与真源树（`*.saga.yaml`）字段一一对应，免去显式编写 5 个泛型参数的 `PhaseAsk.ask[Cmd, Reply, E, R, C]`，并自动混入 `PhaseAwareParticipant`（未绑定的阶段在 `expand` 时自动跳过）：

```scala
import net.imadz.infra.saga.dsl._

val transferOutStep =
  SagaStep.builder[iMadzError, MoneyTransferContext]("transfer-out")
    .inGroup(1)
    .withResilience(ResiliencePolicy(maxRetries = 5))
    .withTimeout(5.seconds)
    .withErrorRules(ErrorRules(
      business = { case iMadzError("60003" | "60004", _) => ErrorAction.NonRetryable },
      describe = e => s"${e.code}: ${e.message}"))
    .prepare
      .ref(_.repository.findCreditBalanceByUserId(fromUserId))
      .command[FundsReservationConfirmation]((txId, replyTo) => ReserveFunds(Id.of(txId), money, replyTo))
      .mapReply(r => r.error.map(Left(_)).getOrElse(Right(SagaResult(r.transferId.toString))))
    .commit
      .ref(_.repository.findCreditBalanceByUserId(fromUserId))
      .command[FundsDeductionConfirmation]((txId, replyTo) => DeductFunds(Id.of(txId), money, replyTo))
      .mapReply(r => r.error.map(Left(_)).getOrElse(Right(SagaResult(r.transferId.toString))))
    .compensate
      .ref(_.repository.findCreditBalanceByUserId(fromUserId))
      .command[FundsReleaseConfirmation]((txId, replyTo) => ReleaseReservedFunds(Id.of(txId), money, replyTo))
      .mapReply(r => r.error.map(Left(_)).getOrElse(Right(SagaResult(r.transferId.toString))))
    .build()
```

*   **针对 `ref` / `command` / `mapReply` 三要素定义的 3 个分步链式 API（核心改进）**：
    1.  **`.ref(ctx => ...)`**：从运行时 `Context` 解析目标 Actor `RecipientRef[Cmd]`，**自动推导命令协议类型 `Cmd`**，无需手写。
    2.  **`.command[Reply]((txId, replyTo) => ...)`**：根据 `(transactionId, replyTo)` 构造命令，仅需声明回复类型 `Reply`。
    3.  **`.mapReply(reply => ...)`**：将 `Reply` 映射为 `Either[E, SagaResult[String]]` 并自动回到 `SagaStepBuilder` 继续链式绑定下一阶段。
    *   *独立构建 `PhaseAsk`*：在 `AskParticipant` 子类中也可直接使用 `PhaseAsk.ref(ctx => ...).command[Reply](...).mapReply[E, R](...)` 三步链式 API。
*   **其他阶段绑定简写**：
    *   `.prepare.direct(fn)` / `.prepareDirect(fn)`（及 `commit` / `compensate`）：面向直接返回 `Future[Either[E, SagaResult[String]]]` 的异步函数。
    *   `.prepareAsk[Cmd, Reply](ref, cmd, map)`：单方法三参数重载。
*   **可复用参与者类（可选）**：若同一参与者逻辑在多个 Saga 间复用，仍可直接继承 `AskParticipant[E, R, C]` 并通过 `SagaStep(id, participant, resilience, stepGroup)` 组装。
*   **双轨错误分类**：`business` 规则分类 `Left(E)`（未匹配默认 NonRetryable）；`thrown` 规则叠加在默认矩阵之上（Timeout/AskTimeout/Connect/SQLTransient → Retryable，其余 → NonRetryable）。

| `SagaStep.builder` DSL 方法 | 对应真源树 (`*.saga.yaml`) | 作用说明 |
|---|---|---|
| `SagaStep.builder[E, C](stepId)` / `stepBuilder[E, C](stepId)` | `steps[].stepId` | 创建指定步骤 ID 的流式构建器（结果类型默认 `String`，可通过 `.withResultType[R]` 切换） |
| `.inGroup(group: Int)` | `steps[].executionGroup` | 指定执行组（组内并行、组间串行、逆序补偿） |
| `.withResilience(policy)` / `.withTimeout(d)` | `steps[].resilience` | 配置重试次数、退避策略、熔断器与单次 Ask 超时 |
| `.withErrorRules(ErrorRules(...))` | `steps[].errorClassification` | 配置业务错误（`Left(E)`）与异常的双轨分类规则 |
| `.prepare.ref(...).command[Reply](...).mapReply(...)` | `steps[].tccBindings.prepare` | 三步链式绑定 Try 阶段（自动推导 `Cmd`；另有 `.prepare.direct(fn)` / `.prepareAsk`） |
| `.commit.ref(...).command[Reply](...).mapReply(...)` | `steps[].tccBindings.commit` | 三步链式绑定 Confirm 阶段（可选；未绑定时自动跳过该阶段） |
| `.compensate.ref(...).command[Reply](...).mapReply(...)` | `steps[].tccBindings.compensate` | 三步链式绑定 Cancel 阶段（可选；未绑定时自动跳过该阶段） |
| `.build()` | — | 生成混入 `PhaseAwareParticipant` 的 `SagaStep[E, R, C]` |

### 2. 定义事务 (`SagaDefinition`)

`steps` 必须是 `(definition, args)` 的纯函数——这正是 journal 无需存参与者的原因：

```scala
object MoneyTransferSagaDefinition {
  final case class TransferArgs(fromUserId: String, toUserId: String, amount: BigDecimal, currency: String)
  object TransferArgs { implicit val format: Format[TransferArgs] = Json.format[TransferArgs] }

  def definition(implicit ec: ExecutionContext, scheduler: Scheduler) =
    SagaDefinition[iMadzError, MoneyTransferContext, TransferArgs](
      name = "money-transfer", version = 1,
      argsCodec = ArgsCodec.playJson[TransferArgs],
      steps = args => {
        val money = Money(args.amount, Currency.withName(args.currency))
        Seq(
          SagaStep.builder[iMadzError, MoneyTransferContext]("transfer-out")
            .inGroup(1)
            .withResilience(ResiliencePolicy(maxRetries = 5))
            .withErrorRules(accountErrorRules)
            .prepare.ref(...).command[FundsReservationConfirmation](...).mapReply(...)
            .commit.ref(...).command[FundsDeductionConfirmation](...).mapReply(...)
            .compensate.ref(...).command[FundsReleaseConfirmation](...).mapReply(...)
            .build(),
          SagaStep.builder[iMadzError, MoneyTransferContext]("transfer-in")
            .inGroup(1)
            .withResilience(ResiliencePolicy(maxRetries = 5))
            .withErrorRules(accountErrorRules)
            .prepare.ref(...).command[RecordIncomingCreditsConfirmation](...).mapReply(...)
            .commit.ref(...).command[CommitIncomingCreditsConfirmation](...).mapReply(...)
            .compensate.ref(...).command[CancelIncomingCreditConfirmation](...).mapReply(...)
            .build()
        )
      },
      preCheck = args => if (args.amount > 0) Right(args) else Left(iMadzError("40001", "amount must be positive")),
      onResult = (args, result) => List(MoneyTransferCompleted(result.snapshot.transactionId, ...)))
}
```

*   `preCheck` 在 coordinator 侧、persist 前执行，拒绝不落任何事件。
*   `onResult` 产生的 `SagaBusinessEvent` 由业务事件投影消费（at-least-once，sink 按 txId 去重）。

### 3. 引导与启动 (Bootstrap)

节点启动时**先注册定义、再初始化分片**（恢复中的在途事务按持久化的 version 解析定义；覆盖注册是部署修复后挂起事务的自愈路径）：

```scala
MoneyTransferSagaDefinition.register      // 每节点一次
initSagaEngine[MoneyTransferContext](sharding, MoneyTransferContext(repository), system)
```

journal pid 方案（getHistory 依赖）：
*   coordinator: `saga-coordinator-$txId`
*   executor:    `saga-executor-$txId-$stepId-$phase`

### 4. 启动事务 (SagaRunner)

```scala
val runner = MoneyTransferSagaDefinition.runner(system, txId => sharding.entityRefFor(SagaTransactionCoordinator.entityTypeKey, txId))

// txId 是调用方幂等键：同 txId + 同 args 重发 → AlreadyRunning / AlreadyFinished；
// 同 txId + 不同 args → ConflictingArgs 拒绝
val terminal: Future[TransactionResult] = runner.run(txId, TransferArgs(...), traceId = "", singleStep = false)

// 持久状态轮询（跨实体重启/节点崩溃）
val snapshot: Future[Option[StatusSnapshot]] = runner.statusOf(txId)
```

`run` 的 Future 由事务终态 TransactionResult 完成（经节点本地完成桥），启动拒绝/兜底超时使其失败。需要持久状态的调用方请轮询 `statusOf`。

## 运维与人工干预

当事务由于外部系统不可用导致 Compensate 失败并进入 `SUSPENDED` 状态时，管理员可通过 `runner.admin` 介入：

```scala
runner.admin.fixStep(txId, "transfer-out", CompensatePhase) // 线下处理后标记该步骤逻辑成功
runner.admin.resolveSuspended(txId)                         // 重新驱动当前阶段
runner.admin.retryPhase(txId)                               // 阶段级重试
runner.admin.proceed(txId)                                  // 单步模式逐组推进
```

## 序列化 (saga_v3)

*   **journal 事件**：proto（`saga_v3.proto`）——TransactionStarted 携带 definitionRef + args + argsHash + StepDescriptor 列表，**不含任何参与者载荷**；跨节点消息（StartSaga/SagaStartReply/TransactionResult/StatusSnapshot）为 CborSerializable 走 jackson-cbor。
*   **业务结果字节**（OperationSucceeded/ManualFixCompleted 的 result）对非 CborSerializable 类型依赖 `allow-java-serialization = on`（已知技术债，见 reference.conf 注释）。
*   快照：继续 java serializer（descriptor 化后 State 全为静态数据）。
*   args 编码必须稳定（argsHash 是幂等判别键）：升级编码格式请 bump definition version，并在途事务按旧 version 继续解析。

## 验收与测试

```bash
sbt sagaCore/test     # 全部单元 + 验收用例（AC-1.1 .. AC-1.12）
sbt acceptance        # 门禁别名（当前 G0.1+G1；G0.2/G0.3 随 Phase B/C 接入）
```

验收包位于 `net.imadz.infra.saga.acceptance`，覆盖：expand 展开正确性、双轨分类、StartSaga 幂等矩阵 7 分支、崩溃恢复（journal-PO 断言）、Attach 三分支、世代号、重入安全、定义漂移、序列化绑定、journal 内容、弹性策略激活、SagaRunner 完成桥。
