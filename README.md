# Repro for rabbitmq-amqp-dotnet-client#191

Minimal program that reproduces
[rabbitmq/rabbitmq-amqp-dotnet-client#191](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues/191):
`AmqpPublisher.PublishAsync` completes its `TaskCompletionSource` twice when a publish races the broker ending the publisher's session.

Tested with RabbitMQ.AMQP.Client 1.5.0 (AMQPNetLite.Core 2.5.5) against RabbitMQ 4.2.

## Run it

With Docker only:

```sh
docker compose up --build --exit-code-from repro
```

Or with the .NET 10 SDK, against a local broker:

```sh
docker compose up -d rabbitmq
dotnet run
```

The program exits with code 1 and prints `REPRODUCED` as soon as it hits the bug, usually within a few seconds.
If it prints `Not reproduced in 200 rounds`, run it again or raise `ROUNDS`, `PUBLISHERS` or `WORKERS` (environment variables).

## What it does

Each round:

1. Opens a connection and attaches 16 publishers, one per queue. They share one session.
2. Floods them with `PublishAsync` from 16 tasks per publisher.
3. Deletes the first publisher's queue.

The broker detaches that link with `amqp:resource-deleted`. The transfers already in flight on it make the broker end the whole session with `amqp:session:unattached-handle`.
A `PublishAsync` that is draining its link's outgoing queue at that moment hits both completion paths for the same message.

## Expected output

Case 2 from the issue: `Send` throws first, so the catch block completes the task. The session abort then releases the delivery, and the outcome callback's `SetResult` throws on the AMQPNetLite reader thread. That closes the whole connection with `amqp:internal-error`:

```
[case 2] AMQPNetLite: Exception occurred: System.InvalidOperationException: An attempt was made to transition a task to a final state when it had already completed.
   at System.Threading.Tasks.TaskCompletionSource`1.SetResult(TResult result)
   at RabbitMQ.AMQP.Client.Impl.AmqpPublisher.<>c__DisplayClass7_0.<PublishAsync>g__OutcomeCallback|0(ILink sender, Message inMessage, Outcome outcome, Object state) in /_/RabbitMQ.AMQP.Client/Impl/AmqpPublisher.cs:line 179
   at Amqp.Delivery.ReleaseAll(Delivery delivery, Error error)
   at Amqp.SenderLink.OnAbort(Error error)
   at Amqp.Link.Abort(Error error, String reason)
   at Amqp.Session.AbortLinks(Error error)
   at Amqp.Session.OnEnd(End end)
   at Amqp.Connection.OnEnd(UInt16 remoteChannel, End end)
   at Amqp.Connection.OnFrame(ByteBuffer buffer)
connection Open -> Reconnecting: Code: amqp:internal-error - Description: An attempt was made to transition a task to a final state when it had already completed.
REPRODUCED in round 2 after 4s: case1=0 case2=1
```

Less often you get case 1: the outcome callback runs first, then `Send` throws, and `PublishAsync` itself throws:

```
[case 1] PublishAsync threw: System.InvalidOperationException: An attempt was made to transition a task to a final state when it had already completed.
   at System.Threading.Tasks.TaskCompletionSource`1.SetResult(TResult result)
   at RabbitMQ.AMQP.Client.Impl.AmqpPublisher.PublishAsync(IMessage message, CancellationToken cancellationToken) in /_/RabbitMQ.AMQP.Client/Impl/AmqpPublisher.cs:line 196
```

The `AmqpException` that `Send` threw in that case is `Unknown link handle: 0`, from `SenderLink.WriteDelivery` taking the next queued delivery after the session ended.

## Results

| Client | Result |
| --- | --- |
| 1.5.0 | Reproduced in 10 of 10 runs, after 1 to 43 rounds (3 to 45 seconds). Case 2 in 9 runs, case 1 in 1. |
| 1.5.0 with the patch below | Not reproduced in 150 rounds. |
| 1.4.1 (AMQPNetLite 2.5.4) | Not reproduced in 3 runs of 200 rounds with this trigger. |

The patch, in `RabbitMQ.AMQP.Client/Impl/AmqpPublisher.cs`, swaps `SetResult`/`SetException` on `publishResultTcs` for `TrySetResult`/`TrySetException` at all three completion points.
