// Reproduces rabbitmq/rabbitmq-amqp-dotnet-client#191:
// AmqpPublisher.PublishAsync completes its TaskCompletionSource twice when a publish
// races the broker ending the publisher's session.
//
// Each round opens a connection, attaches PUBLISHERS publishers (they share one session),
// floods them from WORKERS tasks each, then deletes the first publisher's queue. The broker
// detaches that link (amqp:resource-deleted); the transfers already in flight on it make the
// broker end the whole session (amqp:session:unattached-handle). A PublishAsync that is
// draining its link's queue at that moment throws from inside SenderLink.Send after its own
// message already got an outcome.
using System.Diagnostics;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

const string AlreadyCompleted = "An attempt was made to transition a task to a final state when it had already completed";

string host = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost";
int publishers = int.Parse(Environment.GetEnvironmentVariable("PUBLISHERS") ?? "16");
int workers = int.Parse(Environment.GetEnvironmentVariable("WORKERS") ?? "16");
int maxRounds = int.Parse(Environment.GetEnvironmentVariable("ROUNDS") ?? "200");

int fromPublishAsync = 0; // case 1: PublishAsync throws on the caller's thread
int fromReaderThread = 0; // case 2: the outcome callback throws on AMQPNetLite's reader thread
var reproduced = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

Trace.TraceLevel = TraceLevel.Error;
Trace.TraceListener = (_, format, args) =>
{
    string line = args is { Length: > 0 } ? string.Format(format, args) : format;
    if (!line.StartsWith("Exception occurred") || !line.Contains(AlreadyCompleted)) return;
    Interlocked.Increment(ref fromReaderThread);
    Console.WriteLine($"[case 2] AMQPNetLite: {line}");
    reproduced.TrySetResult();
};

IEnvironment environment = AmqpEnvironment.Create(ConnectionSettingsBuilder.Create().Host(host).Build());
var clock = Stopwatch.StartNew();

for (int round = 1; round <= maxRounds; round++)
{
    await Task.WhenAny(RunRoundAsync(), reproduced.Task);
    if (reproduced.Task.IsCompleted)
    {
        await Task.Delay(500); // let the other threads finish printing
        Console.WriteLine($"REPRODUCED in round {round} after {clock.Elapsed.TotalSeconds:F0}s: case1={fromPublishAsync} case2={fromReaderThread}");
        return 1;
    }

    Console.WriteLine($"round {round}: not yet");
}

Console.WriteLine($"Not reproduced in {maxRounds} rounds");
return 0;

async Task RunRoundAsync()
{
    IConnection connection = await environment.CreateConnectionAsync();
    connection.ChangeState += (_, from, to, error) =>
    {
        if (error is not null) Console.WriteLine($"connection {from} -> {to}: {error}");
    };
    IManagement management = connection.Management();

    var queues = new List<IQueueSpecification>();
    var senders = new List<IPublisher>();
    for (int i = 0; i < publishers; i++)
    {
        IQueueSpecification queue = management.Queue($"repro-191-{Guid.NewGuid()}").Type(QueueType.CLASSIC);
        await queue.DeclareAsync();
        queues.Add(queue);
        senders.Add(await connection.PublisherBuilder().Queue(queue.QueueName).BuildAsync());
    }

    using var stop = new CancellationTokenSource();
    Task[] flood = Enumerable.Range(0, publishers * workers)
        .Select(i => Task.Run(() => PublishUntilStoppedAsync(senders[i % publishers], stop.Token)))
        .ToArray();

    await Task.Delay(50);
    await queues[0].DeleteAsync(); // broker detaches publisher 0, then ends the shared session
    await Task.Delay(200);
    stop.Cancel();
    await Task.WhenAll(flood);

    foreach (IQueueSpecification queue in queues.Skip(1))
    {
        await queue.DeleteAsync();
    }
    await connection.CloseAsync();
}

async Task PublishUntilStoppedAsync(IPublisher publisher, CancellationToken stop)
{
    while (!stop.IsCancellationRequested)
    {
        try
        {
            await publisher.PublishAsync(new AmqpMessage("x")).WaitAsync(TimeSpan.FromSeconds(5));
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains(AlreadyCompleted))
        {
            Interlocked.Increment(ref fromPublishAsync);
            Console.WriteLine($"[case 1] PublishAsync threw: {ex}");
            reproduced.TrySetResult();
        }
        catch (Exception)
        {
            // Expected once the session is gone: Rejected outcomes, PublisherException, timeouts.
        }
    }
}
