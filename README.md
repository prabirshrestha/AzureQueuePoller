# AzureQueuePoller

```c#
Install-Package AzureQueuePoller
```

## Usage

```c#
var cts = new CancellationTokenSource();

var task = CloudQueuePoller.Poll(c =>
{
    var connectionString = "UseDevelopmentStorage=true";
    var storageAccount = CloudStorageAccount.Parse(connectionString);
    var queueClient = storageAccount.CreateCloudQueueClient();
    var queue = queueClient.GetQueueReference(QueueName);

    // you can return different instances of queue depending on your partitioning algorithm.
    // the return queue can be random, if you want to have certain priorities to a particular partition
    c.GetQueue = () => 
    {
        var tcs = new TaskCompletionSource<CloudQueue>();
        tcs.TrySetResult(queue);
        return tcs.Task;
    };

    // the number of messages to dequeue per single rest call to azure
    // azure considers one rest call as a transaction, this helps reduce the azure transaction cost.
    c.DequeueMessageBatchSize = 10;

    // Number of max parallelism to use when dequeuing the queue.
    // the poller can dequeue (DequeueThreadCount * DequeueMessageBatchSize) messages per time.
    c.DequeueMaxDegreeOfParallelism = 1;

    // used only when using OnMessage
    // number of parallelism per dequeue thread
    c.MaxDegreeOfParallelismForMessageProcessPerDequeue = 1;

    // optional cancellation token, if you want to support cancellation of polling. 
    c.CancellationToken = cts.Token;

    // azure message visibility timeout
    c.MessageVisibilityTimeout = TimeSpan.FromSeconds(60);

    // number of times to retry before giving up
    c.RetryCount = 3;

    // callback when an error occurs
    // q: queue
    // ex: the exception that occurred
    // obj: object of type either CloudQueueMessage or IEnumerable<CloudQueueMessage>. (can be null)
    // isMaxRetry: flag to indicate whether the retry has reached the max limit
    c.OnError = (q, ex, obj, isMaxRetry) =>
    {
        System.Diagnostics.Debug.WriteLine("isMaxRetry: " + isMaxRetry + " - " + ex.Message);

        if (isMaxRetry)
        {
            // make sure to persist the message(s) (ex: a poision queue/db) if isMaxRetry is true
            // otherwise the message will be lost forever
            // obj can either be of type IEnumerable<CloudQueueMessage> or CloudQueueMessage or null.
        }

        var tcs = new TaskCompletionSource<object>();
        tcs.TrySetResult(null);
        return tcs.Task;
    };

    // callback to call when messages has been dequeued.
    c.OnMessages = (q, messages) =>
    {
        foreach (var message in messages)
            Console.WriteLine(message.AsString);

        var tcs = new TaskCompletionSource<object>();
        tcs.TrySetResult(null);
        return tcs.Task;
    };

    // Note: If you want to process single message use the singular OnMessage instead.
    // set either OnMessage or OnMessages and not both of them.
    // You might also want to increase the MessageVisibilityTimeout when processing single messages.
    //
    // c.OnMessage = (q, message) =>
    // {
    //     Console.WriteLine(message.AsString);
    //
    //     var tcs = new TaskCompletionSource<object>();
    //     tcs.TrySetResult(null);
    //     return tcs.Task;
    // };
    //
});

await CloudQueuePoller.Delay(TimeSpan.FromSeconds(20));
cts.Cancel();

try
{
    await task;
}
catch (TaskCanceledException ex)
{
    Console.WriteLine("Polling cancelled");
}
```