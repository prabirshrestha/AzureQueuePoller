//#define ASYNC_TARGETTING_PACK

namespace AzureQueuePoller
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.StorageClient;

    /// <summary>
    /// Cloud queue poll settings
    /// </summary>
    public class CloudQueuePollSettings
    {
        /// <summary>
        /// Gets the queue to process. (can implement partitioning algorithms)
        /// </summary>
        public Func<Task<CloudQueue>> GetQueue { get; set; }

        /// <summary>
        /// Gets or sets the task to execute when queue is empty. Put the delay logic here
        /// </summary>
        /// <remarks>
        /// Executing OnQueueShould never throw exception.
        /// </remarks>
        public Func<CloudQueue, Task> OnQueueEmpty { get; set; }

        /// <summary>
        /// Gets or sets the batch message processor.
        /// </summary>
        /// <remarks>
        /// If exception is thrown when calling OnMessages, it will regard these queue messages as failed
        /// </remarks>
        public Func<CloudQueue, IEnumerable<CloudQueueMessage>, Task> OnMessages { get; set; }

        /// <summary>
        /// Gets or sets the message processor.
        /// </summary>
        /// <remarks>
        /// If exception is thrown when calling OnMessage, it will regard this queue message as failed.
        /// </remarks>
        public Func<CloudQueue, CloudQueueMessage, Task> OnMessage { get; set; }

        /// <summary>
        /// Gets or sets the action when an error occurs. (CloudQueue, Exception, CloudQueueMessage/IEnumerable&lt;CloudQueueMessage&gt;, isMaxRetry)
        /// </summary>
        /// <remarks>
        /// This method should never throw exception.
        /// When isMaxRetry is set to true, you would want to add it to a poison queue as it will delete the message.
        /// </remarks>
        public Func<CloudQueue, Exception, object, bool, Task> OnError { get; set; }

        /// <summary>
        /// Gets or sets the number of threads to use for dequeuing.
        /// </summary>
        public int DequeueMaxDegreeOfParallelism { get; set; }

        /// <summary>
        /// Gets or sets the number of messages to dequeue per request.
        /// </summary>
        public int DequeueMessageBatchSize { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of threads to use to process messages per dequeue thread.
        /// </summary>
        /// <remarks>
        /// todo: need a better name for this
        /// </remarks>
        public int MaxDegreeOfParallelismForMessageProcessPerDequeue { get; set; }

        /// <summary>
        /// Gets or sets the number of times to retry before giving up.
        /// </summary>
        public int RetryCount { get; set; }

        /// <summary>
        /// Gets or sets the queue visibility timeout.
        /// </summary>
        public TimeSpan MessageVisibilityTimeout { get; set; }

        /// <summary>
        /// Gets or sets the timeout for requests made to azure queues.
        /// </summary>
        public TimeSpan RequestTimeout { get; set; }

        /// <summary>
        /// Gets or sets the cancellation token.
        /// </summary>
        public CancellationToken CancellationToken { get; set; }

        /// <summary>
        /// Initializes a new instance of <see cref="CloudQueuePollSettings"/>
        /// </summary>
        public CloudQueuePollSettings()
        {
            DequeueMaxDegreeOfParallelism = 1;
            DequeueMessageBatchSize = 32;
            MaxDegreeOfParallelismForMessageProcessPerDequeue = 1;
            RetryCount = 3;
            CancellationToken = CancellationToken.None;
            MessageVisibilityTimeout = TimeSpan.FromSeconds(60);
            RequestTimeout = TimeSpan.FromSeconds(15);
            OnQueueEmpty = queue => CloudQueuePoller.Delay(TimeSpan.FromSeconds(30));
            OnError = (q, ex, obj, isMaxRetry) =>
            {
                Debug.WriteLine("isMaxRetry: " + isMaxRetry + " - " + ex.Message);

                if (isMaxRetry)
                {
                    // make sure to persist message (ex: a poision queue) if isMaxRetry is true
                    // otherwise the message will be lost forever
                    // obj can either be of type IEnumerable<CloudQueueMessage> or CloudQueueMessage or null.
                }

#if ASYNC_TARGETTING_PACK
                return TaskEx.FromResult(0);
#else
                return Task.FromResult(0);
#endif
            };
        }
    }

    /// <summary>
    /// The Azure Cloud Queue Poller
    /// </summary>
    public static class CloudQueuePoller
    {
        /// <summary>
        /// Start polling the azure queue.
        /// </summary>
        /// <param name="config">Configuration callback.</param>
        /// <returns>The task.</returns>
        public static Task Poll(Action<CloudQueuePollSettings> config)
        {
            var settings = new CloudQueuePollSettings();
            if (config != null)
                config(settings);
            return Poll(settings);
        }

        /// <summary>
        /// Delay task.
        /// </summary>
        /// <param name="delay">Timespan.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns></returns>
        public static Task Delay(TimeSpan delay, CancellationToken cancellationToken = default(CancellationToken))
        {
#if ASYNC_TARGETTING_PACK
            return TaskEx.Delay(delay, cancellationToken);
#else
            return Task.Delay(delay, cancellationToken);
#endif
        }

        /// <summary>
        /// Start polling the azure queue.
        /// </summary>
        /// <param name="settings">The poll settings.</param>
        /// <returns>The task.</returns>
        public static Task Poll(CloudQueuePollSettings settings)
        {
            if (settings == null)
                throw new ArgumentNullException("settings");

            if (settings.GetQueue == null)
                throw new ArgumentNullException("Set GetQueue");

            if ((settings.OnMessage == null && settings.OnMessages == null) ||
                (settings.OnMessage != null && settings.OnMessages != null))
            {
                throw new ArgumentException("Set either OnMessage or OnMessages");
            }

            Parallel.For(0, settings.DequeueMaxDegreeOfParallelism, new ParallelOptions { MaxDegreeOfParallelism = settings.DequeueMaxDegreeOfParallelism }, async _ =>
            {
                while (true)
                {
                    if (settings.CancellationToken.IsCancellationRequested)
                        break;

                    CloudQueue queue = null;
                    Exception exception = null;

                    try
                    {
                        queue = await settings.GetQueue();
                    }
                    catch (Exception ex)
                    {
                        exception = ex;
                    }

                    if (exception != null)
                    {
                        await settings.OnError(queue, exception, null, true);
                        continue;
                    }

                    // GetQueue can call cancel and return null
                    if (queue == null)
                        continue;

                    IEnumerable<CloudQueueMessage> messages = null;

                    int tempGetMessagesRetryCount = settings.RetryCount;

                    do
                    {
                        exception = null;

                        try
                        {
                            messages = await TimeoutAfter(Task.Factory.FromAsync<int, TimeSpan, IEnumerable<CloudQueueMessage>>(queue.BeginGetMessages, queue.EndGetMessages, settings.DequeueMessageBatchSize, settings.MessageVisibilityTimeout, null), settings.RequestTimeout);
                            break;
                        }
                        catch (Exception ex)
                        {
                            exception = ex;
                        }

                        --tempGetMessagesRetryCount;

                        if (exception != null)
                            await settings.OnError(queue, exception, null, tempGetMessagesRetryCount == 0);

                    } while (tempGetMessagesRetryCount >= 0);

                    if (exception != null)
                    {
                        continue;
                    }

                    if (!messages.Any())
                    {
                        await settings.OnQueueEmpty(queue);
                        continue;
                    }

                    if (settings.OnMessages != null)
                    {
                        // batch process
                        try
                        {
                            exception = null;
                            await settings.OnMessages(queue, messages);
                            await DeleteMessages(settings, queue, messages);
                        }
                        catch (Exception ex)
                        {
                            exception = ex;
                        }

                        if (exception != null)
                        {
                            var errorOutMessages = messages.Where(m => m.DequeueCount >= settings.RetryCount);
                            var nonErrorOutMessages = messages.Where(m => m.DequeueCount < settings.RetryCount);

                            if (errorOutMessages.Any())
                            {
                                await settings.OnError(queue, exception, errorOutMessages, true);
                                await DeleteMessages(settings, queue, errorOutMessages);
                            }

                            if (nonErrorOutMessages.Any())
                                await settings.OnError(queue, exception, nonErrorOutMessages, false);
                        }
                    }
                    else
                    {
                        // single message process
                        Parallel.ForEach(messages, new ParallelOptions { MaxDegreeOfParallelism = settings.MaxDegreeOfParallelismForMessageProcessPerDequeue }, async message =>
                        {
                            Exception singleMessageProcessException; // need a new local exception here
                            try
                            {
                                singleMessageProcessException = null;
                                await settings.OnMessage(queue, message);
                                await DeleteMessage(settings, queue, message);
                            }
                            catch (Exception ex)
                            {
                                singleMessageProcessException = ex;
                            }

                            if (singleMessageProcessException != null)
                            {
                                if (message.DequeueCount > settings.RetryCount)
                                {
                                    await settings.OnError(queue, singleMessageProcessException, message, true);
                                    await DeleteMessage(settings, queue, message);
                                }
                                else
                                    await settings.OnError(queue, singleMessageProcessException, message, false);
                            }
                        });
                    }
                }
            });

            var tcs = new TaskCompletionSource<object>();
            tcs.TrySetCanceled();
            return tcs.Task;
        }

        private async static Task DeleteMessages(CloudQueuePollSettings settings, CloudQueue queue, IEnumerable<CloudQueueMessage> messages)
        {
            foreach (var message in messages)
            {
                int retryCount = settings.RetryCount;

                do
                {
                    Exception exception = null;
                    try
                    {
                        await TimeoutAfter(Task.Factory.FromAsync(queue.BeginDeleteMessage, queue.EndDeleteMessage, message, null), settings.RequestTimeout);
                        break;
                    }
                    catch (StorageClientException ex)
                    {
                        // http://blog.smarx.com/posts/deleting-windows-azure-queue-messages-handling-exceptions
                        if (ex.ExtendedErrorInformation.ErrorCode == "MessageNotFound")
                        {
                            // pop receipt must be invalid
                            // ignore or log (so we can tune the visibility timeout)
                        }
                        else
                        {
                            // not the error we were expecting
                            exception = ex;
                        }
                    }
                    catch (Exception ex)
                    {
                        exception = ex;
                    }

                    --retryCount;

                    if (exception != null)
                        await settings.OnError(queue, exception, message, retryCount == 0);

                } while (retryCount >= 0);
            }
        }

        private async static Task DeleteMessage(CloudQueuePollSettings settings, CloudQueue queue, CloudQueueMessage message)
        {
            int retryCount = settings.RetryCount;

            do
            {
                Exception exception = null;
                try
                {
                    await TimeoutAfter(Task.Factory.FromAsync(queue.BeginDeleteMessage, queue.EndDeleteMessage, message, null), settings.RequestTimeout);
                }
                catch (StorageClientException ex)
                {
                    // http://blog.smarx.com/posts/deleting-windows-azure-queue-messages-handling-exceptions
                    if (ex.ExtendedErrorInformation.ErrorCode == "MessageNotFound")
                    {
                        // pop receipt must be invalid
                        // ignore or log (so we can tune the visibility timeout)
                    }
                    else
                    {
                        // not the error we were expecting
                        exception = ex;
                    }
                }
                catch (Exception ex)
                {
                    exception = ex;
                }

                --retryCount;

                if (exception != null)
                    await settings.OnError(queue, exception, message, retryCount == 0);

            } while (retryCount >= 0);
        }

        private static async Task<T> TimeoutAfter<T>(Task<T> task, TimeSpan dueDate)
        {
            // http://blogs.msdn.com/b/pfxteam/archive/2011/11/10/10235834.aspx

            // infinite timeout or task already completed
            if (task.IsCompleted || (dueDate.TotalMilliseconds == Timeout.Infinite))
            {
                // Either the task has already completed or timeout will never occur.
                // No proxy necessary.
                return await task;
            }

            // zero timeout
            if (dueDate.TotalMilliseconds <= 0)
            {
                // We've already timed out.
                throw new TimeoutException();
            }

#if ASYNC_TARGETTING_PACK
            if (task == await TaskEx.WhenAny(task, TaskEx.Delay(dueDate)))
#else
            if (task == await Task.WhenAny(task, Task.Delay(dueDate)))
#endif
                return await task;
            else
                throw new TimeoutException();
        }

        private static async Task TimeoutAfter(Task task, TimeSpan dueDate)
        {
            // http://blogs.msdn.com/b/pfxteam/archive/2011/11/10/10235834.aspx)

            // infinite timeout or task already completed
            if (task.IsCompleted || (dueDate.TotalMilliseconds == Timeout.Infinite))
            {
                // Either the task has already completed or timeout will never occur.
                // No proxy necessary.
                await task;
            }

            // zero timeout
            if (dueDate.TotalMilliseconds <= 0)
            {
                // We've already timed out.
                throw new TimeoutException();
            }

#if ASYNC_TARGETTING_PACK
            if (task == await TaskEx.WhenAny(task, TaskEx.Delay(dueDate)))
#else
            if (task == await Task.WhenAny(task, Task.Delay(dueDate)))
#endif
                await task;
            else
                throw new TimeoutException();
        }
    }
}