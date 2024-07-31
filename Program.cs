using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

public class ParallelMap
{
    public static List<TResult> ParallelMapWithLimit<TResult>(
        int maxConcurrency, 
        List<Func<TResult>> tasks, 
        int timeoutMs)
    {
        var results = new TResult[tasks.Count];
        var taskCompletionSource = new TaskCompletionSource<bool>();
        var cancellationTokenSource = new CancellationTokenSource();
        
        // Ensure that the timeout is honored
        var timeoutTask = Task.Delay(timeoutMs, cancellationTokenSource.Token)
            .ContinueWith(t => taskCompletionSource.TrySetResult(false), TaskContinuationOptions.ExecuteSynchronously);
        
        var taskQueue = new Queue<Func<TResult>>(tasks);
        var runningTasks = new List<Task>();
        
        void RunNextTask(int index)
        {
            if (taskQueue.Count == 0 || cancellationTokenSource.Token.IsCancellationRequested)
                return;

            var taskFunc = taskQueue.Dequeue();
            var task = Task.Run(taskFunc, cancellationTokenSource.Token).ContinueWith(t =>
            {
                if (t.IsFaulted || t.IsCanceled)
                {
                    results[index] = default;
                }
                else
                {
                    results[index] = t.Result;
                }

                if (taskQueue.Count > 0)
                {
                    RunNextTask(index);
                }
                else if (runningTasks.All(rt => rt.IsCompleted))
                {
                    taskCompletionSource.TrySetResult(true);
                }
            });

            runningTasks.Add(task);
        }

        for (int i = 0; i < maxConcurrency && taskQueue.Count > 0; i++)
        {
            RunNextTask(i);
        }

        if (!taskCompletionSource.Task.Result)
        {
            throw new TimeoutException("The parallel map operation timed out.");
        }

        return results.ToList();
    }

    public static void Main()
    {
        // Example tasks simulating big data processing
        List<Func<int>> tasks = new List<Func<int>>
        {
            () => ProcessLargeDataSet(1),
            () => ProcessLargeDataSet(2),
            () => ProcessLargeDataSet(3),
            () => ProcessLargeDataSet(4),
            () => ProcessLargeDataSet(5),
        };

        try
        {
            var results = ParallelMapWithLimit(2, tasks, 10000); // Timeout of 10 seconds
            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }
        catch (TimeoutException ex)
        {
            Console.WriteLine(ex.Message);
        }
    }

    // Simulate processing a large dataset
    public static int ProcessLargeDataSet(int taskId)
    {
        Console.WriteLine($"Task {taskId} starting...");
        var random = new Random();
        var data = new int[10000000]; // Simulate large dataset
        for (int i = 0; i < data.Length; i++)
        {
            data[i] = random.Next(1, 100);
        }

        // Simulate work
        var sum = data.Sum();
        Console.WriteLine($"Task {taskId} completed. Sum: {sum}");
        return sum;
    }
}
