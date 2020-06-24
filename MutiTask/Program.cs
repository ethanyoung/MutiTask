using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MutiTask
{
    class Program
    {
        static BlockingCollection<int> before = new BlockingCollection<int>();
        static BlockingCollection<int> before1 = new BlockingCollection<int>();
        static BlockingCollection<int> after = new BlockingCollection<int>();
        static ConcurrentDictionary<int, ConcurrentQueue<MyObject>> mutiQueue = new ConcurrentDictionary<int, ConcurrentQueue<MyObject>>();

        static void Main(string[] args)
        {
            var inputs = new List<MyObject>();
            for(int i = 1; i < 21; i++)
            {
                int Id = 1;
                // 10, 11, 12, 13 should be in order.
                if (i >= 10 && i <= 13)
                {
                    Id = 2;
                }
                var o = new MyObject()
                {
                    Id = Id,
                    SeqId = i,
                };
                inputs.Add(o);
            }

            Parallel.ForEach(inputs, (o) =>
            {
                Add(o);
            });


            Thread.Sleep(10000);

            Console.WriteLine("Before");
            WriteToConsole(before);
            Console.WriteLine("Before1");
            WriteToConsole(before1);
            Console.WriteLine("After");
            WriteToConsole(after);
            Console.ReadLine();
        }

        static Task DummyAsync(int id)
        {
            Thread.Sleep(1000);
            Console.WriteLine($"Thread: {Thread.CurrentThread.ManagedThreadId} adding {id} to after");
            after.Add(id);
            return Task.FromResult(id);
        }
        static void WriteToConsole(IEnumerable items)
        {
            foreach (object o in items)
            {
                Console.Write($"{o}, ");
            }
            Console.Write('\n');

        }

        static void Add(MyObject o)
        {
            Console.WriteLine($"Thread: {Thread.CurrentThread.ManagedThreadId} adding {o.SeqId} in Add");            

            Console.WriteLine($"MutiQueue size: {mutiQueue.Count} Thread: {Thread.CurrentThread.ManagedThreadId}");
            ConcurrentQueue<MyObject> queue;
             
            queue = mutiQueue.GetOrAdd(o.Id, (k) =>
            {
                Console.WriteLine($"No found Thread: {Thread.CurrentThread.ManagedThreadId}");
                var queue = new ConcurrentQueue<MyObject>();
                return queue;
            });
            
            if (queue.Count != 1)
            {
                Console.WriteLine($"Old Queue Size: {queue.Count}");
            }
            queue.Enqueue(o);

            if (queue.Count == 1) 
            { 
                Task.Run(() => {
                    Console.WriteLine($"New Thread: {Thread.CurrentThread.ManagedThreadId}");
                    ProcessQueue(queue);
                    mutiQueue.TryRemove(o.Id, out _);
                });
            }
        }

        static void ProcessQueue(ConcurrentQueue<MyObject> queue)
        {
            while(queue.TryDequeue(out var o))
            {
                Console.WriteLine($"Thread: {Thread.CurrentThread.ManagedThreadId} adding {o.SeqId} to before");
                before.Add(o.SeqId);
                Console.WriteLine($"Thread: {Thread.CurrentThread.ManagedThreadId} adding {o.SeqId} to before1");
                before1.Add(o.SeqId);
                DummyAsync(o.SeqId).Wait();
            }
        }
    }

    class MyObject
    {
        public int Id { set; get; }
        public int SeqId { set; get; }
    }
}
