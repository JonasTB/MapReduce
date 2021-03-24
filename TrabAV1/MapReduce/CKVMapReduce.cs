using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using TrabAV1.WordCount;

namespace TrabAV1.MapReduce
{
    public class CKVMapReduce<IInput, IWord, IKey, IValue> : IMapReduce<IInput, IWord, IKey, IValue>
    {
        internal Func<IInput, IEnumerable<IWord>> Read { get; set; }
        internal Action<KeyValuePair<IKey, IValue>> Write { get; set; }
        internal Func<IKey, IKey, bool> Compare { get; set; }
        internal Func<IWord, IEnumerable<KeyValuePair<IKey, IValue>>> Map { get; set; }
        internal Func<IKey, IEnumerable<IValue>, IValue> Reduce { get; set; }

        private IEnumerable<IWord> Word;
        private List<KeyValuePair<IKey, IValue>[]> Agregg = new();
        private ThreadDictionary<IKey, List<IValue>> Buckets = new(353);
        private List<KeyValuePair<IKey, IValue>> Pairs = new();

        private int ThreadCount { get; set; }
        
        public async Task RunAsync(IInput input, int threadCount)
        {
            //Set thread count for the operations
            ThreadCount = threadCount;
            
            //Parse input into the data type
            //Eg. 'Deer Bear River\nCar Car River\nDeer Car Bear'->['Deer Bear River', 'Car Car River', 'Deer Car Bear']
            Word = Read(input);

            //Build groups from the read data
            //Eg. 'Deer Bear River'->['Deer', 'Bear', 'River']
            await BuildAgregg();
            
            //Associate unique keys within groups
            //Eg. ['Deer', 'Bear', 'River'],['Car', 'Car', 'River']->['Deer'],['Bear'],['Car', 'Car'],['River', 'River]
            await ShuffleGroups();
            
            //Reduce groups
            //Eg. ['Car', 'Car']->['Car': 2]
            await ReduceBuckets();
            
            //Write each finalized pair
            await WritePairs();
            
            //Cleanup
            Word = null;
            Agregg.Clear();
            Buckets.Clear();
            Pairs.Clear();
        }
        
        private TaskAwaiter CompositeTask { get; }

        private int[] level;
        private int[] last_to_enter;
        
        //Peterson's
        private async Task WaitForPermission(int threadId)
        {
            //Validação de ordem em niveis
            for (int i = 0; i < last_to_enter.Length; i++)
            {
                //Marca que a thread está no nivel de validação I
                level[threadId] = i;
                //Marca que a thread foi a ultima a entrar nesse nivel
                last_to_enter[i] = threadId;
                
                //Condição 1: essa thread não foi a ultima a entrar nesse nivel
                Func<bool> cond1 = () => last_to_enter[i] != threadId;
                
                //Condição 2: não existe thread igual ou mais avançada
                Func<bool> cond2 = () =>
                {
                    var val = true;
                    
                    for (int k = 0; k < level.Length; k++)
                    {
                        if (level[k] >= i && k != threadId)
                        {
                            val = false;
                            break;
                        }
                    }
                    return val;
                };
                
                while (true)
                {
                    if (cond1() || cond2())
                        break;
                    else
                        await Task.Yield();
                }
            }
        }

        private void SignalRelease(int threadId)
        {
            level[threadId] = -1;
        }

        private async Task BuildAgregg()
        {
            var operationLists = new List<List<IWord>>();
            //Algoritmo aqui pra separar a data em X grupos na operation list (IMPLEMENTAR AQUI)

            //var tasks = Word
                //.Select(w =>
                //{
                    //KeyValuePair<IKey, IValue>[] Agregg = new KeyValuePair<IKey, IValue>[0];
                    
                var operations = new List<Task>();

                for(int i = 0; i < operationLists.Count; i++){
                    var opList = operationLists[i];
                    var task = Task.Run(() =>
                    {
                        foreach (var op in opList)
                        {
                            var agregg = Map(op).ToArray();
                            WaitForPermission(i);
                            try
                            {
                                Agregg.Add(agregg);
                            }
                            finally
                            {
                                SignalRelease(i);
                            }
                        }
                    });
                operations.Add(task);
            }
            await Task.WhenAll(operations);
        }

        private async Task ShuffleGroups()
        {
            //Gets builds the tasks from each piece of data in each group
            var tasks = Agregg
                .SelectMany(group =>
                {
                    return group.Select(pair =>
                    {
                        List<IValue> bucketList = new();

                        //Non-critical section: Add value to the bucket dictionary
                        //Section is non-criticial due to the dictionary implementation being thread-safe
                        Action nonCrit = () =>
                        {
                            Buckets.AddOrUpdate(pair.Key, () =>
                            {
                                return new List<IValue> {pair.Value};
                            }, (list) =>
                            {
                                list.Add(pair.Value);
                                return list;
                            });
                        };

                        return new ParallelTask
                        {
                            NonCriticalSection = nonCrit,
                            CriticalSection = () => { } //No critical section for operation.
                        };
                    });
                }).ToArray();

            //Run tasks, managed by the Semaphore
            await Semaphore.StartFromTasks(tasks, ThreadCount);
        }

        private async Task ReduceBuckets()
        {
            //Build tasks from each data in each bucket
            var tasks = Buckets
                .Select(b =>
                {
                    KeyValuePair<IKey, IValue> pair = new KeyValuePair<IKey, IValue>();

                    //Non-critical section: apply the reductor function to each value group
                    Action nonCrit = () =>
                    {
                        var value = Reduce(b.Key, b.Value);
                        pair = new KeyValuePair<IKey, IValue>(b.Key, value);
                    };

                    //Critical section: add the reduced group into the key-value pair list.
                    Action crit = () =>
                    {
                        Pairs.Add(pair);
                    };

                    return new ParallelTask
                    {
                        NonCriticalSection = nonCrit,
                        CriticalSection = crit
                    };
                });

            //Run tasks, managed by the Semaphore
            await Semaphore.StartFromTasks(tasks, ThreadCount);
        }

        private async Task WritePairs()
        {
            //Build tasks from key-value pairs
            var tasks = Pairs
                .Select(p =>
                {
                    //Non-critical section: Write finalized key-value pair.
                    Action nonCrit = () =>
                    {
                        Write(p);
                    };

                    return new ParallelTask
                    {
                        CriticalSection = () => { }, //No critical section for operation.
                        NonCriticalSection = nonCrit
                    };
                });

            await Semaphore.StartFromTasks(tasks, ThreadCount);
        }
        
        //Interface implementation: receive arguments from MRBuilder
        public void Build(
            Func<IInput, IEnumerable<IWord>> Read,
            Action<KeyValuePair<IKey, IValue>> Write,
            Func<IKey, IKey, bool> Compare,
            Func<IWord, IEnumerable<KeyValuePair<IKey, IValue>>> Map,
            Func<IKey, IEnumerable<IValue>, IValue> Reduce 
            )
        {
            this.Read = Read;
            this.Write = Write;
            this.Compare = Compare;
            this.Map = Map;
            this.Reduce = Reduce;
        }
    }
}