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
    public class CKVMapReduce<IInput, IWord, IKey, IValue>
    {
        public Func<IInput, IEnumerable<IWord>> Read { get; set; }
        public Action<KeyValuePair<IKey, IValue>> Write { get; set; }
        public Func<IKey, IKey, bool> Compare { get; set; }
        public Func<IWord, IEnumerable<KeyValuePair<IKey, IValue>>> Map { get; set; }
        public Func<IKey, IEnumerable<IValue>, IValue> Reduce { get; set; }

        private IEnumerable<IWord> Word;
        private List<KeyValuePair<IKey, IValue>[]> Agregg = new();
        private ThreadDictionary<IKey, List<IValue>> Buckets = new(353);
        private List<KeyValuePair<IKey, IValue>> Pairs = new();

        private int ThreadCount { get; set; }
        
        public async Task RunAsync(IInput input, int threadCount)
        {
            ThreadCount = threadCount;
            Word = Read(input);
            await BuildAgregg();
            await ShuffleGroups();
            await ReduceBuckets();
            await WritePairs();
            
            Word = null;
            Agregg.Clear();
            Buckets.Clear();
            Pairs.Clear();
        }

        private int[] level;
        private int[] last_to_enter;

        private void ResetSemaphore()
        {
            level = new int[ThreadCount];
            last_to_enter = new int[ThreadCount - 1];
        }
        private async Task WaitForPermission(int threadId)
        {
            for (int i = 0; i < last_to_enter.Length; i++)
            {
                level[threadId] = i;

                last_to_enter[i] = threadId;
                
                Func<bool> cond1 = () => last_to_enter[i] != threadId;

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
            ResetSemaphore();
            var operationLists = new List<List<IWord>>();
            
            for (int i = 0; i < ThreadCount; i++){
                operationLists.Add(new List<IWord>());
            }

            int counter = 0;

            foreach(var d in Word){
                if (counter >= ThreadCount){
                    counter = 0;
                }

                operationLists[counter].Add(d);
                counter++;
            }
                    
            var operations = new List<Task>();

                for(int i = 0; i < operationLists.Count; i++){
                    var opList = operationLists[i];
                    var i1 = i;
                    var i2 = i;
                    var task = Task.Run( async () =>
                    {
                        foreach (var op in opList)
                        {
                            var agregg = Map(op).ToArray();
                            await WaitForPermission(i1);
                            try
                            {
                                Agregg.Add(agregg);
                            }
                            finally
                            {
                                SignalRelease(i1);
                            }
                        }
                    });
                operations.Add(task);
            }
            await Task.WhenAll(operations);
        }

        private async Task ShuffleGroups()
        {
            ResetSemaphore();
            var operationLists = new List<List<KeyValuePair<IKey, IValue>[]>>();

            for (int i = 0; i < ThreadCount; i++){
                operationLists.Add(new List<KeyValuePair<IKey, IValue>[]>());
            }

            int counter = 0;

            foreach(var d in Agregg){
                if (counter >= ThreadCount){
                    counter = 0;
                }

                operationLists[counter].Add(d);
                counter++;
            }
                    
            var operations = new List<Task>();

            for(int i = 0; i < operationLists.Count; i++){
                var opList = operationLists[i];
                var i1 = i;
                var task = Task.Run( async () =>
                {
                    foreach (var op in opList)
                    {
                        foreach (var pair in op)
                        {
                            await WaitForPermission(i1);
                            try
                            {
                                Buckets.AddOrUpdate(pair.Key, () =>
                                {
                                    return new List<IValue> {pair.Value};
                                }, list =>
                                {
                                   list.Add(pair.Value);
                                   return list;
                                });
                            }
                            finally
                            {
                                SignalRelease(i1);
                            }
                        }
                    }
                });
                operations.Add(task);
            }
            await Task.WhenAll(operations);
        }

        private async Task ReduceBuckets()
        {
            ResetSemaphore();
            var operationLists = new List<List<KeyValuePair<IKey, List<IValue>>>>();
            
            for (int i = 0; i < ThreadCount; i++){
                operationLists.Add(new List<KeyValuePair<IKey, List<IValue>>>());
            }

            int counter = 0;

            foreach(var d in Buckets){
                if (counter >= ThreadCount){
                    counter = 0;
                }

                operationLists[counter].Add(d);
                counter++;
            }
                    
            var operations = new List<Task>();

            for(int i = 0; i < operationLists.Count; i++){
                var opList = operationLists[i];
                var i1 = i;
                var task = Task.Run( async () =>
                {
                    foreach (var op in opList)
                    {
                        var pair = Reduce(op.Key, op.Value);
                        await WaitForPermission(i1);
                        try
                        {
                            Pairs.Add(new KeyValuePair<IKey, IValue>(op.Key, pair));
                        }
                        finally
                        {
                            SignalRelease(i1);
                        }
                    }
                });
                operations.Add(task);
            }
            await Task.WhenAll(operations);
        }

        private async Task WritePairs()
        {
            ResetSemaphore();
            var operationLists = new List<List<KeyValuePair<IKey, IValue>>>();
            
            for (int i = 0; i < ThreadCount; i++){
                operationLists.Add(new List<KeyValuePair<IKey, IValue>>());
            }

            int counter = 0;

            foreach(var d in Pairs){
                if (counter >= ThreadCount){
                    counter = 0;
                }

                operationLists[counter].Add(d);
                counter++;
            }
                    
            var operations = new List<Task>();

            for(int i = 0; i < operationLists.Count; i++){
                var opList = operationLists[i];
                var i1 = i;
                var task = Task.Run( async () =>
                {
                    foreach (var op in opList)
                    {
                        await WaitForPermission(i1);
                        try
                        {
                            Write(op);
                        }
                        finally
                        {
                            SignalRelease(i1);
                        }
                    }
                });
                operations.Add(task);
            }
            await Task.WhenAll(operations);
        }
    }
}