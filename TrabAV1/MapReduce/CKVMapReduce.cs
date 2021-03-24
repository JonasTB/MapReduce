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
        
        private TaskAwaiter CompositeTask { get; }

        private int[] level;
        private int[] last_to_enter;
        
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
            var operationLists = new List<List<IWord>>();
            
            for (int i = 0; i < ThreadCount; i++){
                operationLists.Add(new List<IWord>());
            }

            int counter = 0;

            foreach(var d in Word){
                if (counter > ThreadCount){
                    counter = 0;
                }

                operationLists[counter].Add(d);
            }
                    
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
            var operationLists = new List<List<IValue>>();

            for (int i = 0; i < ThreadCount; i++){
                operationLists.Add(new List<IValue>());
            }

            int counter = 0;

            foreach(var d in Agregg){
                if (counter > ThreadCount){
                    counter = 0;
                }

                operationLists[counter].Add(d);
            }
                    
            var operations = new List<Task>();

            for(int i = 0; i < operationLists.Count; i++){
                var opList = operationLists[i];
                var task = Task.Run(() =>
                {
                    foreach (var op in opList)
                    {
                        var buckets = Map(op).ToArray();
                        WaitForPermission(i);
                        try
                        {
                            Buckets.Add(buckets);
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

        private async Task ReduceBuckets()
        {
           
        }

        private async Task WritePairs()
        {
            
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