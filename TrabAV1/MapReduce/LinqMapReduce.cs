using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace TrabAV1.MapReduce
{
    public class LinqMapReduce<IInput, IWord, IKey, IValue> : IMapReduce<IInput, IWord, IKey, IValue>
    {
        internal Func<IInput, IEnumerable<IWord>> Read { get; set; }
        internal Action<KeyValuePair<IKey, IValue>> Write { get; set; }
        internal Func<IKey, IKey, bool> Compare { get; set; }
        internal Func<IWord, IEnumerable<KeyValuePair<IKey, IValue>>> Map { get; set; }
        internal Func<IKey, IEnumerable<IValue>, IValue> Reduce { get; set; }
        
        
        private ConcurrentBag<IEnumerable<KeyValuePair<IKey, IValue>>> Agregg = new();
        private ConcurrentDictionary<IKey, ConcurrentBag<IValue>> Buckets = new();
        private ConcurrentBag<KeyValuePair<IKey, IValue>> Pairs = new();

        public void Run(IInput input)
        {
            var readData = Read(input);
            
            readData
                .AsParallel()
                .ForAll(MapData);
            
            Agregg
                .AsParallel()
                .ForAll(Shuffle);
            
            Buckets
                .AsParallel()
                .ForAll(ReduceBucket);
            
            Pairs
                .AsParallel()
                .ForAll(Write);
            
            Cleanup();
        }

        private void MapData(IWord word)
        {
            var agregg = Map(word);
            Agregg.Add(agregg);
        }
        
        private void Shuffle(IEnumerable<KeyValuePair<IKey, IValue>> agregg)
        {
            agregg
                .AsParallel()
                .ForAll(pair =>
                {
                    Buckets.AddOrUpdate(pair.Key, (_) => new ConcurrentBag<IValue> {pair.Value}, (_, val) =>
                    {
                        val.Add(pair.Value);
                        return val;
                    });
                });
        }

        private void ReduceBucket(KeyValuePair<IKey, ConcurrentBag<IValue>> bucket)
        {
            var value = Reduce(bucket.Key, bucket.Value.ToArray());
            Pairs.Add(new KeyValuePair<IKey, IValue>(bucket.Key, value));
        }

        private void Cleanup()
        {
            Agregg.Clear();
            Buckets.Clear();
            Pairs.Clear();
        }

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