using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace TrabAV1.MapReduce
{
    public class LinqMapReduce<
        TInput,
        TWord,
        TKey,
        TValue> : IMapReduce<TInput, TWord, TKey, TValue>
    {
        internal Func<TInput, IEnumerable<TWord>> Read { get; set; }
        internal Action<KeyValuePair<TKey, TValue>> Write { get; set; }
        internal Func<TKey, TKey, bool> Compare { get; set; }
        internal Func<TWord, IEnumerable<KeyValuePair<TKey, TValue>>> Map { get; set; }
        internal Func<TKey, IEnumerable<TValue>, TValue> Reduce { get; set; }
        
        
        private ConcurrentBag<IEnumerable<KeyValuePair<TKey, TValue>>> Agregg = new();
        private ConcurrentDictionary<TKey, ConcurrentBag<TValue>> Buckets = new();
        private ConcurrentBag<KeyValuePair<TKey, TValue>> Pairs = new();

        public void Run(TInput input)
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

        private void MapData(TWord word)
        {
            var agregg = Map(word);
            Agregg.Add(agregg);
        }
        
        private void Shuffle(IEnumerable<KeyValuePair<TKey, TValue>> agregg)
        {
            agregg
                .AsParallel()
                .ForAll(pair =>
                {
                    Buckets.AddOrUpdate(pair.Key, (_) => new ConcurrentBag<TValue> {pair.Value}, (_, val) =>
                    {
                        val.Add(pair.Value);
                        return val;
                    });
                });
        }

        private void ReduceBucket(KeyValuePair<TKey, ConcurrentBag<TValue>> bucket)
        {
            var value = Reduce(bucket.Key, bucket.Value.ToArray());
            Pairs.Add(new KeyValuePair<TKey, TValue>(bucket.Key, value));
        }

        private void Cleanup()
        {
            Agregg.Clear();
            Buckets.Clear();
            Pairs.Clear();
        }

        public void Build(
            Func<TInput, IEnumerable<TWord>> Read,
            Action<KeyValuePair<TKey, TValue>> Write,
            Func<TKey, TKey, bool> Compare,
            Func<TWord, IEnumerable<KeyValuePair<TKey, TValue>>> Map,
            Func<TKey, IEnumerable<TValue>, TValue> Reduce
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