using System;
using System.Collections.Generic;
using TrabAV1.MapReduce;

namespace TrabAV1.MapReduce
{
    public abstract class MapReduce
    {
        public static MORBuilder<MORInput> WithInput<MORInput>()
        {
            return new();
        }
    }
    
    public class MORBuilder<IInput>
    {
        public MorBuilder<IInput, IWord> WithReader<IWord>(Func<IInput, IEnumerable<IWord>> readFunction)
        {
            return new()
            {
                Read = readFunction
            };
        }
    }

    public class MorBuilder<IInput, IWord> : MORBuilder<IInput>
    {
        public Func<IInput, IEnumerable<IWord>> Read { get; init; }

        public MorBuilder<IInput, IWord, TKey, TValue> WithMapper<TKey, TValue>(
            Func<IWord, IEnumerable<KeyValuePair<TKey, TValue>>> mapper)
        {
            return new()
            {
                Read = Read,
                Map = mapper
            };
        }
        
    }

    public class MorBuilder<TInput, TData, TKey, TValue> : MorBuilder<TInput, TData>
    {
        public static Func<TKey, TKey, bool> defaultCompare
            = (k1, k2) => k1.Equals(k2);
        public static Action<KeyValuePair<TKey, TValue>> defaultWrite
            = (pair) => Console.WriteLine($"Key: {pair.Key} | Value: {pair.Value}");
        
        internal Func<TData, IEnumerable<KeyValuePair<TKey, TValue>>> Map { get; init; }
        internal Func<TKey, TKey, bool> Compare { get; set; } = defaultCompare;
        internal Func<TKey, IEnumerable<TValue>, TValue> Reduce { get; set; }
        internal Action<KeyValuePair<TKey, TValue>> Write { get; set; } = defaultWrite;

        public MorBuilder<TInput, TData, TKey, TValue> WithComparer(Func<TKey, TKey, bool> comparer)
        {
            Compare = comparer;
            return this;
        }
        
        public MorBuilder<TInput, TData, TKey, TValue> WithReducer(Func<TKey, IEnumerable<TValue>, TValue> reducer)
        {
            Reduce = reducer;
            return this;
        }

        public MorBuilder<TInput, TData, TKey, TValue> WithWriter(Action<KeyValuePair<TKey, TValue>> writer)
        {
            Write = writer;
            return this;
        }

        public TMapReduce Build<TMapReduce>() where TMapReduce : IMapReduce<TInput, TData, TKey, TValue>, new()
        {
            if (Reduce == null)
                //CHANGE
                throw new ArgumentException("Reducer cannot be null", nameof(Reduce));

            var mapReduce = new TMapReduce();
            mapReduce.Build(Read, Write, Compare, Map, Reduce);
            return mapReduce;
        }
    }
}