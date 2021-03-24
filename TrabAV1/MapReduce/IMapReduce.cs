using System;
using System.Collections.Generic;

namespace TrabAV1.MapReduce
{
    public interface IMapReduce<IInput, IWord, IKey, IValue>
    {
        public void Build(
            Func<IInput, IEnumerable<IWord>> Read, 
            Action<KeyValuePair<IKey, IValue>> Write,
            Func<IKey, IKey, bool> Compare,
            Func<IWord, IEnumerable<KeyValuePair<IKey, IValue>>> Map,
            Func<IKey, IEnumerable<IValue>, IValue> Reduce
        );
    }
}