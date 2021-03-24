using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace TrabAV1.WordCount
{
    public class ThreadDictionary<IKey,IValue> : IDictionary<IKey, IValue>
    {
        private class Bucket : IEnumerable<KeyValuePair<IKey, IValue>>
        {
            public IKey Key { get; set; }
            public IValue Value { get; set; }
            public Bucket Skip { get; set; }

            public KeyValuePair<IKey, IValue> GetPair()
            {
                return new (Key, Value);
            }

            public IEnumerator<KeyValuePair<IKey, IValue>> GetEnumerator()
            {
                var bucket = this;
                while (bucket != null)
                {
                    yield return bucket.GetPair();
                    bucket = bucket.Skip;
                }
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }
        
        private Bucket[] Buckets { get; set; }

        public ThreadDictionary()
        {
            Buckets = new Bucket[0];
        }

        public ThreadDictionary(int bucketCount)
        {
            Buckets = new Bucket[bucketCount];
        }

        private void InnerAdd(IKey key, IValue value)
        {
            var index = GetIndex(key);

            if (Buckets[index] == null)
            {
                Buckets[index] = new()
                {
                    Key = key,
                    Value = value
                };
            }
            else
            {
                var bucket = Buckets[index];
                while (bucket.Skip != null)
                    bucket = bucket.Skip;
                bucket.Skip = new()
                {
                    Key = key,
                    Value = value
                };
            }
        }

        public IEnumerator<KeyValuePair<IKey, IValue>> GetEnumerator()
        {
            lock (Buckets)
            {
                return Buckets.SelectMany(b =>
                {
                    if (b == null)
                        return new KeyValuePair<IKey, IValue>[0];
                    else
                        return b.ToArray();
                }).GetEnumerator();  
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Add(KeyValuePair<IKey, IValue> item)
        {
            lock (Buckets)
            {
                InnerAdd(item.Key, item.Value);
            }
        }

        public void Clear()
        {
            lock (Buckets)
            {
                Buckets = new Bucket[1];       
            }
        }

        public bool Contains(KeyValuePair<IKey, IValue> item)
        {
            lock (Buckets)
            {
                return this.ToArray().Contains(item);
            }
        }

        public void CopyTo(KeyValuePair<IKey, IValue>[] array, int arrayIndex)
        {
            lock (Buckets)
            {
                var pairs = this.ToArray();
                for (int i = arrayIndex; i < arrayIndex + pairs.Length; i++)
                {
                    array[i] = pairs[i - arrayIndex];
                }
            }
        }

        public bool Remove(KeyValuePair<IKey, IValue> item)
        {
            throw new System.NotImplementedException();
        }

        public int Count => Buckets.ToArray().Length;
        public bool IsReadOnly => false;
        
        public void Add(IKey key, IValue value)
        {
            lock (Buckets)
            {
                InnerAdd(key, value);
            }
        }

        public bool ContainsKey(IKey key)
        {
            lock (Buckets)
            {
                var index = GetIndex(key);
                var val = Buckets[index]?.Any(b => EqualityComparer<IKey>.Default.Equals(b.Key, key));
                return val.HasValue && val.Value;
            }
        }

        public bool Remove(IKey key)
        {
            throw new System.NotImplementedException();
        }

        public bool TryGetValue(IKey key, out IValue value)
        {
            lock (Buckets)
            {
                var index = GetIndex(key);
                var bucket = Buckets[index];

                while (bucket != null)
                {
                    if (EqualityComparer<IKey>.Default.Equals(bucket.Key, key))
                    {
                        value = bucket.Value;
                        return true;
                    }

                    bucket = bucket.Skip;
                }

                value = default;
                return false;
            }
        }

        public void AddOrUpdate(IKey key, Func<IValue> add, Func<IValue, IValue> update)
        {
            lock (Buckets)
            {
                if (ContainsKey(key))
                {
                    this[key] = update(this[key]);
                }
                else
                {
                    this[key] = add();
                }
            }
        }
        
        public IValue this[IKey key]
        {
            get
            {
                lock (Buckets)
                {
                    var hasValue = TryGetValue(key, out var value);
                    if (hasValue)
                        return value;
                    throw new KeyNotFoundException();
                }
            }
            set
            {
                lock (Buckets)
                {
                    var index = GetIndex(key);
                    var bucket = Buckets[index];

                    while (bucket != null)
                    {
                        if (EqualityComparer<IKey>.Default.Equals(bucket.Key, key))
                        {
                            bucket.Value = value;
                            return;
                        }

                        bucket = bucket.Skip;
                    }
                    
                    InnerAdd(key, value);
                }
            }
        }

        public ICollection<IKey> Keys => Buckets.SelectMany(b => b.Select(kvp => kvp.Key)).ToList();
        public ICollection<IValue> Values => Buckets.SelectMany(b => b.Select(kvp => kvp.Value)).ToList();

        private int GetIndex(IKey obj) => (int) ((uint) obj.GetHashCode() % Buckets.Length);
    }
}