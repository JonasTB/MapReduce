using System;

namespace AV1Map.ReadThreads
{
    public readonly struct ParallelTask
    {
        public Action NonCriticalSection { get; init; }
        public Action CriticalSection { get; init; }
    }
}