using System;

namespace AV1Map.ReadThreads
{
    public readonly struct ParallelTask
    {
        //Action representing the non-critical section
        public Action NonCriticalSection { get; init; }
        //Action representing the critical section
        public Action CriticalSection { get; init; }
    }
}