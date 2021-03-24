using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using TrabAV1.MapReduce;

namespace TrabAV1
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var text = "Davi Jonas Davi\nAllan Rodrigo Jonas\nRodrigo Jonas Allan\nDavi Rodrigo Allan";

            var mapReduce = new CKVMapReduce<string, string, string, int>
            {
                Read = input => input.Split("\n"),
                Map = data =>
                {
                    var keys = data.Split(" ");
                    return keys.Select(k => new KeyValuePair<string, int>(k, 1));
                },
                Reduce = (word, instances) => instances.Sum(),
                Write = pair => System.Console.WriteLine($"Key: {pair.Key} | Value: {pair.Value}")
            };
            await mapReduce.RunAsync(text, 4);
        }
    }
}