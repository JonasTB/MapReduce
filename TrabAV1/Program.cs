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
            var text = string.Join("\n", Enumerable.Repeat("Deer Bear River\nCar Car River\nDeer Car Bear", 500));

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
            var sw = Stopwatch.StartNew();
            await mapReduce.RunAsync(text, 4);
            sw.Stop();

            Console.WriteLine($"Low-Level Map Reduce ran in {sw.ElapsedMilliseconds}ms");
            
            //Ex. Output
            //Key: Deer | Value: 1000
            //Key: Bear | Value: 1000
            //Key: River | Value: 1000
            //Key: Car | Value: 1500
            //Low-Level Map Reduce ran in 76ms
        }
    }
}