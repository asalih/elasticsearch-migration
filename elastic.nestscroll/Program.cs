using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Mail;
using System.Text;
using System.Threading.Tasks;

namespace elastic.nestscroll
{
    class Program
    {
        static Nest.ElasticClient clientSource;
        static Nest.ElasticClient clientTarget;
        static void Main(string[] args)
        {
            Console.WriteLine("source uri:");
            string suri = Console.ReadLine();

            Nest.ConnectionSettings settings = new Nest.ConnectionSettings(new Uri(suri));
            clientSource = new Nest.ElasticClient(settings);

            Console.WriteLine("target uri:");
            string turi = Console.ReadLine();
            Nest.ConnectionSettings settingsT = new Nest.ConnectionSettings(new Uri("http://10.25.30.110:9200"));
            clientTarget = new Nest.ElasticClient(settingsT);

            Console.WriteLine("source index:");
            string idxs = Console.ReadLine();
            Console.WriteLine("source type:");
            string typs = Console.ReadLine();

            Console.WriteLine("target index:");
            string idxt = Console.ReadLine();
            Console.WriteLine("target type:");
            string typt = Console.ReadLine();

            Console.Clear();
            Console.WriteLine("do you want to proceed? (Y/N)");
            Console.WriteLine(string.Format("{0}/{1} = {2}/{3}", idxs, typs, idxt, typt));
            string ans = Console.ReadLine();

            if(!string.IsNullOrEmpty(ans) && ans.ToLower() != "y")
            {
                return;
            }

            Stopwatch sw = new Stopwatch();
            sw.Start();
            
            MigrateData(idxs, typs, idxt, typt);
            
            sw.Stop();

            Console.WriteLine(string.Format("{0}/{1} = {2}/{3} done.", idxs, typs, idxt, typt));

            Console.WriteLine(sw.Elapsed.ToString());
            Console.ReadLine();

            
        }

        public static void MigrateData(string indexSource, string typeSource, string indexTarget, string typeTarget)
        {

            var scanResults = clientSource.Search<dynamic>(s => s
            .Index(indexSource)
            .Type(typeSource)
            .From(0)
            .Size(250)
            .MatchAll()
            .Scroll("5s")
            );

            BulkIndex(scanResults, clientTarget, indexTarget, typeTarget);
            var scrolls = 0;
            var results = clientSource.Scroll<dynamic>(s => s.Scroll("5s").ScrollId(scanResults.ScrollId));
            while (results.Documents.Any())
            {
                BulkIndex(results, clientTarget, indexTarget, typeTarget);
                results = clientSource.Scroll<dynamic>(s => s.Scroll("5s").ScrollId(results.ScrollId));
                scrolls++;
                Console.Clear();
                Console.WriteLine(scrolls);
            }

        }

        public static void ClearData(string indexTarget, string typeTarget)
        {

            var scanResults = clientTarget.Search<dynamic>(s => s
            .Index(indexTarget)
            .Type(typeTarget)
            .From(0)
            .Size(250)
            .MatchAll()
            .Scroll("5s")
            );

            BulkDelete(scanResults, clientTarget, indexTarget, typeTarget);
            var scrolls = 0;
            var results = clientTarget.Scroll<dynamic>(s => s.Scroll("5s").ScrollId(scanResults.ScrollId));
            while (results.Documents.Any())
            {

                BulkDelete(results, clientTarget, indexTarget, typeTarget);
                results = clientTarget.Scroll<dynamic>(s => s.Scroll("5s").ScrollId(results.ScrollId));
                scrolls++;
                Console.Clear();
                Console.WriteLine(scrolls);
            }

        }

        public static void BulkIndex(dynamic result, Nest.ElasticClient clTarget, string indexTarget, string typeTarget) {
            
            var req = new Nest.BulkDescriptor();
            foreach (var item in result.Hits)
            {
                req.Index<dynamic>(r => r.Index(indexTarget).Type(typeTarget).Id(item.Id).Document(item.Source));
            }


           clTarget.Bulk(req);


        }

        public static void BulkDelete(dynamic result, Nest.ElasticClient clTarget, string indexTarget, string typeTarget)
        {


            var req = new Nest.BulkDescriptor();
            foreach (var item in result.Hits)
            {
                req.Delete<dynamic>(r => r.Index(indexTarget).Type(typeTarget).Id(item.Id));
            }


            clTarget.Bulk(req);


        }


    }
}
