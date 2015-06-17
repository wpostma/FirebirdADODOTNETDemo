using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Newtonsoft.Json;

using Couchbase;
using Couchbase.Core;
using Couchbase.Configuration.Client;
//using Couchbase.IO;
//using Couchbase.Management;
//using Couchbase.N1QL;
//using Couchbase.Views;
//using Couchbase.Utils;

using JsonDictionary = System.Collections.Generic.Dictionary<string, object>;
using JsonTableDictionary = System.Collections.Generic.Dictionary<string, System.Collections.Generic.Dictionary<string, object>>;
using JsonDictionaryItem = System.Collections.Generic.KeyValuePair<string, System.Collections.Generic.Dictionary<string, object>>;


namespace FirebirdTest1
{
    class CouchbaseExportWriter
    {
        private ClientConfiguration _config;
        private Cluster _cluster;
        private IBucket _bucket;

        public ClientConfiguration config { get; set; }
        public Cluster cluster { get; set; }
        public IBucket bucket { get; set; }

        // serverurl = "http://couchbase1.ramsoft.biz:8091/pools"
        public CouchbaseExportWriter( string serverUrl)
        {
            _config = new ClientConfiguration();
            _config.Servers.Add(new Uri(serverUrl));
            _cluster = new Cluster(_config);
        }

        public void openBucket(string bucketName)
        {
            _bucket = _cluster.OpenBucket(bucketName);
            
        }

        public void upsert( Couchbase.Document<dynamic> document)
        {
            _bucket.Upsert(document);
        }

        public void upsert(string id, JsonDictionary documentDictionary )
        {
            var couchDoc = new Couchbase.Document<dynamic> 
            { Id = id,
              Content = documentDictionary
            };

            _bucket.Upsert(couchDoc);
        }

        public void delete(string id)
        {
            bucket.Remove(id);
        }
           
    }
}
