using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Couchbase;
using Couchbase.Core;
using Couchbase.Configuration.Client;
using Couchbase.N1QL;

using JsonDictionary = System.Collections.Generic.Dictionary<string, object>;
using JsonTableDictionary = System.Collections.Generic.Dictionary<string, System.Collections.Generic.Dictionary<string, object>>;
using JsonDictionaryItem = System.Collections.Generic.KeyValuePair<string, System.Collections.Generic.Dictionary<string, object>>;


// CouchbaseExportWriter usage:
//
// var writer = new CouchbaseExportWriter("http://host.domain.com:8091/pools"); 
// writer.openBucket("default");
// JsonDictionary dict = GetSomeData();
// foreach (var item in dict)
// {
//                item.Value["_document_type"] = "STUDYACCESS";
//                item.Value["_document_origin"] = "BULK_IMPORT";
//                writer.upsert(item.Key, item.Value);
//
//                count++;
//  };


namespace FirebirdTest1
{
    [Serializable()]
    public class CouchbaseExportWriterFailureException : System.ApplicationException
    {
        public CouchbaseExportWriterFailureException() { }
        public CouchbaseExportWriterFailureException(string message) : base(message) { }
    }

    class CouchbaseExportWriter
    {
        private ClientConfiguration _config;
        private Cluster _cluster;
        private IBucket _bucket;

        public ClientConfiguration config { get; set; }
        public Cluster cluster { get; set; }
        public IBucket bucket { get; set; }

        // serverurl = "http://hostname:8091/pools" 
        public CouchbaseExportWriter( string serverUrl)
        {
            _config = new ClientConfiguration();
            _config.Servers.Add(new Uri(serverUrl));
            _cluster = new Cluster(_config);


        }
        public int getBucketItemCount()
        {
            if (_bucket != null)
            {
                string qryText = "SELECT COUNT(*) FROM `" + _bucket.Name + "`";
                var queryRequest = new QueryRequest(qryText);
                var result = _bucket.Query<dynamic>(queryRequest);
                var row = result.Rows[0];
                return row["$1"]; // Get value for default $1 result
            }
            else
            {
                return 0;
            }

        }

        public void openBucket(string bucketName)
        {
            _bucket = _cluster.OpenBucket(bucketName);
            
        }

        public void upsert( Couchbase.Document<dynamic> document)
        {
           IDocumentResult<dynamic> result = _bucket.Upsert(document);
            if (result.Status != Couchbase.IO.ResponseStatus.Success)
            {
                throw new CouchbaseExportWriterFailureException(result.Exception.Message);
            }
                        
        }

        public void upsert(string id, JsonDictionary documentDictionary )
        {
            var couchDoc = new Couchbase.Document<dynamic> 
            { Id = id,
              Content = documentDictionary
            };

            IDocumentResult<dynamic> result = _bucket.Upsert(couchDoc);
            if (result.Status != Couchbase.IO.ResponseStatus.Success)
            {
                throw new CouchbaseExportWriterFailureException(result.Exception.Message);
            }
        }

        public void delete(string id)
        {
            bucket.Remove(id);
        }
           
    }
}
