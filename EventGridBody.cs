using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CrossTenantBlobADLSReplication
{

    public class EventGridBody
    {
        public string api { get; set; }
        public string clientRequestId { get; set; }
        public string requestId { get; set; }
        public string eTag { get; set; }
        public string contentType { get; set; }
        public int contentLength { get; set; }
        public string blobType { get; set; }
        public string blobUrl { get; set; }
        public string url { get; set; }
        public string sequencer { get; set; }
        public string identity { get; set; }
        public Storagediagnostics storageDiagnostics { get; set; }
    }

    public class Storagediagnostics
    {
        public string batchId { get; set; }
    }

}
