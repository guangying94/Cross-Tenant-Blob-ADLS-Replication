using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CrossTenantBlobADLSReplication
{
    public class EventHubMessageBody
    {
        public string FileUrl { get; set; }
        public string Status { get; set; }
    }
}
