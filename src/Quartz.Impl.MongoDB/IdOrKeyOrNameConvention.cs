using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Bson.Serialization.Conventions;

namespace Quartz.Impl.MongoDB
{
    public class IdOrKeyOrNameConvention : IIdMemberConvention
    {
        public string FindIdMember(Type type)
        {
            if (type.GetProperty("Id") != null)
                return "Id";

            if (type.GetProperty("Key") != null)
                return "Key";

            if (type.GetProperty("Name") != null)
                return "Name";
            
            return null;
        }
    }
}
