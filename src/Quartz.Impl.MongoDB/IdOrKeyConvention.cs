using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Bson.Serialization.Conventions;

namespace Quartz.Impl.MongoDB
{
    public class IdOrKeyConvention : IIdMemberConvention
    {
        public string FindIdMember(Type type)
        {
            if (type.GetProperty("Id") != null)
                return "Id";

            if (type.GetProperty("Key") != null)
                return "Key";
            
            return null;
        }
    }
}
