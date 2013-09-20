﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using MongoDB.Bson.Serialization;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Options;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Bson.IO;
using System.Globalization;

namespace Quartz.Impl.MongoDB
{
    /// <summary>
    /// Represents a serializer for DateTimeOffsets.
    /// </summary>
    public class DateTimeOffsetSerializer : BsonBaseSerializer
    {
        // private static fields
        private static DateTimeOffsetSerializer __instance = new DateTimeOffsetSerializer();

        // constructors
        /// <summary>
        /// Initializes a new instance of the DateTimeSerializer class.
        /// </summary>
        public DateTimeOffsetSerializer()
            : base(new DateTimeSerializationOptions())
        {
        }

        // public static properties
        /// <summary>
        /// Gets an instance of the DateTimeSerializer class.
        /// </summary>
        public static DateTimeOffsetSerializer Instance
        {
            get { return __instance; }
        }

        // public methods
        /// <summary>
        /// Deserializes an object from a BsonReader.
        /// </summary>
        /// <param name="bsonReader">The BsonReader.</param>
        /// <param name="nominalType">The nominal type of the object.</param>
        /// <param name="actualType">The actual type of the object.</param>
        /// <param name="options">The serialization options.</param>
        /// <returns>An object.</returns>
        public override object Deserialize(
            BsonReader bsonReader,
            Type nominalType,
            Type actualType,
            IBsonSerializationOptions options)
        {
            VerifyTypes(nominalType, actualType, typeof(DateTimeOffset));
            var dateTimeSerializationOptions = EnsureSerializationOptions<DateTimeSerializationOptions>(options);

            var bsonType = bsonReader.GetCurrentBsonType();
            DateTimeOffset value;
            switch (bsonType)
            {
                case BsonType.DateTime:
                    // use an intermediate BsonDateTime so MinValue and MaxValue are handled correctly
                    value = (new BsonDateTime(bsonReader.ReadDateTime())).ToUniversalTime();
                    break;
                case BsonType.Document:
                    bsonReader.ReadStartDocument();
                    bsonReader.ReadDateTime("DateTimeUTC"); // ignore value (use Ticks instead)
                    value = new DateTime(bsonReader.ReadInt64("Ticks"), DateTimeKind.Utc);
                    bsonReader.ReadEndDocument();
                    break;
                case BsonType.Int64:
                    value = new DateTime(bsonReader.ReadInt64(), DateTimeKind.Utc);
                    break;
                case BsonType.String:
                    // note: we're not using XmlConvert because of bugs in Mono
                    if (dateTimeSerializationOptions.DateOnly)
                    {
                        value = DateTime.SpecifyKind(DateTime.ParseExact(bsonReader.ReadString(), "yyyy-MM-dd", null), DateTimeKind.Utc);
                    }
                    else
                    {
                        var formats = new string[] { "yyyy-MM-ddK", "yyyy-MM-ddTHH:mm:ssK", "yyyy-MM-ddTHH:mm:ss.FFFFFFFK" };
                        value = DateTime.ParseExact(bsonReader.ReadString(), formats, null, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
                    }
                    break;
                default:
                    var message = string.Format("Cannot deserialize DateTimeOffset from BsonType {0}.", bsonType);
                    throw new FormatException(message);
            }

            return value;
        }

        /// <summary>
        /// Serializes an object to a BsonWriter.
        /// </summary>
        /// <param name="bsonWriter">The BsonWriter.</param>
        /// <param name="nominalType">The nominal type.</param>
        /// <param name="value">The object.</param>
        /// <param name="options">The serialization options.</param>
        public override void Serialize(
            BsonWriter bsonWriter,
            Type nominalType,
            object value,
            IBsonSerializationOptions options)
        {
            var dateTime = (DateTimeOffset)value;
            var dateTimeSerializationOptions = EnsureSerializationOptions<DateTimeSerializationOptions>(options);

            DateTime utcDateTime;
            utcDateTime = BsonUtils.ToUniversalTime(dateTime.UtcDateTime);
            var millisecondsSinceEpoch = BsonUtils.ToMillisecondsSinceEpoch(utcDateTime);

            switch (dateTimeSerializationOptions.Representation)
            {
                case BsonType.DateTime:
                    bsonWriter.WriteDateTime(millisecondsSinceEpoch);
                    break;
                case BsonType.Document:
                    bsonWriter.WriteStartDocument();
                    bsonWriter.WriteDateTime("DateTimeUTC", millisecondsSinceEpoch);
                    bsonWriter.WriteInt64("Ticks", utcDateTime.Ticks);
                    bsonWriter.WriteEndDocument();
                    break;
                case BsonType.Int64:
                    bsonWriter.WriteInt64(utcDateTime.Ticks);
                    break;
                case BsonType.String:
                    if (dateTimeSerializationOptions.DateOnly)
                    {
                        bsonWriter.WriteString(dateTime.ToString("yyyy-MM-dd"));
                    }
                    else
                    {
                        // we're not using XmlConvert.ToString because of bugs in Mono
                        if (dateTime == DateTime.MinValue || dateTime == DateTime.MaxValue)
                        {
                            // serialize MinValue and MaxValue as Unspecified so we do NOT get the time zone offset
                            dateTime = DateTime.SpecifyKind(dateTime.UtcDateTime, DateTimeKind.Unspecified);
                        }
                        else if (dateTime.UtcDateTime.Kind == DateTimeKind.Unspecified)
                        {
                            // serialize Unspecified as Local se we get the time zone offset
                            dateTime = DateTime.SpecifyKind(dateTime.UtcDateTime, DateTimeKind.Local);
                        }
                        bsonWriter.WriteString(dateTime.ToString("yyyy-MM-ddTHH:mm:ss.FFFFFFFK"));
                    }
                    break;
                default:
                    var message = string.Format("'{0}' is not a valid DateTimeOffset representation.", dateTimeSerializationOptions.Representation);
                    throw new BsonSerializationException(message);
            }
        }
    }
}