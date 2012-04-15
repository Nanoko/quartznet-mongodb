using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Configuration;
using System.Linq;
using System.Text;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using System.Reflection;
using MongoDB.Bson.Serialization.Conventions;
using Quartz.Impl.Triggers;
using MongoDB.Bson.Serialization.Options;

namespace Quartz.Impl.MongoDB
{
    public class NaiveJobStore : Quartz.Spi.IJobStore
    {
        private readonly string _connectionString;

        private MongoDatabase _database;
        private string _instanceId;
        private string _instanceName;
        private int _threadPoolSize;
        private Spi.ITypeLoadHelper _loadHelper;
        private Spi.ISchedulerSignaler _signaler;
		
        private static readonly object Sync = new object();
        private static readonly TimeSpan _lockTimeout = new TimeSpan(0, 10, 0); // 10 minutes

        /// <summary>
		/// Initializes a new instance of the <see cref="JobStore"/> class
		/// using a dictionary of configured settings.
		/// </summary>
		public NaiveJobStore()
		{
            string connectionString = ConfigurationManager.ConnectionStrings["quartznet-mongodb"].ConnectionString;

			//
			// If there is no connection string to use then throw an 
			// exception to abort construction.
			//

			if (connectionString.Length == 0)
				throw new ApplicationException("Connection string is missing for the MongoDB job store.");

			_connectionString = connectionString;

            lock (Sync)
            {
                this._database = MongoDatabase.Create(_connectionString);

                this.Jobs.EnsureIndex(IndexKeys.Ascending("Group"));

                this.Triggers.EnsureIndex(IndexKeys.Ascending("Group"));
                this.Triggers.EnsureIndex(IndexKeys.Ascending("JobGroup"));
            }
		}

        static NaiveJobStore()
        {
            var myConventions = new ConventionProfile();
            myConventions.SetIdMemberConvention(new IdOrKeyOrNameConvention());
            BsonClassMap.RegisterConventions(
                myConventions,
                t => true
            );

            BsonSerializer.RegisterSerializer(
                typeof(DateTimeOffset),
                new DateTimeOffsetSerializer()
            );

            BsonSerializer.RegisterSerializer(
                typeof(DateTime),
                new DateTimeSerializer()
            );

            BsonSerializer.RegisterSerializer(
                typeof(JobKey),
                new JobKeySerializer()
            );

            BsonSerializer.RegisterSerializer(
                typeof(TriggerKey),
                new TriggerKeySerializer()
            );

            BsonSerializer.RegisterSerializer(
                typeof(JobDetailImpl),
                new JobDetailImplSerializer()
            );

            BsonClassMap.RegisterClassMap<JobDetailImpl>(cm =>
            {
                cm.AutoMap();
                cm.SetDiscriminator("JobDetailImpl");
            });

            BsonSerializer.RegisterSerializer(
                typeof(JobDataMap),
                new JobDataMapSerializer()
            );

            BsonClassMap.RegisterClassMap<AbstractTrigger>(cm =>
            {
                cm.AutoMap();
                cm.SetIsRootClass(true);
            });

            BsonClassMap.RegisterClassMap<SimpleTriggerImpl>(cm =>
            {
                cm.AutoMap();
                cm.MapField("complete");
                cm.MapField("nextFireTimeUtc");
                cm.MapField("previousFireTimeUtc");
            });
        }

        private MongoCollection Calendars { get { return this._database.GetCollection("Calendars"); } }
        private MongoCollection Jobs { get { return this._database.GetCollection("Jobs"); } }
        private MongoCollection Triggers { get { return this._database.GetCollection("Triggers"); } }
        private MongoCollection TriggerLocks { get { return this._database.GetCollection("TriggerLocks"); } }

        public IList<Spi.IOperableTrigger> AcquireNextTriggers(DateTimeOffset noLaterThan, int maxCount, TimeSpan timeWindow)
        {
            DateTimeOffset limit = DateTime.Now + timeWindow;
            if (noLaterThan < limit)
            {
                limit = noLaterThan;
            }

            IList<Spi.IOperableTrigger> acquiredTriggers = new List<Spi.IOperableTrigger>();
            
            /*this.TriggerLocks.Remove(Query.LTE("Expires", DateTime.Now.Ticks));*/

            var eligibleTriggers = this.Triggers
                .FindAs<Spi.IOperableTrigger>(
                    Query.And(
                        Query.LTE("nextFireTimeUtc.Ticks", limit.DateTime.Ticks),
                        Query.NE("State", TriggerState.Paused.ToString())));

            /*foreach (Spi.IOperableTrigger trigger in eligibleTriggers)
            {
                BsonDocument triggerLock = new BsonDocument();
                triggerLock.Add("_id", trigger.Key.ToBsonDocument());
                triggerLock.Add("InstanceId", this._instanceId);
                triggerLock.Add("Expires", (DateTime.Now + _lockTimeout).Ticks);

                this.TriggerLocks.Insert<BsonDocument>(triggerLock);
                var result = this.TriggerLocks.FindOneAs<BsonDocument>(
                    Query.And(
                        Query.EQ("_id", trigger.Key.ToBsonDocument()),
                        Query.EQ("InstanceId", this._instanceId)));
                if (!result.IsBsonNull)
                {
                    acquiredTriggers.Add(trigger);
                }

                if (acquiredTriggers.Count == maxCount) break;
            }*/

            return eligibleTriggers
                .OrderBy(t => t.GetNextFireTimeUtc())
                .Take(maxCount)
                .ToList();
        }

        public bool CheckExists(TriggerKey triggerKey)
        {
            var result = this.Triggers.FindOneByIdAs<BsonDocument>(triggerKey.ToBsonDocument());
            return !result.IsBsonNull;
        }

        public bool CheckExists(JobKey jobKey)
        {
            var result = this.Jobs.FindOneByIdAs<BsonDocument>(jobKey.ToBsonDocument());
            return !result.IsBsonNull;
        }

        public void ClearAllSchedulingData()
        {
            this.Calendars.RemoveAll();
            this.Jobs.RemoveAll();
            this.Triggers.RemoveAll();
        }

        public bool Clustered
        {
            get { return true; }
        }

        public long EstimatedTimeToReleaseAndAcquireTrigger
        {
            get { return 200; }
        }

        public IList<string> GetCalendarNames()
        {
            return this.Calendars
                .Distinct("Name")
                .Select(g => g.AsString)
                .ToList();
        }

        public IList<string> GetJobGroupNames()
        {
            return this.Jobs
                .Distinct("Group")
                .Select(g => g.AsString)
                .ToList();
        }

        public Collection.ISet<JobKey> GetJobKeys(Matchers.GroupMatcher<JobKey> matcher)
        {
            var result = this.Jobs
                .FindAs<IJobDetail>(
                    Query.EQ("Group", matcher.CompareToValue))
                .Select(j => j.Key);

            return new Collection.HashSet<JobKey>(result);
        }

        public int GetNumberOfCalendars()
        {
            return (int)this.Calendars
                .Count();
        }

        public int GetNumberOfJobs()
        {
            return (int)this.Jobs
                .Count();
        }

        public int GetNumberOfTriggers()
        {
            return (int)this.Triggers
                .Count();
        }

        public Collection.ISet<string> GetPausedTriggerGroups()
        {
            BsonJavaScript map = new BsonJavaScript("function(){emit(this.Group, this.State == \""+TriggerState.Paused.ToString()+"\");}");
            BsonJavaScript reduce = new BsonJavaScript("function(key, values){var result = {isPaused: true};values.foreach(function(value){result.isPaused = result.isPaused && value;});return result;}");

            var result = this.Triggers.MapReduce(map, reduce);
            var groups = result.InlineResults.Where(r => r.GetElement("value").Value.AsBoolean == true).Select(r => r.GetElement("_id").Value.AsString);
            return new Collection.HashSet<string>(groups);
        }

        public IList<string> GetTriggerGroupNames()
        {
            return this.Triggers
                .Distinct("Group")
                .Select(g => g.AsString)
                .ToList();
        }

        public Collection.ISet<TriggerKey> GetTriggerKeys(Matchers.GroupMatcher<TriggerKey> matcher)
        {
            var result = this.Triggers
                .FindAs<Spi.IOperableTrigger>(
                    Query.EQ("Group", matcher.CompareToValue))
                .Select(t => t.Key);

            return new Collection.HashSet<TriggerKey>(result);
        }

        public TriggerState GetTriggerState(TriggerKey triggerKey)
        {
            var result = this.Triggers.FindOneByIdAs<BsonDocument>(triggerKey.ToBsonDocument());
            switch (result.GetElement("State").Value.AsString)
            {
                case "Blocked":
                    return TriggerState.Blocked;
                case "Complete":
                    return TriggerState.Complete;
                case "Error":
                    return TriggerState.Error;
                case "Normal":
                    return TriggerState.Normal;
                case "Paused":
                    return TriggerState.Paused;
                default:
                    return TriggerState.None;
            }
        }

        public IList<Spi.IOperableTrigger> GetTriggersForJob(JobKey jobKey)
        {
            return this.Triggers
                .FindAs<Spi.IOperableTrigger>(
                    Query.EQ("JobKey", jobKey.ToBsonDocument()))
                .ToList();
        }

        public void Initialize(Spi.ITypeLoadHelper loadHelper, Spi.ISchedulerSignaler signaler)
        {
            this._loadHelper = loadHelper;
            this._signaler = signaler;
        }

        public string InstanceId
        {
            set { this._instanceId = value; }
        }

        public string InstanceName
        {
            set { this._instanceName = value; }
        }

        public bool IsJobGroupPaused(string groupName)
        {
            BsonJavaScript map = new BsonJavaScript("function(){emit(this.JobGroup, this.State == \"" + TriggerState.Paused.ToString() + "\");}");
            BsonJavaScript reduce = new BsonJavaScript("function(key, values){var result = {isPaused: true};values.foreach(function(value){result.isPaused = result.isPaused && value;});return result;}");

            var result = this.Triggers.MapReduce(map, reduce);
            var groups = result.InlineResults.Where(r => r.GetElement("value").Value.AsBoolean == true).Select(r => r.GetElement("_id").Value.AsString);
            return groups
                .Where(g => g.Equals(groupName))
                .Count() > 0;
        }

        public bool IsTriggerGroupPaused(string groupName)
        {
            return this.GetPausedTriggerGroups()
                .Where(g => g.Equals(groupName))
                .Count() > 0;
        }

        public void PauseAll()
        {
            this.Triggers.Update(
                Query.Null,
                Update.Set("State", TriggerState.Paused.ToString()));
        }

        public void PauseJob(JobKey jobKey)
        {
            this.Triggers.Update(
                Query.And(Query.EQ("JobGroup", jobKey.Group), Query.EQ("JobName", jobKey.Name)),
                Update.Set("State", TriggerState.Paused.ToString()));
        }

        public IList<string> PauseJobs(Matchers.GroupMatcher<JobKey> matcher)
        {
            var result = this.Triggers.Update(
                Query.EQ("JobGroup", matcher.CompareToValue),
                Update.Set("State", TriggerState.Paused.ToString()));

            return new List<string>(); // TODO
        }

        public void PauseTrigger(TriggerKey triggerKey)
        {
            this.Triggers.Update(
                Query.EQ("_id", triggerKey.ToBsonDocument()),
                Update.Set("State", TriggerState.Paused.ToString()));
        }

        public Collection.ISet<string> PauseTriggers(Matchers.GroupMatcher<TriggerKey> matcher)
        {
            var result = this.Triggers.Update(
                Query.EQ("Group", matcher.CompareToValue),
                Update.Set("State", TriggerState.Paused.ToString()));

            return new Collection.HashSet<string>(); // TODO
        }

        public void ReleaseAcquiredTrigger(Spi.IOperableTrigger trigger)
        {
            this.TriggerLocks.Remove(
                Query.And(
                    Query.EQ("_id", trigger.Key.ToBsonDocument()),
                    Query.EQ("InstanceId", this._instanceId)));
        }

        public bool RemoveCalendar(string calName)
        {
            var result = this.Calendars.Remove(
                Query.EQ("Name", calName));

            return result.Ok;
        }

        public bool RemoveJob(JobKey jobKey)
        {
            var result = this.Jobs.Remove(
                Query.EQ("_id", jobKey.ToBsonDocument()));

            return result.Ok;
        }

        public bool RemoveJobs(IList<JobKey> jobKeys)
        {
            foreach (JobKey jobKey in jobKeys)
            {
                if (!this.RemoveJob(jobKey))
                    return false;
            }

            return true;
        }

        public bool RemoveTrigger(TriggerKey triggerKey)
        {
            var result = this.Triggers.Remove(
                Query.EQ("_id", triggerKey.ToBsonDocument()));

            return result.Ok;
        }

        public bool RemoveTriggers(IList<TriggerKey> triggerKeys)
        {
            foreach (TriggerKey triggerKey in triggerKeys)
            {
                if (!this.RemoveTrigger(triggerKey))
                    return false;
            }

            return true;
        }

        public bool ReplaceTrigger(TriggerKey triggerKey, Spi.IOperableTrigger newTrigger)
        {
            var result = this.Triggers.Update(
                Query.EQ("_id", triggerKey.ToBsonDocument()),
                Update.Replace(newTrigger));

            return result.Ok;
        }

        public void ResumeAll()
        {
            this.Triggers.Update(
                Query.Null,
                Update.Set("State", TriggerState.Normal.ToString()));
        }

        public void ResumeJob(JobKey jobKey)
        {
            this.Triggers.Update(
                Query.And(Query.EQ("JobGroup", jobKey.Group), Query.EQ("JobName", jobKey.Name)),
                Update.Set("State", TriggerState.Normal.ToString()));
        }

        public Collection.ISet<string> ResumeJobs(Matchers.GroupMatcher<JobKey> matcher)
        {
            var result = this.Triggers.Update(
                Query.EQ("JobGroup", matcher.CompareToValue),
                Update.Set("State", TriggerState.Normal.ToString()));

            return new Collection.HashSet<string>(); // TODO
        }

        public void ResumeTrigger(TriggerKey triggerKey)
        {
            this.Triggers.Update(
                Query.EQ("_id", triggerKey.ToBsonDocument()),
                Update.Set("State", TriggerState.Normal.ToString()));
        }

        public IList<string> ResumeTriggers(Matchers.GroupMatcher<TriggerKey> matcher)
        {
            var result = this.Triggers.Update(
                Query.EQ("Group", matcher.CompareToValue),
                Update.Set("State", TriggerState.Normal.ToString()));

            return new List<string>(); // TODO
        }

        public ICalendar RetrieveCalendar(string calName)
        {
            return this.Calendars
                .FindOneByIdAs<ICalendar>(calName);
        }

        public IJobDetail RetrieveJob(JobKey jobKey)
        {
            return this.Jobs
                .FindOneByIdAs<IJobDetail>(jobKey.ToBsonDocument());
        }

        public Spi.IOperableTrigger RetrieveTrigger(TriggerKey triggerKey)
        {
            return this.Triggers
                .FindOneByIdAs<Spi.IOperableTrigger>(triggerKey.ToBsonDocument());
        }

        public void SchedulerPaused()
        {
            ////throw new NotImplementedException();
        }

        public void SchedulerResumed()
        {
            ////throw new NotImplementedException();
        }

        public void SchedulerStarted()
        {
            ////throw new NotImplementedException();
        }

        public void Shutdown()
        {
            this._database.Server.Disconnect();
        }

        public void StoreCalendar(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers)
        {
            throw new NotImplementedException();
        }

        public void StoreJob(IJobDetail newJob, bool replaceExisting)
        {
            if (newJob == null)
                throw new ArgumentNullException("newJob");

            if (replaceExisting)
            {
                this.Jobs.Save(newJob.ToBsonDocument());
            }
            else
            {
                this.Jobs.Insert(newJob.ToBsonDocument());
            }
        }

        public void StoreJobAndTrigger(IJobDetail newJob, Spi.IOperableTrigger newTrigger)
        {
            if (newJob == null)
                throw new ArgumentNullException("newJob");

            if (newTrigger == null)
                throw new ArgumentNullException("newTrigger");

            this.Jobs.Save(newJob.ToBsonDocument());
            this.Triggers.Save(newTrigger.ToBsonDocument());
        }

        public void StoreJobsAndTriggers(IDictionary<IJobDetail, IList<ITrigger>> triggersAndJobs, bool replace)
        {
            foreach (IJobDetail job in triggersAndJobs.Keys)
            {
                this.StoreJob(job, replace);
                foreach (ITrigger trigger in triggersAndJobs[job])
                {
                    this.StoreTrigger((Spi.IOperableTrigger)trigger, replace);
                }
            }
        }

        public void StoreTrigger(Spi.IOperableTrigger newTrigger, bool replaceExisting)
        {
            if (newTrigger == null)
                throw new ArgumentNullException("newTrigger");

            if (replaceExisting)
            {
                this.Triggers.Save(newTrigger.ToBsonDocument());
            }
            else
            {
                this.Triggers.Insert(newTrigger.ToBsonDocument());
            }
        }

        public bool SupportsPersistence
        {
            get { return true; }
        }

        public int ThreadPoolSize
        {
            set { this._threadPoolSize = value; }
        }

        public void TriggeredJobComplete(Spi.IOperableTrigger trigger, IJobDetail jobDetail, SchedulerInstruction triggerInstCode)
        {
            switch (triggerInstCode)
            {
                case SchedulerInstruction.DeleteTrigger:
                    this.Triggers.Remove(
                        Query.EQ("_id", trigger.Key.ToBsonDocument()));
                    break;
                case SchedulerInstruction.ReExecuteJob:
                    // TODO ??
                    break;
                case SchedulerInstruction.SetAllJobTriggersComplete:
                    this.Triggers.Update(
                        Query.EQ("JobKey", jobDetail.Key.ToBsonDocument()),
                        Update.Set("State", TriggerState.Complete.ToString()));
                    break;
                case SchedulerInstruction.SetAllJobTriggersError:
                    this.Triggers.Update(
                        Query.EQ("JobKey", jobDetail.Key.ToBsonDocument()),
                        Update.Set("State", TriggerState.Error.ToString()));
                    break;
                case SchedulerInstruction.SetTriggerComplete:
                    this.Triggers.Update(
                        Query.EQ("_id", trigger.Key.ToBsonDocument()),
                        Update.Set("State", TriggerState.Complete.ToString()));
                    break;
                case SchedulerInstruction.SetTriggerError:
                    this.Triggers.Update(
                        Query.EQ("_id", trigger.Key.ToBsonDocument()),
                        Update.Set("State", TriggerState.Error.ToString()));
                    break;
                default: // SchedulerInstruction.NoInstruction
                    break;
            }

            _signaler.SignalSchedulingChange(new DateTimeOffset());
            /*this.TriggerLocks.Remove(
                Query.And(
                    Query.EQ("_id", trigger.Key.ToBsonDocument()),
                    Query.EQ("InstanceId", this._instanceId)));*/
            this.Triggers.Update(
                        Query.EQ("_id", trigger.Key.ToBsonDocument()),
                        Update.Set("FireInstanceId", null));
        }

        public IList<Spi.TriggerFiredResult> TriggersFired(IList<Spi.IOperableTrigger> triggers)
        {
            IList<Spi.TriggerFiredResult> firedTriggers = new List<Spi.TriggerFiredResult>();
            
            foreach (Spi.IOperableTrigger trigger in triggers)
            {
                trigger.Triggered(null);
                trigger.FireInstanceId = this._instanceId;
                this.StoreTrigger(trigger, true);
                DateTimeOffset? previousFireTimeUtc = trigger.GetPreviousFireTimeUtc();
                
                IJobDetail jobDetail = RetrieveJob(trigger.JobKey);
                Spi.TriggerFiredBundle bundle = new Spi.TriggerFiredBundle(jobDetail, trigger, null, false, DateTime.Now, trigger.GetPreviousFireTimeUtc(), previousFireTimeUtc, trigger.GetNextFireTimeUtc());
                Spi.TriggerFiredResult result = new Spi.TriggerFiredResult(bundle);
                firedTriggers.Add(result);
            }

            return firedTriggers;
        }

        public static string GetConnectionString(IDictionary config)
        {
#if !NET_1_1 && !NET_1_0
            //
            // First look for a connection string name that can be 
            // subsequently indexed into the <connectionStrings> section of 
            // the configuration to get the actual connection string.
            //

            var connectionStringName = (string)config["connectionStringName"] ?? string.Empty;

            if (connectionStringName.Length > 0)
            {
                var settings = ConfigurationManager.ConnectionStrings[connectionStringName];

                if (settings == null)
                    return string.Empty;

                return settings.ConnectionString ?? string.Empty;
            }
#endif

            //
            // Connection string name not found so see if a connection 
            // string was given directly.
            //

            var connectionString = (string)config["connectionString"] ?? string.Empty;

            if (connectionString.Length > 0)
                return connectionString;

            //
            // As a last resort, check for another setting called 
            // connectionStringAppKey. The specifies the key in 
            // <appSettings> that contains the actual connection string to 
            // be used.
            //

            var connectionStringAppKey = (string)config["connectionStringAppKey"] ?? string.Empty;

            return connectionStringAppKey.Length == 0 ? string.Empty : ConfigurationManager.AppSettings[connectionStringAppKey];
        }
    }
}
