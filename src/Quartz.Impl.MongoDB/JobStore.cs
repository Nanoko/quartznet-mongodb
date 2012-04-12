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

namespace Quartz.Impl.MongoDB
{
    public class JobStore : Quartz.Spi.IJobStore
    {
        private readonly string _connectionString;

        private MongoDatabase _database;
        private string _instanceId;
        private string _instanceName;
        private int _threadPoolSize;
        private Spi.ITypeLoadHelper _loadHelper;
        private Spi.ISchedulerSignaler _signaler;
		
        private static readonly object Sync = new object();

        /// <summary>
		/// Initializes a new instance of the <see cref="JobStore"/> class
		/// using a dictionary of configured settings.
		/// </summary>
		public JobStore()
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
                this.Triggers.EnsureIndex(IndexKeys.Ascending("JobKey"));
            }
		}

        static JobStore()
        {
            var myConventions = new ConventionProfile();
            myConventions.SetIdMemberConvention(new KeyOrIdConvention());
            BsonClassMap.RegisterConventions(
                myConventions,
                t => true
            );

            BsonSerializer.RegisterSerializer(
                typeof(JobKey),
                new JobKeySerializer()
            );

            BsonSerializer.RegisterSerializer(
                typeof(JobDetailImpl),
                new JobDetailImplSerializer()
            );

            BsonSerializer.RegisterSerializer(
                typeof(JobDataMap),
                new JobDataMapSerializer()
            );

            BsonSerializer.RegisterSerializer(
                typeof(TriggerKey),
                new TriggerKeySerializer()
            );
        }

        private MongoCollection Calendars { get { return this._database.GetCollection("Calendars"); } }
        private MongoCollection Jobs { get { return this._database.GetCollection("Jobs"); } }
        private MongoCollection Triggers { get { return this._database.GetCollection("Triggers"); } }

        public IList<Spi.IOperableTrigger> AcquireNextTriggers(DateTimeOffset noLaterThan, int maxCount, TimeSpan timeWindow)
        {
            throw new NotImplementedException();
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
            throw new NotImplementedException();
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
            throw new NotImplementedException();
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
            throw new NotImplementedException();
        }

        public bool IsTriggerGroupPaused(string groupName)
        {
            throw new NotImplementedException();
        }

        public void PauseAll()
        {
            throw new NotImplementedException();
        }

        public void PauseJob(JobKey jobKey)
        {
            throw new NotImplementedException();
        }

        public IList<string> PauseJobs(Matchers.GroupMatcher<JobKey> matcher)
        {
            throw new NotImplementedException();
        }

        public void PauseTrigger(TriggerKey triggerKey)
        {
            throw new NotImplementedException();
        }

        public Collection.ISet<string> PauseTriggers(Matchers.GroupMatcher<TriggerKey> matcher)
        {
            throw new NotImplementedException();
        }

        public void ReleaseAcquiredTrigger(Spi.IOperableTrigger trigger)
        {
            throw new NotImplementedException();
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
            throw new NotImplementedException();
        }

        public void ResumeJob(JobKey jobKey)
        {
            throw new NotImplementedException();
        }

        public Collection.ISet<string> ResumeJobs(Matchers.GroupMatcher<JobKey> matcher)
        {
            throw new NotImplementedException();
        }

        public void ResumeTrigger(TriggerKey triggerKey)
        {
            throw new NotImplementedException();
        }

        public IList<string> ResumeTriggers(Matchers.GroupMatcher<TriggerKey> matcher)
        {
            throw new NotImplementedException();
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
            throw new NotImplementedException();
        }

        public IList<Spi.TriggerFiredResult> TriggersFired(IList<Spi.IOperableTrigger> triggers)
        {
            throw new NotImplementedException();
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
