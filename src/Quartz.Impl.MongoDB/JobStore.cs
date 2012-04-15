#region License
/* 
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved. 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not 
 * use this file except in compliance with the License. You may obtain a copy 
 * of the License at 
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0 
 *   
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT 
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the 
 * License for the specific language governing permissions and limitations 
 * under the License.
 * 
 * Above license applies to original "RAMJobStore" code
 * MongoDB adaptation by Nanoko
 * 
 */
#endregion

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;

using Common.Logging;

using Quartz.Collection;
using Quartz.Impl;
using Quartz.Impl.Matchers;
using Quartz.Spi;
using System.Configuration;
using MongoDB.Driver;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Bson.Serialization;
using Quartz.Impl.Triggers;
using MongoDB.Bson;
using MongoDB.Driver.Builders;

namespace Quartz.Impl.MongoDB
{
    /// <summary>
    /// This class implements a <see cref="IJobStore" /> that
    /// utilizes MongoDB as its storage device.
    /// <para>
    /// This <see cref="IJobStore" /> is an effort to get a robust
    /// persistent job store. Any contribution is apprecied.
    /// </para>
    /// </summary>
    /// <author>James House</author>
    /// <author>Sharada Jambula</author>
    /// <author>Marko Lahma (.NET)</author>
    /// <author>Renaud Calmont (MongoDB)</author>
    public class JobStore : IJobStore
    {
        private readonly object lockObject = new object();
        private TimeSpan misfireThreshold = TimeSpan.FromSeconds(5);
        private ISchedulerSignaler signaler;

        private readonly ILog log;

        private MongoDatabase database;
        private string instanceId;

        private MongoCollection Calendars { get { return this.database.GetCollection("Calendars"); } }
        private MongoCollection Jobs { get { return this.database.GetCollection("Jobs"); } }
        private MongoCollection Triggers { get { return this.database.GetCollection("Triggers"); } }
        private MongoCollection TriggerStates { get { return this.database.GetCollection("TriggerStates"); } }
        private MongoCollection PausedTriggerGroups { get { return this.database.GetCollection("PausedTriggerGroups"); } }
        private MongoCollection PausedJobGroups { get { return this.database.GetCollection("PausedJobGroups"); } }
        private MongoCollection BlockedJobs { get { return this.database.GetCollection("BlockedJobs"); } }
        private MongoCollection Instances { get { return this.database.GetCollection("Instances"); } }

        /// <summary>
        /// Initializes a new instance of the <see cref="JobStore"/> class.
        /// </summary>
        public JobStore()
        {
            log = LogManager.GetLogger(GetType());

            string connectionString = ConfigurationManager.ConnectionStrings["quartznet-mongodb"].ConnectionString;

            //
            // If there is no connection string to use then throw an 
            // exception to abort construction.
            //

            if (connectionString.Length == 0)
                throw new ApplicationException("Connection string is missing for the MongoDB job store.");

            lock (lockObject)
            {
                this.database = MongoDatabase.Create(connectionString);

                this.Jobs.EnsureIndex(IndexKeys.Ascending("Group"));

                this.Triggers.EnsureIndex(IndexKeys.Ascending("Group"));
                this.Triggers.EnsureIndex(IndexKeys.Ascending("JobGroup"));
                this.Triggers.EnsureIndex(IndexKeys.Ascending("JobKey"));
            }
        }

        /// <summary>
        /// Initializes the <see cref="JobStore"/> class.
        /// </summary>
        static JobStore()
        {
            var myConventions = new ConventionProfile();
            myConventions.SetIdMemberConvention(new IdOrKeyOrNameConvention());
            BsonClassMap.RegisterConventions(
                myConventions,
                t => true
            );

            /*BsonSerializer.RegisterSerializer(
                typeof(DateTimeOffset),
                new DateTimeOffsetSerializer()
            );

            BsonSerializer.RegisterSerializer(
                typeof(DateTime),
                new DateTimeSerializer()
            );*/

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

        /// <summary> 
        /// The time span by which a trigger must have missed its
        /// next-fire-time, in order for it to be considered "misfired" and thus
        /// have its misfire instruction applied.
        /// </summary>
        [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
        public virtual TimeSpan MisfireThreshold
        {
            get { return misfireThreshold; }
            set
            {
                if (value.TotalMilliseconds < 1)
                {
                    throw new ArgumentException("Misfirethreashold must be larger than 0");
                }
                misfireThreshold = value;
            }
        }

        private static long ftrCtr = SystemTime.UtcNow().Ticks;

        /// <summary>
        /// Gets the fired trigger record id.
        /// </summary>
        /// <returns>The fired trigger record id.</returns>
        protected virtual string GetFiredTriggerRecordId()
        {
            long value = Interlocked.Increment(ref ftrCtr);
            return Convert.ToString(value, CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Called by the QuartzScheduler before the <see cref="IJobStore" /> is
        /// used, in order to give the it a chance to Initialize.
        /// </summary>
        public virtual void Initialize(ITypeLoadHelper loadHelper, ISchedulerSignaler s)
        {
            signaler = s;
            Log.Info("MongoDB JobStore initialized.");
        }

        /// <summary>
        /// Called by the QuartzScheduler to inform the <see cref="IJobStore" /> that
        /// the scheduler has started.
        /// </summary>
        public virtual void SchedulerStarted()
        {
            // nothing to do
        }

        /// <summary>
        /// Called by the QuartzScheduler to inform the JobStore that
        /// the scheduler has been paused.
        /// </summary>
        public void SchedulerPaused()
        {
            // nothing to do
        }

        /// <summary>
        /// Called by the QuartzScheduler to inform the JobStore that
        /// the scheduler has resumed after being paused.
        /// </summary>
        public void SchedulerResumed()
        {
            // nothing to do
        }

        /// <summary>
        /// Called by the QuartzScheduler to inform the <see cref="IJobStore" /> that
        /// it should free up all of it's resources because the scheduler is
        /// shutting down.
        /// </summary>
        public virtual void Shutdown()
        {
        }

        /// <summary>
        /// Returns whether this instance supports persistence.
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        public virtual bool SupportsPersistence
        {
            get { return true; }
        }


        /// <summary>
        /// Clears (deletes!) all scheduling data - all <see cref="IJob"/>s, <see cref="ITrigger" />s
        /// <see cref="ICalendar"/>s.
        /// </summary>
        public void ClearAllSchedulingData()
        {
            lock (lockObject)
            {
                // unschedule jobs (delete triggers)
                this.Triggers.RemoveAll();
                this.TriggerStates.RemoveAll();
                this.PausedTriggerGroups.RemoveAll();

                // delete jobs
                this.Jobs.RemoveAll();
                this.BlockedJobs.RemoveAll();
                this.PausedJobGroups.RemoveAll();

                // delete calendars
                this.Calendars.RemoveAll();
            }
        }


        protected ILog Log
        {
            get { return log; }
        }

        /// <summary>
        /// Store the given <see cref="IJobDetail" /> and <see cref="ITrigger" />.
        /// </summary>
        /// <param name="newJob">The <see cref="IJobDetail" /> to be stored.</param>
        /// <param name="newTrigger">The <see cref="ITrigger" /> to be stored.</param>
        public virtual void StoreJobAndTrigger(IJobDetail newJob, IOperableTrigger newTrigger)
        {
            StoreJob(newJob, false);
            StoreTrigger(newTrigger, false);
        }

        /// <summary>
        /// Returns true if the given job group is paused.
        /// </summary>
        /// <param name="groupName">Job group name</param>
        /// <returns></returns>
        public virtual bool IsJobGroupPaused(string groupName)
        {
            var result = this.PausedJobGroups.FindOneByIdAs<BsonDocument>(groupName);
            return !result.IsBsonNull;
        }

        /// <summary>
        /// returns true if the given TriggerGroup is paused.
        /// </summary>
        /// <param name="groupName"></param>
        /// <returns></returns>
        public virtual bool IsTriggerGroupPaused(string groupName)
        {
            var result = this.PausedTriggerGroups.FindOneByIdAs<BsonDocument>(groupName);
            return !result.IsBsonNull;
        }

        /// <summary>
        /// Store the given <see cref="IJob" />.
        /// </summary>
        /// <param name="newJob">The <see cref="IJob" /> to be stored.</param>
        /// <param name="replaceExisting">If <see langword="true" />, any <see cref="IJob" /> existing in the
        /// <see cref="IJobStore" /> with the same name and group should be
        /// over-written.</param>
        public virtual void StoreJob(IJobDetail newJob, bool replaceExisting)
        {
            bool repl = false;

            lock (lockObject)
            {

                if (this.CheckExists(newJob.Key))
                {
                    if (!replaceExisting)
                    {
                        throw new ObjectAlreadyExistsException(newJob);
                    }

                    repl = true;
                }

                if (!repl)
                {
                    // try insert new
                    this.Jobs.Insert(newJob.ToBsonDocument());
                }
                else
                {
                    // force upsert
                    this.Jobs.Save(newJob.ToBsonDocument());
                }
            }
        }

        /// <summary>
        /// Remove (delete) the <see cref="IJob" /> with the given
        /// name, and any <see cref="ITrigger" /> s that reference
        /// it.
        /// </summary>
        /// <returns>
        /// 	<see langword="true" /> if a <see cref="IJob" /> with the given name and
        /// group was found and removed from the store.
        /// </returns>
        public virtual bool RemoveJob(JobKey jobKey)
        {
            bool found;

            lock (lockObject)
            {
                // keep separated to clean up any staled trigger
                IList<IOperableTrigger> triggersForJob = this.GetTriggersForJob(jobKey);
                foreach (IOperableTrigger trigger in triggersForJob)
                {
                    this.RemoveTrigger(trigger.Key);
                }

                found = this.CheckExists(jobKey);

                if (found)
                {
                    this.Jobs.Remove(
                    Query.EQ("_id", jobKey.ToBsonDocument()));

                    this.BlockedJobs.Remove(
                        Query.EQ("_id", jobKey.ToBsonDocument()));

                    var others = this.Jobs.FindAs<BsonDocument>(
                        Query.EQ("Group", jobKey.Group));

                    if (others.Count() == 0)
                    {
                        this.PausedJobGroups.Remove(
                            Query.EQ("_id", jobKey.Group));
                    }
                }
            }

            return found;
        }

        public bool RemoveJobs(IList<JobKey> jobKeys)
        {
            bool allFound = true;

            lock (lockObject)
            {
                foreach (JobKey key in jobKeys)
                {
                    allFound = RemoveJob(key) && allFound;
                }
            }

            return allFound;
        }

        public bool RemoveTriggers(IList<TriggerKey> triggerKeys)
        {
            bool allFound = true;

            lock (lockObject)
            {
                foreach (TriggerKey key in triggerKeys)
                {
                    allFound = RemoveTrigger(key) && allFound;
                }
            }

            return allFound;
        }

        public void StoreJobsAndTriggers(IDictionary<IJobDetail, IList<ITrigger>> triggersAndJobs, bool replace)
        {
            lock (lockObject)
            {
                // make sure there are no collisions...
                if (!replace)
                {
                    foreach (IJobDetail job in triggersAndJobs.Keys)
                    {
                        if (CheckExists(job.Key))
                        {
                            throw new ObjectAlreadyExistsException(job);
                        }
                        foreach (ITrigger trigger in triggersAndJobs[job])
                        {
                            if (CheckExists(trigger.Key))
                            {
                                throw new ObjectAlreadyExistsException(trigger);
                            }
                        }
                    }
                }
                // do bulk add...
                foreach (IJobDetail job in triggersAndJobs.Keys)
                {
                    StoreJob(job, true);
                    foreach (ITrigger trigger in triggersAndJobs[job])
                    {
                        StoreTrigger((IOperableTrigger)trigger, true);
                    }
                }
            }
        }

        /// <summary>
        /// Remove (delete) the <see cref="ITrigger" /> with the
        /// given name.
        /// </summary>
        /// <returns>
        /// 	<see langword="true" /> if a <see cref="ITrigger" /> with the given
        /// name and group was found and removed from the store.
        /// </returns>
        public virtual bool RemoveTrigger(TriggerKey triggerKey)
        {
            return RemoveTrigger(triggerKey, true);
        }

        /// <summary>
        /// Store the given <see cref="ITrigger" />.
        /// </summary>
        /// <param name="newTrigger">The <see cref="ITrigger" /> to be stored.</param>
        /// <param name="replaceExisting">If <see langword="true" />, any <see cref="ITrigger" /> existing in
        /// the <see cref="IJobStore" /> with the same name and group should
        /// be over-written.</param>
        public virtual void StoreTrigger(IOperableTrigger newTrigger, bool replaceExisting)
        {
            lock (lockObject)
            {
                if (this.CheckExists(newTrigger.Key))
                {
                    if (!replaceExisting)
                    {
                        throw new ObjectAlreadyExistsException(newTrigger);
                    }

                    // don't delete orphaned job, this trigger has the job anyways
                    this.RemoveTrigger(newTrigger.Key, false);
                }

                if (this.RetrieveJob(newTrigger.JobKey) == null)
                {
                    throw new JobPersistenceException("The job (" + newTrigger.JobKey +
                                                      ") referenced by the trigger does not exist.");
                }

                this.Triggers.Save(newTrigger);

                BsonDocument triggerState = new BsonDocument();
                triggerState.Add("_id", newTrigger.Key.ToBsonDocument());
                triggerState.Add("State", "Waiting");

                if (this.PausedTriggerGroups.FindOneByIdAs<BsonDocument>(newTrigger.Key.Group) != null
                    || this.PausedJobGroups.FindOneByIdAs<BsonDocument>(newTrigger.JobKey.Group) != null)
                {
                    triggerState["State"] = "Paused";
                    if (this.BlockedJobs.FindOneByIdAs<BsonDocument>(newTrigger.JobKey.ToBsonDocument()) != null)
                    {
                        triggerState["State"] = "PausedAndBlocked";
                    }
                }
                else if (this.BlockedJobs.FindOneByIdAs<BsonDocument>(newTrigger.JobKey.ToBsonDocument()) != null)
                {
                    triggerState["State"] = "Blocked";
                }

                this.TriggerStates.Save(triggerState);
            }
        }

        /// <summary>
        /// Remove (delete) the <see cref="ITrigger" /> with the
        /// given name.
        /// 
        /// </summary>
        /// <returns>
        /// 	<see langword="true" /> if a <see cref="ITrigger" /> with the given
        /// name and group was found and removed from the store.
        /// </returns>
        /// <param name="key">The <see cref="ITrigger" /> to be removed.</param>
        /// <param name="removeOrphanedJob">Whether to delete orpahaned job details from scheduler if job becomes orphaned from removing the trigger.</param>
        public virtual bool RemoveTrigger(TriggerKey key, bool removeOrphanedJob)
        {
            bool found;
            lock (lockObject)
            {
                var trigger = this.RetrieveTrigger(key);
                found = trigger != null;

                if (found)
                {
                    this.Triggers.Remove(
                        Query.EQ("_id", trigger.Key.ToBsonDocument()));
                    this.TriggerStates.Remove(
                        Query.EQ("_id", trigger.Key.ToBsonDocument()));

                    if (removeOrphanedJob)
                    {
                        IJobDetail jobDetail = this.RetrieveJob(trigger.JobKey);
                        IList<IOperableTrigger> trigs = this.GetTriggersForJob(jobDetail.Key);
                        if ((trigs == null
                                || trigs.Count == 0)
                            && !jobDetail.Durable)
                        {
                            if (this.RemoveJob(jobDetail.Key))
                            {
                                signaler.NotifySchedulerListenersJobDeleted(jobDetail.Key);
                            }
                        }
                    }
                }
            }

            return found;
        }


        /// <summary>
        /// Replaces the trigger.
        /// </summary>
        /// <param name="triggerKey">The <see cref="TriggerKey"/> of the <see cref="ITrigger" /> to be replaced.</param>
        /// <param name="newTrigger">The new trigger.</param>
        /// <returns></returns>
        public virtual bool ReplaceTrigger(TriggerKey triggerKey, IOperableTrigger newTrigger)
        {
            bool found;

            lock (lockObject)
            {
                IOperableTrigger oldTrigger = this.Triggers.FindOneByIdAs<IOperableTrigger>(triggerKey.ToBsonDocument());
                found = oldTrigger != null;

                if (found)
                {
                    if (!oldTrigger.JobKey.Equals(newTrigger.JobKey))
                    {
                        throw new JobPersistenceException("New trigger is not related to the same job as the old trigger.");
                    }

                    this.RemoveTrigger(triggerKey);

                    try
                    {
                        this.StoreTrigger(newTrigger, false);
                    }
                    catch (JobPersistenceException)
                    {
                        this.StoreTrigger(oldTrigger, false); // put previous trigger back...
                        throw;
                    }
                }
            }

            return found;
        }

        /// <summary>
        /// Retrieve the <see cref="IJobDetail" /> for the given
        /// <see cref="IJob" />.
        /// </summary>
        /// <returns>
        /// The desired <see cref="IJob" />, or null if there is no match.
        /// </returns>
        public virtual IJobDetail RetrieveJob(JobKey jobKey)
        {
            lock (lockObject)
            {
                return this.Jobs
                    .FindOneByIdAs<IJobDetail>(jobKey.ToBsonDocument());
            }
        }

        /// <summary>
        /// Retrieve the given <see cref="ITrigger" />.
        /// </summary>
        /// <returns>
        /// The desired <see cref="ITrigger" />, or null if there is no match.
        /// </returns>
        public virtual IOperableTrigger RetrieveTrigger(TriggerKey triggerKey)
        {
            lock (lockObject)
            {
                return this.Triggers
                    .FindOneByIdAs<Spi.IOperableTrigger>(triggerKey.ToBsonDocument());
            }
        }

        /// <summary>
        /// Determine whether a <see cref="IJob"/> with the given identifier already 
        /// exists within the scheduler.
        /// </summary>
        /// <param name="jobKey">the identifier to check for</param>
        /// <returns>true if a Job exists with the given identifier</returns>
        public bool CheckExists(JobKey jobKey)
        {
            lock (lockObject)
            {
                return this.Jobs.FindOneByIdAs<BsonDocument>(jobKey.ToBsonDocument()) != null;
            }
        }

        /// <summary>
        /// Determine whether a <see cref="ITrigger" /> with the given identifier already 
        /// exists within the scheduler.
        /// </summary>
        /// <param name="triggerKey">triggerKey the identifier to check for</param>
        /// <returns>true if a Trigger exists with the given identifier</returns>
        public bool CheckExists(TriggerKey triggerKey)
        {
            lock (lockObject)
            {
                return this.Triggers.FindOneByIdAs<BsonDocument>(triggerKey.ToBsonDocument()) != null;
            }
        }

        /// <summary>
        /// Get the current state of the identified <see cref="ITrigger" />.
        /// </summary>
        /// <seealso cref="TriggerState.Normal" />
        /// <seealso cref="TriggerState.Paused" />
        /// <seealso cref="TriggerState.Complete" />
        /// <seealso cref="TriggerState.Error" />
        /// <seealso cref="TriggerState.Blocked" />
        /// <seealso cref="TriggerState.None"/>
        public virtual TriggerState GetTriggerState(TriggerKey triggerKey)
        {
            lock (lockObject)
            {
                BsonDocument triggerState = this.TriggerStates.FindOneByIdAs<BsonDocument>(triggerKey.ToBsonDocument());

                if (triggerState.IsBsonNull)
                {
                    return TriggerState.None;
                }
                if (triggerState["State"] == "Complete")
                {
                    return TriggerState.Complete;
                }
                if (triggerState["State"] == "Paused")
                {
                    return TriggerState.Paused;
                }
                if (triggerState["State"] == "PausedAndBlocked")
                {
                    return TriggerState.Paused;
                }
                if (triggerState["State"] == "Blocked")
                {
                    return TriggerState.Blocked;
                }
                if (triggerState["State"] == "Error")
                {
                    return TriggerState.Error;
                }

                return TriggerState.Normal;
            }
        }

        /// <summary>
        /// Store the given <see cref="ICalendar" />.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="calendar">The <see cref="ICalendar" /> to be stored.</param>
        /// <param name="replaceExisting">If <see langword="true" />, any <see cref="ICalendar" /> existing
        /// in the <see cref="IJobStore" /> with the same name and group
        /// should be over-written.</param>
        /// <param name="updateTriggers">If <see langword="true" />, any <see cref="ITrigger" />s existing
        /// in the <see cref="IJobStore" /> that reference an existing
        /// Calendar with the same name with have their next fire time
        /// re-computed with the new <see cref="ICalendar" />.</param>
        public virtual void StoreCalendar(string name, ICalendar calendar, bool replaceExisting,
                                          bool updateTriggers)
        {
            calendar = (ICalendar)calendar.Clone();

            lock (lockObject)
            {
                if (this.Calendars.FindOneByIdAs<BsonDocument>(name) != null
                    && replaceExisting == false)
                {
                    throw new ObjectAlreadyExistsException(string.Format(CultureInfo.InvariantCulture, "Calendar with name '{0}' already exists.", name));
                }

                this.Calendars.Save(calendar);

                if (updateTriggers)
                {
                    var triggers = this.Triggers.FindAs<IOperableTrigger>(Query.EQ("CalendarName", name));
                    foreach (IOperableTrigger trigger in triggers)
                    {
                        trigger.UpdateWithNewCalendar(calendar, MisfireThreshold);
                        this.Triggers.Save(trigger);
                    }
                }
            }
        }

        /// <summary>
        /// Remove (delete) the <see cref="ICalendar" /> with the
        /// given name.
        /// <para>
        /// If removal of the <see cref="ICalendar" /> would result in
        /// <see cref="ITrigger" />s pointing to non-existent calendars, then a
        /// <see cref="JobPersistenceException" /> will be thrown.</para>
        /// </summary>
        /// <param name="calName">The name of the <see cref="ICalendar" /> to be removed.</param>
        /// <returns>
        /// 	<see langword="true" /> if a <see cref="ICalendar" /> with the given name
        /// was found and removed from the store.
        /// </returns>
        public virtual bool RemoveCalendar(string calName)
        {
            if (this.Triggers.FindAs<BsonDocument>(Query.EQ("CalendarName", calName)) != null)
            {
                throw new JobPersistenceException("Calender cannot be removed if it is referenced by a Trigger!");
            }

            this.Calendars.Remove(
                Query.EQ("_id", calName));

            return true;
        }

        /// <summary>
        /// Retrieve the given <see cref="ITrigger" />.
        /// </summary>
        /// <param name="calName">The name of the <see cref="ICalendar" /> to be retrieved.</param>
        /// <returns>
        /// The desired <see cref="ICalendar" />, or null if there is no match.
        /// </returns>
        public virtual ICalendar RetrieveCalendar(string calName)
        {
            lock (lockObject)
            {
                return this.Calendars
                    .FindOneByIdAs<ICalendar>(calName);
            }
        }

        /// <summary>
        /// Get the number of <see cref="IJobDetail" /> s that are
        /// stored in the <see cref="IJobStore" />.
        /// </summary>
        public virtual int GetNumberOfJobs()
        {
            lock (lockObject)
            {
                return (int)this.Jobs.Count();
            }
        }

        /// <summary>
        /// Get the number of <see cref="ITrigger" /> s that are
        /// stored in the <see cref="IJobStore" />.
        /// </summary>
        public virtual int GetNumberOfTriggers()
        {
            lock (lockObject)
            {
                return (int)this.Triggers.Count();
            }
        }

        /// <summary>
        /// Get the number of <see cref="ICalendar" /> s that are
        /// stored in the <see cref="IJobStore" />.
        /// </summary>
        public virtual int GetNumberOfCalendars()
        {
            lock (lockObject)
            {
                return (int)this.Calendars.Count();
            }
        }

        /// <summary>
        /// Get the names of all of the <see cref="IJob" /> s that
        /// match the given group matcher.
        /// </summary>
        public virtual Collection.ISet<JobKey> GetJobKeys(GroupMatcher<JobKey> matcher)
        {
            lock (lockObject)
            {
                var result = this.Jobs
                    .FindAs<IJobDetail>(
                        Query.EQ("Group", matcher.CompareToValue))
                    .Select(j => j.Key);

                return new Collection.HashSet<JobKey>(result);
            }
        }

        /// <summary>
        /// Get the names of all of the <see cref="ICalendar" /> s
        /// in the <see cref="IJobStore" />.
        /// <para>
        /// If there are no ICalendars in the given group name, the result should be
        /// a zero-length array (not <see langword="null" />).
        /// </para>
        /// </summary>
        public virtual IList<string> GetCalendarNames()
        {
            lock (lockObject)
            {
                return this.Calendars
                    .Distinct("Name")
                    .Select(g => g.AsString)
                    .ToList();
            }
        }

        /// <summary>
        /// Get the names of all of the <see cref="ITrigger" /> s
        /// that have the given group name.
        /// </summary>
        public virtual Collection.ISet<TriggerKey> GetTriggerKeys(GroupMatcher<TriggerKey> matcher)
        {
            lock (lockObject)
            {
                var result = this.Triggers
                    .FindAs<Spi.IOperableTrigger>(
                        Query.EQ("Group", matcher.CompareToValue))
                    .Select(t => t.Key);

                return new Collection.HashSet<TriggerKey>(result);
            }
        }

        /// <summary>
        /// Get the names of all of the <see cref="IJob" />
        /// groups.
        /// </summary>
        public virtual IList<string> GetJobGroupNames()
        {
            lock (lockObject)
            {
                return this.Jobs
                    .Distinct("Group")
                    .Select(g => g.AsString)
                    .ToList();
            }
        }

        /// <summary>
        /// Get the names of all of the <see cref="ITrigger" /> groups.
        /// </summary>
        public virtual IList<string> GetTriggerGroupNames()
        {
            lock (lockObject)
            {
                return this.Triggers
                    .Distinct("Group")
                    .Select(g => g.AsString)
                    .ToList();
            }
        }

        /// <summary>
        /// Get all of the Triggers that are associated to the given Job.
        /// <para>
        /// If there are no matches, a zero-length array should be returned.
        /// </para>
        /// </summary>
        public virtual IList<IOperableTrigger> GetTriggersForJob(JobKey jobKey)
        {
            lock (lockObject)
            {
                return this.Triggers
                    .FindAs<Spi.IOperableTrigger>(
                        Query.EQ("JobKey", jobKey.ToBsonDocument()))
                    .ToList();
            }
        }

        /// <summary> 
        /// Pause the <see cref="ITrigger" /> with the given name.
        /// </summary>
        public virtual void PauseTrigger(TriggerKey triggerKey)
        {
            lock (lockObject)
            {
                this.TriggerStates.Update(
                    Query.And(
                        Query.EQ("_id", triggerKey.ToBsonDocument()),
                        Query.EQ("State", "Blocked")),
                    Update.Set("State", "PausedAndBlocked"));

                this.TriggerStates.Update(
                    Query.And(
                        Query.EQ("_id", triggerKey.ToBsonDocument()),
                        Query.NE("State", "Blocked")),
                    Update.Set("State", "Paused"));
            }
        }

        /// <summary>
        /// Pause all of the <see cref="ITrigger" />s in the given group.
        /// <para>
        /// The JobStore should "remember" that the group is paused, and impose the
        /// pause on any new triggers that are added to the group while the group is
        /// paused.
        /// </para>
        /// </summary>
        public virtual Collection.ISet<string> PauseTriggers(GroupMatcher<TriggerKey> matcher)
        {
            IList<string> pausedGroups;

            lock (lockObject)
            {
                pausedGroups = new List<string>();

                StringOperator op = matcher.CompareWithOperator;
                /*if (op == StringOperator.Equality)
                {
                    if (pausedTriggerGroups.Add(matcher.CompareToValue))
                    {
                        pausedGroups.Add(matcher.CompareToValue);
                    }
                }
                else
                {
                    foreach (string group in triggersByGroup.Keys)
                    {
                        if (op.Evaluate(group, matcher.CompareToValue))
                        {
                            if (pausedTriggerGroups.Add(matcher.CompareToValue))
                            {
                                pausedGroups.Add(group);
                            }
                        }
                    }
                }*/
                throw new NotImplementedException();

                foreach (string pausedGroup in pausedGroups)
                {
                    Collection.ISet<TriggerKey> keys = GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(pausedGroup));

                    foreach (TriggerKey key in keys)
                    {
                        PauseTrigger(key);
                    }
                }
            }
            return new Collection.HashSet<string>(pausedGroups);
        }

        /// <summary> 
        /// Pause the <see cref="IJobDetail" /> with the given
        /// name - by pausing all of its current <see cref="ITrigger" />s.
        /// </summary>
        public virtual void PauseJob(JobKey jobKey)
        {
            lock (lockObject)
            {
                IList<IOperableTrigger> triggersForJob = this.GetTriggersForJob(jobKey);
                foreach (IOperableTrigger trigger in triggersForJob)
                {
                    this.PauseTrigger(trigger.Key);
                }
            }
        }

        /// <summary>
        /// Pause all of the <see cref="IJobDetail" />s in the
        /// given group - by pausing all of their <see cref="ITrigger" />s.
        /// <para>
        /// The JobStore should "remember" that the group is paused, and impose the
        /// pause on any new jobs that are added to the group while the group is
        /// paused.
        /// </para>
        /// </summary>
        public virtual IList<string> PauseJobs(GroupMatcher<JobKey> matcher)
        {
            List<string> pausedGroups = new List<String>();
            lock (lockObject)
            {
                StringOperator op = matcher.CompareWithOperator;
                /*if (op == StringOperator.Equality)
                {
                    if (pausedJobGroups.Add(matcher.CompareToValue))
                    {
                        pausedGroups.Add(matcher.CompareToValue);
                    }
                }
                else
                {
                    foreach (String group in jobsByGroup.Keys)
                    {
                        if (op.Evaluate(group, matcher.CompareToValue))
                        {
                            if (pausedJobGroups.Add(group))
                            {
                                pausedGroups.Add(group);
                            }
                        }
                    }
                }*/
                throw new NotImplementedException();

                foreach (string groupName in pausedGroups)
                {
                    foreach (JobKey jobKey in GetJobKeys(GroupMatcher<JobKey>.GroupEquals(groupName)))
                    {
                        IList<IOperableTrigger> triggers = GetTriggersForJob(jobKey);
                        foreach (IOperableTrigger trigger in triggers)
                        {
                            PauseTrigger(trigger.Key);
                        }
                    }
                }
            }
            return pausedGroups;
        }

        /// <summary>
        /// Resume (un-pause) the <see cref="ITrigger" /> with the given key.
        /// </summary>
        /// <remarks>
        /// If the <see cref="ITrigger" /> missed one or more fire-times, then the
        /// <see cref="ITrigger" />'s misfire instruction will be applied.
        /// </remarks>
        public virtual void ResumeTrigger(TriggerKey triggerKey)
        {
            lock (lockObject)
            {
                IOperableTrigger trigger = this.Triggers.FindOneByIdAs<IOperableTrigger>(triggerKey.ToBsonDocument());

                // does the trigger exist?
                if (trigger == null)
                {
                    return;
                }

                BsonDocument triggerState = this.TriggerStates.FindOneByIdAs<BsonDocument>(triggerKey.ToBsonDocument());
                // if the trigger is not paused resuming it does not make sense...
                if (triggerState["State"] != "Paused" &&
                    triggerState["State"] != "PausedAndBlocked")
                {
                    return;
                }

                if (this.BlockedJobs.FindOneByIdAs<BsonDocument>(trigger.JobKey.ToBsonDocument()) != null)
                {
                    triggerState["State"] = "Blocked";
                }
                else
                {
                    triggerState["State"] = "Waiting";
                }

                this.ApplyMisfire(trigger);

                this.TriggerStates.Save(triggerState);
            }
        }

        /// <summary>
        /// Resume (un-pause) all of the <see cref="ITrigger" />s in the
        /// given group.
        /// <para>
        /// If any <see cref="ITrigger" /> missed one or more fire-times, then the
        /// <see cref="ITrigger" />'s misfire instruction will be applied.
        /// </para>
        /// </summary>
        public virtual IList<string> ResumeTriggers(GroupMatcher<TriggerKey> matcher)
        {
            Collection.ISet<string> groups = new Collection.HashSet<string>();
            lock (lockObject)
            {
                Collection.ISet<TriggerKey> keys = GetTriggerKeys(matcher);

                foreach (TriggerKey triggerKey in keys)
                {
                    groups.Add(triggerKey.Group);
                    /*TriggerWrapper tw;
                    if (triggersByKey.TryGetValue(triggerKey, out tw))
                    {
                        String jobGroup = tw.jobKey.Group;
                        if (pausedJobGroups.Contains(jobGroup))
                        {
                            continue;
                        }
                    }*/
                    throw new NotImplementedException();
                    ResumeTrigger(triggerKey);
                }
                foreach (String group in groups)
                {
                    this.PausedTriggerGroups.Remove(
                        Query.EQ("_id", group));
                }
            }

            return new List<string>(groups);
        }

        /// <summary>
        /// Resume (un-pause) the <see cref="IJobDetail" /> with
        /// the given name.
        /// <para>
        /// If any of the <see cref="IJob" />'s<see cref="ITrigger" /> s missed one
        /// or more fire-times, then the <see cref="ITrigger" />'s misfire
        /// instruction will be applied.
        /// </para>
        /// </summary>
        public virtual void ResumeJob(JobKey jobKey)
        {
            lock (lockObject)
            {
                IList<IOperableTrigger> triggersForJob = GetTriggersForJob(jobKey);
                foreach (IOperableTrigger trigger in triggersForJob)
                {
                    this.ResumeTrigger(trigger.Key);
                }
            }
        }

        /// <summary>
        /// Resume (un-pause) all of the <see cref="IJobDetail" />s
        /// in the given group.
        /// <para>
        /// If any of the <see cref="IJob" /> s had <see cref="ITrigger" /> s that
        /// missed one or more fire-times, then the <see cref="ITrigger" />'s
        /// misfire instruction will be applied.
        /// </para>
        /// </summary>
        public virtual Collection.ISet<string> ResumeJobs(GroupMatcher<JobKey> matcher)
        {
            Collection.ISet<string> resumedGroups = new Collection.HashSet<string>();
            lock (lockObject)
            {
                Collection.ISet<JobKey> keys = GetJobKeys(matcher);

                foreach (string pausedJobGroup in this.PausedJobGroups.FindAllAs<string>())
                {
                    if (matcher.CompareWithOperator.Evaluate(pausedJobGroup, matcher.CompareToValue))
                    {
                        resumedGroups.Add(pausedJobGroup);
                    }
                }

                foreach (String resumedGroup in resumedGroups)
                {
                    this.PausedTriggerGroups.Remove(
                        Query.All("_id", resumedGroup));
                }

                foreach (JobKey key in keys)
                {
                    IList<IOperableTrigger> triggers = GetTriggersForJob(key);
                    foreach (IOperableTrigger trigger in triggers)
                    {
                        ResumeTrigger(trigger.Key);
                    }
                }
            }

            return resumedGroups;
        }

        /// <summary>
        /// Pause all triggers - equivalent of calling <see cref="PauseTriggers" />
        /// on every group.
        /// <para>
        /// When <see cref="ResumeAll" /> is called (to un-pause), trigger misfire
        /// instructions WILL be applied.
        /// </para>
        /// </summary>
        /// <seealso cref="ResumeAll()" /> 
        public virtual void PauseAll()
        {
            lock (lockObject)
            {
                IList<string> triggerGroupNames = GetTriggerGroupNames();

                foreach (string groupName in triggerGroupNames)
                {
                    this.PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName));
                }
            }
        }

        /// <summary>
        /// Resume (un-pause) all triggers - equivalent of calling <see cref="ResumeTriggers" />
        /// on every trigger group and setting all job groups unpaused />.
        /// <para>
        /// If any <see cref="ITrigger" /> missed one or more fire-times, then the
        /// <see cref="ITrigger" />'s misfire instruction will be applied.
        /// </para>
        /// </summary>
        /// <seealso cref="PauseAll()" />
        public virtual void ResumeAll()
        {
            lock (lockObject)
            {
                // TODO need a match all here!
                this.PausedJobGroups.RemoveAll();
                IList<string> triggerGroupNames = this.GetTriggerGroupNames();

                foreach (string groupName in triggerGroupNames)
                {
                    this.ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName));
                }
            }
        }

        /// <summary>
        /// Applies the misfire.
        /// </summary>
        /// <param name="tw">The trigger wrapper.</param>
        /// <returns></returns>
        protected virtual bool ApplyMisfire(IOperableTrigger trigger)
        {
            DateTimeOffset misfireTime = SystemTime.UtcNow();
            if (MisfireThreshold > TimeSpan.Zero)
            {
                misfireTime = misfireTime.AddMilliseconds(-1 * MisfireThreshold.TotalMilliseconds);
            }

            DateTimeOffset? tnft = trigger.GetNextFireTimeUtc();
            if (!tnft.HasValue || tnft.Value > misfireTime
                || trigger.MisfireInstruction == MisfireInstruction.IgnoreMisfirePolicy)
            {
                return false;
            }

            ICalendar cal = null;
            if (trigger.CalendarName != null)
            {
                cal = this.RetrieveCalendar(trigger.CalendarName);
            }

            signaler.NotifyTriggerListenersMisfired((IOperableTrigger)trigger.Clone());

            trigger.UpdateAfterMisfire(cal);
            this.Triggers.Save(trigger);

            if (!trigger.GetNextFireTimeUtc().HasValue)
            {
                this.TriggerStates.Update(
                    Query.EQ("_id", trigger.Key.ToBsonDocument()),
                    Update.Set("State", "Complete"));

                signaler.NotifySchedulerListenersFinalized(trigger);
            }
            else if (tnft.Equals(trigger.GetNextFireTimeUtc()))
            {
                return false;
            }

            return true;
        }

        /// <summary>
        /// Get a handle to the next trigger to be fired, and mark it as 'reserved'
        /// by the calling scheduler.
        /// </summary>
        /// <seealso cref="ITrigger" />
        public virtual IList<IOperableTrigger> AcquireNextTriggers(DateTimeOffset noLaterThan, int maxCount, TimeSpan timeWindow)
        {
            lock (lockObject)
            {
                // multiple instances management
                this.Instances.Save(new BsonDocument(
                    new BsonElement("_id", this.instanceId),
                    new BsonElement("Expires", (SystemTime.UtcNow() + new TimeSpan(0, 10, 0)).Ticks)));

                this.Instances.Remove(
                    Query.LT("Expires", SystemTime.UtcNow().Ticks));

                IEnumerable<BsonValue> activeInstances = this.Instances.Distinct("_id");

                this.TriggerStates.Update(
                    Query.NotIn("InstanceId", activeInstances),
                    Update.Unset("InstanceId")
                        .Set("State", "Waiting"));

                List<IOperableTrigger> result = new List<IOperableTrigger>();
                Collection.ISet<JobKey> acquiredJobKeysForNoConcurrentExec = new Collection.HashSet<JobKey>();
                DateTimeOffset? firstAcquiredTriggerFireTime = null;

                this.TriggerStates.Update(
                    Query.EQ("State", "Waiting"),
                    Update.Set("InstanceId", this.instanceId)
                        .Set("State", "Locked"));

                var lockedTriggerKeys = this.TriggerStates.Distinct("_id",
                    Query.And(
                        Query.EQ("InstanceId", this.instanceId),
                        Query.EQ("State", "Locked")));

                var lockedTriggers = this.Triggers.FindAs<Spi.IOperableTrigger>(
                    Query.In("_id", lockedTriggerKeys));
                
                foreach (IOperableTrigger trigger in lockedTriggers)
                {
                    if (trigger.GetNextFireTimeUtc() == null)
                    {
                        continue;
                    }

                    // it's possible that we've selected triggers way outside of the max fire ahead time for batches 
                    // (up to idleWaitTime + fireAheadTime) so we need to make sure not to include such triggers.  
                    // So we select from the first next trigger to fire up until the max fire ahead time after that...
                    // which will perfectly honor the fireAheadTime window because the no firing will occur until
                    // the first acquired trigger's fire time arrives.
                    if (firstAcquiredTriggerFireTime != null && trigger.GetNextFireTimeUtc() > (firstAcquiredTriggerFireTime.Value + timeWindow))
                    {
                        break;
                    }

                    if (this.ApplyMisfire(trigger))
                    {
                        continue;
                    }

                    if (trigger.GetNextFireTimeUtc() > noLaterThan + timeWindow)
                    {
                        break;
                    }

                    // If trigger's job is set as @DisallowConcurrentExecution, and it has already been added to result, then
                    // put it back into the timeTriggers set and continue to search for next trigger.
                    JobKey jobKey = trigger.JobKey;
                    IJobDetail job = this.Jobs.FindOneByIdAs<IJobDetail>(jobKey.ToBsonDocument());
                    if (job.ConcurrentExectionDisallowed)
                    {
                        if (acquiredJobKeysForNoConcurrentExec.Contains(jobKey))
                        {
                            continue; // go to next trigger in store.
                        }
                        else
                        {
                            acquiredJobKeysForNoConcurrentExec.Add(jobKey);
                        }
                    }

                    this.TriggerStates.Update(
                        Query.EQ("_id", trigger.Key.ToBsonDocument()),
                        Update.Set("State", "Acquired")
                            .Set("InstanceId", this.instanceId));

                    trigger.FireInstanceId = GetFiredTriggerRecordId();
                    this.Triggers.Save(trigger);
                    result.Add(trigger);

                    if (firstAcquiredTriggerFireTime == null)
                    {
                        firstAcquiredTriggerFireTime = trigger.GetNextFireTimeUtc();
                    }

                    if (result.Count == maxCount)
                    {
                        break;
                    }
                }

                this.TriggerStates.Update(
                    Query.And(
                        Query.EQ("InstanceId", this.instanceId),
                        Query.EQ("State", "Locked")),
                    Update.Unset("InstanceId")
                        .Set("State", "Waiting"));

                return result;
            }
        }

        /// <summary>
        /// Inform the <see cref="IJobStore" /> that the scheduler no longer plans to
        /// fire the given <see cref="ITrigger" />, that it had previously acquired
        /// (reserved).
        /// </summary>
        public virtual void ReleaseAcquiredTrigger(IOperableTrigger trigger)
        {
            lock (lockObject)
            {
                /*TriggerWrapper tw;
                if (triggersByKey.TryGetValue(trigger.Key, out tw) && tw.state == InternalTriggerState.Acquired)
                {
                    tw.state = InternalTriggerState.Waiting;
                    timeTriggers.Add(tw);
                }*/
                throw new NotImplementedException();
            }
        }

        /// <summary>
        /// Inform the <see cref="IJobStore" /> that the scheduler is now firing the
        /// given <see cref="ITrigger" /> (executing its associated <see cref="IJob" />),
        /// that it had previously acquired (reserved).
        /// </summary>
        public virtual IList<TriggerFiredResult> TriggersFired(IList<IOperableTrigger> triggers)
        {
            lock (lockObject)
            {
                List<TriggerFiredResult> results = new List<TriggerFiredResult>();

                foreach (IOperableTrigger trigger in triggers)
                {
                    // was the trigger deleted since being acquired?
                    if (this.Triggers.FindOneByIdAs<BsonDocument>(trigger.Key.ToBsonDocument()) == null)
                    {
                        continue;
                    }
                    // was the trigger completed, paused, blocked, etc. since being acquired?
                    BsonDocument triggerState = this.TriggerStates.FindOneByIdAs<BsonDocument>(trigger.Key.ToBsonDocument());
                    if (triggerState["State"] != "Acquired")
                    {
                        continue;
                    }

                    ICalendar cal = null;
                    if (trigger.CalendarName != null)
                    {
                        cal = this.RetrieveCalendar(trigger.CalendarName);
                        if (cal == null)
                        {
                            continue;
                        }
                    }

                    DateTimeOffset? prevFireTime = trigger.GetPreviousFireTimeUtc();

                    // call triggered on our copy, and the scheduler's copy
                    trigger.Triggered(cal);
                    this.Triggers.Save(trigger);
                    //tw.state = TriggerWrapper.STATE_EXECUTING;
                    triggerState["State"] = "Executing";
                    this.TriggerStates.Save(triggerState);

                    TriggerFiredBundle bndle = new TriggerFiredBundle(this.RetrieveJob(trigger.JobKey),
                                                                      trigger,
                                                                      cal,
                                                                      false, SystemTime.UtcNow(),
                                                                      trigger.GetPreviousFireTimeUtc(), prevFireTime,
                                                                      trigger.GetNextFireTimeUtc());

                    IJobDetail job = bndle.JobDetail;

                    if (job.ConcurrentExectionDisallowed)
                    {
                        var jobTriggers = this.GetTriggersForJob(job.Key);
                        IEnumerable<BsonDocument> triggerKeys = jobTriggers.Select(t => t.Key.ToBsonDocument());
                        this.TriggerStates.Update(
                            Query.And(
                                Query.In("_id", triggerKeys),
                                Query.EQ("State", "Waiting")),
                            Update.Set("State", "Blocked"));

                        this.TriggerStates.Update(
                            Query.And(
                                Query.In("_id", triggerKeys),
                                Query.EQ("State", "Paused")),
                            Update.Set("State", "PausedAndBlocked"));
                        
                        this.BlockedJobs.Save(job.Key.ToBsonDocument());
                    }

                    results.Add(new TriggerFiredResult(bndle));
                }
                return results;
            }
        }

        /// <summary> 
        /// Inform the <see cref="IJobStore" /> that the scheduler has completed the
        /// firing of the given <see cref="ITrigger" /> (and the execution its
        /// associated <see cref="IJob" />), and that the <see cref="JobDataMap" />
        /// in the given <see cref="IJobDetail" /> should be updated if the <see cref="IJob" />
        /// is stateful.
        /// </summary>
        public virtual void TriggeredJobComplete(IOperableTrigger trigger, IJobDetail jobDetail,
                                                 SchedulerInstruction triggerInstCode)
        {
            lock (lockObject)
            {
                this.TriggerStates.Update(
                        Query.EQ("_id", trigger.Key.ToBsonDocument()),
                        Update.Unset("InstanceId")
                            .Set("State", "Waiting"));

                // It's possible that the job is null if:
                //   1- it was deleted during execution
                //   2- RAMJobStore is being used only for volatile jobs / triggers
                //      from the JDBC job store

                if (jobDetail.PersistJobDataAfterExecution)
                {
                    this.Jobs.Update(
                        Query.EQ("_id", jobDetail.Key.ToBsonDocument()),
                        Update.Set("JobDataMap", jobDetail.JobDataMap.ToBsonDocument()));
                }

                if (jobDetail.ConcurrentExectionDisallowed)
                {
                    IList<Spi.IOperableTrigger> jobTriggers = this.GetTriggersForJob(jobDetail.Key);
                    IEnumerable<BsonDocument> triggerKeys = jobTriggers.Select(t => t.Key.ToBsonDocument());
                    this.TriggerStates.Update(
                        Query.And(
                            Query.In("_id", triggerKeys),
                            Query.EQ("State", "Blocked")),
                        Update.Set("State", "Waiting"));

                    this.TriggerStates.Update(
                        Query.And(
                            Query.In("_id", triggerKeys),
                            Query.EQ("State", "PausedAndBlocked")),
                        Update.Set("State", "Paused"));

                    signaler.SignalSchedulingChange(null);
                }

                // even if it was deleted, there may be cleanup to do
                this.BlockedJobs.Remove(
                    Query.EQ("_id", jobDetail.Key.ToBsonDocument()));

                // check for trigger deleted during execution...
                if (triggerInstCode == SchedulerInstruction.DeleteTrigger)
                {
                    log.Debug("Deleting trigger");
                    DateTimeOffset? d = trigger.GetNextFireTimeUtc();
                    if (!d.HasValue)
                    {
                        // double check for possible reschedule within job 
                        // execution, which would cancel the need to delete...
                        d = trigger.GetNextFireTimeUtc();
                        if (!d.HasValue)
                        {
                            this.RemoveTrigger(trigger.Key);
                        }
                        else
                        {
                            log.Debug("Deleting cancelled - trigger still active");
                        }
                    }
                    else
                    {
                        this.RemoveTrigger(trigger.Key);
                        signaler.SignalSchedulingChange(null);
                    }
                }
                else if (triggerInstCode == SchedulerInstruction.SetTriggerComplete)
                {
                    this.TriggerStates.Update(
                        Query.EQ("_id", trigger.Key.ToBsonDocument()),
                        Update.Set("State", "Complete"));
                    
                    signaler.SignalSchedulingChange(null);
                }
                else if (triggerInstCode == SchedulerInstruction.SetTriggerError)
                {
                    Log.Info(string.Format(CultureInfo.InvariantCulture, "Trigger {0} set to ERROR state.", trigger.Key));
                    this.TriggerStates.Update(
                        Query.EQ("_id", trigger.Key.ToBsonDocument()),
                        Update.Set("State", "Error"));

                    signaler.SignalSchedulingChange(null);
                }
                else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersError)
                {
                    Log.Info(string.Format(CultureInfo.InvariantCulture, "All triggers of Job {0} set to ERROR state.", trigger.JobKey));
                    IList<Spi.IOperableTrigger> jobTriggers = this.GetTriggersForJob(jobDetail.Key);
                    IEnumerable<BsonDocument> triggerKeys = jobTriggers.Select(t => t.Key.ToBsonDocument());
                    this.TriggerStates.Update(
                        Query.In("_id", triggerKeys),
                        Update.Set("State", "Error"));

                    signaler.SignalSchedulingChange(null);
                }
                else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersComplete)
                {
                    IList<Spi.IOperableTrigger> jobTriggers = this.GetTriggersForJob(jobDetail.Key);
                    IEnumerable<BsonDocument> triggerKeys = jobTriggers.Select(t => t.Key.ToBsonDocument());
                    this.TriggerStates.Update(
                        Query.In("_id", triggerKeys),
                        Update.Set("State", "Complete"));

                    signaler.SignalSchedulingChange(null);
                }
            }
        }

        /// <summary>
        /// Inform the <see cref="IJobStore" /> of the Scheduler instance's Id, 
        /// prior to initialize being invoked.
        /// </summary>
        public virtual string InstanceId
        {
            set { this.instanceId = value; }
        }

        /// <summary>
        /// Inform the <see cref="IJobStore" /> of the Scheduler instance's name, 
        /// prior to initialize being invoked.
        /// </summary>
        public virtual string InstanceName
        {
            set { }
        }

        public int ThreadPoolSize
        {
            set { }
        }

        public long EstimatedTimeToReleaseAndAcquireTrigger
        {
            get { return 200; }
        }

        public bool Clustered
        {
            get { return true; }
        }

        /// <seealso cref="IJobStore.GetPausedTriggerGroups()" />
        public virtual Collection.ISet<string> GetPausedTriggerGroups()
        {
            return new Collection.HashSet<string>(this.PausedTriggerGroups.FindAllAs<string>());
        }
    }
}