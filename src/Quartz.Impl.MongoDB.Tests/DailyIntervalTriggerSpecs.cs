using System;

using Machine.Specifications;

using Quartz.Impl.Triggers;

using System.Linq;

using Quartz.Spi;

namespace Quartz.Impl.MongoDB.Tests
{
    public class DailyIntervalTriggerSpecs
    {
        public class When_a_daily_interval_trigger_has_been_stored_in_the_mongo_db
        {
            private Establish context = () =>
                {
                    germanTimeZone = TimeZoneInfo.FindSystemTimeZoneById("W. Europe Standard Time");
                    SystemTime.UtcNow = () => new DateTimeOffset(new DateTime(2012, 12, 13, 23, 0, 0));
                    var tomorrow = new DateTime(2012, 12, 14, 10, 10, 0);
                    expectedNextFireTime = new DateTimeOffset(tomorrow, germanTimeZone.GetUtcOffset(tomorrow));

                    jobStore = new JobStore();
                    jobStore.ClearAllSchedulingData();

                    trigger = TriggerBuilder.Create()
                        .ForJob(jobKey)
                        .WithDailyTimeIntervalSchedule(x => x.InTimeZone(germanTimeZone).OnEveryDay()
                            .StartingDailyAt(new TimeOfDay(10, 10))
                            .EndingDailyAt(new TimeOfDay(10,20)))
                        .Build();
                    jobDetail = JobBuilder.Create<TestJob>().WithIdentity("test").Build();
                };

            private Because of = () => jobStore.StoreJobAndTrigger(jobDetail, (IOperableTrigger)trigger);

            private It should_be_able_to_get_the_trigger_back_from_the_db = () =>
                {
                    var triggersForJob = jobStore.GetTriggersForJob(jobKey).ToList();
                    triggersForJob.Count.ShouldEqual(1);
                };

            private It should_retrieve_the_correct_time_zone = () =>
                {
                    var trigger = (DailyTimeIntervalTriggerImpl)jobStore.GetTriggersForJob(jobKey).Single();
                    trigger.TimeZone.ShouldEqual(germanTimeZone);
                };

            private It should_retrieve_the_start_and_end_time_of_day = () =>
                {
                    var trigger = (DailyTimeIntervalTriggerImpl)jobStore.GetTriggersForJob(jobKey).Single();
                    trigger.StartTimeOfDay.ShouldEqual(new TimeOfDay(10, 10));
                    trigger.EndTimeOfDay.ShouldEqual(new TimeOfDay(10, 20));
                };

            private It should_trigger_next_run_between_scheduled_starting_and_ending_time_of_day = () =>
                {
                    var trigger = (DailyTimeIntervalTriggerImpl)jobStore.GetTriggersForJob(jobKey).Single();

                    trigger.Triggered(null);

                    trigger.GetNextFireTimeUtc()
                           .ShouldEqual(expectedNextFireTime);
                };

            private static JobStore jobStore;

            private static ITrigger trigger;

            private static IJobDetail jobDetail;

            private static TimeZoneInfo germanTimeZone;

            private static JobKey jobKey = new JobKey("test");

            private static DateTimeOffset expectedNextFireTime;
        }
    }
}
