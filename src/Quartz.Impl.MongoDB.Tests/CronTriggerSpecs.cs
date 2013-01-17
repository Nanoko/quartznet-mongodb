using System;
using System.Linq;

using Machine.Specifications;

using Quartz.Impl.Triggers;
using Quartz.Spi;

namespace Quartz.Impl.MongoDB.Tests
{
    public class CronTriggerSpecs
    {
        public class When_a_cron_trigger_with_a_daily_schedule_at_5_o_clock_german_local_time_is_retrieved_from_the_job_store
        {
            private Establish context = () =>
                {
                    expectedTimeZone = TimeZoneInfo.FindSystemTimeZoneById("W. Europe Standard Time");
                    SystemTime.UtcNow = () => new DateTimeOffset(new DateTime(2012, 12, 12, 8, 0, 0));
                    var tomorrow = new DateTime(2012, 12, 13, 5, 0, 0);
                    expectedNextFireTime = new DateTimeOffset(tomorrow, expectedTimeZone.GetUtcOffset(tomorrow));

                    jobStore = new JobStore();
                    jobStore.ClearAllSchedulingData();

                    trigger =
                        TriggerBuilder.Create()
                                      .ForJob(jobKey)
                                      .WithCronSchedule("0 0 5 ? * *", x => x.InTimeZone(expectedTimeZone))
                                      .Build();
                    jobDetail = JobBuilder.Create<TestJob>().WithIdentity("test").Build();
                    jobStore.StoreJobAndTrigger(jobDetail, (IOperableTrigger)trigger);
                };

            private Because of =
                () => retrievedCronTrigger = (CronTriggerImpl)jobStore.GetTriggersForJob(jobKey).Single();

            private It should_still_be_configured_for_the_german_time_zone = () => retrievedCronTrigger.TimeZone.ShouldEqual(expectedTimeZone);

            private It should_schedule_the_trigger_next_time_on_5_o_clock_german_local_time = () =>
                { 
                    retrievedCronTrigger.Triggered(null);
                    var nextFireTimeUtc = retrievedCronTrigger.GetNextFireTimeUtc();
                    nextFireTimeUtc.ShouldEqual(expectedNextFireTime);
                };

            private static JobStore jobStore;

            private static ITrigger trigger;

            private static IJobDetail jobDetail;

            private static TimeZoneInfo expectedTimeZone;

            private static JobKey jobKey = new JobKey("test");

            private static CronTriggerImpl retrievedCronTrigger;

            private static DateTimeOffset expectedNextFireTime;
        }
    }
}