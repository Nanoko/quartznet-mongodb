using System;

using Machine.Specifications;
namespace Quartz.Impl.MongoDB.Tests
{
    using System.Linq;

    using Quartz.Spi;

    public class DailyIntervalTriggerSpecs
    {
        public class When_a_daily_interval_trigger_has_been_stored_in_the_mongo_db
        {
            private Establish context = () =>
                {
                    jobStore = new JobStore();
                    jobStore.ClearAllSchedulingData();
                    
                    germanTimeZone = TimeZoneInfo.FindSystemTimeZoneById("W. Europe Standard Time");
                    trigger = TriggerBuilder.Create()
                        .ForJob(new JobKey("test"))
                        .WithDailyTimeIntervalSchedule(x => x.InTimeZone(germanTimeZone).OnEveryDay().StartingDailyAt(new TimeOfDay(10, 10)))
                        .Build();
                    jobDetail = JobBuilder.Create<TestJob>().WithIdentity("test").Build();
                };

            private Because of = () => jobStore.StoreJobAndTrigger(jobDetail, (IOperableTrigger)trigger);

            private It should_be_able_to_get_the_trigger_back_from_the_db = () =>
                {
                    var triggersForJob = jobStore.GetTriggersForJob(new JobKey("test")).ToList();
                    triggersForJob.Count.ShouldEqual(1);
                };

            private It should_retrieve_the_correct_time_zone = () =>
                {
                    var trigger = (IDailyTimeIntervalTrigger)jobStore.GetTriggersForJob(new JobKey("test")).Single();
                    trigger.TimeZone.ShouldEqual(germanTimeZone);
                };

            private static JobStore jobStore;

            private static ITrigger trigger;

            private static IJobDetail jobDetail;

            private static TimeZoneInfo germanTimeZone;
        }
    }
}
