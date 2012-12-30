using Machine.Specifications;
namespace Quartz.Impl.MongoDB.Tests
{
    using System;
    using System.Collections.Specialized;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;

    using Common.Logging.Simple;

    using Quartz.Spi;

    public class TriggerSpecs
    {
        public class When_a_daily_interval_trigger_has_been_stored_in_the_mongo_db
        {
            private Establish context = () =>
                {
                    jobStore = new JobStore();
                    jobStore.ClearAllSchedulingData();
                    trigger = TriggerBuilder.Create()
                        .ForJob(new JobKey("test"))
                        .WithDailyTimeIntervalSchedule(x => x.OnEveryDay().StartingDailyAt(new TimeOfDay(10, 10)))
                        .Build();
                    jobDetail = JobBuilder.Create<TestJob>().WithIdentity("test").Build();
                };

            private Because of = () => jobStore.StoreJobAndTrigger(jobDetail, (IOperableTrigger)trigger);

            private It should_be_able_to_get_the_trigger_back_from_the_db = () =>
                {
                    var triggersForJob = jobStore.GetTriggersForJob(new JobKey("test")).ToList();
                    triggersForJob.Count.ShouldEqual(1);
                };

            private static JobStore jobStore;

            private static ITrigger trigger;

            private static IJobDetail jobDetail;
        }
    }

    public class TestJob: IJob
    {
        public void Execute(IJobExecutionContext context)
        {
            throw new NotImplementedException();
        }
    }
}
