using System;

namespace Quartz.Impl.MongoDB.Tests
{
    public class TestJob: IJob
    {
        public void Execute(IJobExecutionContext context)
        {
            throw new NotImplementedException();
        }
    }
}