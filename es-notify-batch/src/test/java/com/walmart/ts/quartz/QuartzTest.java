package com.walmart.ts.quartz;

import org.junit.Test;
import org.quartz.CronExpression;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

public class QuartzTest {

	@Test
	public void quartzTest() throws Exception {
		SchedulerFactory sf = new StdSchedulerFactory();
		Scheduler sched = sf.getScheduler();
		
		String cron = "0/5 * * * * ?";
		
		if(CronExpression.isValidExpression(cron)){
			throw new Exception("Invalid Cron!");
		}
		
		JobDetail job = JobBuilder.newJob(SimpleJob.class)
			    .withIdentity("job1", "group1")
			    .build();

			Trigger trigger = TriggerBuilder.newTrigger()
			    .withIdentity("trigger1", "group1")
//			    .withSchedule(CronScheduleBuilder.cronSchedule(cron))
			    .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(5).repeatForever())
			    .build();

			sched.scheduleJob(job, trigger);
			sched.start();
			Thread.sleep(300L * 1000L);
			sched.shutdown(true);
	}	
}
