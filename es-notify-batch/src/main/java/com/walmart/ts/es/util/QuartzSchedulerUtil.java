package com.walmart.ts.es.util;

import org.apache.log4j.Logger;
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.PersistJobDataAfterExecution;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import com.walmart.ts.es.exception.ValidationException;
import com.walmart.ts.es.model.Alert;
import com.walmart.ts.es.quartz.jobs.MonitorStorageJob;

/**
 * Class schedules jobs given alert information for each application.
 * @author caburse
 *
 */
@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class QuartzSchedulerUtil {
	
	private static final Logger LOGGER = Logger.getLogger(QuartzSchedulerUtil.class);
	
	public static void scheduleTasks(Alert... alerts) throws SchedulerException, ValidationException {
		//Instantiating scheduler
		SchedulerFactory sf = new StdSchedulerFactory();
		Scheduler sched = sf.getScheduler();
		
		//Adding each alert as a job to scheduler
		for(Alert alert : alerts){

			//Validate cron string
			if (!CronExpression.isValidExpression(alert.getCron())) {
				throw new ValidationException("Invalid Cron!"+alert.getCron());
			}
			
			//Creating Job giving it the name of the source app
			JobDetail job = JobBuilder.newJob(MonitorStorageJob.class)
					.withIdentity(alert.getSource(), Scheduler.DEFAULT_GROUP)		
					.build();

			//Add data
			job.getJobDataMap().put(job.getKey().getName(), alert);
			
			//Trigger schedules jobs based on cron settings
			Trigger trigger = TriggerBuilder
					.newTrigger()
					.withIdentity(alert.getSource()+"_trigger", Scheduler.DEFAULT_GROUP)
					.withSchedule(CronScheduleBuilder.cronSchedule(alert.getCron()))
//					.withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(5).repeatForever())
					.build();
			
			//Add job to schedule
			sched.scheduleJob(job, trigger);
		}
		try{
			//Start scheduled jobs
			sched.start();
			LOGGER.debug("Scheduler Initiated!");
			//Giving job a chance to start
			Thread.sleep(300L * 1000L);
			sched.shutdown(true);		
		} catch (SchedulerException e) {
			throw(e);
		} catch (Exception e) {
			LOGGER.error(e);
		}
	}
}
