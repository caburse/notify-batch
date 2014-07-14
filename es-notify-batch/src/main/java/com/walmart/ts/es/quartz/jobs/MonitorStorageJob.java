package com.walmart.ts.es.quartz.jobs;

import java.util.Calendar;

import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.walmart.ts.es.constants.Constants;
import com.walmart.ts.es.constants.PropertiesConstants;
import com.walmart.ts.es.model.Alert;
import com.walmart.ts.es.model.DurationType;
import com.walmart.ts.es.util.MailUtil;
import com.walmart.ts.es.util.PropertyUtil;

/**
 * Quartz Scheduled Execution
 * @author caburse
 *
 */
public class MonitorStorageJob implements Job{

	private static final Logger LOGGER = Logger.getLogger(MonitorStorageJob.class);
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		
		Alert alert = (Alert)context.getJobDetail().getJobDataMap().get(context.getJobDetail().getKey().getName());
		
		Calendar cal = Calendar.getInstance();
		long end = cal.getTimeInMillis();
		
		//Subtract duration of DurationType from current time.
		cal.set(DurationType.fromString(alert.getAlertDurationType()).getType(), 
				cal.get(
						DurationType.fromString(alert.getAlertDurationType()).getType()) 
						- 
						alert.getAlertDuration());
				
		long begin = cal.getTimeInMillis();
		
		Settings settings = ImmutableSettings.settingsBuilder()
				.put(Constants.CLUSTER_NAME,
						PropertyUtil.getInstance().getString(PropertiesConstants.ES_CLUSTER))
				.put(Constants.CLIENT_TRANSPORT, true)
				.build();
		
		Client client = new TransportClient(settings)
		.addTransportAddress(
				new InetSocketTransportAddress(
						PropertyUtil.getInstance().getString(PropertiesConstants.ES_SERVER),
						PropertyUtil.getInstance().getInt(PropertiesConstants.ES_PORT)));
		
		QueryBuilder query = QueryBuilders.boolQuery()
//				.must(QueryBuilders.matchQuery(Constants.FIELD_SOURCE,alert.getSource()))
//				.must(QueryBuilders.matchQuery(alert.getFieldKey(),alert.getFieldValue()))
				.must(QueryBuilders.matchAllQuery())
				.must(QueryBuilders.rangeQuery(Constants.FIELD_TIMESTAMP)
							.from(begin)
							.to(end));
				
		SearchResponse response = client.prepareSearch()				
				.setQuery(query)
				.execute().actionGet();
		
//		LOGGER.debug("query: " + query);		
		LOGGER.info("response: " + response);
		LOGGER.info("total hits: " + response.getHits().getTotalHits());
		
		client.close();
		boolean notify = response.getHits().getTotalHits() > alert.getMaxOccurance();
		if(notify){
			//TODO call ticketing service
			MailUtil.postMail(alert.getSource()+" Logging Threshold exceeded", alert.getSource()+" exceeded Max Occurence of " + alert.getFieldValue() + " in " + alert.getFieldKey(), alert.getUserId()+"@wal-mart.com");
			try{
				LOGGER.info("Job: " + context.getJobDetail().getKey().getName() + " obj: "+alert.getSource());
				synchronized (context) {
					context.wait(alert.getBlackoutDuration()* 60 * 1000);	
				}
//				Thread.sleep(alert.getBlackoutDuration()* 60 * 1000);				
			}catch(InterruptedException ie){
				LOGGER.fatal(ie);
			}catch(Throwable t){
				LOGGER.fatal(t);
			}
		}else{
			LOGGER.info(alert.getSource() + " below threshold!");
		}
	}

}
