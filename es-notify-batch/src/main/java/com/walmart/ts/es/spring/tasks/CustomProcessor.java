package com.walmart.ts.es.spring.tasks;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.batch.item.ItemProcessor;

import com.walmart.ts.es.constants.Constants;
import com.walmart.ts.es.constants.PropertiesConstants;
import com.walmart.ts.es.model.Alert;
import com.walmart.ts.es.model.DurationType;
import com.walmart.ts.es.util.PropertyUtil;

public class CustomProcessor implements ItemProcessor<List<Alert>,List<Alert>> {
	
	private static final Logger LOGGER = Logger.getLogger(CustomProcessor.class);
	
	@Override
	public List<Alert> process(List<Alert> alerts) throws Exception {
		List<Alert> notifications = new ArrayList<Alert>();
		for(Alert alert : alerts){
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
					.must(QueryBuilders.matchQuery(Constants.FIELD_SOURCE,alert.getSource()))
					.must(QueryBuilders.matchQuery(alert.getFieldKey(),alert.getFieldValue()))
	//				.must(QueryBuilders.matchAllQuery())
					.must(QueryBuilders.rangeQuery(Constants.FIELD_TIMESTAMP)
								.from(begin)
								.to(end));
					
			SearchResponse response = client.prepareSearch()				
					.setQuery(query)
					.execute().actionGet();
			
			LOGGER.info("response: " + response);
			LOGGER.info("total hits: " + response.getHits().getTotalHits());
			
			client.close();
			boolean notify = response.getHits().getTotalHits() > alert.getMaxOccurance();
			if(notify){
				notifications.add(alert);							
			}
		}
		return notifications;
	}
}