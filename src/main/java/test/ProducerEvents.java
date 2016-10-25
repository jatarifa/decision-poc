package test;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import com.stratio.decision.api.IStratioStreamingAPI;
import com.stratio.decision.api.StratioStreamingAPIFactory;
import com.stratio.decision.api.messaging.ColumnNameType;
import com.stratio.decision.api.messaging.ColumnNameValue;
import com.stratio.decision.commons.constants.ColumnType;
import com.stratio.decision.commons.exceptions.StratioEngineConnectionException;
import com.stratio.decision.commons.exceptions.StratioStreamingException;

@Component
@DependsOn("consumer")
public class ProducerEvents implements InitializingBean {        

	private Log log = LogFactory.getLog(getClass());
	private String streamName = "inputStream";
	
	@Override
	public void afterPropertiesSet() throws Exception {
		Thread.sleep(10000);
		IStratioStreamingAPI api = getApi();
		
		if(!api.listStreams().contains(streamName))
		{
			ColumnNameType firstStreamColumn= new ColumnNameType("name", ColumnType.STRING);
			ColumnNameType secondStreamColumn = new ColumnNameType("data", ColumnType.DOUBLE);
			List<ColumnNameType> columnList = Arrays.asList(firstStreamColumn, secondStreamColumn);
			api.createStream(streamName, columnList);
			
			api.addQuery(streamName, "from inputStream#window.time(1 min) select name, avg(data) as average group by name insert into medias_1");
			api.addQuery("medias_1", "from medias_1[average > 90 and name=='fail'] insert into alarms_1");
			
			api.listenStream("alarms_1");
		}
		
		new Thread(() -> {
			List<String> names = Arrays.asList("ok", "fail", "warn");
			while (true) {
				int numEvents = RandomUtils.nextInt(5);
				for (int i = 0; i < numEvents; i++) {
					int n = RandomUtils.nextInt(3);
					if (n < 2)
						insert(api, RandomUtils.nextInt(100), names.get(RandomUtils.nextInt(3)));
					else
						insert(api, RandomUtils.nextInt(100) + 100, names.get(RandomUtils.nextInt(3)));
				}
				try {
					Thread.sleep(RandomUtils.nextInt(4) * 1000);
				} 
				catch (InterruptedException e) {}
			}
		}).start();
	}
	
	private IStratioStreamingAPI getApi() throws StratioEngineConnectionException {
		return StratioStreamingAPIFactory.create()
										 .withServerConfig("localhost:9092","localhost:2181")
										 .init();
	}
	
	private void insert(IStratioStreamingAPI api, double value, String name)
	{
		ColumnNameValue firstColumnValue = new ColumnNameValue("name", name);
		ColumnNameValue secondColumnValue = new ColumnNameValue("data", value);
		List<ColumnNameValue> streamData = Arrays.asList(firstColumnValue, secondColumnValue);
		try {
			log.info("sending : (" + firstColumnValue.columnName() + "." + firstColumnValue.columnValue() +
										 "," + secondColumnValue.columnName() + "." + secondColumnValue.columnValue() + ")");
		    api.insertData(streamName, streamData);
		} catch(StratioStreamingException ssEx) {
		    ssEx.printStackTrace();
		}
	}
}
