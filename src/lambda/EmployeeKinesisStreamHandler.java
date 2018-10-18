package lambda;

import com.amazonaws.kinesis.deagg.RecordDeaggregator;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.gson.Gson;

public class EmployeeKinesisStreamHandler implements RequestHandler<KinesisEvent, Object> {

	@Override
	public Object handleRequest(KinesisEvent event, Context context) {
		LambdaLogger logger = context.getLogger();
		RecordDeaggregator.stream(event.getRecords().stream(), userRecord -> {
			String rawJson = new String(userRecord.getData().array());
			logger.log("Raw data from Kinesis: " + rawJson);

			//Employee employee = new Gson().fromJson(rawJson, Employee.class);
		});

		return null;
	}


}