package lambda;

import com.amazonaws.kinesis.deagg.RecordDeaggregator;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.gson.Gson;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.dao.DaoManager;
import com.j256.ormlite.jdbc.JdbcConnectionSource;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.table.TableUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class EmployeeKinesisStreamHandler implements RequestHandler<KinesisEvent, Object> {
	private static String jdbcConnection = System.getenv("jdbcConnection");
	private static String userName = System.getenv("userName");
	private static String password = System.getenv("password");


	@Override
	public Object handleRequest(KinesisEvent event, Context context) {
		LambdaLogger logger = context.getLogger();

		ConnectionSource connectionSource = null;
		Dao<Employee, String> employeeDao = null;
		try {
			logger.log("jdbc: " + jdbcConnection);
			connectionSource = new JdbcConnectionSource(jdbcConnection, userName, password);
			employeeDao = DaoManager.createDao(connectionSource, Employee.class);
			TableUtils.createTableIfNotExists(connectionSource, Employee.class);
			logger.log("finished creating table");
			final Dao<Employee, String> finalEmployeeDao = employeeDao;
			RecordDeaggregator.stream(event.getRecords().stream(), userRecord -> {
				String rawJson = new String(userRecord.getData().array());
				logger.log("Raw data from Kinesis: " + rawJson);

				Employee employee = new Gson().fromJson(rawJson, Employee.class);
				try {
					finalEmployeeDao.create(employee);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			});

		} catch (SQLException e) {
			e.printStackTrace();
		}

		//---------------------
		try {
			List<Employee> employeeList = employeeDao.queryForAll();
			logger.log("found how many records? " + employeeList.size());
			logger.log("first name is: " + employeeList.get(0).getName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		//---------------------

		try {
			connectionSource.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}


}