package com.unimas.she.presto.functions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;

import io.airlift.slice.Slice;

public class UnimasJdbcPushDown {
	
	@ScalarFunction("execute_statement")
	@Description("Returns results")
	@SqlType(StandardTypes.DOUBLE)
	public static double statementExecute(@SqlType(StandardTypes.VARCHAR) Slice val) throws SQLException {
		String url = "jdbc:postgresql://10.68.23.21:5432/las";
		Properties properties = new Properties();
		properties.setProperty("user", "postgres");
		properties.setProperty("password", "postgres");
		Connection connection = DriverManager.getConnection(url, properties);
		Statement statement = connection.createStatement();
		ResultSet resultSet = statement.executeQuery(val.toStringUtf8());
		String name = null;
		while (resultSet.next()) {
			name = resultSet.getString(1);
			break;
		}
		return Double.valueOf(name);
	}
}
