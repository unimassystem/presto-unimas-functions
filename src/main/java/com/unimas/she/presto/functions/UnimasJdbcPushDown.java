package com.unimas.she.presto.functions;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public class UnimasJdbcPushDown {
	
	
		
	private static String getLocation() {
		String filePath = null;
		URL url = UnimasJdbcPushDown.class.getProtectionDomain().getCodeSource().getLocation();
		try {
			filePath = java.net.URLDecoder.decode(url.getPath(), "utf-8");

		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return filePath.substring(0, filePath.lastIndexOf("/") + 1);
	}
	
	private static void registerDriver(String jarFile,String className) {
				
		File file = new File(jarFile);
		
		URL url = null;
		try {
			url = file.toURI().toURL();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
		
		URLClassLoader loader =  new URLClassLoader(new URL[]{url});
		Driver driver = null;
		
		try {
			driver = (Driver) Class.forName(className, true, loader).newInstance();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
			DriverManager.registerDriver(new DriverShim(driver));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	static {
		String path = getLocation();
		registerDriver( path + "postgresql-42.1.4.jar","org.postgresql.Driver");
	}

	@ScalarFunction("execute_statement")
	@Description("Returns results")
	@SqlType(StandardTypes.VARCHAR)
	public static Slice statementExecute(@SqlType(StandardTypes.VARCHAR) Slice uri,
			@SqlType(StandardTypes.VARCHAR) Slice user,
			@SqlType(StandardTypes.VARCHAR) Slice passwd ,
			@SqlType(StandardTypes.VARCHAR) Slice sql) throws SQLException {
		
		String url = uri.toStringUtf8();
		
		Properties properties = new Properties();
		properties.setProperty("user", user.toStringUtf8());
		properties.setProperty("password", passwd.toStringUtf8());
		
		Connection connection = DriverManager.getConnection(url, properties);
		
		Statement statement = connection.createStatement();
		
		ResultSet resultSet = statement.executeQuery(sql.toStringUtf8());
		
		int columnCount = resultSet.getMetaData().getColumnCount();
		ImmutableList.Builder<String> columns = ImmutableList.builder();
		
		for(int i = 0;i < columnCount ;i ++) {
			columns.add(resultSet.getMetaData().getColumnName(i+ 1));
		}
		ImmutableList.Builder<Object> results = ImmutableList.builder();
		
		while (resultSet.next()) {
			ImmutableList.Builder<String> row = ImmutableList.builder();
			for(int i = 0;i < columnCount;i ++) {
				row.add(resultSet.getString(i + 1));
			}
			results.add(row.build());
		}
		ImmutableMap.Builder<String, Object> res = ImmutableMap.builder();
		res.put("MetaData",columns.build());
		res.put("Results",results.build());
		
		ObjectMapper mapper = new ObjectMapper();
		
		String json = null;
		try {
			json = mapper.writeValueAsString(res.build());
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		connection.close();
		return Slices.utf8Slice(json);
	}
}
