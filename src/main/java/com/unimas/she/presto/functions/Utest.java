package com.unimas.she.presto.functions;

import static com.unimas.she.presto.functions.UnimasGeoFunctions.*;
import static com.unimas.she.presto.functions.UnimasJdbcPushDown.*;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import io.airlift.slice.Slices;

public class Utest {
	static void bench() {
		long begin;
		long end;
		long times = 100000000;
		begin = System.nanoTime();
		for(int i = 0 ;i < times;i ++) {
			geoDistance(39.904211, 117.407395, 39.904211, 116.407395);
		}
		end = System.nanoTime();
		System.out.println("test1--------->" + (end - begin));
		
		
		begin = System.nanoTime();
		for(int i = 0 ;i < times  ;i ++) {
			geoDistance2(39.904211, 117.407395, 39.904211, 116.407395);
		}
		end = System.nanoTime();
		System.out.println("test2--------->" + (end - begin));
		
		
		begin = System.nanoTime();
		for(int i = 0 ;i < times  ;i ++) {
			geoDistance3(39.904211, 117.407395, 39.904211, 116.407395);
		}
		end = System.nanoTime();
		System.out.println("test3--------->" + (end - begin));
	}
	

	
	static void pgTest() throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
		
		File file = new File("/tmp/postgresql-42.1.4.jar");
		
		URL url=file.toURI().toURL();  
		
		URLClassLoader loader =  new URLClassLoader(new URL[]{url},System.class.getClassLoader());
		
		Class<?> cls = loader.loadClass("org.postgresql.Driver");

		Driver driver = (Driver) cls.newInstance();
		
		DriverManager.registerDriver(new DriverShim(driver));
		
		
		String jdbcUri = "jdbc:postgresql://10.68.23.21:5432/las";
		Properties properties = new Properties();
		properties.setProperty("user", "postgres");
		properties.setProperty("password", "postgres");
		Connection connection = DriverManager.getConnection(jdbcUri, properties);

		Statement statement = connection.createStatement();
		ResultSet resultSet = statement.executeQuery("select count(1) from zjhm where id > 100");
		while (resultSet.next()) {
			String name = resultSet.getString(1);
			System.out.println(name);
		}
		loader.close();
	}
	

	public static void main(String[] args) throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
		statementExecute(Slices.utf8Slice("jdbc:postgresql://10.68.23.21:5432/las"),
				Slices.utf8Slice("postgres"),
				Slices.utf8Slice("postgres"),
				Slices.utf8Slice("select * from zjhm where id > 20001 limit 10"));
		int a = -1;
		System.out.println(a * 3600);
	}
}
