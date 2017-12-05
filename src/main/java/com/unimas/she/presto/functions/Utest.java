package com.unimas.she.presto.functions;
import static com.unimas.she.presto.functions.UnimasGeoFunctions.*;
import static com.unimas.she.presto.functions.UnimasStringFunctions.*;

import java.sql.Connection;
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
	
	
	static void pgTest() throws SQLException {
		String url = "jdbc:postgresql://10.68.23.21:5432/las";
		Properties properties = new Properties();
		properties.setProperty("user", "postgres");
		properties.setProperty("password", "postgres");
		Connection connection = DriverManager.getConnection(url, properties);

		Statement statement = connection.createStatement();
		ResultSet resultSet = statement.executeQuery("select count(1) from zjhm");
		while (resultSet.next()) {
			String name = resultSet.getString(1);
			System.out.println(name);
		}

	}
	

	public static void main(String[] args) throws SQLException {
//		System.out.println(geoDistance(39.941,116.45,39.94,116.451));
//		System.out.println(geoDistance2(39.941,116.45,39.94,116.451));
//		System.out.println(geoDistance3(39.941,116.45,39.94,116.451));
//		bench();
		pgTest();
	}
}
