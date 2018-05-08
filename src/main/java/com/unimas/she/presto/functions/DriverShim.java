package com.unimas.she.presto.functions;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class DriverShim implements Driver{
	private Driver driver;
	
	DriverShim(Driver d) {
		this.driver = d;
	}
	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		// TODO Auto-generated method stub
		return this.driver.connect(url, info);
	}
	@Override
	public boolean acceptsURL(String url) throws SQLException {
		// TODO Auto-generated method stub
		return this.driver.acceptsURL(url);
	}
	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		// TODO Auto-generated method stub
		return this.driver.getPropertyInfo(url, info);
	}
	@Override
	public int getMajorVersion() {
		// TODO Auto-generated method stub
		return this.driver.getMajorVersion();
	}
	@Override
	public int getMinorVersion() {
		// TODO Auto-generated method stub
		return this.driver.getMinorVersion();
	}
	@Override
	public boolean jdbcCompliant() {
		// TODO Auto-generated method stub
		return this.driver.jdbcCompliant();
	}
	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		// TODO Auto-generated method stub
		return this.driver.getParentLogger();
	}
}
