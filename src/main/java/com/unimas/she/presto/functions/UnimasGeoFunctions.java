package com.unimas.she.presto.functions;

import static java.lang.Math.toRadians;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.math3.fitting.PolynomialCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoint;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceUtils;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;

public class UnimasGeoFunctions {
	private static double[] fitter;
	static {
		fitter = trainPolyFit(3, 10000);
	}

	public static boolean checkX(double x) {
		return x < -180 || x > 180 ? false : true;
	}

	public static boolean checkY(double y) {
		return y < -90 || y > 90 ? false : true;
	}

	public static double distanceSimplify(double Lat_A, double Lng_A, double Lat_B, double Lng_B) {
		if (Lat_A == Lat_B && Lng_A == Lng_B) {
			return 0;
		}
		double ra = 6378.140;
		double rb = 6356.755;
		double flatten = (ra - rb) / ra;
		double rad_lat_A = Math.toRadians(Lat_A);
		double rad_lng_A = Math.toRadians(Lng_A);
		double rad_lat_B = Math.toRadians(Lat_B);
		double rad_lng_B = Math.toRadians(Lng_B);
		double pA = Math.atan(rb / ra * Math.tan(rad_lat_A));
		double pB = Math.atan(rb / ra * Math.tan(rad_lat_B));
		double xx = Math
				.acos(Math.sin(pA) * Math.sin(pB) + Math.cos(pA) * Math.cos(pB) * Math.cos(rad_lng_A - rad_lng_B));
		double c1 = (Math.sin(xx) - xx) * Math.pow((Math.sin(pA) + Math.sin(pB)), 2) / Math.pow(Math.cos(xx / 2), 2);
		double c2 = (Math.sin(xx) + xx) * Math.pow((Math.sin(pA) - Math.sin(pB)), 2) / Math.pow(Math.sin(xx / 2), 2);
		double dr = flatten / 8 * (c1 - c2);
		return ra * (xx + dr);
	}

	public static double distanceSpatial(double Lat_A, double Lng_A, double Lat_B, double Lng_B) {
		return DistanceUtils.degrees2Dist(
				SpatialContext.GEO.calcDistance(SpatialContext.GEO.getShapeFactory().pointXY(Lng_A, Lat_A),
						SpatialContext.GEO.getShapeFactory().pointXY(Lng_B, Lat_B)),
				DistanceUtils.EARTH_MEAN_RADIUS_KM);
	}

	/**
	 *
	 * @param degree
	 *            代表你用几阶去拟合
	 * @param Length
	 *            把10 --60 分成多少个点去拟合，越大应该越精确
	 * @return
	 */
	public static double[] trainPolyFit(int degree, int Length) {
		PolynomialCurveFitter polynomialCurveFitter = PolynomialCurveFitter.create(degree);
		double minLat = 10.0; // 中国最低纬度
		double maxLat = 60.0; // 中国最高纬度
		double interv = (maxLat - minLat) / (double) Length;
		List<WeightedObservedPoint> weightedObservedPoints = new ArrayList<WeightedObservedPoint>();
		for (int i = 0; i < Length; i++) {
			WeightedObservedPoint weightedObservedPoint = new WeightedObservedPoint(1, minLat + (double) i * interv,
					Math.cos(toRadians(minLat + (double) i * interv)));
			weightedObservedPoints.add(weightedObservedPoint);
		}
		return polynomialCurveFitter.fit(weightedObservedPoints);
	}

	public static double distanceSimplifyMore(double lat1, double lng1, double lat2, double lng2, double[] a) {
		double dx = lng1 - lng2; // 经度差
		double dy = lat1 - lat2; // 纬度差值
		double b = (lat1 + lat2) / 2.0; // 平均纬度
		// 2) 计算东西方向距离和南北方向距离(单位：米)，东西距离采用三阶多项式
		double Lx = (a[3] * b * b * b + a[2] * b * b + a[1] * b + a[0]) * toRadians(dx)
				* DistanceUtils.EARTH_MEAN_RADIUS_KM; // 东西距离
		double Ly = toRadians(dy) * DistanceUtils.EARTH_MEAN_RADIUS_KM; // 南北距离
		// 3) 用平面的矩形对角距离公式计算总距离
		return Math.sqrt(Lx * Lx + Ly * Ly);
	}

	@ScalarFunction("geo_distance")
	@Description("Returns distance")
	@SqlType(StandardTypes.DOUBLE)
	public static double geoDistance(@SqlType(StandardTypes.DOUBLE) double Lat_A,
			@SqlType(StandardTypes.DOUBLE) double Lng_A, @SqlType(StandardTypes.DOUBLE) double Lat_B,
			@SqlType(StandardTypes.DOUBLE) double Lng_B) {
		if (!checkY(Lat_A) || !checkX(Lng_A) || !checkY(Lat_B) || !checkX(Lng_B)) {
			return Double.MAX_VALUE;
		}
		return distanceSimplifyMore(Lat_A, Lng_A, Lat_B, Lng_B, fitter);
	}
	
	@ScalarFunction("geo_distance2")
	@Description("Returns distance")
	@SqlType(StandardTypes.DOUBLE)
	public static double geoDistance2(@SqlType(StandardTypes.DOUBLE) double Lat_A,
			@SqlType(StandardTypes.DOUBLE) double Lng_A, @SqlType(StandardTypes.DOUBLE) double Lat_B,
			@SqlType(StandardTypes.DOUBLE) double Lng_B) {
		if (!checkY(Lat_A) || !checkX(Lng_A) || !checkY(Lat_B) || !checkX(Lng_B)) {
			return Double.MAX_VALUE;
		}
		return distanceSpatial(Lat_A, Lng_A, Lat_B, Lng_B);
	}

	@ScalarFunction("geo_distance3")
	@Description("Returns distance")
	@SqlType(StandardTypes.DOUBLE)
	public static double geoDistance3(@SqlType(StandardTypes.DOUBLE) double Lat_A,
			@SqlType(StandardTypes.DOUBLE) double Lng_A, @SqlType(StandardTypes.DOUBLE) double Lat_B,
			@SqlType(StandardTypes.DOUBLE) double Lng_B) {
		if (!checkY(Lat_A) || !checkX(Lng_A) || !checkY(Lat_B) || !checkX(Lng_B)) {
			return Double.MAX_VALUE;
		}
		return distanceSimplify(Lat_A, Lng_A, Lat_B, Lng_B);
	}
	
	
	
	@ScalarFunction("geo_hash")
	@Description("Returns hash")
	@SqlType(StandardTypes.DOUBLE)
	public static double geoHash(@SqlType(StandardTypes.DOUBLE) double Lat,
			@SqlType(StandardTypes.DOUBLE) double Lng, @SqlType(StandardTypes.INTEGER) int numberOfCharacters) {
		if (!checkY(Lat) || !checkX(Lng)) {
			return Double.MAX_VALUE;
		}
		
		
		return 0;
	}
	
	
	
	
}
