package com.unimas.she.presto.functions;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;

public class UnimasStringFunctions {

	@ScalarFunction("unimas_hash_code")
	@Description("Returns hash_code")
	@SqlType(StandardTypes.DOUBLE)
	public static double unimasHashCode(@SqlType(StandardTypes.VARCHAR) Slice val) {
		return Math.abs(val.toStringUtf8().hashCode());
	}
}
