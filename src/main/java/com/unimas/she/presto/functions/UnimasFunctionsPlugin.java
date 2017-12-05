package com.unimas.she.presto.functions;

import java.util.Set;
import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableSet;

public class UnimasFunctionsPlugin implements Plugin {

	@Override
	public Set<Class<?>> getFunctions() {
		return ImmutableSet.<Class<?>>builder()
				.add(UnimasGeoFunctions.class)
				.add(UnimasStringFunctions.class)
				.add(UnimasJdbcPushDown.class)
				.build();
	}
}
