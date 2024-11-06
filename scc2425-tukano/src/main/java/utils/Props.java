package utils;


import java.util.Arrays;


public class Props {

	public static void load(String[] keyValuePairs) {
		System.out.println(Arrays.asList( keyValuePairs));
		for( var pair: keyValuePairs ) {
			var parts = pair.split("=");
			if( parts.length == 2) 
				System.setProperty(parts[0], parts[1]);
		}
	}

	/*public static String get(String key, String defaultValue) {
		var val = System.getProperty(key);
		return val == null ? defaultValue : val;
	}*/

	/*public static <T> T get(String key, Class<T> clazz) {
		var val = System.getProperty(key);
		if( val == null )
			return null;
		return JSON.decode(val, clazz);
	}*/

	/*public static void load( String resourceFile ) {
		try( var in = Props.class.getClassLoader().getResourceAsStream(resourceFile) ) {
			var reader = new InputStreamReader(in);
			var props = new Properties();
			props.load(reader);
			props.forEach( (k,v) -> System.setProperty(k.toString(), v.toString()));
			System.getenv().forEach( System::setProperty );
		}
		catch( Exception x  ) {
			x.printStackTrace();
		}

	}*/
}
