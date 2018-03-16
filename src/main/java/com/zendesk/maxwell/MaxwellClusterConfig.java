package com.zendesk.maxwell;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import java.net.URISyntaxException;
import java.util.Properties;

/**
 * Created by liutao on 2018/3/16.
 */
public class MaxwellClusterConfig extends MaxwellConfig {

	public String zookeeperServers;

	public MaxwellClusterConfig(String argv[]) {
		super(argv);

		OptionSet options = buildOptionParser().parse(argv);
		Properties properties;

		if (options.has("config")) {
			properties = parseFile((String) options.valueOf("config"), true);
		} else {
			properties = parseFile(DEFAULT_CONFIG_FILE, false);
		}
		this.zookeeperServers = fetchOption("zk_servers", options, properties, "localhost:2181");
	}

	@Override
	protected OptionParser buildOptionParser() {
		OptionParser parser = super.buildOptionParser();
		parser.accepts( "zk_servers", "Zookeeper servers. default: localhost:2181" ).withRequiredArg();
		return parser;
	}

	private Properties parseFile(String filename, Boolean abortOnMissing) {
		Properties p = readPropertiesFile(filename, abortOnMissing);
		if ( p == null )
			p = new Properties();

		return p;
	}

}
