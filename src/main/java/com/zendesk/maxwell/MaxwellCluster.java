package com.zendesk.maxwell;

import com.djdch.log4j.StaticShutdownCallbackRegistry;
import com.zendesk.maxwell.util.Logging;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.CancelLeadershipException;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;


/**
 * Created by liutao on 2018/3/13.
 */
//public class MaxwellCluster extends LeaderSelectorListenerAdapter implements Runnable, Closeable {
public class MaxwellCluster extends LeaderSelectorListenerAdapter implements Runnable, Closeable {

	private String name;
	private String clientID;
	private String leaderPath;
	private String[] maxwellArgs;

	private LeaderSelector leaderSelector;

	private CuratorFramework curatorClient;
	private Maxwell maxwell;
	private volatile boolean isLeader;

	private static final String MAXWELL_PATH = "/maxwell/leader/";

	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellCluster.class);

	public MaxwellCluster(CuratorFramework curator, MaxwellConfig config, String[] args) throws Exception {
		String path = MAXWELL_PATH + config.clientID;

		this.clientID = config.clientID;
		this.leaderPath = path;
		this.curatorClient = curator;
		this.isLeader = false;
		this.name = clientID + ":" + getId();
		this.maxwellArgs = args;

		if ( config.log_level != null )
			Logging.setLevel(config.log_level);

		maxwell = new Maxwell(config);

		// create a leader selector using the given path for management
		// all participants in a given leader selection must use the same path
		// MaxwellCluster here is also a LeaderSelectorListener but this isn't required
		leaderSelector = new LeaderSelector(curatorClient, leaderPath, this);
		leaderSelector.setId(this.name);

		// for most cases you will want your instance to requeue when it relinquishes leadership
		leaderSelector.autoRequeue();
	}

	public void run() {
		try {
			start();
		} catch (Exception e) {
			LOGGER.error("maxwell encountered an exception", e);
		}
	}

	private void start() throws IOException {
		LOGGER.info("MaxwellCluster start");
		// the selection for this instance doesn't start until the leader selector is started
		// leader selection is done in the background so this call to leaderSelector.start() returns immediately
		leaderSelector.start();
	}

	@Override
	public void close() throws IOException {
		LOGGER.info("MaxwellCluster closed");

		maxwell.terminate();
		curatorClient.close();
		leaderSelector.close();
	}

	@Override
	public void takeLeadership(CuratorFramework client) throws Exception {
		// we are now the leader. This method should not return until we want to relinquish leadership
		LOGGER.info("{}: I'm now the leader", this.name);
		try {
			isLeader = false;
			maxwell.run();
		} finally {
			LOGGER.info("I relinquish leadership");
		}
	}

	private void resetMaxwell() {
		LOGGER.info("MaxwellCluster reset");

		try {
			isLeader = false;
			maxwell.terminate();

			// config reset metrics
			MaxwellConfig config = new MaxwellConfig(maxwellArgs);
			maxwell = new Maxwell(config);
		} catch (Exception e) {
			LOGGER.warn("Create Maxwell error: {}", e);
		}
	}

	@Override
	public void stateChanged(CuratorFramework curatorClient, ConnectionState newState) {
		LOGGER.info(String.format("%s - %s", newState.name(), MAXWELL_PATH));

		if (newState == ConnectionState.CONNECTED) {
			LOGGER.warn("Zookeeper connection CONNECTED");
		}
		else if (newState == ConnectionState.SUSPENDED) {
			LOGGER.warn("Zookeeper connection SUSPENDED");
		}
		else if (newState == ConnectionState.LOST) {
			LOGGER.warn("Zookeeper connection LOST, cancel leadership");
			resetMaxwell();
			throw new CancelLeadershipException();
		}
		else if (newState == ConnectionState.RECONNECTED) {
			LOGGER.info("ConnectionState RECONNECTED");
		}
		else if (newState == ConnectionState.READ_ONLY) {
			LOGGER.info("ConnectionState READ_ONLY");
		}
	}

	private static String getId() {
		try {
			return InetAddress.getLocalHost().toString();
		}
		catch (UnknownHostException e) {
			return "hostname not configured";
		}
	}

	public static void main(String[] args) {
		try {
			Logging.setupLogBridging();

			MaxwellClusterConfig config = new MaxwellClusterConfig(args);

			if ("maxwell".equals(config.clientID)) {
				throw new URISyntaxException("clientID is maxwell", "client_id with default value not allowed");
			}

			RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);
			CuratorFramework curator = CuratorFrameworkFactory.newClient(config.zookeeperServers, retryPolicy);
			curator.start();

			MaxwellCluster cluster = new MaxwellCluster(curator, config, args);

			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					CloseableUtils.closeQuietly(cluster);
					StaticShutdownCallbackRegistry.invoke();
				}
			});

			cluster.start();

			while (!cluster.isLeader) {
				TimeUnit.SECONDS.sleep(3);
			}
		} catch ( SQLException e ) {
			// catch SQLException explicitly because we likely don't care about the stacktrace
			LOGGER.error("SQLException: {}", e.getLocalizedMessage());
			System.exit(1);
		} catch ( URISyntaxException e ) {
			// catch URISyntaxException explicitly as well to provide more information to the user
			LOGGER.error("Syntax issue with URI, check for client_id, misconfigured host, port, database, or JDBC options (see RFC 2396)");
			LOGGER.error("URISyntaxException: {}", e.getLocalizedMessage());
			System.exit(1);
		} catch ( Exception e ) {
			e.printStackTrace();
			System.exit(1);
		}
	}

}
