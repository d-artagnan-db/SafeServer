package pt.uminho.haslab.smcoprocessors;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import pt.uminho.haslab.smcoprocessors.CMiddleware.MessageBroker;
import pt.uminho.haslab.smcoprocessors.CMiddleware.Relay;
import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.CMiddleware.SharemindMessageBroker;
import pt.uminho.haslab.smcoprocessors.SecretSearch.AbstractSearchValue;
import pt.uminho.haslab.smcoprocessors.SecretSearch.Column;
import pt.uminho.haslab.smcoprocessors.SecretSearch.ComposedSearchValue;
import pt.uminho.haslab.smcoprocessors.SecretSearch.ContextPlayer;
import pt.uminho.haslab.smcoprocessors.SecretSearch.NopSearchValue;
import pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition;
import pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition.Condition;
import static pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition.Condition.And;
import static pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition.Condition.Equal;
import static pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition.Condition.GreaterOrEqualThan;
import static pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition.Condition.Less;
import static pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition.Condition.Nop;
import pt.uminho.haslab.smcoprocessors.SecretSearch.SecureRegionScanner;
import pt.uminho.haslab.smcoprocessors.protocolresults.ResultsIdentifiersMissmatch;
import pt.uminho.haslab.smcoprocessors.protocolresults.ResultsLengthMissmatch;
import pt.uminho.haslab.smhbase.interfaces.Player;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSecretFunctions;

public class SmpcCoprocessor extends BaseRegionObserver {

	private static final Log LOG = LogFactory.getLog(SmpcCoprocessor.class
			.getName());

	/* Regio Enviorment */
	private RegionCoprocessorEnvironment env;

	/* Configuration related to the Player and SearchEndpoint */
	private SmpcConfiguration searchConf;

	/* Holds the value if it was the first execution or not. By default is not. */
	private boolean wasFirst;

	/* Broker used to exchange message between local players and relay */
	private MessageBroker broker;

	private Relay relay;

	/**
	 * Check how many times the start method is instantiated
	 * 
	 * @param e
	 * @throws IOException
	 */
	@Override
	public void start(CoprocessorEnvironment e) throws IOException {
		env = (RegionCoprocessorEnvironment) e;
		Configuration conf = e.getConfiguration();
		searchConf = new SmpcConfiguration(conf);
		LOG.info("Starting coprocessor "
				+ env.getRegion().getRegionNameAsString());

		if (!playerHasStarted()) {
			wasFirst = true;
			broker = new SharemindMessageBroker();
			relay = searchConf.createRelay(broker);

			// Wait some time before tryng to connect with other region servers
			LOG.debug("Player " + searchConf.getPlayerID()
					+ " is going to wait for other players");
			waitOtherPlayers();
			initiateSharedResources(searchConf);
			if (searchConf.getPreRandomSize() > 0) {
				SharemindSecretFunctions.initRandomElemes(
						searchConf.getPreRandomSize(), searchConf.getnBits());
			}
			LOG.info("Resources initated " + searchConf.getPlayerIDasString());
		} else {
			LOG.debug("Second start " + searchConf.getPlayerIDasString());
			setupSharedResources(searchConf);
		}

	}

	@Override
	public void stop(CoprocessorEnvironment e) throws IOException {
		if (wasFirst) {
			if (!searchConf.isIsDevelopment()) {
				relay.stopRelay();
			} else {
				/*
				 * In development mode clusters are not concurrent and the stop
				 * requests by default waits for the other players to cancel
				 * their channel. This only happens if every relay stops the
				 * execution concurerntly. This way the server socket is simpli
				 * closed.
				 */
				relay.forceStopRelay();
			}
		}
	}

	public void setupSharedResources(SmpcConfiguration conf) {
		Map<String, Object> values = (Map<String, Object>) env.getSharedData()
				.get(searchConf.getPlayerIDasString());
		relay = (Relay) values.get(SharedResourcesIdentifiers.RELAY);
		broker = (MessageBroker) values.get(SharedResourcesIdentifiers.BROKER);
	}
	private void initiateSharedResources(SmpcConfiguration searchConf) {
		Map<String, Object> values = new HashMap<String, Object>();
		values.put(SharedResourcesIdentifiers.RELAY, relay);
		values.put(SharedResourcesIdentifiers.BROKER, broker);
		env.getSharedData().put(searchConf.getPlayerIDasString(), values);

	}

	private class WaitOthers extends Thread {

		private final int waitTime;

		public WaitOthers(int waitTime) {
			this.waitTime = waitTime;
		}

		@Override
		public void run() {
			try {
				LOG.debug("Going to sleep and wait for other players");
				Thread.sleep(waitTime);
				relay.bootRelay();
			} catch (InterruptedException ex) {
				LOG.error(ex);
			}
		}
	}

	/**
	 * Helping function that checks if the region server player has started the
	 * resources required to execute MPC. This is required because a
	 * regionServer calls the start() method multiple times.
	 */

	private boolean playerHasStarted() {

		return env.getSharedData()
				.containsKey(searchConf.getPlayerIDasString());
	}

	private void waitOtherPlayers() {
		LOG.debug("waiting for other players " + searchConf.getWaitTime());
		new WaitOthers(searchConf.getWaitTime()).start();

		if (!searchConf.isIsDevelopment()) {

			/* In development clusters are not launched concurrently */
			try {
				LOG.debug("Waiting for relay start");
				broker.waitRelayStart();
			} catch (InterruptedException ex) {
				LOG.error("Error on waiting for other players");
				LOG.error(ex);
			}
		}
	}

	private Player getPlayer(RequestIdentifier identifier) {
		return new ContextPlayer(relay, identifier,
				this.searchConf.getPlayerID(), broker);
	}

	private List<Cell> secretGetSearch(byte[] secret,
			OperationWithAttributes op, Condition cond,
			RegionCoprocessorEnvironment env, boolean stopOnMatch)
			throws IOException, ResultsLengthMissmatch,
			ResultsIdentifiersMissmatch {

		Column col = new Column(this.searchConf.getSecretFamily(),
				this.searchConf.getSecretQualifier());
		byte[] requestID = op.getAttribute("requestID");
		byte[] regionID = env.getRegion().getStartKey();
		LOG.debug("RequestID " + Arrays.toString(requestID));
		LOG.debug("RegionID " + Arrays.toString(regionID));
		RequestIdentifier ident = new RequestIdentifier(requestID, regionID);
		Player player = getPlayer(ident);
		int nbits = this.searchConf.getnBits();
		String targetPlayerS = new String(op.getAttribute("targetPlayer"));
		LOG.debug("TargetPlayer " + targetPlayerS);
		int targetPlayer = Integer.parseInt(targetPlayerS);

		if (this.searchConf.getPlayerID() == targetPlayer) {
			LOG.debug("Is target player");
			((SharemindPlayer) player).setTargetPlayer();
		}
		List<byte[]> secrets = new ArrayList<byte[]>();
		secrets.add(secret);

		SearchCondition searchCondition = AbstractSearchValue
				.conditionTransformer(cond, nbits, secrets, targetPlayer);
		SecureRegionScanner search = new SecureRegionScanner(searchCondition,
				env, player, this.searchConf, stopOnMatch, col);
		List<Cell> results = new ArrayList<Cell>();
		search.next(results);
		search.close();
		return results;

	}

	private RegionScanner secretScanSearch(byte[] startRow, byte[] stopRow,
			OperationWithAttributes op, RegionCoprocessorEnvironment env,
			Filter filter) throws IOException, ResultsLengthMissmatch,
			ResultsIdentifiersMissmatch {

		Column col = new Column(this.searchConf.getSecretFamily(),
				this.searchConf.getSecretQualifier());

		byte[] requestID = op.getAttribute("requestID");
		byte[] regionID = env.getRegion().getStartKey();

		LOG.debug("RequestID is " + Arrays.toString(requestID));
		LOG.debug("RegionID is " + Arrays.toString(regionID));

		RequestIdentifier ident = new RequestIdentifier(requestID, regionID);
		Player player = getPlayer(ident);

		int nbits = this.searchConf.getnBits();
		String targetPlayerS = new String(op.getAttribute("targetPlayer"));
		int targetPlayer = Integer.parseInt(targetPlayerS);
		LOG.debug("Is target player " + targetPlayer);
		if (this.searchConf.getPlayerID() == targetPlayer) {
			LOG.debug("Is going to set target Player");
			((SharemindPlayer) player).setTargetPlayer();
		}

		SearchCondition startKeySearch = null;
		SearchCondition endKeySearch = null;

		if (startRow.length != 0) {
			List<byte[]> startRows = new ArrayList<byte[]>();
			startRows.add(startRow);

			startKeySearch = AbstractSearchValue.conditionTransformer(
					GreaterOrEqualThan, nbits, startRows, targetPlayer);
		}

		if (stopRow.length != 0) {
			List<byte[]> stopRows = new ArrayList<byte[]>();
			stopRows.add(stopRow);

			endKeySearch = AbstractSearchValue.conditionTransformer(Less,
					nbits, stopRows, targetPlayer);
		}

		SearchCondition keySearch = null;

		if (startRow.length != 0 && stopRow.length != 0) {
			keySearch = new ComposedSearchValue(And, startKeySearch,
					endKeySearch, targetPlayer);
		} else if (startRow.length != 0 && stopRow.length == 0) {
			keySearch = startKeySearch;
		} else if (startRow.length == 0 && stopRow.length != 0) {
			keySearch = endKeySearch;
		} else if (startRow.length == 0 && stopRow.length == 0) {
			keySearch = new NopSearchValue(Nop, targetPlayer);
		}

		boolean stopOnMatch = false;
		SearchCondition finalCondition = keySearch;

		// if (filter != null) {
		// byte[] filterRow = null;
		// CompareOp filterCompare = null;
		// Filter innerFilter = null;//

		// if (filter instanceof WhileMatchFilter) {
		// stopOnMatch = true;
		// WhileMatchFilter wmFilter = (WhileMatchFilter) filter;
		// innerFilter = wmFilter.getFilter();
		// }//

		// if (innerFilter instanceof RowFilter) {
		// RowFilter rFilter = (RowFilter) innerFilter;
		// filterRow = rFilter.getComparator().getValue();
		// filterCompare = rFilter.getOperator();
		// }//

		//
		// if (filterCompare != null) {
		// SearchCondition innerFilterCondition = AbstractSearchValue
		// .conditionTransformer(filterCompare, nbits, filterRow, targetPlayer);
		// finalCondition = new ComposedSearchValue(And, innerFilterCondition,
		// keySearch, targetPlayer);
		// }
		// }

		return new SecureRegionScanner(finalCondition, env, player,
				this.searchConf, stopOnMatch, col);

	}

	private List<Cell> getRowWithoutSearch(byte[] rowID) throws IOException {
		// Get that bypasses this observer or it will call preGetOP again.
		Get get = new Get(rowID);
		List<Cell> cells = env.getRegion().get(get, false);
		return cells;
	}
	@Override
	public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> e,
			final Get get, final List<Cell> results) throws IOException {

		LOG.debug("start of preGetOp function");
		String table = env.getRegion().getTableDesc().getNameAsString();

		if (!table.contains("hbase")) {

			LOG.debug("preGetOp evaluated on table " + table);

			byte[] row = get.getRow();

			try {
				byte[] cachedID = get.getAttribute("cachedID");
				if (cachedID == null) {
					LOG.debug("Going to performe a secret search on data");
					List<Cell> searchResults = secretGetSearch(row, get, Equal,
							e.getEnvironment(), true);

					results.addAll(searchResults);
				} else {
					LOG.debug("Going to direct row access");
					List<Cell> res = getRowWithoutSearch(cachedID);
					results.addAll(res);
				}
				e.bypass();

			} catch (ResultsLengthMissmatch ex) {
				LOG.error(ex);
				throw new IllegalStateException(ex);
			} catch (ResultsIdentifiersMissmatch ex) {
				LOG.error(ex);
				throw new IllegalStateException(ex);
			}

		}
	}

	@Override
	public RegionScanner postScannerOpen(
			final ObserverContext<RegionCoprocessorEnvironment> c,
			final Scan scan, final RegionScanner s) {

		LOG.debug("start of postScannerOpen function");

		String table = env.getRegion().getTableDesc().getNameAsString();

		if (!table.contains("hbase")) {
			LOG.debug("postScannerOpen evaluated on " + table);

			try {

				byte[] startRow = scan.getStartRow();
				byte[] endRow = scan.getStopRow();

				if (startRow.length == 0) {
					LOG.debug("Starting row does not contain a value");
				} else {
					LOG.debug("Startting row is " + new BigInteger(startRow));
				}

				if (endRow.length == 0) {
					LOG.debug("Ending Row does not contain a value");
				} else {
					LOG.debug("Ending row is " + new BigInteger(endRow));
				}

				LOG.debug("Going to evalauate secretScanSearchr");

				RegionCoprocessorEnvironment ev = c.getEnvironment();
				Filter f = scan.getFilter();

				return secretScanSearch(startRow, endRow, scan, ev, f);

			} catch (ResultsLengthMissmatch ex) {
				LOG.error(ex);
				throw new IllegalStateException(ex);
			} catch (ResultsIdentifiersMissmatch ex) {
				LOG.error(ex);
				throw new IllegalStateException(ex);
			} catch (IOException ex) {
				LOG.debug(ex);
				throw new IllegalStateException(ex);
			}
		}
		return s;
	}

}
