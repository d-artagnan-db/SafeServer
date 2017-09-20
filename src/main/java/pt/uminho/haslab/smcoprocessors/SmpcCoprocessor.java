package pt.uminho.haslab.smcoprocessors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import pt.uminho.haslab.smcoprocessors.comunication.MessageBroker;
import pt.uminho.haslab.smcoprocessors.comunication.Relay;
import pt.uminho.haslab.smcoprocessors.comunication.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.comunication.SharemindMessageBroker;
import pt.uminho.haslab.smcoprocessors.protocolresults.ResultsIdentifiersMismatch;
import pt.uminho.haslab.smcoprocessors.protocolresults.ResultsLengthMismatch;
import pt.uminho.haslab.smcoprocessors.secretSearch.*;
import pt.uminho.haslab.smcoprocessors.secureRegionScanner.Column;
import pt.uminho.haslab.smcoprocessors.secureRegionScanner.SecureRegionScanner;
import pt.uminho.haslab.smhbase.interfaces.Player;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSecretFunctions;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static pt.uminho.haslab.smcoprocessors.secretSearch.SearchCondition.Condition.*;

public class SmpcCoprocessor extends BaseRegionObserver {

	private static final Log LOG = LogFactory.getLog(SmpcCoprocessor.class
			.getName());

	/* Region Environment */
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
			relay.bootRelay();
			// Wait some time before trying to connect with other region servers
			LOG.debug("Player " + searchConf.getPlayerID()
					+ " is waiting for Relay server to start");
			try {
				waitServerStart();
			} catch (InterruptedException e1) {
				LOG.error("Relay not booted correctly "
						+ e1.getLocalizedMessage());
				throw new IllegalStateException(e1);
			}

			initiateSharedResources(searchConf);

			if (searchConf.getPreRandomSize() > 0) {
				LOG.debug("Defining "
						+ searchConf.getPreRandomSize()
						+ " as the number of random numbers on the SMPC library");
				SharemindSecretFunctions.initRandomElemes(
						searchConf.getPreRandomSize(), searchConf.getnBits());
			}
			LOG.info("Resources initiated " + searchConf.getPlayerIDasString());
		} else {
			LOG.debug("Second start " + searchConf.getPlayerIDasString());
			setupSharedResources(searchConf);
		}

	}

	@Override
	public void stop(CoprocessorEnvironment e) throws IOException {
		if (wasFirst) {
			if (!searchConf.isDevelopment()) {
				relay.stopRelay();
			} else {
				/*
				 * In development mode clusters are not concurrent and the stop
				 * requests by default waits for the other players to cancel
				 * their channel. This only happens if every relay stops the
				 * execution concurrently. This way the server socket is forced
				 * to close.
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

	/**
	 * Helping function that checks if the region server player has started the
	 * resources required to execute MPC. This is required because a
	 * regionServer calls the start() method multiple times.
	 */

	private boolean playerHasStarted() {

		return env.getSharedData()
				.containsKey(searchConf.getPlayerIDasString());
	}

	private void waitServerStart() throws InterruptedException {
		LOG.debug("Waiting for signal of Relay Start");
		broker.waitRelayStart();
		LOG.debug("Relay start signal received");
	}

	private Player getPlayer(RequestIdentifier identifier) {
		return new ContextPlayer(relay, identifier,
				this.searchConf.getPlayerID(), broker);
	}

	private Column getSearchColumn(OperationWithAttributes op) {

		byte[] secretFamily = op
				.getAttribute(OperationAttributesIdentifiers.SecretFamily);
		byte[] secretQualifier = op
				.getAttribute(OperationAttributesIdentifiers.SecretQualifier);

		if (LOG.isDebugEnabled()) {
			LOG.debug("SecretFamily " + new String(secretFamily));
			LOG.debug("SecretQualifier " + new String(secretQualifier));
		}
		return new Column(secretFamily, secretQualifier);

	}

	private RequestIdentifier getRequestIdentifier(OperationWithAttributes op,
			RegionCoprocessorEnvironment env) {

		byte[] requestID = op
				.getAttribute(OperationAttributesIdentifiers.RequestIdentifier);
		byte[] regionID = env.getRegion().getStartKey();

		if (LOG.isDebugEnabled()) {
			LOG.debug("RequestID " + new String(requestID));
			LOG.debug("RegionID " + new String(regionID));
		}
		return new RequestIdentifier(requestID, regionID);

	}

	private int checkTargetPlayer(Player player, OperationWithAttributes op,
			RequestIdentifier ident) {
		String targetPlayerS = new String(
				op.getAttribute(OperationAttributesIdentifiers.TargetPlayer));
		LOG.debug("TargetPlayer " + targetPlayerS);
		int targetPlayer = Integer.parseInt(targetPlayerS);

		if (this.searchConf.getPlayerID() == targetPlayer) {
			LOG.debug("This RegionServer is target player for the request "
					+ ident.hashCode());
			((SharemindPlayer) player).setTargetPlayer();
		}
		return targetPlayer;
	}

	private List<Cell> secretGetSearch(byte[] secret,
			OperationWithAttributes op, RegionCoprocessorEnvironment env)
			throws ResultsLengthMismatch, ResultsIdentifiersMismatch {

		Column col = getSearchColumn(op);
		RequestIdentifier ident = getRequestIdentifier(op, env);
		relay.registerRequest(ident);
		Player player = getPlayer(ident);
		int nbits = this.searchConf.getnBits();
		int targetPlayer = checkTargetPlayer(player, op, ident);

		List<byte[]> secrets = new ArrayList<byte[]>();
		secrets.add(secret);
		LOG.debug(player.getPlayerID() + " is creating search condition with "
				+ nbits + " nbits " + " and the secret "
				+ new BigInteger(secret));
		SearchCondition searchCondition = AbstractSearchValue
				.conditionTransformer(Equal, nbits, secrets, targetPlayer);
		LOG.debug(player.getPlayerID() + " created secure RegionScanner");
		SecureRegionScanner search;
		List<Cell> results = new ArrayList<Cell>();

		try {
			search = new SecureRegionScanner(searchCondition, env, player,
					this.searchConf, true, col);
			LOG.debug("Iterating over RegionScanner to find match");
			search.next(results);
			search.close();
			LOG.debug("Found match " + results);
		} catch (NotServingRegionException e) {
			LOG.debug("Region was closed");
			LOG.error(e);
			return results;
		} catch (IOException e) {
			LOG.error(e);
			throw new IllegalStateException(e);
		}

		return results;

	}

	private RegionScanner secretScanSearch(byte[] startRow, byte[] stopRow,
			OperationWithAttributes op, RegionCoprocessorEnvironment env,
			Filter filter) throws IOException, ResultsLengthMismatch,
			ResultsIdentifiersMismatch {

		Column col = getSearchColumn(op);
		RequestIdentifier ident = getRequestIdentifier(op, env);
		relay.registerRequest(ident);
		Player player = getPlayer(ident);
		int nbits = this.searchConf.getnBits();
		int targetPlayer = checkTargetPlayer(player, op, ident);

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
				this.searchConf, false, col);

	}

	private List<Cell> getRowWithoutSearch(byte[] rowID) {
		// Get that bypasses this observer or it will call preGetOP again.
		Get get = new Get(rowID);
		try {
			return env.getRegion().get(get, false);
		} catch (IOException e) {
			LOG.debug(e);
			throw new IllegalStateException(e);
		}
	}

	private boolean isProtectedColumn(OperationWithAttributes op) {
		byte[] isProtected = op
				.getAttribute(OperationAttributesIdentifiers.ProtectedColumn);
		return isProtected != null;
	}

	@Override
	public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> e,
			final Get get, final List<Cell> results) {

		boolean protectedColumn = isProtectedColumn(get);
		LOG.debug(new BigInteger(get.getRow()) + " region name is "
				+ env.getRegion().getRegionNameAsString() + " with state "
				+ env.getRegion().isAvailable() + " - "
				+ env.getRegion().isClosed() + " - "
				+ env.getRegion().isClosing());

		/* *
		 * Verifies if the get is going to process over a protected columns. The
		 * systems is only considering a get over a single protected column.
		 */
		if (protectedColumn && env.getRegion().isAvailable()) {
			LOG.debug("Protected column");

			String table = env.getRegion().getTableDesc().getNameAsString();

			if (!table.contains("hbase")) {

				LOG.debug("preGetOp evaluated on table " + table);

				byte[] row = get.getRow();

				try {

					byte[] directAccess = get
							.getAttribute(OperationAttributesIdentifiers.DirectAccess);
					LOG.debug("DirectAccess " + directAccess + " getRow "
							+ new BigInteger(row));

					if (directAccess == null) {
						LOG.debug("Going to perform a secret search on data");
						List<Cell> searchResults = secretGetSearch(row, get,
								e.getEnvironment());

						results.addAll(searchResults);
					} else {
						// If the direct access row exists, it has the rowID to
						// retrieve.
						LOG.debug("Going to direct row access");
						List<Cell> res = getRowWithoutSearch(directAccess);
						results.addAll(res);
					}
					e.bypass();

				} catch (ResultsLengthMismatch ex) {
					LOG.error(ex);
					throw new IllegalStateException(ex);
				} catch (ResultsIdentifiersMismatch ex) {
					LOG.error(ex);
					throw new IllegalStateException(ex);
				}

			}
		}
	}

	@Override
	public RegionScanner postScannerOpen(
			final ObserverContext<RegionCoprocessorEnvironment> c,
			final Scan scan, final RegionScanner s) {

		/* *
		 * Verifies if the Scan is going to process over a protected columns.
		 * The systems is only considering a Scan over a single protected
		 * column.
		 */
		boolean protectedColumn = isProtectedColumn(scan);

		if (protectedColumn) {
			LOG.debug("Protected column ");
			String table = env.getRegion().getTableDesc().getNameAsString();

			if (!table.contains("hbase")) {
				LOG.debug("postScannerOpen evaluated on " + table);
				try {

					byte[] startRow = scan.getStartRow();
					byte[] endRow = scan.getStopRow();

					if (startRow.length == 0) {
						LOG.debug("Starting row does not contain a value");
					} else {
						LOG.debug("Startting row is "
								+ new BigInteger(startRow));
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

				} catch (ResultsLengthMismatch ex) {
					LOG.error(ex);
					throw new IllegalStateException(ex);
				} catch (ResultsIdentifiersMismatch ex) {
					LOG.error(ex);
					throw new IllegalStateException(ex);
				} catch (IOException ex) {
					LOG.debug(ex);
					throw new IllegalStateException(ex);
				}
			}
		}

		return s;
	}

}
