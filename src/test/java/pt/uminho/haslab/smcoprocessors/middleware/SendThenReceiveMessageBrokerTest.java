package pt.uminho.haslab.smcoprocessors.middleware;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.math.BigInteger;
import java.util.ArrayList;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.SecretSearch.ContextPlayer;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.TestRegionServer;
import pt.uminho.haslab.testingutils.ValuesGenerator;

@RunWith(Parameterized.class)
public class SendThenReceiveMessageBrokerTest {

	private final List<BigInteger> peerOne;

	private final List<BigInteger> peerTwo;

	private final List<BigInteger> peerThree;
	@Parameterized.Parameters
	public static Collection nbitsValues() {
		return ValuesGenerator.SendAndReceiveMessageBrokerTest2();
	}

	public SendThenReceiveMessageBrokerTest(List<BigInteger> peerOne,
			List<BigInteger> peerTwo, List<BigInteger> peerThree) {
		this.peerOne = peerOne;
		this.peerTwo = peerTwo;
		this.peerThree = peerThree;

	}

	private class RegionServer extends TestRegionServer {
		private final List<BigInteger> toSend;
		public List<BigInteger> recVal;

		public int playerSource;
		public int playerDest;

		public RegionServer(int playerID, List<BigInteger> toSend,
				int playerDest, int playerSource) throws IOException {
			super(playerID);
			this.toSend = toSend;
			this.playerSource = playerSource;
			this.playerDest = playerDest;
			recVal = new ArrayList<BigInteger>();
		}

		@Override
		public void doComputation() {
			long contextId = 0;
			List<ContextPlayer> players = new ArrayList<ContextPlayer>();

			for (BigInteger value : toSend) {
				byte[] reqID = ("" + contextId).getBytes();
				byte[] regionID = "1".getBytes();
				RequestIdentifier ident = new RequestIdentifier(reqID, regionID);
				ContextPlayer r = new ContextPlayer(relay, ident, playerID,
						broker);

				r.sendValueToPlayer(playerDest, value);
				contextId++;
				players.add(r);
			}

			for (ContextPlayer p : players) {
				BigInteger rec = p.getValue(this.playerSource);
				recVal.add(rec);
			}
		}
	}

	@Test
	public void testMessageRightRotate() throws InterruptedException,
			IOException {

		RegionServer a = new RegionServer(0, peerOne, 1, 2);

		RegionServer b = new RegionServer(1, peerTwo, 2, 0);

		RegionServer c = new RegionServer(2, peerThree, 0, 1);

		a.startRegionServer();
		b.startRegionServer();
		c.startRegionServer();

		a.stopRegionServer();
		b.stopRegionServer();
		c.stopRegionServer();

		validateMessageRightRotate(a, b, c);
		// Wait some time before next test
		Thread.sleep(5000);
	}

	// @Test
	public void testMessageLeftRotate() throws IOException,
			InterruptedException {

		RegionServer a = new RegionServer(0, peerOne, 2, 1);

		RegionServer b = new RegionServer(1, peerTwo, 0, 2);

		RegionServer c = new RegionServer(2, peerThree, 1, 0);

		a.startRegionServer();
		b.startRegionServer();
		c.startRegionServer();
		a.stopRegionServer();
		b.stopRegionServer();
		c.stopRegionServer();

		validateMessageLeftRotate(a, b, c);
		// Wait some time before next test
		Thread.sleep(10000);
	}

	private void validateMessageRightRotate(RegionServer a, RegionServer b,
			RegionServer c) {

		assertEquals(a.recVal.size(), peerThree.size());
		assertEquals(b.recVal.size(), peerOne.size());
		assertEquals(c.recVal.size(), peerTwo.size());

		for (int i = 0; i < a.recVal.size(); i++) {
			assertEquals(peerOne.get(i), b.recVal.get(i));
			assertEquals(peerThree.get(i), a.recVal.get(i));
			assertEquals(peerTwo.get(i), c.recVal.get(i));

		}
	}

	private void validateMessageLeftRotate(RegionServer a, RegionServer b,
			RegionServer c) {

		assertEquals(a.recVal.size(), peerTwo.size());
		assertEquals(b.recVal.size(), peerThree.size());
		assertEquals(c.recVal.size(), peerOne.size());

		for (int i = 0; i < a.recVal.size(); i++) {
			assertEquals(peerOne.get(i), c.recVal.get(i));
			assertEquals(peerThree.get(i), b.recVal.get(i));
			assertEquals(peerTwo.get(i), a.recVal.get(i));

		}
	}
}
