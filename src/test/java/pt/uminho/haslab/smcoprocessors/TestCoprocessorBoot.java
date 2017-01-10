package pt.uminho.haslab.smcoprocessors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Test;
import pt.uminho.haslab.testingutils.ShareCluster;
import static junit.framework.TestCase.assertEquals;

public class TestCoprocessorBoot {

	private final ShareCluster clusters;

	public TestCoprocessorBoot() throws Exception {

		List<String> resources = new ArrayList<String>();
		for (int i = 0; i < 3; i++) {
			resources.add("hbase-site-" + i + ".xml");
		}

		clusters = new ShareCluster(resources);

	}

	@After
	public void tearDown() throws IOException, InterruptedException {
		clusters.tearDown();
		// Wait for ports to be free for next tests
		Thread.sleep(30000);
	}

	@Test
	public void testSuccessfulBoot() {
		assertEquals(true, clusters.mastersAreActive());
	}
}
