package pt.uminho.haslab.smcoprocessors.benchmarks;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import pt.uminho.haslab.testingutils.ShareCluster;

public class ProfileBaseline {
    
    
    public static void main(String[] args) throws Exception{
        List<String> resources = new ArrayList<String>();

        resources.add("hbase-site.xml");

		ShareCluster clusters = new ShareCluster(resources, 3);
		//Thread.sleep(30000);
        clusters.createTables("usertable", "col1");
		Scanner reader = new Scanner(System.in); // Reading from System.in
		System.out.println("Enter a nsumber: ");
		int n = reader.nextInt();

		clusters.tearDown();
    }
    
}
