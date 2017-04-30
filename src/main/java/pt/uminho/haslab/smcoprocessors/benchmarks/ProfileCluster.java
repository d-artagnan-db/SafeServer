package pt.uminho.haslab.smcoprocessors.benchmarks;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import pt.uminho.haslab.testingutils.ShareCluster;


public class ProfileCluster {
    
    
    
    public static void main(String[] args) throws Exception{
        List<String> resources = new ArrayList<String>();

		for (int i = 0; i < 3; i++) {
			resources.add("hbase-site-" + i + ".xml");

		}

		ShareCluster clusters = new ShareCluster(resources);
		Thread.sleep(30000);

        Scanner reader = new Scanner(System.in);  // Reading from System.in
        System.out.println("Enter a number: ");
        int n = reader.nextInt();
        
        clusters.tearDown();
        
        
    }
    
}
