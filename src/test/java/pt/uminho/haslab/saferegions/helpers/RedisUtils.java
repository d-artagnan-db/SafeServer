package pt.uminho.haslab.saferegions.helpers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class RedisUtils {

    public static void initializeRedisContainer() throws IOException {
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            System.out.println("Sleep exception in initializeRedisContainer");
            throw new IllegalStateException(e);
        }
        String cmd = "docker stop teste-redis; docker rm teste-redis;docker run --name teste-redis -p 6379:6379 -d redis";
        StringBuffer output = new StringBuffer();

        try {
            Process p = Runtime.getRuntime().exec(new String[]{"csh", "-c", cmd});
            p.waitFor();
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(p.getInputStream()));

            String line = "";
            while ((line = reader.readLine()) != null) {
                output.append(line + "\n");
            }

        } catch (Exception e) {
            System.out.println("Exception " + e);
            e.printStackTrace();
        }

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            System.out.println("Sleep exception in initializeRedisContainer");
            throw new IllegalStateException(e);
        }
    }
}
