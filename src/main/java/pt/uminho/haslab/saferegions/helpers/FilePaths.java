package pt.uminho.haslab.saferegions.helpers;

import java.net.URL;
import java.net.URLClassLoader;

public class FilePaths {

    public static String getPath(String filename){
        ClassLoader cl = ClassLoader.getSystemClassLoader();

        URL[] urls = ((URLClassLoader)cl).getURLs();

        for(URL url: urls){
            if(url.getFile().contains(filename)){
                return url.getFile();
            }
        }
        return null;
    }
}
