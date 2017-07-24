package pt.uminho.haslab.smcoprocessors.discovery;

public class RegionLocation {
    
    private final String ip;
    private final int port;
    
    public RegionLocation(String ip, int port){
        this.ip = ip;
        this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }
}
