package pt.uminho.haslab.smcoprocessors.middleware.discovery;

public class RegionLocation {

    private final int playerID;
    private final String ip;
    private final int port;

    public RegionLocation(int playerID, String ip, int port) {
        this.playerID = playerID;
        this.ip = ip;
        this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public int getPlayerID() {
        return playerID;
    }
}
