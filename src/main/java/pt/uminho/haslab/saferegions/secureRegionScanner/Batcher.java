package pt.uminho.haslab.saferegions.secureRegionScanner;

import pt.uminho.haslab.saferegions.SmpcConfiguration;

public class Batcher implements BatchOracle {

    private final SmpcConfiguration config;

    public Batcher(SmpcConfiguration config) {
        this.config = config;
    }

    public int batchSize() {
        return config.getBatchSize();
    }

}
