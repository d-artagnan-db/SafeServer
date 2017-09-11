package pt.uminho.haslab.smcoprocessors.secureRegionScanner;

import pt.uminho.haslab.smcoprocessors.SmpcConfiguration;

public class Batcher implements BatchOracle {

    private final SmpcConfiguration config;

    public Batcher(SmpcConfiguration config) {
        this.config = config;
    }

    public int batchSize() {
        return config.getBatchSize();
    }

}
