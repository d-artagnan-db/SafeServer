package pt.uminho.haslab.smcoprocessors;

import pt.uminho.haslab.testingutils.ScanValidator;

public class ScanSearchWithStartKeyTest extends ScanSearchEndpointTest {

    public ScanSearchWithStartKeyTest() throws Exception {
        super();
    }

    @Override
    protected byte[] getStartKey(ScanValidator validator) {
        return validator.generateStartKey();
    }

    // return empty key;
    @Override
    protected byte[] getStopKey(ScanValidator validator) {
        return null;
    }

}
