package pt.uminho.haslab.smcoprocessors.helpers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import pt.uminho.haslab.safemapper.DatabaseSchema;
import pt.uminho.haslab.safemapper.Family;
import pt.uminho.haslab.safemapper.Qualifier;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.smcoprocessors.OperationAttributesIdentifiers;
import pt.uminho.haslab.smcoprocessors.SmpcConfiguration;
import pt.uminho.haslab.smcoprocessors.secureRegionScanner.HandleSafeFilter;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.interfaces.Dealer;
import pt.uminho.haslab.smhbase.interfaces.SharedSecret;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindDealer;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSharedSecret;
import pt.uminho.haslab.testingutils.ClusterScanResult;
import pt.uminho.haslab.testingutils.ClusterTables;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestClusterTables extends ClusterTables {

	static final Log LOG = LogFactory.getLog(TestClusterTables.class.getName());

	private boolean usesMPC;
	private TableSchema schema;

	public TestClusterTables(List<Configuration> configs, TableName tbname)
			throws IOException {
		super(configs, tbname);
	}


	public void usesMpc(){
		this.usesMPC = true;
	}

	public void setSchema(TableSchema schema){
	    this.schema = schema;
    }


	public ClusterTables put(Put put) throws InterruptedIOException, RetriesExhaustedWithDetailsException {

		if(!usesMPC){
			for (HTable table : tables) {
				table.put(put);
			}
		}else{

		    //assuming row identifiers are not protected with secret sharing, just columns

            byte[] identifier = put.getRow();
            Put p1 = new Put(identifier);
            Put p2 = new Put(identifier);
            Put p3 = new Put(identifier);

            for(Family fam: schema.getColumnFamilies()){
               for(Qualifier qual: fam.getQualifiers()){
                   byte[] cfb = fam.getFamilyName().getBytes();
                   byte[] cqb = qual.getName().getBytes();
                   List<Cell> cell = put.get(cfb, cqb);


                   //System.out.println("Cell size is "+ cell.size());
                   byte[] val  = CellUtil.cloneValue(cell.get(0));

                   if(qual.getCryptoType() == DatabaseSchema.CryptoType.SMPC){
                       try {
                           String safeQual = qual.getName();
                           byte[] bSafeQual = safeQual.getBytes();
                           //System.out.println("Format size is " + qual.getFormatSize());
                           //System.out.println("Value size is " + val.length);
						   System.out.println("bufer input is " +val);
                           Dealer dealer = new SharemindDealer(qual.getFormatSize());

						   BigInteger bigVal = new BigInteger(val);
						   System.out.println("put BigVal is "+ bigVal);
                           SharemindSharedSecret secret = (SharemindSharedSecret) dealer
                                   .share(bigVal);
                           System.out.println("Val  "+ new BigInteger(val) + " is encoded in secrets " + secret.getU1() + " : " + secret.getU2() + " : " + secret.getU3());

                           p1.add(cfb, bSafeQual, secret.getU1().toByteArray());
                           p2.add(cfb, bSafeQual, secret.getU2().toByteArray());
                           p3.add(cfb, bSafeQual, secret.getU3().toByteArray());
                       } catch (InvalidNumberOfBits invalidNumberOfBits) {
                           LOG.debug("Number of bytes is not valid "+ qual.getFormatSize());
                           throw new IllegalStateException(invalidNumberOfBits);
                       } catch (InvalidSecretValue invalidSecretValue) {
                           LOG.debug("Invalid secret value");
                           throw new IllegalStateException(invalidSecretValue);
                       }
                       String originalVaLQual = qual.getName() + "_original";
                       byte[] bOriginalValQual = originalVaLQual.getBytes();

                       p1.add(cfb, bOriginalValQual, val);
                       p2.add(cfb, bOriginalValQual, val);
                       p3.add(cfb, bOriginalValQual, val);

                   }else{
                       p1.add(cfb, cqb, val);
                       p2.add(cfb, cqb, val);
                       p3.add(cfb, cqb, val);
                   }
               }
            }
            tables.get(0).put(p1);
            tables.get(1).put(p2);
            tables.get(2).put(p3);
		}

		return this;
	}


    public ClusterScanResult scan(Scan oScan, boolean vanilla) throws IOException, InterruptedException {
	    if(vanilla){
	    	System.out.println(oScan);
	    	System.out.println(oScan.getFilter());
            return super.scan(oScan);
	    }else{
	        Filter originalFilter = oScan.getFilter();
	        List<Filter> parsedFilters = handleFilterWithProtectedColumns(originalFilter);
            List<Scan> scans = plainScans();
            System.out.println(parsedFilters);
            assert parsedFilters != null;

            for(Filter f: parsedFilters){
                System.out.println(f);
            }
            for (int i = 0; i < scans.size(); i++) {
                Scan scan = scans.get(i);
                scan.setFilter(parsedFilters.get(i));
                scan.setAttribute(OperationAttributesIdentifiers.RequestIdentifier, "1".getBytes());
                scan.setAttribute(OperationAttributesIdentifiers.TargetPlayer, "1".getBytes());
                scan.setAttribute(OperationAttributesIdentifiers.ScanType.ProtectedColumnScan.name(), "true".getBytes());
            }
            return scan(scans);
        }

    }

    private List<Scan> plainScans(){
        List<Scan> scans = new ArrayList<Scan>();

        Scan scanC1 = new Scan();
        Scan scanC2 = new Scan();
        Scan scanC3 = new Scan();

        scans.add(scanC1);
        scans.add(scanC2);
        scans.add(scanC3);
        return scans;
    }


    private List<Filter> handleFilterWithProtectedColumns(Filter original){

	    if(original instanceof SingleColumnValueFilter){
            return handleSingleColumnValueFilter((SingleColumnValueFilter) original);
        }else if( original instanceof FilterList){
	    	return handleFilterList((FilterList) original);
		}else if(original instanceof WhileMatchFilter){
            return handleWhileMatchFilter((WhileMatchFilter) original);
        }
        return null;
    }

    private List<Filter> handleWhileMatchFilter(WhileMatchFilter original){
        Filter f = original.getFilter();
        List<Filter> handledFilter = handleFilterWithProtectedColumns(f);

        List<Filter> resFilters  = new ArrayList<Filter>();

        assert handledFilter != null;
        for(Filter filt: handledFilter){
            resFilters.add(new WhileMatchFilter(filt));
        }

        return resFilters;
    }


    private List<Filter> handleFilterList(FilterList filter){
        List<Filter>  list = filter.getFilters();
        List<List<Filter>> resultInnerFilters = new ArrayList<List<Filter>>();

        assert list != null;
        for(Filter f: list){
            System.out.println("Handling inner filters");
            resultInnerFilters.add(handleFilterWithProtectedColumns(f));
        }

        List<Filter>  results = new ArrayList<Filter>();

        for(int i = 0; i < 3; i++){
            System.out.println("Creating resulting filter "+i);
            FilterList fRes = new FilterList(filter.getOperator());

            for(List<Filter> handledFilter: resultInnerFilters ){

                if(handledFilter.get(i) instanceof SingleColumnValueFilter){
                    SingleColumnValueFilter f = (SingleColumnValueFilter) handledFilter.get(i);
                    System.out.println("Filter in pos "+ i + " gets value " + new BigInteger(f.getComparator().getValue()));
                }
                fRes.addFilter(handledFilter.get(i));

            }
            results.add(fRes);
        }
        System.out.println("Return resulting filter");
        return results;
    }

    private List<Filter> handleSingleColumnValueFilter(SingleColumnValueFilter filter){

        byte[] family = filter.getFamily();
        byte[] qualifier = filter.getQualifier();
        byte[] value = filter.getComparator().getValue();
        CompareFilter.CompareOp operator = filter.getOperator();
        List<Filter> fList = new ArrayList<Filter>();

        if(HandleSafeFilter.isProtectedColumn(schema, family, qualifier)){
            String sFamily = new String(family);
            String sQualifier = new String(qualifier);

            int formatSize = schema.getFormatSizeFromQualifier(sFamily, sQualifier);
            try {
                Dealer dealer = new SharemindDealer(formatSize);
                byte[] sQualifierMod = sQualifier.getBytes();
                BigInteger bigVal = new BigInteger(value);
                SharemindSharedSecret secret = (SharemindSharedSecret) dealer.share(bigVal);
                System.out.println("Filter value " + bigVal + " is encoded in secrets " + secret.getU1() + " : " + secret.getU2() + " : " + secret.getU3());
                fList.add(new SingleColumnValueFilter(family, sQualifierMod, operator, secret.getU1().toByteArray()));
                fList.add(new SingleColumnValueFilter(family, sQualifierMod, operator, secret.getU2().toByteArray()));
                fList.add(new SingleColumnValueFilter(family, sQualifierMod, operator, secret.getU3().toByteArray()));
            } catch (InvalidNumberOfBits invalidNumberOfBits) {
                LOG.debug(invalidNumberOfBits);
                throw new IllegalStateException(invalidNumberOfBits);
            } catch (InvalidSecretValue invalidSecretValue) {
                LOG.debug(invalidSecretValue);
                throw new IllegalStateException(invalidSecretValue);
            }
        }else{
            fList.add(filter);
            fList.add(filter);
            fList.add(filter);
        }

        return fList;
    }


    /*
         * Returns -1 if no match is found on the search. Otherwise it returns the
         * row key
         */
	public BigInteger equalScanEndpoint(SharedSecret cmpValue, int requestID,
			String secretFamily, String secretQualifier,
			SmpcConfiguration config) throws Throwable {
		LOG.debug(" Val " + cmpValue.unshare() + " request " + requestID);

		int playerID = 1;
		SharemindSharedSecret sSecret = (SharemindSharedSecret) cmpValue;
		List<BigInteger> value = new ArrayList<BigInteger>();
		value.add(sSecret.getU1());
		value.add(sSecret.getU2());
		value.add(sSecret.getU3());
		List<Scan> scans = new ArrayList<Scan>();

		Scan scanC1 = new Scan();
		Scan scanC2 = new Scan();
		Scan scanC3 = new Scan();

		scans.add(scanC1);
		scans.add(scanC2);
		scans.add(scanC3);
		byte[] requestIDba = ("" + requestID).getBytes();
		byte[] playerIDba = ("" + playerID).getBytes();

		for (int i = 0; i < scans.size(); i++) {
			Scan scan = scans.get(i);
			scan.setAttribute(OperationAttributesIdentifiers.TargetPlayer,
					playerIDba);
			scan.setAttribute(OperationAttributesIdentifiers.RequestIdentifier,
					requestIDba);
			scan.setAttribute(OperationAttributesIdentifiers.ProtectedColumn,
					"true".getBytes());
			scan.setAttribute(OperationAttributesIdentifiers.SecretFamily,
					secretFamily.getBytes());
			scan.setAttribute(OperationAttributesIdentifiers.SecretQualifier,
					secretQualifier.getBytes());
			scan.setAttribute(OperationAttributesIdentifiers.ScanForEqualVal,
					value.get(i).toByteArray());
		}
		ClusterScanResult results = this.scan(scans);
		results.setColumnFamily(secretFamily.getBytes());
		results.setColumnQualifier(secretQualifier.getBytes());
		List<Result> decResults = results.getDecodedResults();
		LOG.debug("Results size is " + decResults);
		if (!results.isConsistant()) {
			throw new IllegalStateException(
					"One Result was empty but the others" + "were not");
		}

		if (decResults.isEmpty()) {
			return BigInteger.valueOf(-1);
		} else {
			LOG.debug("DecResults is " + decResults.get(0));
			LOG.debug("DecResults  BigInteger is "
					+ new BigInteger(decResults.get(0).getRow()));

			return new BigInteger(decResults.get(0).getRow());
		}
	}

	protected void scanSetRow(Dealer dealer, List<Scan> scans, byte[] row,
			String attribute) throws InvalidSecretValue {
		if (row != null) {
			SharemindSharedSecret startSharedSecret = (SharemindSharedSecret) dealer
					.share(new BigInteger(row));
			List<BigInteger> secrets = new ArrayList<BigInteger>();
			secrets.add(startSharedSecret.getU1());
			secrets.add(startSharedSecret.getU2());
			secrets.add(startSharedSecret.getU3());

			for (int i = 0; i < scans.size(); i++) {
				Scan scan = scans.get(i);
				scan.setAttribute(attribute, secrets.get(i).toByteArray());

			}
		}
	}

	private List<Scan> getScans() throws InvalidSecretValue {
		List<Scan> scans = new ArrayList<Scan>();
		scans.add(new Scan());
		scans.add(new Scan());
		scans.add(new Scan());
		return scans;
	}

	private void setScanAttributes(List<Scan> scans, byte[] playerID,
			byte[] requestID, byte[] protectedColumn, byte[] secretFamily,
			byte[] secretQualifier) {
		for (Scan scan : scans) {
			scan.setAttribute(OperationAttributesIdentifiers.TargetPlayer,
					playerID);
			scan.setAttribute(OperationAttributesIdentifiers.RequestIdentifier,
					requestID);
			scan.setAttribute(OperationAttributesIdentifiers.ProtectedColumn,
					protectedColumn);
			scan.setAttribute(OperationAttributesIdentifiers.SecretFamily,
					secretFamily);
			scan.setAttribute(OperationAttributesIdentifiers.SecretQualifier,
					secretQualifier);
		}
	}

	public List<Result> scanEndpoint(byte[] startRow, byte[] stopRow,
			int requestID, SmpcConfiguration config, Dealer dealer,
			String secretFamily, String secretQualifier) throws IOException,
			InterruptedException, InvalidSecretValue {

		int playerID = 1;
		LOG.debug("Start row are " + Arrays.toString(startRow) + " / "
				+ Arrays.toString(stopRow));

		List<Scan> scans = getScans();
		byte[] requestIDba = ("" + requestID).getBytes();
		byte[] playerIDba = ("" + playerID).getBytes();
		setScanAttributes(scans, playerIDba, requestIDba, "true".getBytes(),
				secretFamily.getBytes(), secretQualifier.getBytes());
		scanSetRow(dealer, scans, startRow,
				OperationAttributesIdentifiers.ScanStartVal);
		scanSetRow(dealer, scans, stopRow,
				OperationAttributesIdentifiers.ScanStopVal);
		ClusterScanResult results = this.scan(scans);

		if (!results.isConsistant()) {
			String error = "One Result was empty but the others were not";
			LOG.debug(error);
			throw new IllegalStateException(error);
		}

		if (!results.notEmpty()) {
			LOG.debug("Results are empty");
			return new ArrayList<Result>();
		}

		results.setColumnFamily(secretFamily.getBytes());
		results.setColumnQualifier(secretQualifier.getBytes());
		return results.getDecodedResults();
	}

}
