package pt.uminho.haslab.saferegions.helpers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import pt.uminho.haslab.safemapper.DatabaseSchema;
import pt.uminho.haslab.safemapper.Family;
import pt.uminho.haslab.safemapper.Qualifier;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.saferegions.OperationAttributesIdentifiers;
import pt.uminho.haslab.smpc.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smpc.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smpc.interfaces.Dealer;
import pt.uminho.haslab.smpc.sharemindImp.IntSharemindDealer;
import pt.uminho.haslab.smpc.sharemindImp.SharemindDealer;
import pt.uminho.haslab.smpc.sharemindImp.SharemindSharedSecret;
import pt.uminho.haslab.testingutils.ClusterScanResult;
import pt.uminho.haslab.testingutils.ClusterTables;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static pt.uminho.haslab.safemapper.DatabaseSchema.CryptoType.SMPC;

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


                   byte[] val  = CellUtil.cloneValue(cell.get(0));

                   if (qual.getCryptoType() == SMPC || qual.getCryptoType() == DatabaseSchema.CryptoType.ISMPC) {
                       try {
                           String safeQual = qual.getName();
                           byte[] bSafeQual = safeQual.getBytes();
                           Dealer dealer = new SharemindDealer(qual.getFormatSize());

                           if (qual.getCryptoType() == DatabaseSchema.CryptoType.ISMPC) {
                               int value = ByteBuffer.wrap(val).getInt();
                               IntSharemindDealer intSharemindDealer = new IntSharemindDealer();
                               int[] secrets = intSharemindDealer.share(value);
                               Put[] puts = new Put[3];
                               puts[0] = p1;
                               puts[1] = p2;
                               puts[2] = p3;
                               //LOG.debug("Inserted value " + value + " with secrets " + Arrays.toString(secrets));

                               for (int i = 0; i < secrets.length; i++) {
                                   int secretValue = secrets[i];
                                   Put p = puts[i];
                                   ByteBuffer byteBuffer = ByteBuffer.allocate(4);
                                   byteBuffer.putInt(secretValue);
                                   byteBuffer.flip();
                                   p.add(cfb, bSafeQual, byteBuffer.array());
                               }


                           } else {
                               //LOG.info("Inserting as BigInteger");
                               BigInteger bigVal = new BigInteger(val);
                               //LOG.debug(safeQual + " - Putting secure value "+ bigVal);
                               SharemindSharedSecret secret = (SharemindSharedSecret) dealer
                                       .share(bigVal);

                               p1.add(cfb, bSafeQual, secret.getU1().toByteArray());
                               p2.add(cfb, bSafeQual, secret.getU2().toByteArray());
                               p3.add(cfb, bSafeQual, secret.getU3().toByteArray());
                           }
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
            return super.scan(oScan);
	    }else{
	        Filter originalFilter = oScan.getFilter();
	        List<Filter> parsedFilters = handleFilterWithProtectedColumns(originalFilter);
            List<Scan> scans = plainScans();
            assert parsedFilters != null;

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
            resultInnerFilters.add(handleFilterWithProtectedColumns(f));
        }

        List<Filter>  results = new ArrayList<Filter>();

        for(int i = 0; i < 3; i++){
            FilterList fRes = new FilterList(filter.getOperator());

            for(List<Filter> handledFilter: resultInnerFilters ){
                fRes.addFilter(handledFilter.get(i));
            }
            results.add(fRes);
        }
        return results;
    }

    private List<Filter> handleSingleColumnValueFilter(SingleColumnValueFilter filter){

        byte[] family = filter.getFamily();
        byte[] qualifier = filter.getQualifier();
        byte[] value = filter.getComparator().getValue();
        CompareFilter.CompareOp operator = filter.getOperator();
        List<Filter> fList = new ArrayList<Filter>();

        if (DatabaseSchema.isProtectedColumn(schema, family, qualifier)) {
            String sFamily = new String(family);
            String sQualifier = new String(qualifier);

            DatabaseSchema.CryptoType type = schema.getCryptoTypeFromQualifier(sFamily, sQualifier);
            if (type == SMPC) {
                int formatSize = schema.getFormatSizeFromQualifier(sFamily, sQualifier);
                try {
                    Dealer dealer = new SharemindDealer(formatSize);
                    byte[] sQualifierMod = sQualifier.getBytes();
                    BigInteger bigVal = new BigInteger(value);
                    SharemindSharedSecret secret = (SharemindSharedSecret) dealer.share(bigVal);
                    fList.add(new SingleColumnValueFilter(family, sQualifierMod, operator, secret.getU1().toByteArray()));
                    fList.add(new SingleColumnValueFilter(family, sQualifierMod, operator, secret.getU2().toByteArray()));
                    fList.add(new SingleColumnValueFilter(family, sQualifierMod, operator, secret.getU3().toByteArray()));

                    LOG.debug("Filter value is encoded in " + secret.getU1() + " : " + secret.getU2() + " : " + secret.getU3());
                } catch (InvalidNumberOfBits | InvalidSecretValue ex) {
                    LOG.debug(ex);
                    throw new IllegalStateException(ex);
                }
            } else {
                IntSharemindDealer dealer = new IntSharemindDealer();
                try {
                    int iValue = ByteBuffer.wrap(value).getInt();
                    int[] secrets = dealer.share(iValue);
                    byte[] sQualifierMod = sQualifier.getBytes();

                    for(int i = 0; i < 3; i++){
                        int secret = secrets[i];
                        ByteBuffer byteBuffer  = ByteBuffer.allocate(4);
                        byteBuffer.putInt(secret);
                        byteBuffer.flip();
                        fList.add(new SingleColumnValueFilter(family, sQualifierMod, operator, byteBuffer.array()));
                    }
                    //LOG.debug("Value to search " + iValue +  " encoded in  integer secrets " + Arrays.toString(secrets));

                } catch (InvalidSecretValue invalidSecretValue) {
                    throw new IllegalStateException(invalidSecretValue);
                }
            }
        }else{
            fList.add(filter);
            fList.add(filter);
            fList.add(filter);
        }

        return fList;
    }
}
