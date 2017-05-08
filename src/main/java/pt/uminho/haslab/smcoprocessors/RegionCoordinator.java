package pt.uminho.haslab.smcoprocessors;

import java.io.IOException;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.RegionPlan;


public class RegionCoordinator extends BaseMasterObserver {
	private static final Log LOG = LogFactory.getLog(RegionCoordinator.class
			.getName());

  @Override
  public void start(CoprocessorEnvironment ctx) throws IOException {
      System.out.println("Start");
    LOG.debug("start Coprocessor");
    
    HTableInterface hti = ctx.getTable(TableName.META_TABLE_NAME);
    
  }

    @Override
   public void preCreateTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException{
       //ctx.getEnvironment().getMasterServices().getAssignmentManager().getRegionStates().getRegionsOfTable(TableName.META_TABLE_NAME).get(0).getRegion
      System.out.println("preCreateTAble");
      LOG.debug("preCreateTable");
      LOG.debug("HTableDescription "+ desc.toString());
      for(HRegionInfo info: regions){
          LOG.debug("RegionInfo "+ info.toString());
      }

   }
   
    @Override
   public void postCreateTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException{
       System.out.println("postCreateTAble");
      LOG.debug("postCreateTable");
      LOG.debug("HTableDescription "+ desc.toString());
      for(HRegionInfo info: regions){
          
          LOG.debug("RegionInfo "+ info.toString());
      }
   }
  
  @Override
  public void preMasterInitialization(
      ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
      System.out.println("preMasterInitialization");
      LOG.debug("preMasterInitialization");
  }

  @Override
  public void preAssign(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo regionInfo) throws IOException {
      System.out.println("PreAssign");
      LOG.debug("Going to pre assign");
      LOG.debug("preAssign " + regionInfo.getRegionNameAsString());
  }
  
  @Override
  public void postAssign(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo regionInfo) throws IOException {
      System.out.println("PostAssign");
      LOG.debug("Going to post assign");
      LOG.debug("postAssign " + regionInfo.getRegionNameAsString());
  }
  
  @Override
  public void preMove(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo region, ServerName srcServer, ServerName destServer) throws IOException {
      System.out.println("Going to preMove");
      LOG.debug("Going to preMove");
      LOG.debug("preMove Region " + region.getRegionNameAsString());
      LOG.debug("preMove srcServer " + srcServer);
      LOG.debug("preMove destServer " + destServer);
  }
 @Override
  public void postMove(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final HRegionInfo region, final ServerName srcServer,
      final ServerName destServer) throws IOException {
      System.out.println("PostMove");
      LOG.debug("PostMove");
      
  }
  
  @Override
  public void preBalance(final ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException{
      System.out.println("PreBalance");
      
      LOG.debug("PreBalance");
  };
  
  @Override
  public void postBalance(final ObserverContext<MasterCoprocessorEnvironment> ctx, List<RegionPlan> plans)
      throws IOException{
      System.out.println("PreBalance");
      LOG.debug("PreBalance");
  };
  
}
