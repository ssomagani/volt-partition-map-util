import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;

public class Application {
	
	static ClientConfig config;
	static ClientImpl client;

	public static void main(String[] args) throws Exception {
		
	    String hostStr = "localhost";
	    if(args.length != 0)
	        hostStr = args[0];
		
		config = new ClientConfig();
		config.setTopologyChangeAware(true);
		
		client = (ClientImpl) ClientFactory.createClient(config);
		connect(hostStr);
		
		// Calling the @Statistics to initialize the Hashinator 
		Object[] statsArgs = {"PROCEDURE", 0};
		client.callProcedure("@Statistics", statsArgs);
		
        Object[] partArgs = {"PLANNER", 0};
        ClientResponse resp = client.callProcedure("@Statistics", partArgs);
        VoltTable partSiteMapTable = resp.getResults()[0];
        HashMap<Long, String> partSiteMap = new HashMap<>();
        HashMap<Long, String> partHostMap = new HashMap<>();
        while(partSiteMapTable.advanceRow()) {
            partSiteMap.put(partSiteMapTable.getLong(4), partSiteMapTable.getLong(3) + "@" + partSiteMapTable.getString(2));
            partHostMap.put(partSiteMapTable.getLong(4), partSiteMapTable.getString(2));
        }
        
		Object[] topoArgs = {"TOPO", 0};
		ClientResponse topo = client.callProcedure("@Statistics", topoArgs);
        VoltTable topoTable = topo.getResults()[0];
        System.out.println("----------------------------------------------");
        System.out.println("| Host              Partition  Sites  Leader |");
        System.out.println("------------------ ---------- ------ ---------");
        while(topoTable.advanceRow()) {
            System.out.println(String.format("%1s %16s %10s %6s %7s %1s", "|", partHostMap.get(topoTable.getLong(0)), topoTable.getLong(0), topoTable.getString(1), topoTable.getString(2), "|"));
        }
        System.out.println("----------------------------------------------");
        
		if(!client.isHashinatorInitialized())
			throw new Exception("Hashinator not initialized. Cannot Proceed.");
		
        String partColType = args[1];
        String partColVal = args[2];

        long partId = client.getPartitionForParameter(VoltType.typeFromString(partColType).getValue(), partColVal);
        
		System.out.println("Partition column value " + partColVal + " --> PartitionId:SiteId@Hostname = " 
	+ partId + ":" + partSiteMap.get(partId));
	}
	
	private static void connect(String servers) throws InterruptedException {
		System.out.println("Connecting to VoltDB cluster ...");

		String[] serverArray = servers.split(",");
		final CountDownLatch connections = new CountDownLatch(serverArray.length);

		// use a new thread to connect to each server
		for (final String server : serverArray) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					connectToOneServerWithRetry(server);
					connections.countDown();
				}
			}).start();
		}
		// block until all have connected
		connections.await();
	}
	
	static void connectToOneServerWithRetry(String server) {
        int sleep = 1000;
        while (true) {
            try {
                client.createConnection(server);
                break;
            }
            catch (Exception e) {
                System.err.printf("Connection failed - retrying in %d second(s).\n", sleep / 1000);
                try { Thread.sleep(sleep); } catch (Exception interruted) {}
                if (sleep < 8000) sleep += sleep;
            }
        }
        System.out.printf("Connected to VoltDB node at: %s.\n", server);
    }
}
