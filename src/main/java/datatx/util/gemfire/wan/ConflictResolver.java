package datatx.util.gemfire.wan;

import java.util.Date;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.util.GatewayConflictHelper;
import com.gemstone.gemfire.cache.util.GatewayConflictResolver;
import com.gemstone.gemfire.cache.util.TimestampedEntryEvent;
import com.gemstone.gemfire.pdx.PdxInstance;

public class ConflictResolver implements GatewayConflictResolver, Declarable {
	private final Logger LOG = LogManager.getLogger(ConflictResolver.class);

	@Override
	public void onEvent(TimestampedEntryEvent event, GatewayConflictHelper helper) {
		if (event.getOperation() == Operation.UPDATE
				&& event.getNewDistributedSystemID() != event.getOldDistributedSystemID()) {
			if (event.getNewTimestamp() > event.getOldTimestamp()) {
				helper.changeEventValue(event.getNewValue());
				LOG.warn("Update conflict resolved for Region: " + event.getRegion().getName()
						+ " Key: " + event.getKey() + "/n"
						+ " New Timestamp: " + new Date(event.getNewTimestamp()) 
						+ " New Value: " + ((PdxInstance) event.getNewValue()).toString()
						+ "/n"
						+ " Old Timestamp: " + new Date(event.getOldTimestamp())  
						+ " Old Value: " + ((PdxInstance) event.getOldValue()).toString());
			}
		}
	}

	@Override
	public void init(Properties arg0) {
	}

}
