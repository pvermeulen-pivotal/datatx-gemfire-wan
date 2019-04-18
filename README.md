# GemFire WAN Conflict Resolver
## Gemire V8
An WAN gateway conflict resolver example. Checks that the operation is an update, old distributed-system-id is not equal to new distributed-sytem-id and uses the GatewayConflictHelper to change the event value to the new value.

### GemFire
<gateway-conflict-resolver>
    <class-name>datatx.util.gemfire.wan.ConflictResolver</class-name>
</gateway-conflict-resolver>

### Spring
<gfe:gateway-conflict-resolver ref="ConflictResolver" />

<bean id="ConflictResolver" class="datatx.util.gemfire.wan.ConflictResolver" />

### Server Log
Logs a conflict warning 
Update conflict resolved for Region ${region-name} Key: ${region-key}
New Timestamp: ${timestamp} New Value: ${object value}
Old Timestamp: ${timestamp} Old Value: ${object value}
