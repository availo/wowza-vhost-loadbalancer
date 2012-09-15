# WowzaMediaServer VHost-capable LoadBalancer

## Important notice

This code is still only briefly tested. Please use this module with
caution, and be sure to install it in a lab environment before
considering it for production use.

Feel free to send any questions, suggestions or comments to me on
github -- at -- segfault.no

## Prerequisites:

### Original LoadBalancer 2.0
This module depends on the original LoadBalancer 2.0 in order to run:
http://www.wowza.com/forums/content.php?108

Pretty much all the code is based on the source files included in the
original addon.

### json-simple
Grab "json-simple" from http://code.google.com/p/json-simple/

The version used while developing this was json-simple-1.1.1.jar, but
newer versions should work as well.

### Optional: Wowza IDE 2 (for compiling / extending)
http://www.wowza.com/media-server/developers#wowza-ide

## Installing and configuring the module

#### Add the .jar-file to all servers (required on both the Listener and the Senders):
Copy lib/availo-vhostloadbalancer.jar to the [install-dir]/lib/ folder of Wowza
Media Server 2 or 3.

### Configuring the LoadBalancerListener

#### Step 1
Follow the instructions under "To setup a load balancer 'listener'" in
README.html from the original LoadBalancer 2.0 application.
Download this from http://www.wowza.com/forums/content.php?108 if you haven't
already done so.

#### Step 2
Change the &lt;ServerListener&gt;&lt;BaseClass&gt; in Server.xml (from "listener"-step 4
in the original README.html) to the following value:
```xml
<BaseClass>com.availo.wms.plugin.vhostloadbalancer.ServerListenerLoadBalancerListener</BaseClass>
```

The "loadBalancerListenerRedirectorClass"-property in Server.xml (also from
step 4 in the original documentation) on the LoadBalancer Listener needs to be
updated. New value:
```xml
<Value>com.availo.wms.plugin.vhostloadbalancer.LoadBalancerRedirectorBandwidth</Value>
```

#### Step 3
In every active VHost.xml file (as defined in VHosts.xml), change the
BaseClass (from step 5 in the original README.html) to the following value:
```xml
<BaseClass>com.availo.wms.plugin.vhostloadbalancer.HTTPLoadBalancerRedirector</BaseClass>
```

Change all instances of "enableServerInfoXML" to "enableServerInfo", as this
version has support for more output formats. (JSON)

If you only added the HTTPProvider to one VHost.xml file, you need to duplicate
the config in all your other active vhosts, since this is how the loadbalancer
can determine what VHost to redirect the client to.


### Configuring the LoadBalancerSenders

#### Step 1
Follow the instructions under "To setup a load balancer 'sender' on an 'edge'
server" from README.html in the original LoadBalancer 2.0 application.
Download this from http://www.wowza.com/forums/content.php?108 if you haven't
already done so.

#### Step 2
Change the &lt;ServerListener&gt;&lt;BaseClass&gt; (from step 3 in the original
README.html) to the following value:
```xml
<BaseClass>com.availo.wms.plugin.vhostloadbalancer.ServerListenerLoadBalancerSender</BaseClass>
```

The "loadBalancerSenderMonitorClass"-property, also in Server.xml, needs to be
updated as well. New value:
```xml
<Value>com.availo.wms.plugin.vhostloadbalancer.LoadBalancerMonitorVhost</Value>
```

#### Step 3 (optional)
While still in Server.xml, you may also add a different weight for the current
server. The server weight defaults to 1. Weight works by increasing the capacity
a particular server has, compared to other servers.

If all servers has the same capacity, skipping this option completely is fine.

To use a different weight, add the following property to the bottom of Server.xml:
```xml
<Property>
	<Name>loadBalancerSenderServerWeight</Name>
	<Value>5</Value>
	<Type>Integer</Type>
</Property>
```

In the example above, the weight of 5 would mean that a particular server can
handle 5 times the traffic compared to a server with the default weight of 1.

If you have three 1Gbps servers and assume that their bottleneck is the network,
and one 10Gbps server that can handle ~6Gbps, you would use a weight of 1 for
the three first servers, and a weight of 6 for the last one.

For two bundled 1Gbps interfaces on one server, you would use a weight of 2.

Please be adviced: a server can *not* have a weight of 0. The correct way
to handle this is to pause (or stop) the server, as described in README.html
from the original LoadBalancer 2.0 module.

#### Step 4 (optional)
If you *know* you will only use VHost-aware loadbalancers, you may remove the
loadBalancerSenderRedirectAddress-property in Server.xml, as this will never
be used when all senders and listeners support VHost-loadbalancing.

NB: I have not done extensive testing after removing this value completely.
If you want to be cautious, you may want to just remove the "[redirect-address]",
and leave it blank.

#### Step 5 (semi-optional)
Add a "loadBalancerVhostRedirectAddress" property to all active VHost.xml files:
```xml
<Property>
	<Name>loadBalancerVhostRedirectAddress</Name>
	<Value>[vhost-ip-address]</Value>
</Property>
```

This IP address should be the same as you have defined in the first <HostPort>
section for the respective VHost.

If this property is skipped, or left blank, the module will make an educated
guess on what IP address to use for this particular VHost. It will always
use the first IP address that listens to either port 80 or port 1935.

#### Step 6
If the "Get least loaded server using Netconnection redirect"-method will be used,
change the ModuleLoadBalancerRedirector-module in all relevant Application.xml files
to the following:
```xml
<Class>com.availo.wms.plugin.vhostloadbalancer.ModuleLoadBalancerRedirector</Class>
```

Please note that the "redirectScheme" property will *not* be used for any incoming
HTTP or RTSP connections. Currently 'http://' is hardcoded as the protocol for
"cupertino" and "san jose"-connections.

The same goes for "redirectAppName". This property will only be used by RTMP
connections, as both RTSP and HTTP requires the full URL when sending the
redirect. (RTMP makes a separate play()-call that the other protocols don't.)
