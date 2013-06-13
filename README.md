# WowzaMediaServer VHost-capable LoadBalancer

## Description

The "VHost Load Balancer" module extends the regular LoadBalancer-
module for Wowza Media Server with the following functionality:

 * VHost support (the old one redirects to one IP address per server.)
 * Load balancing based on bandwidth instead of total connections.
 * "Weight"-option per server, to support a mix of inhomogeneous servers.
 * Redirecting HTTP- (HLS/Cupertino + HDS/San Jose) and RTSP-connections.
 * Supports DVR-streams as of version 2.0b (June 2013).

## Important notice

This code is still only briefly tested. Please use this module with
caution, and be sure to install it in a lab environment before
considering it for production use.

To make debugging / troubleshooting easier, change logging type from
"INFO" to "DEBUG" in log4j.properties. (Don't use this setting in
production - your log files will become *very* verbose.)

Feel free to send any questions, suggestions or comments to me on
github -- at -- segfault.no.

## Changelog

### 2013-06-13 - Version 2.0 (beta) - Support for Wowza 3.6.x and DVR
 * Support for Wowza 3.6.x, which changed the way sessionIds are handled
   in the playlists for Cupertino streaming.
   Redirecting from Wowza < 3.6.x to newer edge server seems to work, but
   redirecting from Wowza >= 3.6.x to older edge servers will *not* work,
   due to the new sessionId handling.
 * Bugfix in the HTTP redirect modules: redirectAppName and redirectPort
   would only be read once, and used for all other applications.
   Thanks to Gelencsér István for reporting this issue.
 * Support for DVR streams, which require that session Ids are valid for
   the edge servers. Thanks to Thomas Swedin for reporting this problem.

### 2012-11-15

 * Added support for SMIL files
 * Updated the code to work with HTTP playlists (cupertino + san jose) in
   Wowza Media Server 3.5.0, which was released on November 9th.
 * Uploaded a precompiled .jar-file for 3.5.0, and renamed the old .jar-file.
   No configuration changes are required, but the old file, named
   'availo-vhostloadbalancer.jar', needs to be replaced with the new file.

### 2012-12-05 - bugfixes, new license and new features

 * All files that have been rewritten are now using Apache 2 License.
   (The exceptions are the two ServerListener-classes)
 * Added an optional "?redirect=true" parameter for RTMP/RTSP/HTTP requests.
   Will allow clients to get redirected when redirectOnConnect = false.
 * Complete rewrite of the HTTP redirect. Now using a ConfigCache to allow
   redirecting based on the config properties in Application.xml
 * redirectOnConnect, redirectAppName and redirectPort is now respected.
 * Removed the old 3.1.2 .jar-file and renamed the 3.5.0-file. The current
   availo-vhost-loadbalancer-3.jar" file should hopefully work on both/all
   current updates of Wowza Media Server 3.x.

## Prerequisites

### Supported wowza versions
The module has been tested with Wowza versions 3.1.2, 3.5.x and 3.6.2,
but only 3.5.x and 3.6.2 are currently actively tested by me.

Wowza 2.x is currently not supported, and will require modifications
to work. There are no plans to support Wowza 2.x, due to lack of a valid
license for this platform.

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
Copy lib/availo-vhost-loadbalancer-3.jar to the [install-dir]/lib/ folder of Wowza.

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

#### Step 4a
If the "Get least loaded server using Netconnection redirect"-method will be used,
or you want to redirect HDS (San Jose) or HLS (Cupertino) streams, change the
ModuleLoadBalancerRedirector-module in all relevant Application.xml files
to the following:
```xml
<Class>com.availo.wms.plugin.vhostloadbalancer.ModuleLoadBalancerRedirector</Class>
```


Please note that the "redirectScheme" property is deprecated in the VHost
LoadBalancer, and will *not* be used at all for any incoming HTTP or RTSP
connections. Currently 'http://' is hardcoded as the protocol for "cupertino"
and "san jose"-connections.

redirectAppName, redirectPort and redirectOnConnect is supported, however.

If "redirectOnConnect" is disabled, clients that should be redirected can pass
along a "?redirect=true" parameter. (Only in the netConnect call for RTMP.)

#### Step 4b
In addition to the ModuleLoadBalancerRedirector, you will need a module that
adds absolute URLs for the edge-server to the Playlist.m3u8 and manifest.f4m
files in order to make HTTP streaming work as intended.

Without this change, flowplayer and OSMF will keep directing all requests
to the loadbalancer.

This is done by replacing the following two lines in HTTPStreamers.xml for *all*
VHosts you wish to use the load balancer with:

```xml
<!--<BaseClass>com.wowza.wms.httpstreamer.cupertinostreaming.httpstreamer.HTTPStreamerAdapterCupertinoStreamer</BaseClass>-->
<BaseClass>com.availo.wms.httpstreamer.HTTPStreamerAdapterCupertinoRedirector</BaseClass>
```

```xml
<!--<BaseClass>com.wowza.wms.httpstreamer.sanjosestreaming.httpstreamer.HTTPStreamerAdapterSanJoseStreamer</BaseClass>-->
<BaseClass>com.availo.wms.httpstreamer.HTTPStreamerAdapterSanJoseRedirector</BaseClass>
```


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
<Value>com.availo.wms.plugin.vhostloadbalancer.LoadBalancerMonitorVHost</Value>
```


At this point, the basic functionality of the load balancer should be working.

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

