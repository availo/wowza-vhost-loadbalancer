/**
 * LoadBalancerMonitorVHost.java
 *
 *
 *    Copyright 2012 Brynjar Eide
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.availo.wms.plugin.vhostloadbalancer;

import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import com.wowza.wms.application.WMSProperties;
import com.wowza.wms.logging.WMSLoggerFactory;
//import com.wowza.wms.logging.*;
import com.wowza.wms.plugin.loadbalancer.*;
import com.wowza.wms.vhost.IVHost;
import com.wowza.wms.vhost.VHostSingleton;
import com.wowza.wms.vhost.HostPort;
import com.wowza.wms.vhost.HostPortList;
import com.wowza.wms.server.*;
import org.json.simple.JSONObject;

/**
 * Load Balancer Monitor with support for weighted servers and vhosts.
 * 
 * Uses bandwidth ("outTransferRate" as the measurement, and not connections,  since one server
 * could have hundreds of low-bandwidth mobile streaming clients, depending on the applications.
 * 
 * Based on the "LoadBalancerMonitorDefaultCustom" class found in the API documentation for LoadBalancer 2.0
 * (http://www.wowza.com/forums/content.php?108)
 * 
 * @author Brynjar Eide <brynjar@availo.no>
 * @version 2.0b, 2013-06-13
 *
 */
public class LoadBalancerMonitorVHost extends LoadBalancerMonitorDefault {
	public void appendToMessage(LoadBalancerSender loadBalancerSender, StringBuffer message) {
		boolean isDebugLog = WMSLoggerFactory.getLogger(LoadBalancerMonitorVHost.class).isDebugEnabled();
		super.appendToMessage(loadBalancerSender, message);

		// First find the global variables per server, in addition to the options handled by the original LoadBalancer module.
		Server server = Server.getInstance();
		WMSProperties props = server.getProperties();
		String serverWeight = props.getPropertyStr("loadBalancerSenderServerWeight", "1");
		// Store it in a HashMap for the time being, and then add it to the JSON object later on
		Map<String, Object> vhostProperties = new HashMap<String, Object>();

		// Then read the vhost properties, which will override the server defaults, if defined.
		List<?> vhostNames = VHostSingleton.getVHostNames();
		Iterator<?> vhostIterator = vhostNames.iterator();
		while (vhostIterator.hasNext()) {
			String vhostName = (String) vhostIterator.next();
			// Uncomment if you need even more debug output. This will produce way too much output, even for regular debugging.
			//WMSLoggerFactory.getLogger(LoadBalancerMonitorVhost.class).debug("LoadBalancerMonitorVHost.appendToMessage: VHost:\t" + vhostName);
			IVHost vhost = (IVHost)VHostSingleton.getInstance(vhostName);
			WMSProperties vhostprops = vhost.getProperties();

			String vhostRedirectAddress = vhostprops.getPropertyStr("loadBalancerVhostRedirectAddress", null);
			if (vhostRedirectAddress == null || vhostRedirectAddress == "") {
				// If no redirect address is defined for this VHost, parse the VHosts.xml file for the first IP address that listens to port 1935 *or* 80
				HostPortList hostPortList = vhost.getHostPortsList();
				for (int i = 0; i < hostPortList.size(); i++) {
					HostPort hostPort = hostPortList.get(i);
					if (hostPort.getPort() == 1935 || hostPort.getPort() == 80) {
						vhostRedirectAddress = hostPort.getAddressStr();
					}
					/*if (isDebugLog) {
						// This is just *too* much spam.
						//WMSLoggerFactory.getLogger(LoadBalancerMonitorVhost.class).debug("LoadBalancerMonitorVHost.appendToMessage: hostport debug: " + hostPort.getAddressStr() + ":" + hostPort.getPort());
					}*/
				}
			}
			Map<String, String> map = new HashMap<String, String>();
			map.put("redirectAddress", vhostRedirectAddress);
			// Disabled, since the benefits of adding a vhost-specific weight doesn't seem to outweigh (no pun intended) the added complexity.
			/*String vhostWeight = vhostprops.getPropertyStr("loadBalancerVhostWeight", null);
			map.put("weight", vhostWeight);*/
			vhostProperties.put(vhostName, map);
		}

		// Even though the default message is only pseudo-JSON, we'll use proper JSON encoded values for our custom properties.
		JSONObject customProperties = new JSONObject();

		// Eclipse will throw "Type safety" warnings here due to how json-simple is written (at least that's my understanding.)
		/*
		 * Contains all the properties found in the respective VHost.xml files
		 */
		customProperties.put("vhosts", vhostProperties);

		/*
		 *  Used to define how much a server can handle compared to the other servers.
		 *  A 10Ggbps could for example have a weight of 7, compared to a weight of 1 on an 1Gbps server.
		 *  (Which ideally would mean that the 10Gbps would serve 7Gbps when the 1Gbps server maxed out.)
		 */
		customProperties.put("weight", serverWeight);

		/*
		 * We'll use this to know where to redirect users, based on what vhost address they connect to on the load balancer.
		 */
		message.append("customProperties:" + customProperties + "\n");

		if (isDebugLog) {
			WMSLoggerFactory.getLogger(LoadBalancerMonitorVHost.class).debug("LoadBalancerMonitorVHost.appendToMessage: Properties: " + customProperties);
		}
	}
}
