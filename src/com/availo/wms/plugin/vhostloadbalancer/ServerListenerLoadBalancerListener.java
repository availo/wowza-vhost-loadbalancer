/*
 * This file is almost completely unchanged, except for standard Eclipse-formatting,
 * and is just added for completeness' sake.
 */

package com.availo.wms.plugin.vhostloadbalancer;

import java.net.InetSocketAddress;

import org.apache.commons.modeler.*;

import com.wowza.wms.admin.*;
import com.wowza.wms.application.*;
import com.wowza.wms.logging.*;
import com.wowza.wms.server.*;
import com.wowza.wms.plugin.loadbalancer.*;

public class ServerListenerLoadBalancerListener implements IServerNotify2 {

	class LoadBalancerServerMonitor implements ILoadBalancerServerNotify {
		public void onServerCreate(LoadBalancerServer loadBalancerServer) {
			try {
				Registry.getRegistry(null, null).registerComponent(loadBalancerServer, AdminAgent.AGENT_DOMAINNAME + ":loadBalancer=LoadBalancer," + "loadBalancerServers=LoadBalancerServers,serverId=" + loadBalancerServer.getServerId() + ",name=LoadBalancerServer", loadBalancerServer.getClass().getName());
			} catch (Exception e) {
				WMSLoggerFactory.getLogger(LoadBalancerServerMonitor.class).error("LoadBalancerServerMonitor.onServerCreate: " + e.toString());
			}
		}

		public void onServerDestory(LoadBalancerServer loadBalancerServer) {
			try {
				Registry.getRegistry(null, null).unregisterComponent(AdminAgent.AGENT_DOMAINNAME + ":loadBalancer=LoadBalancer," + "loadBalancerServers=LoadBalancerServers,serverId=" + loadBalancerServer.getServerId() + ",name=LoadBalancerServer");
			} catch (Exception e) {
				WMSLoggerFactory.getLogger(LoadBalancerServerMonitor.class).error("LoadBalancerServerMonitor.onServerDestory: " + e.toString());
			}
		}
	}

	public static final String PROP_LOADBALANCERLISTENER = "WowzaProLoadBalancerListener";

	private LoadBalancerListener loadBalancerListener = null;

	public void onServerConfigLoaded(IServer server) {
	}

	public void onServerCreate(IServer server) {
	}

	public void onServerInit(IServer server) {
		WMSLoggerFactory.getLogger(ServerListenerLoadBalancerListener.class).info("ServerListenerLoadBalancerListener.onServerInit");

		WMSProperties props = server.getProperties();

		String key = "";
		String ipAddress = null;
		int port = 1934;
		String redirectorClass = null;
		int messageTimeout = 5000;

		key = props.getPropertyStr("loadBalancerListenerKey", key);
		ipAddress = props.getPropertyStr("loadBalancerListenerIpAddress", ipAddress);
		port = props.getPropertyInt("loadBalancerListenerPort", port);
		redirectorClass = props.getPropertyStr("loadBalancerListenerRedirectorClass", redirectorClass);
		messageTimeout = props.getPropertyInt("loadBalancerListenerMessageTimeout", messageTimeout);

		ILoadBalancerRedirector loadBalancerRedirector = null;
		if (redirectorClass != null) {
			try {
				Class loadBalancerRedirectorClass = Class.forName(redirectorClass);
				if (loadBalancerRedirectorClass != null)
					loadBalancerRedirector = (ILoadBalancerRedirector) loadBalancerRedirectorClass.newInstance();
			} catch (Exception e) {
				WMSLoggerFactory.getLogger(ServerListenerLoadBalancerSender.class).error("ServerListenerLoadBalancerListener.onServerInit[" + redirectorClass + "]: " + e.toString());
			}
		}

		if (loadBalancerRedirector == null) {
			loadBalancerRedirector = new LoadBalancerRedirectorBandwidth();
		}

		while (true) {
			if (ipAddress == null)
				break;
			ipAddress = ipAddress.trim();
			if (ipAddress.length() <= 0) {
				ipAddress = null;
				break;
			}
			if (ipAddress.equals("*")) {
				ipAddress = null;
				break;
			}
			break;
		}

		InetSocketAddress socketAddress = ipAddress == null ? new InetSocketAddress(port) : new InetSocketAddress(ipAddress, port);

		loadBalancerListener = new LoadBalancerListener();
		server.getProperties().put(PROP_LOADBALANCERLISTENER, loadBalancerListener);

		loadBalancerListener.setRedirector(loadBalancerRedirector);
		loadBalancerListener.setKey(key);
		loadBalancerListener.setSocketAddress(socketAddress);
		loadBalancerListener.addListener(new LoadBalancerServerMonitor());
		loadBalancerListener.setMessageTimeout(messageTimeout);

		loadBalancerListener.setPriority(Thread.NORM_PRIORITY);
		loadBalancerListener.setDaemon(true);
		loadBalancerListener.start();

		try {
			Registry.getRegistry(null, null).registerComponent(loadBalancerListener, AdminAgent.AGENT_DOMAINNAME + ":loadBalancer=LoadBalancer," + "name=LoadBalancerListener", loadBalancerListener.getClass().getName());
			Registry.getRegistry(null, null).registerComponent(loadBalancerListener.getRedirector(), AdminAgent.AGENT_DOMAINNAME + ":loadBalancer=LoadBalancer," + "name=LoadBalancerRedirector", loadBalancerListener.getRedirector().getClass().getName());
		} catch (Exception e) {
			WMSLoggerFactory.getLogger(ServerListenerLoadBalancerListener.class).error("ServerListenerLoadBalancerListener.onServerInit: " + e.toString());
		}

	}

	public void onServerShutdownStart(IServer server) {
		WMSLoggerFactory.getLogger(ServerListenerLoadBalancerListener.class).info("ServerListenerLoadBalancerListener.onServerShutdownStart");

		if (loadBalancerListener != null)
			loadBalancerListener.quit();
		loadBalancerListener = null;

		try {
			Registry.getRegistry(null, null).unregisterComponent(AdminAgent.AGENT_DOMAINNAME + ":loadBalancer=LoadBalancer," + "name=LoadBalancerListener");
			Registry.getRegistry(null, null).unregisterComponent(AdminAgent.AGENT_DOMAINNAME + ":loadBalancer=LoadBalancer," + "name=LoadBalancerRedirector");
		} catch (Exception e) {
			WMSLoggerFactory.getLogger(ServerListenerLoadBalancerListener.class).error("ServerListenerLoadBalancerListener.onServerShutdownStart: " + e.toString());
		}
	}

	public void onServerShutdownComplete(IServer server) {
	}

}
