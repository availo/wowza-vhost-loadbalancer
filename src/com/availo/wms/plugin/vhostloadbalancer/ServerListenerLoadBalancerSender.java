/*
 * This file is currently almost completely unchanged, except for standard
 * Eclipse-formatting, and is just added for completeness' sake.
 */

package com.availo.wms.plugin.vhostloadbalancer;

import java.io.*;

import org.apache.commons.modeler.*;

import com.wowza.util.*;
import com.wowza.wms.admin.*;
import com.wowza.wms.bootstrap.*;
import com.wowza.wms.logging.*;
import com.wowza.wms.server.*;
import com.wowza.wms.application.*;
import com.wowza.wms.plugin.loadbalancer.*;

public class ServerListenerLoadBalancerSender implements IServerNotify2 {
	public static final String PROP_LOADBALANCERSENDER = "WowzaProLoadBalancerSender";

	private LoadBalancerSender loadBalancerSender = null;
	private LoadBalancerWorker loadBalancerWorker = null;

	public void onServerConfigLoaded(IServer server) {
	}

	public void onServerCreate(IServer server) {
	}

	public void onServerInit(IServer server) {
		WMSLoggerFactory.getLogger(ServerListenerLoadBalancerSender.class).info("ServerListenerLoadBalancerSender.onServerInit");

		String targetPath = Bootstrap.getServerHome(Bootstrap.CONFIGHOME) + "/conf/loadbalancertargets.txt";
		String redirectAddress = "localhost";
		String monitorClass = null;
		int messageInterval = 2500;
		
		WMSProperties props = server.getProperties();

		targetPath = props.getPropertyStr("loadBalancerSenderTargetPath", targetPath);
		redirectAddress = props.getPropertyStr("loadBalancerSenderRedirectAddress", redirectAddress);
		monitorClass = props.getPropertyStr("loadBalancerSenderMonitorClass", monitorClass);
		messageInterval = props.getPropertyInt("loadBalancerSenderMessageInterval", messageInterval);

		if (redirectAddress != null) {
			redirectAddress = SystemUtils.expandEnvironmentVariables(redirectAddress);
		}

		if (targetPath != null) {
			targetPath = SystemUtils.expandEnvironmentVariables(targetPath);
		}

		ILoadBalancerMonitor loadBalancerMonitor = null;
		if (monitorClass != null) {
			try {
				Class loadBalancerMonitorClass = Class.forName(monitorClass);
				if (loadBalancerMonitorClass != null)
					loadBalancerMonitor = (ILoadBalancerMonitor) loadBalancerMonitorClass.newInstance();
			} catch (Exception e) {
				WMSLoggerFactory.getLogger(ServerListenerLoadBalancerSender.class).error("ServerListenerLoadBalancerSender.onServerInit[" + monitorClass + "]: " + e.toString());
			}
		}

		if (loadBalancerMonitor == null)
			loadBalancerMonitor = new LoadBalancerMonitorDefault();

		loadBalancerSender = new LoadBalancerSender();
		loadBalancerWorker = new LoadBalancerWorker(loadBalancerSender);

		server.getProperties().put(PROP_LOADBALANCERSENDER, loadBalancerSender);
		
		loadBalancerSender.init();

		loadBalancerSender.setTargetFile(new File(targetPath));
		loadBalancerSender.setRedirectAddress(redirectAddress);
		loadBalancerSender.setLoadBalancerMonitor(loadBalancerMonitor);

		loadBalancerSender.setStatus(LoadBalancerServer.STATUS_RUNNING);
		loadBalancerSender.run();

		loadBalancerWorker.setWorkInterval(messageInterval);
		loadBalancerWorker.setDaemon(true);
		loadBalancerWorker.setName("LoadBalancerSender");
		loadBalancerWorker.start();

		try {
			Registry.getRegistry(null, null).registerComponent(loadBalancerSender, AdminAgent.AGENT_DOMAINNAME + ":loadBalancer=LoadBalancer," + "name=LoadBalancerSender", loadBalancerSender.getClass().getName());
		} catch (Exception e) {
			WMSLoggerFactory.getLogger(ServerListenerLoadBalancerSender.class).error("ServerListenerLoadBalancerSender.onServerInit: " + e.toString());
		}
	}

	public void onServerShutdownStart(IServer server) {
		WMSLoggerFactory.getLogger(ServerListenerLoadBalancerSender.class).info("ServerListenerLoadBalancerSender.onServerShutdownStart");

		if (loadBalancerWorker != null) {
			loadBalancerWorker.quit();
		}
		loadBalancerWorker = null;

		loadBalancerSender.setStatus(LoadBalancerServer.STATUS_STOPPED);
		loadBalancerSender.run();
		loadBalancerSender.shutdown();

		try {
			Registry.getRegistry(null, null).unregisterComponent(AdminAgent.AGENT_DOMAINNAME + ":loadBalancer=LoadBalancer," + "name=LoadBalancerSender");
		} catch (Exception e) {
			WMSLoggerFactory.getLogger(ServerListenerLoadBalancerSender.class).error("ServerListenerLoadBalancerSender.onServerShutdownStart: " + e.toString());
		}

	}

	public void onServerShutdownComplete(IServer server) {
	}
}
