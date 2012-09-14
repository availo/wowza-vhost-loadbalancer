package com.availo.wms.plugin.vhostloadbalancer;

import java.net.*;

import com.wowza.wms.httpstreamer.model.IHTTPStreamerSession;
import com.wowza.wms.module.*;
import com.wowza.wms.request.*;
import com.wowza.wms.server.*;
import com.wowza.wms.amf.*;
import com.wowza.wms.application.*;
import com.wowza.wms.client.*;
import com.wowza.wms.plugin.loadbalancer.*;

public class ModuleLoadBalancerRedirector extends ModuleBase {
	private LoadBalancerListener listener = null;
	private LoadBalancerRedirectorBandwidth redirector = null;

	private String redirectAppName = null;
	private int redirectPort = -1;
	private String redirectScheme = null;
	private boolean redirectOnConnect = true;

	private String getFullName(IApplicationInstance appInstance) {
		return appInstance.getVHost().getName() + "/" + appInstance.getApplication().getName() + "/" + appInstance.getName();
	}

	private String getFullName(IClient client) {
		return client.getAppInstance().getVHost().getName() + "/" + client.getAppInstance().getApplication().getName() + "/" + client.getAppInstance().getName();
	}

	public void onAppStart(IApplicationInstance appInstance) {
		this.redirectAppName = appInstance.getProperties().getPropertyStr("redirectAppName", this.redirectAppName);
		this.redirectPort = appInstance.getProperties().getPropertyInt("redirectPort", this.redirectPort);
		this.redirectScheme = appInstance.getProperties().getPropertyStr("redirectScheme", this.redirectScheme);
		this.redirectOnConnect = appInstance.getProperties().getPropertyBoolean("redirectOnConnect", this.redirectOnConnect);

		if (redirectAppName != null)
			redirectAppName = redirectAppName.startsWith("/") ? redirectAppName : "/" + redirectAppName;

		while (true) {
			this.listener = (LoadBalancerListener) Server.getInstance().getProperties().get(ServerListenerLoadBalancerListener.PROP_LOADBALANCERLISTENER);
			if (this.listener == null) {
				getLogger().warn("ModuleLoadBalancerRedirector.onAppStart[" + getFullName(appInstance) + "]: LoadBalancerListener not found. All connections to this application will be refused.");
				break;
			}

			this.redirector = (LoadBalancerRedirectorBandwidth) this.listener.getRedirector();
			if (this.redirector == null) {
				getLogger().warn("ModuleLoadBalancerRedirector.onAppStart[" + getFullName(appInstance) + "]: ILoadBalancerRedirector not found. All connections to this application will be refused.");
				break;
			}
			break;
		}
		getLogger().info("ModuleLoadBalancerRedirector.onAppStart: " + getFullName(appInstance));
	}

	public void onAppStop(IApplicationInstance appInstance) {
		getLogger().info("ModuleLoadBalancerRedirector.onAppStop: " + getFullName(appInstance));
	}

	public void getLoadBalancerRedirect(IClient client, RequestFunction function, AMFDataList params) {
		boolean isDebugLog = getLogger().isDebugEnabled();

		String ret = "unknown";
		String vhostName = client.getAppInstance().getVHost().getName();
		while (true) {
			if (this.redirector == null) {
				if (isDebugLog)
					getLogger().debug("ModuleLoadBalancerRedirector.onConnect[" + getFullName(client) + "]: ILoadBalancerRedirector not found.");
				break;
			}

			LoadBalancerRedirect redirect = this.redirector.getRedirect(vhostName);
			if (redirect == null) {
				client.rejectConnection("ModuleLoadBalancerRedirector.onConnect[" + getFullName(client) + "]: Redirect failed.");
				if (isDebugLog)
					getLogger().debug("ModuleLoadBalancerRedirector.onConnect[" + getFullName(client) + "]: Redirect failed.");
				break;
			}

			ret = redirect.getHost();
			break;
		}

		sendResult(client, params, new AMFDataItem(ret));
	}

	public void onConnect(IClient client, RequestFunction function, AMFDataList params) {
		getLogger().info("ModuleLoadBalancerRedirector.onConnect: " + getFullName(client));
		if (this.redirectOnConnect) {
			boolean isDebugLog = getLogger().isDebugEnabled();
			String vhostName = client.getAppInstance().getVHost().getName();
			while (true) {
				if (this.redirector == null) {
					client.rejectConnection("ModuleLoadBalancerRedirector.onConnect[" + getFullName(client) + "]: ILoadBalancerRedirector not found.");
					if (isDebugLog)
						getLogger().debug("ModuleLoadBalancerRedirector.onConnect[" + getFullName(client) + "]: ILoadBalancerRedirector not found.");
					break;
				}

				LoadBalancerRedirect redirect = this.redirector.getRedirect(vhostName);
				if (redirect == null) {
					client.rejectConnection("ModuleLoadBalancerRedirector.onConnect[" + getFullName(client) + "]: Redirect failed.");
					if (isDebugLog)
						getLogger().debug("ModuleLoadBalancerRedirector.onConnect[" + getFullName(client) + "]: Redirect failed.");
					break;
				}

				String uriStr = client.getUri();
				if (uriStr == null) {
					client.rejectConnection("ModuleLoadBalancerRedirector.onConnect[" + getFullName(client) + "]: URI missing.");
					if (isDebugLog)
						getLogger().debug("ModuleLoadBalancerRedirector.onConnect[" + getFullName(client) + "]: URI missing.");
					break;
				}

				try {
					URI uri = new URI(uriStr);

					String scheme = redirectScheme == null ? uri.getScheme() : redirectScheme;
					int port = redirectPort > 0 ? redirectPort : uri.getPort();
					String host = redirect.getHost();
					String path = redirectAppName != null ? redirectAppName : uri.getPath();

					URI newUri = new URI(scheme, uri.getUserInfo(), host, port, path, uri.getQuery(), uri.getFragment());
					if (isDebugLog)
						getLogger().debug("ModuleLoadBalancerRedirector.onConnect[" + getFullName(client) + "]: from:" + uriStr + " to:" + newUri.toString());

					String queryString = (client.getQueryStr() == "") ? "" : "?" + client.getQueryStr(); // ADDED
					client.redirectConnection(newUri.toString() + queryString); // CHANGED
																				// }
					// client.redirectConnection(newUri.toString());
				} catch (Exception e) {
					client.rejectConnection("ModuleLoadBalancerRedirector.onConnect[" + getFullName(client) + "]: Exception: " + e.toString());
					if (isDebugLog)
						getLogger().debug("ModuleLoadBalancerRedirector.onConnect[" + getFullName(client) + "]: Exception: " + e.toString());
				}
				break;
			}
		}
	}

	/**
	 * Add redirect functionality for iOS-streams
	 * @TODO This commented-out function is only here for reference right now.
	 * Gratefully borrowed from Oleg Tokar (http://www.wowza.com/forums/showthread.php?17957-loadbalance-for-ios&p=92189#post92189)
	 * @param httpSession
	 */
	/*public void onHTTPSessionCreate(IHTTPStreamerSession httpSession) {
        String queryServer = httpSession.getServerIp();
        String clientIP = httpSession.getIpAddress();
        String queryUri = httpSession.getUri();
        int queryPort = httpSession.getServerPort();
        String queryStr = httpSession.getQueryStr();
        //getRedirector();
        LoadBalancerRedirect redirect = this.redirector==null?null:this.redirector.getRedirect();
        String redirectHost = redirect.getHost();
        getLogger().info("-=redirector=- "+queryServer +" " +queryUri +" "+queryStr);
        getLogger().info(clientIP +"[Redirected to:] "+"http://"+redirectHost+":"+queryPort+"/"+queryUri);
        httpSession.redirectSession("http://"+redirectHost+":"+queryPort+"/"+queryUri+"?"+queryStr);
    }*/
}
