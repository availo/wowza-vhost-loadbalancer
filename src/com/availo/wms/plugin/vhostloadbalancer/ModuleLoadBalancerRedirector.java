package com.availo.wms.plugin.vhostloadbalancer;

import java.net.*;

import com.wowza.wms.httpstreamer.model.IHTTPStreamerSession;
import com.wowza.wms.rtp.model.RTPSession;
import com.wowza.wms.module.*;
import com.wowza.wms.request.*;
import com.wowza.wms.server.*;
import com.wowza.wms.amf.*;
import com.wowza.wms.application.*;
import com.wowza.wms.client.*;
import com.wowza.wms.plugin.loadbalancer.*;

/**
 * Wowza application redirector class with support for HTTP and RTMP redirecting per VHost
 * 
 * This module should be included in application configs (Application.xml)
 * 
 * @author Brynjar Eide <brynjar@availo.no>
 * @version 1.0b, 2012-09-20
 *
 */
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

		if (redirectAppName != null) {
			redirectAppName = redirectAppName.startsWith("/") ? redirectAppName : "/" + redirectAppName;
		}

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
					getLogger().debug("ModuleLoadBalancerRedirector.getLoadBalancerRedirect[" + getFullName(client) + "]: ILoadBalancerRedirector not found.");
				break;
			}

			LoadBalancerRedirect redirect = this.redirector.getRedirect(vhostName);
			if (redirect == null) {
				client.rejectConnection("ModuleLoadBalancerRedirector.getLoadBalancerRedirect[" + getFullName(client) + "]: Redirect failed.");
				if (isDebugLog)
					getLogger().debug("ModuleLoadBalancerRedirector.getLoadBalancerRedirect[" + getFullName(client) + "]: Redirect failed.");
				break;
			}

			ret = redirect.getHost();
			break;
		}

		sendResult(client, params, new AMFDataItem(ret));
	}

	/**
	 * RTMP redirect
	 * @param client
	 * @param function
	 * @param params
	 */
	public void onConnect(IClient client, RequestFunction function, AMFDataList params) {
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
				getLogger().debug("ModuleLoadBalancerRedirector.onConnect[" + getFullName(client) + "]: old URI:" + uriStr);
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

					// Only add the "?stuff" parameters if the querystring has actual data
					String queryString = (client.getQueryStr() != null && client.getQueryStr() != "") ? "?" + client.getQueryStr() : "";

					getLogger().debug("ModuleLoadBalancerRedirector.onConnect[" + getFullName(client) + "]: creating new URI:" + scheme + "," + uri.getUserInfo() + "," + host + "," + port + "," + path + "," + uri.getQuery() + "," + uri.getFragment());
					URI newUri = new URI(scheme, uri.getUserInfo(), host, port, path, uri.getQuery(), uri.getFragment());
					if (isDebugLog) {
						getLogger().debug("ModuleLoadBalancerRedirector.onConnect[" + getFullName(client) + "]: from:" + uriStr + " to:" + newUri.toString() + queryString);
					}

					// Execute the redirect
					client.redirectConnection(newUri.toString() + queryString);

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
	 * Redirect functionality for iOS-streams (HLS / Cupertino) and Adobe HTTP streams (HDS / San Jose)
	 * 
	 * This prepares a header that will be read by the HTTPStreamers for HLS and HDS,
	 * and then used to generate an absolute URL to the edge server
	 * @param httpSession
	 */
	public void onHTTPSessionCreate(IHTTPStreamerSession httpSession) {
		String vhostName = httpSession.getVHost().getName();
		IApplicationInstance appInstance = httpSession.getAppInstance();

		boolean isDebugLog = getLogger().isDebugEnabled();
		while (true) {
			if (this.redirector == null) {
				//httpSession.rejectSession("ModuleLoadBalancerRedirector.onHTTPSessionCreate[" + getFullName(appInstance) + "]: ILoadBalancerRedirector not found.");
				httpSession.rejectSession(); // TODO figure out a way to add an error message to the client, like rejectConnection(String errorStr) offers
				if (isDebugLog)
					getLogger().debug("ModuleLoadBalancerRedirector.onHTTPSessionCreate[" + getFullName(appInstance) + "]: ILoadBalancerRedirector not found.");
				break;
			}

			LoadBalancerRedirect redirect = this.redirector.getRedirect(vhostName);
			if (redirect == null) {
				//client.rejectConnection("ModuleLoadBalancerRedirector.onHTTPSessionCreate[" + getFullName(appInstance) + "]: Redirect failed.");
				httpSession.rejectSession(); // TODO figure out a way to add an error message to the client, like rejectConnection(String errorStr) offers
				if (isDebugLog)
					getLogger().debug("ModuleLoadBalancerRedirector.onHTTPSessionCreate[" + getFullName(appInstance) + "]: Redirect failed.");
				break;
			}

			if (httpSession.getUri() == null) {
				//client.rejectConnection("ModuleLoadBalancerRedirector.onHTTPSessionCreate[" + getFullName(appInstance) + "]: URI missing.");
				httpSession.rejectSession(); // TODO figure out a way to add an error message to the client, like rejectConnection(String errorStr) offers
				if (isDebugLog)
					getLogger().debug("ModuleLoadBalancerRedirector.onHTTPSessionCreate[" + getFullName(appInstance) + "]: URI missing.");
				break;
			}

	        try {
				int port = redirectPort > 0 ? redirectPort : httpSession.getServerPort();
				String host = redirect.getHost();
				String loadbalancerTarget = host;

				if (port != 0 && port != 80) {
					// Only add the port if it's a valid port, and it's not the default HTTP port.
					loadbalancerTarget += ":" + port;
				}
				if (isDebugLog) {
					getLogger().debug("ModuleLoadBalancerRedirector.onHTTPSessionCreate[" + getFullName(appInstance) + "]: Adding HTTP Header 'X-LoadBalancer-Targer: " + loadbalancerTarget + "'");
				}
				httpSession.setUserHTTPHeader("X-LoadBalancer-Target", loadbalancerTarget);

			} catch (Exception e) {
				//httpSession.rejectSession("ModuleLoadBalancerRedirector.onHTTPSessionCreate[" + getFullName(appInstance) + "]: Exception: " + e.toString());
				httpSession.rejectSession(); // TODO Add an error message to the client
				if (isDebugLog)
					getLogger().error("ModuleLoadBalancerRedirector.onHTTPSessionCreate[" + getFullName(appInstance) + "]: Exception: " + e.toString());
			}
			break;
		}
    }

	/**
	 * Redirect functionality for RTSP-streams
	 * @param rtpSession
	 */
	public void onRTPSessionCreate(RTPSession rtpSession) {
		String vhostName = rtpSession.getVHost().getName();
		IApplicationInstance appInstance = rtpSession.getAppInstance();

		if (this.redirectOnConnect) {
			boolean isDebugLog = getLogger().isDebugEnabled();
			while (true) {
				if (this.redirector == null) {
					//rtpSession.rejectSession("ModuleLoadBalancerRedirector.onrtpSessionCreate[" + getFullName(appInstance) + "]: ILoadBalancerRedirector not found.");
					rtpSession.rejectSession(); // TODO figure out a way to add an error message to the client, like rejectConnection(String errorStr) offers
					if (isDebugLog)
						getLogger().debug("ModuleLoadBalancerRedirector.onRTPSessionCreate[" + getFullName(appInstance) + "]: ILoadBalancerRedirector not found.");
					break;
				}

				LoadBalancerRedirect redirect = this.redirector.getRedirect(vhostName);
				if (redirect == null) {
					//client.rejectConnection("ModuleLoadBalancerRedirector.onRTPSessionCreate[" + getFullName(appInstance) + "]: Redirect failed.");
					rtpSession.rejectSession(); // TODO figure out a way to add an error message to the client, like rejectConnection(String errorStr) offers
					if (isDebugLog)
						getLogger().debug("ModuleLoadBalancerRedirector.onRTPSessionCreate[" + getFullName(appInstance) + "]: Redirect failed.");
					break;
				}

				if (rtpSession.getUri() == null) {
					//client.rejectConnection("ModuleLoadBalancerRedirector.onRTPSessionCreate[" + getFullName(appInstance) + "]: URI missing.");
					rtpSession.rejectSession(); // TODO figure out a way to add an error message to the client, like rejectConnection(String errorStr) offers
					if (isDebugLog)
						getLogger().debug("ModuleLoadBalancerRedirector.onRTPSessionCreate[" + getFullName(appInstance) + "]: URI missing.");
					break;
				}

				// There we go... RTPSession also includes protocol, hostname and port in getUri().
				String uriStr = rtpSession.getUri();
				getLogger().debug("ModuleLoadBalancerRedirector.onRTPSessionCreate[" + getFullName(appInstance) + "]: old URI:" + uriStr);

				try {
					URI uri = new URI(uriStr);

					int port = redirectPort > 0 ? redirectPort : uri.getPort();
					String host = redirect.getHost();
					String path = uri.getPath();

					getLogger().debug("ModuleLoadBalancerRedirector.onRTPSessionCreate[" + getFullName(appInstance) + "]: creating new URI:" + uri.getScheme() + "," + uri.getUserInfo() + "," + host + "," + port + "," + path + "," + uri.getQuery() + "," + uri.getFragment());
					URI newUri = new URI(uri.getScheme(), uri.getUserInfo(), host, port, path, uri.getQuery(), uri.getFragment());

					if (isDebugLog) {
						getLogger().debug("ModuleLoadBalancerRedirector.onRTPSessionCreate[" + getFullName(appInstance) + "]: from:" + uriStr + " to:" + newUri.toString());
					}

					// Only add the "?stuff" parameters if the querystring has actual data
					String queryString = (rtpSession.getQueryStr() != null && rtpSession.getQueryStr() != "") ? "?" + rtpSession.getQueryStr() : "";
					rtpSession.redirectSession(newUri.toString() + queryString);

				} catch (Exception e) {
					//rtpSession.rejectSession("ModuleLoadBalancerRedirector.onRTPSessionCreate[" + getFullName(appInstance) + "]: Exception: " + e.toString());
					rtpSession.rejectSession(); // TODO figure out a way to add an error message to the client, like rejectConnection(String errorStr) offers
					if (isDebugLog)
						getLogger().error("ModuleLoadBalancerRedirector.onRTPSessionCreate[" + getFullName(appInstance) + "]: Exception: " + e.toString());
				}
				break;
			}
		}
	}
}
