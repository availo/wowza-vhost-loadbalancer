package com.availo.wms.plugin.vhostloadbalancer;

import java.net.*;

import com.availo.wms.plugin.vhostloadbalancer.ConfigCache.MissingPropertyException;
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
	
	/**
	 * LoadBalancerListener is where we'll get our LoadBalancerRedirectorBandwidth from
	 */
	private static LoadBalancerListener listener = null;
	
	/**
	 * LoadBalancerRedirectorBandwidth knows the IP address (or hostname) of the server that is deemed to be most suitable for the next connection
	 */
	private static LoadBalancerRedirectorBandwidth redirector = null;
	
	/**
	 * Whether or not LoadBalancerRedirectorBandwidth has been successfully initialized
	 */
	private static boolean initialized = false;
	
	/**
	 * ConfigCache is used to keep the configuration properties from Application.xml in memory,
	 * since the HTTPStreamerAdapter* redirector classes requires knowledge of the settings as well.
	 */
	private static ConfigCache config;

	/**
	 * Optional property that translates the application name used on the LoadBalancerListener and redirect to a new name used on an edge server
	 */
	private String redirectAppName;
	
	/**
	 * Optional property that overrides the incoming port and redirect to a permanent port for all protocols 
	 */
	private int redirectPort;
	
	/**
	 * Semi-optional property that decides whether a connection should be automatically redirected, or if it needs to ask for a redirect. (RTMP-only) 
	 */
	private boolean redirectOnConnect;

	/**
	 * @deprecated Should not be used, unless it is required for RTMP->RTMPT. It is only considered for RTMP connections, and is ignored for RTSP/HTTP requests.
	 */
	private String redirectScheme;

	/**
	 * Shortcut used for adding 'vhostname/applicationname/instance' to logs
	 * @param appInstance
	 * @return
	 */
	private String getFullName(IApplicationInstance appInstance) {
		return appInstance.getVHost().getName() + "/" + appInstance.getApplication().getName() + "/" + appInstance.getName();
	}

	/**
	 * Shortcut used for adding 'vhostname/applicationname/instance' to logs
	 * @param appInstance
	 * @return
	 */
	private String getFullName(IClient client) {
		return client.getAppInstance().getVHost().getName() + "/" + client.getAppInstance().getApplication().getName() + "/" + client.getAppInstance().getName();
	}

	/**
	 * Called automatically by Wowza
	 * @param appInstance
	 */
	public void onAppStart(IApplicationInstance appInstance) {
		if (config == null) {
			config = ConfigCache.getInstance();
		}
		
		getLogger().info("ModuleLoadBalancerRedirector.onAppStart[" + getFullName(appInstance) + "]: Loading ConfigCache + properties.");
		if (config.loadProperties(appInstance)) {
			try {
				redirectAppName = config.getRedirectAppName(appInstance);
				redirectScheme = config.getRedirectScheme(appInstance);
				redirectPort = config.getRedirectPort(appInstance);
				redirectOnConnect = config.getRedirectOnConnect(appInstance);
			} catch (MissingPropertyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		if (redirectAppName != null) {
			redirectAppName = redirectAppName.startsWith("/") ? redirectAppName : "/" + redirectAppName;
		}
		getLogger().info("ModuleLoadBalancerRedirector.onAppStart[" + getFullName(appInstance) + "]: Preparing to initialize LoadBalancerRedirector.");
		initRedirector();
	}
	
	private void initRedirector() {
		listener = (LoadBalancerListener) Server.getInstance().getProperties().get(ServerListenerLoadBalancerListener.PROP_LOADBALANCERLISTENER);
		if (listener == null) {
			getLogger().warn("ModuleLoadBalancerRedirector.initRedirector: LoadBalancerListener not found. All connections to this application will be refused.");
			return;
		}
		else {
			redirector = (LoadBalancerRedirectorBandwidth) listener.getRedirector();
		}
		if (redirector == null) {
			getLogger().warn("ModuleLoadBalancerRedirector.initRedirector: ILoadBalancerRedirector not found. All connections to this application will be refused.");
			return;
		}
		initialized = true;
	}

	public void onAppStop(IApplicationInstance appInstance) {
		config.expireProperties(appInstance);
		getLogger().info("ModuleLoadBalancerRedirector.onAppStop: " + getFullName(appInstance));
	}

	/**
	 * This function can be called by a RTMP-based flash client, if redirectOnConnect is false
	 * @param client
	 * @param function
	 * @param params Action Message Format-response to the client
	 */
	public void getLoadBalancerRedirect(IClient client, RequestFunction function, AMFDataList params) {
		boolean isDebugLog = getLogger().isDebugEnabled();

		String ret = "unknown";
		String vhostName = client.getAppInstance().getVHost().getName();
		if (initialized == false) {
			initRedirector();
		}

		LoadBalancerRedirect redirect = redirector.getRedirect(vhostName);
		if (redirect == null) {
			client.rejectConnection("ModuleLoadBalancerRedirector.getLoadBalancerRedirect[" + getFullName(client) + "]: Redirect failed.");
			getLogger().warn("ModuleLoadBalancerRedirector.getLoadBalancerRedirect[" + getFullName(client) + "]: Redirect failed.");
		}
		ret = redirect.getHost();
		sendResult(client, params, new AMFDataItem(ret));
	}

	/**
	 * RTMP redirect
	 * @param client
	 * @param function
	 * @param params
	 */
	public void onConnect(IClient client, RequestFunction function, AMFDataList params) {
		if (redirectOnConnect) {
			boolean isDebugLog = getLogger().isDebugEnabled();
			String vhostName = client.getAppInstance().getVHost().getName();
			if (redirector == null) {
				client.rejectConnection("ModuleLoadBalancerRedirector.onConnect[" + getFullName(client) + "]: ILoadBalancerRedirector not found.");
				getLogger().warn("ModuleLoadBalancerRedirector.onConnect[" + getFullName(client) + "]: ILoadBalancerRedirector not found.");
				return;
			}

			LoadBalancerRedirect redirect = redirector.getRedirect(vhostName);
			if (redirect == null) {
				client.rejectConnection("ModuleLoadBalancerRedirector.onConnect[" + getFullName(client) + "]: Redirect failed.");
				getLogger().warn("ModuleLoadBalancerRedirector.onConnect[" + getFullName(client) + "]: Redirect failed.");
				return;
			}

			String uriStr = client.getUri();
			getLogger().debug("ModuleLoadBalancerRedirector.onConnect[" + getFullName(client) + "]: old URI:" + uriStr);
			if (uriStr == null) {
				client.rejectConnection("ModuleLoadBalancerRedirector.onConnect[" + getFullName(client) + "]: URI missing.");
				getLogger().warn("ModuleLoadBalancerRedirector.onConnect[" + getFullName(client) + "]: URI missing.");
				return;
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
				getLogger().debug("ModuleLoadBalancerRedirector.onConnect[" + getFullName(client) + "]: from:" + uriStr + " to:" + newUri.toString() + queryString);

				// Execute the redirect
				client.redirectConnection(newUri.toString() + queryString);

			} catch (Exception e) {
				client.rejectConnection("ModuleLoadBalancerRedirector.onConnect[" + getFullName(client) + "]: Exception: " + e.toString());
				getLogger().debug("ModuleLoadBalancerRedirector.onConnect[" + getFullName(client) + "]: Exception: " + e.toString());
			}
		}
	}

	/**
	 * Redirect functionality for iOS-streams (HLS / Cupertino) and Adobe HTTP streams (HDS / San Jose)
	 * 
	 * This prepares a header that will be read by the HTTPStreamers for HLS and HDS.
	 * If the header is not present, the HTTPStreamers will handle the request normally, without a redirect.
	 * @param httpSession
	 */
	public void onHTTPSessionCreate(IHTTPStreamerSession httpSession) {
		String vhostName = httpSession.getVHost().getName();
		IApplicationInstance appInstance = httpSession.getAppInstance();

		boolean isDebugLog = getLogger().isDebugEnabled();

		if (initialized == false) {
			initRedirector();
		}

		if (redirector == null) {
			//httpSession.rejectSession("ModuleLoadBalancerRedirector.onHTTPSessionCreate[" + getFullName(appInstance) + "]: ILoadBalancerRedirector not found.");
			httpSession.rejectSession(); // TODO figure out a way to add an error message to the client, like rejectConnection(String errorStr) offers
			getLogger().warn("ModuleLoadBalancerRedirector.onHTTPSessionCreate[" + getFullName(appInstance) + "]: ILoadBalancerRedirector not found.");
			return;
		}

		LoadBalancerRedirect redirect = redirector.getRedirect(vhostName);
		if (redirect == null) {
			//client.rejectConnection("ModuleLoadBalancerRedirector.onHTTPSessionCreate[" + getFullName(appInstance) + "]: Redirect failed.");
			httpSession.rejectSession(); // TODO figure out a way to add an error message to the client, like rejectConnection(String errorStr) offers
			getLogger().warn("ModuleLoadBalancerRedirector.onHTTPSessionCreate[" + getFullName(appInstance) + "]: Redirect failed.");
			return;
		}

		if (httpSession.getUri() == null) {
			//client.rejectConnection("ModuleLoadBalancerRedirector.onHTTPSessionCreate[" + getFullName(appInstance) + "]: URI missing.");
			httpSession.rejectSession(); // TODO figure out a way to add an error message to the client, like rejectConnection(String errorStr) offers
			getLogger().warn("ModuleLoadBalancerRedirector.onHTTPSessionCreate[" + getFullName(appInstance) + "]: URI missing.");
			return;
		}

        try {
			String host = redirect.getHost();
			String loadbalancerTarget = host;
			getLogger().debug("ModuleLoadBalancerRedirector.onHTTPSessionCreate[" + getFullName(appInstance) + "]: Adding HTTP Header 'X-LoadBalancer-Targer: " + loadbalancerTarget + "'");
			httpSession.setUserHTTPHeader("X-LoadBalancer-Target", loadbalancerTarget);

		} catch (Exception e) {
			//httpSession.rejectSession("ModuleLoadBalancerRedirector.onHTTPSessionCreate[" + getFullName(appInstance) + "]: Exception: " + e.toString());
			httpSession.rejectSession(); // TODO Add an error message to the client
			getLogger().error("ModuleLoadBalancerRedirector.onHTTPSessionCreate[" + getFullName(appInstance) + "]: Exception: " + e.toString());
		}
    }

	/**
	 * Redirect functionality for RTSP-streams
	 * @param rtpSession
	 */
	public void onRTPSessionCreate(RTPSession rtpSession) {
		String vhostName = rtpSession.getVHost().getName();
		IApplicationInstance appInstance = rtpSession.getAppInstance();

		if (redirectOnConnect) {
			boolean isDebugLog = getLogger().isDebugEnabled();
			if (initialized == false) {
				initRedirector();
			}

			if (redirector == null) {
				//rtpSession.rejectSession("ModuleLoadBalancerRedirector.onrtpSessionCreate[" + getFullName(appInstance) + "]: ILoadBalancerRedirector not found.");
				rtpSession.rejectSession(); // TODO figure out a way to add an error message to the client, like rejectConnection(String errorStr) offers
				getLogger().warn("ModuleLoadBalancerRedirector.onRTPSessionCreate[" + getFullName(appInstance) + "]: ILoadBalancerRedirector not found.");
				return;
			}

			LoadBalancerRedirect redirect = redirector.getRedirect(vhostName);
			if (redirect == null) {
				//client.rejectConnection("ModuleLoadBalancerRedirector.onRTPSessionCreate[" + getFullName(appInstance) + "]: Redirect failed.");
				rtpSession.rejectSession(); // TODO figure out a way to add an error message to the client, like rejectConnection(String errorStr) offers
				getLogger().warn("ModuleLoadBalancerRedirector.onRTPSessionCreate[" + getFullName(appInstance) + "]: Redirect failed.");
				return;
			}

			if (rtpSession.getUri() == null) {
				//client.rejectConnection("ModuleLoadBalancerRedirector.onRTPSessionCreate[" + getFullName(appInstance) + "]: URI missing.");
				rtpSession.rejectSession(); // TODO figure out a way to add an error message to the client, like rejectConnection(String errorStr) offers
				getLogger().warn("ModuleLoadBalancerRedirector.onRTPSessionCreate[" + getFullName(appInstance) + "]: URI missing.");
				return;
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

				getLogger().debug("ModuleLoadBalancerRedirector.onRTPSessionCreate[" + getFullName(appInstance) + "]: from:" + uriStr + " to:" + newUri.toString());

				// Only add the "?stuff" parameters if the querystring has actual data
				String queryString = (rtpSession.getQueryStr() != null && rtpSession.getQueryStr() != "") ? "?" + rtpSession.getQueryStr() : "";
				rtpSession.redirectSession(newUri.toString() + queryString);

			} catch (Exception e) {
				//rtpSession.rejectSession("ModuleLoadBalancerRedirector.onRTPSessionCreate[" + getFullName(appInstance) + "]: Exception: " + e.toString());
				rtpSession.rejectSession(); // TODO figure out a way to add an error message to the client, like rejectConnection(String errorStr) offers
				getLogger().error("ModuleLoadBalancerRedirector.onRTPSessionCreate[" + getFullName(appInstance) + "]: Exception: " + e.toString());
			}
		}
	}
}
