/**
 * ModuleLoadBalancerRedirector.java
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

import com.availo.wms.plugin.vhostloadbalancer.ConfigCache.MissingPropertyException;
import com.wowza.wms.httpstreamer.model.IHTTPStreamerSession;
import com.wowza.wms.rtp.model.RTPSession;
import com.wowza.wms.module.*;
import com.wowza.wms.request.*;
import com.wowza.wms.server.*;
//import com.wowza.wms.stream.IMediaStream;
import com.wowza.wms.amf.*;
import com.wowza.wms.application.*;
import com.wowza.wms.client.*;
import com.wowza.wms.plugin.loadbalancer.*;

import java.io.BufferedReader;
import java.io.IOException;
//import java.io.InputStream;
import java.io.InputStreamReader;
//import java.io.StringWriter;
import java.net.*;
import java.util.regex.Pattern;

/**
 * Wowza application redirector class with support for HTTP and RTMP redirecting per VHost
 * 
 * This module should be included in application configs (Application.xml)
 * 
 * @author Brynjar Eide <brynjar@availo.no>
 * @version 2.0b, 2013-06-13
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
	 * Optional property that specifiees whether fetch a valid sessionId from the edge server, or use the sessionId from the loadbalancer 
	 */
	private boolean rewriteSessionId;

	/**
	 * @deprecated Should not be used, unless it is required for RTMP->RTMPT. It is only considered for RTMP connections, and is ignored for RTSP/HTTP requests.
	 */
	private String redirectScheme;

	/**
	 * Shortcut used for adding 'vhostname/applicationname/instance' to logs
	 * @param appInstance
	 * @return
	 */
	private String logPrefix(String functionName, IApplicationInstance appInstance) {
		return String.format("%s.%s[%s/%s/%s]", this.getClass().getName(), functionName, appInstance.getVHost().getName(), appInstance.getApplication().getName(), appInstance.getName());
	}

	/**
	 * Called automatically by Wowza when an application starts.
	 * @param appInstance
	 */
	@SuppressWarnings("deprecation")
	public void onAppStart(IApplicationInstance appInstance) {
		getLogger().info(logPrefix("onAppStart", appInstance) + ": Preparing to initialize LoadBalancerRedirector.");
		if (!initialized) {
			init();
		}
		if (config.loadProperties(appInstance)) {
			getLogger().info(logPrefix("onAppStart", appInstance) + ": Loading ConfigCache + properties.");
			try {
				redirectAppName = config.getRedirectAppName(appInstance);
				redirectScheme = config.getRedirectScheme(appInstance); // This is required if we want to support legacy configs.
				redirectPort = config.getRedirectPort(appInstance);
				redirectOnConnect = config.getRedirectOnConnect(appInstance);
				rewriteSessionId = config.getRewriteSessionId(appInstance);
			} catch (MissingPropertyException e) {
				e.printStackTrace();
			}
		}
	}

	public void onAppStop(IApplicationInstance appInstance) {
		getLogger().info(logPrefix("onAppStop", appInstance) + ": Stopping application and expiring all properties.");
		config.expireProperties(appInstance);
	}
	
	/**
	 * Initialize the module with objects that can be stored for all instances (one per application)
	 */
	private boolean init() {
		initialized = false;
		config = ConfigCache.getInstance();
		if (config == null) {
			getLogger().warn("ModuleLoadBalancerRedirector.init: ConfigCache not found. All connections to this application will be refused.");
			return false;
		}
		listener = (LoadBalancerListener) Server.getInstance().getProperties().get(ServerListenerLoadBalancerListener.PROP_LOADBALANCERLISTENER);
		if (listener == null) {
			getLogger().warn("ModuleLoadBalancerRedirector.init: LoadBalancerListener not found. All connections to this application will be refused.");
			return false;
		}
		redirector = (LoadBalancerRedirectorBandwidth) listener.getRedirector();
		if (redirector == null) {
			getLogger().warn("ModuleLoadBalancerRedirector.init: LoadBalancerRedirectorBandwidth not found. All connections to this application will be refused.");
			return false;
		}
		initialized = true;
		return initialized;
	}

	/**
	 * This function can be called by a RTMP-based flash client, if redirectOnConnect is false
	 * @deprecated Only kept here since the official documentation uses it.
	 * @param client
	 * @param function
	 * @param params Action Message Format-response to the client
	 */
	public void getLoadBalancerRedirect(IClient client, RequestFunction function, AMFDataList params) {
		IApplicationInstance appInstance = client.getAppInstance();

		String ret = "unknown";
		String vhostName = appInstance.getVHost().getName();

		LoadBalancerRedirect redirect = redirector.getRedirect(vhostName);
		if (redirect == null) {
			client.rejectConnection(logPrefix("getLoadBalancerRedirect", appInstance) + ": Redirect failed.");
			getLogger().warn(logPrefix("getLoadBalancerRedirec", appInstance) + ": Redirect failed.");
		}
		ret = redirect.getHost();
		sendResult(client, params, new AMFDataItem(ret));
	}

	/**
	 * Parse the query string from "netConnection" requests and run it through onRTMPRequest
	 * @param client
	 * @param function
	 * @param params
	 */
	public void onConnect(IClient client, RequestFunction function, AMFDataList params) {
		String queryStr = client.getQueryStr();
		if (redirectOnConnect || isRedirectRequest(client, queryStr)) {
			onRTMPRequest(client, function, params);
		}
	}
	
	private boolean isRedirectRequest(IClient client, String queryStr) {
		boolean redirectRequest = false;
		if (queryStr != null) {
			String matchString = "(^redirect=(true|1).*)|(.*[\\?\\&]redirect=(true|1).*)";
			redirectRequest = queryStr.matches(matchString);
			if (!redirectOnConnect && redirectRequest) {
				getLogger().debug(logPrefix("onRTMPRequest", client.getAppInstance()) + ": redirectOnConnect is disabled, but '" + queryStr + "' matches 'redirect=true'.");
			}
			else {
				getLogger().debug(logPrefix("onRTMPRequest", client.getAppInstance()) + ": redirectOnConnect is disabled, and '" + queryStr + "' does not match 'redirect=true'");
			}
		}
		return redirectRequest;
	}
	
	/**
	 * RTMP redirect
	 * @param client
	 * @param function
	 * @param params
	 */
	public void onRTMPRequest(IClient client, RequestFunction function, AMFDataList params) {
		getLogger().debug(logPrefix("onRTMPRequest", client.getAppInstance()) + ": Checking if we are going to handle this request, or ignore it.");

		if (initialized || init()) {
			IApplicationInstance appInstance = client.getAppInstance();
			String vhostName = client.getAppInstance().getVHost().getName();
			getLogger().debug(logPrefix("onRTMPRequest", client.getAppInstance()) + ": Trying to redirect client.");
			if (redirector == null) {
				client.rejectConnection(logPrefix("onRTMPRequest", appInstance) + ": LoadBalancerRedirectorBandwidth not found.");
				getLogger().warn(logPrefix("onRTMPRequest", appInstance) + ": LoadBalancerRedirectorBandwidth not found.");
			}

			LoadBalancerRedirect redirect = redirector.getRedirect(vhostName);
			if (redirect == null) {
				client.rejectConnection(logPrefix("onRTMPRequest", appInstance) + ": Redirect failed.");
				getLogger().warn(logPrefix("onRTMPRequest", appInstance) +
						": LoadBalancerRedirect server not found - no active edge servers available for vhost '" + appInstance.getVHost().getName() + "'?");
			}

			String uriStr = client.getUri();
			getLogger().debug(logPrefix("onRTMPRequest", appInstance) + ": old URI:" + uriStr);
			if (uriStr == null) {
				client.rejectConnection(logPrefix("onRTMPRequest", appInstance) + ": URI missing.");
				getLogger().warn(logPrefix("onRTMPRequest", appInstance) + ": URI missing.");
			}

			try {
				URI uri = new URI(uriStr);

				String scheme = redirectScheme == null ? uri.getScheme() : redirectScheme;
				int port = redirectPort > 0 ? redirectPort : uri.getPort();
				String host = redirect.getHost();
				String path = redirectAppName != null ? redirectAppName : uri.getPath();
				if (path != null) {
					path = path.startsWith("/") ? path : "/" + path;
				}

				// Only add the "?stuff" parameters if the querystring has actual data
				String queryString = (client.getQueryStr() != null && client.getQueryStr() != "") ? "?" + client.getQueryStr() : "";

				getLogger().debug(logPrefix("onRTMPRequest", appInstance) + ": creating new URI:" + scheme + "," + uri.getUserInfo() + "," + host + "," + port + "," + path + "," + uri.getQuery() + "," + uri.getFragment());
				URI newUri = new URI(scheme, uri.getUserInfo(), host, port, path, uri.getQuery(), uri.getFragment());
				getLogger().debug(logPrefix("onRTMPRequest", appInstance) + ": from:" + uriStr + " to:" + newUri.toString() + queryString);

				// Execute the redirect
				client.redirectConnection(newUri.toString() + queryString);
				client.rejectConnection(logPrefix("onRTMPRequest", appInstance) + ": Redirected to '" + newUri.toString() + queryString + "'");
				return;

			} catch (Exception e) {
				client.rejectConnection(logPrefix("onRTMPRequest", appInstance) + ": Exception: " + e.toString());
				getLogger().debug(logPrefix("onRTMPRequest", appInstance) + ": Exception: " + e.toString());
			}
		}
	}
	
	// Seems like client.redirectConnection() only works in the onConnect-stage. Oh well.
	/*public void play(IClient client, RequestFunction function, AMFDataList params) {
		String streamName = params.getString(PARAM1);

		String queryStr = "";
		if (streamName != null) {
			int streamQueryIdx = streamName.indexOf("?");
			if (streamQueryIdx >= 0) {
				queryStr = streamName.substring(streamQueryIdx+1);
				streamName = streamName.substring(0, streamQueryIdx);
			}
		}
		if (redirectOnConnect || isRedirectRequest(client, queryStr)) {
			onRTMPRequest(client, function, params);
		}
		else {
			// This means redirects are disabled, and we should probably serve the streams normally instead.
			this.invokePrevious(client, function, params);
		}
	}*/

	/**
	 * Redirect functionality for iOS-streams (HLS / Cupertino) and Adobe HTTP streams (HDS / San Jose)
	 * 
	 * This prepares a header that will be read by the HTTPStreamers for HLS and HDS.
	 * If the header is not present, the HTTPStreamers will handle the request normally, without a redirect.
	 * @param httpSession
	 */
	public void onHTTPSessionCreate(IHTTPStreamerSession httpSession) {
		getLogger().debug(logPrefix("onHTTPSessionCreate", httpSession.getAppInstance()) + ": Checking if we are going to handle this request, or ignore it.");
		boolean redirectRequest = false;
		// Check if redirect=true if redirectOnConnect is disabled
		if (!redirectOnConnect && httpSession.getQueryStr() != null) {
			String matchString = "(^redirect=(true|1).*)|(.*[\\?\\&]redirect=(true|1).*)";
			redirectRequest = httpSession.getQueryStr().matches(matchString);
			if (redirectRequest) {
				getLogger().debug(logPrefix("onHTTPSessionCreate", httpSession.getAppInstance()) + ": redirectOnConnect is disabled, but 'redirect=true' is specified in the HTTP request.");
			}
		}
		
		if (initialized || init()) {
			if (redirectOnConnect || redirectRequest) {
				IApplicationInstance appInstance = httpSession.getAppInstance();
				LoadBalancerRedirect redirect = null;
				if (redirector.getRedirect(appInstance.getVHost().getName()) == null) {
					getLogger().warn(logPrefix("onHTTPSessionCreate", appInstance) +
							": LoadBalancerRedirect server not found - no active edge servers available for vhost '" + appInstance.getVHost().getName() + "'?");
				}
				else {
					redirect = redirector.getRedirect(appInstance.getVHost().getName());
			        try {
			    		String loadbalancerTargetProtocol = "http://";
			    		String loadbalancerTarget = redirect.getHost();
			    		String uriStr = loadbalancerTargetProtocol + httpSession.getServerIp() + ":" + httpSession.getServerPort() + "/" + httpSession.getUri();
			    		String queryString = (httpSession.getQueryStr() != null && httpSession.getQueryStr() != "") ? "?" + httpSession.getQueryStr() : "";

			    		// Always specify the X-LoadBalancer-Target, so the HTTPStreamers can be sure they are supposed to redirect
			    		httpSession.setUserHTTPHeader("X-LoadBalancer-Target", loadbalancerTarget);
			    		
			    		// Only add redirect to a specific port if we're using a non-standard port. (Ignore -1, which is the default value.)
			    		if (redirectPort > 0 && redirectPort != 80 && redirectPort != 443) {
			    			loadbalancerTarget += ":" + redirectPort;
			    		}
			    		if (redirectPort == 443) {
			    			loadbalancerTargetProtocol = "https://"; // FIXME This is an ugly way of figuring out whether to use https or not
			    		}
			    		
			    		URI uri = new URI(uriStr);
			    		String loadbalancerTargetPath = uri.getPath();

			    		// Check if we're redirecting to a different application, and rewrite if this is the case
			    		if (redirectAppName != null && redirectAppName != appInstance.getApplication().getName()) {
			    			String origName = appInstance.getApplication().getName();
			    			getLogger().debug(String.format("%s: redirectAppName '%s' differs from the current appName '%s' in target path '%s'. Trying to rewrite.", logPrefix("serviceMsg", appInstance), redirectAppName, origName, loadbalancerTargetPath));
			    			String searchAppName = "^/?" + appInstance.getApplication().getName();
			    			loadbalancerTargetPath = loadbalancerTargetPath.replaceFirst(searchAppName, redirectAppName);
			    		}
			    		if (loadbalancerTargetPath != null) {
			    			loadbalancerTargetPath = loadbalancerTargetPath.startsWith("/") ? loadbalancerTargetPath : "/" + loadbalancerTargetPath;
			    		}

			    		getLogger().debug(String.format("%s: New path:  '%s'", logPrefix("serviceMsg", appInstance), loadbalancerTargetPath));
			    		
			    		String baseUrl = loadbalancerTargetProtocol + loadbalancerTarget + loadbalancerTargetPath;			        	
						getLogger().debug(logPrefix("onHTTPSessionCreate", appInstance) + ": Adding HTTP Header 'X-LoadBalancer-Targer: " + loadbalancerTarget + "'");
						
						httpSession.setUserHTTPHeader("X-LoadBalancer-Orig", uri.toString() + queryString);
						//httpSession.setUserHTTPHeader("X-LoadBalancer-Dest", baseUrl + queryString);
						//if (queryString.toLowerCase().contains("?dvr") || queryString.toLowerCase().contains("&dvr")) {
						if (rewriteSessionId) {
							int edgeSessionId = getEdgeSessionId(appInstance, baseUrl, queryString);
							if (edgeSessionId > 0) {
								getLogger().debug(logPrefix("onHTTPSessionCreate", appInstance) + ": Created and found an edge session id. Adding HTTP Header 'X-LoadBalancer-SessionId: " + edgeSessionId + "'");
								httpSession.setUserHTTPHeader("X-LoadBalancer-SessionId", Integer.toString(edgeSessionId));
							}
						}
						//}
						
						//URI newUri = new URI(uri.toString());
						//httpSession.redirectSession(baseUrl + queryString);
						return;
			
					} catch (Exception e) {
						getLogger().error(logPrefix("onHTTPSessionCreate", appInstance) + ": Exception: " + e.toString());
					}
					getLogger().warn(logPrefix("onHTTPSessionCreate", appInstance) + ": Redirect failed - could not add 'X-LoadBalancer-Target' HTTP header.");
				}
				getLogger().debug(logPrefix("onHTTPSessionCreate", httpSession.getAppInstance()) + ": Tried to redirect, but failed. Rejecting session.");
				httpSession.rejectSession(); // TODO Add an error message to the client
			}
			getLogger().debug(logPrefix("onHTTPSessionCreate", httpSession.getAppInstance()) + ": Ignoring request.");
		}
		else {
			getLogger().warn(logPrefix("onHTTPSessionCreate", httpSession.getAppInstance()) + ": Could not initialize the LoadBalancer module.");
		}
    }

	/**
	 * Redirect functionality for RTSP-streams
	 * @param rtpSession
	 */
	public void onRTPSessionCreate(RTPSession rtpSession) {
		getLogger().debug(logPrefix("onRTPSessionCreate", rtpSession.getAppInstance()) + ": Checking if we are going to handle this request, or ignore it.");
		boolean redirectRequest = false;
		// Check if redirect=true if redirectOnConnect is disabled
		if (!redirectOnConnect && rtpSession.getQueryStr() != null) {
			String matchString = "(^redirect=(true|1).*)|(.*[\\?\\&]redirect=(true|1).*)";
			redirectRequest = rtpSession.getQueryStr().matches(matchString);
			if (redirectRequest) {
				getLogger().debug(logPrefix("onRTPSessionCreate", rtpSession.getAppInstance()) + ": redirectOnConnect is disabled, but 'redirect=true' is specified in the RTSP request.");
			}
		}
		if (initialized || init()) {
			if (redirectOnConnect || redirectRequest) {
				IApplicationInstance appInstance = rtpSession.getAppInstance();
				LoadBalancerRedirect redirect = null;
				if (redirector.getRedirect(appInstance.getVHost().getName()) == null) {
					getLogger().warn(logPrefix("onRTPSessionCreate", appInstance) +
							": LoadBalancerRedirect server not found - no active edge servers available for vhost '" + appInstance.getVHost().getName() + "'?");
				}
				else {
					redirect = redirector.getRedirect(appInstance.getVHost().getName());
			        try {
						// RTPSession includes protocol, hostname and port in getUri().
						String uriStr = rtpSession.getUri();
						getLogger().debug(logPrefix("onRTPSessionCreate", appInstance) + ": old URI:" + uriStr);
			
						URI uri = new URI(uriStr);
			
						int port = redirectPort > 0 ? redirectPort : uri.getPort();
						String host = redirect.getHost();
						String path = uri.getPath();
						// Check if we're redirecting to a different application, and rewrite if this is the case
						if (redirectAppName != null && redirectAppName != appInstance.getApplication().getName()) {
							String origName = appInstance.getApplication().getName();
							getLogger().debug(String.format("%s: redirectAppName '%s' differs from the current appName '%s'. Trying to rewrite.", logPrefix("serviceMsg", appInstance), redirectAppName, origName));
							String searchAppName = "^/?" + appInstance.getApplication().getName();
							path = path.replaceFirst(searchAppName, redirectAppName);
						}
						if (path != null) {
							path = path.startsWith("/") ? path : "/" + path;
						}
			
						getLogger().debug(logPrefix("onRTPSessionCreate", appInstance) + ": creating new URI:" + uri.getScheme() + "," + uri.getUserInfo() + "," + host + "," + port + "," + path + "," + uri.getQuery() + "," + uri.getFragment());
						URI newUri = new URI(uri.getScheme(), uri.getUserInfo(), host, port, path, uri.getQuery(), uri.getFragment());
			
						getLogger().debug(logPrefix("onRTPSessionCreate", appInstance) + ": from:" + uriStr + " to:" + newUri.toString());
			
						// Only add the "?stuff" parameters if the querystring has actual data
						String queryString = (rtpSession.getQueryStr() != null && rtpSession.getQueryStr() != "") ? "?" + rtpSession.getQueryStr() : "";
						rtpSession.redirectSession(newUri.toString() + queryString);
						return;
			
					} catch (Exception e) {
						rtpSession.rejectSession(); // TODO figure out a way to add an error message to the client, like rejectConnection(String errorStr) offers
						getLogger().error(logPrefix("onRTPSessionCreate", appInstance) + ": Exception: " + e.toString());
					}
				}
				getLogger().debug(logPrefix("onRTPSessionCreate", rtpSession.getAppInstance()) + ": Tried to redirect, but failed. Rejecting session.");
				rtpSession.rejectSession(); // TODO Add an error message to the client
			}
			getLogger().debug(logPrefix("onRTPSessionCreate", rtpSession.getAppInstance()) + ": Ignoring request.");
		}
		getLogger().warn(logPrefix("onRTPSessionCreate", rtpSession.getAppInstance()) + ": Could not initialize the LoadBalancer module.");
	}
	
	/**
	 * Connect to the edge server, create a session there and grab the session Id
	 * 
	 * This is required if ?DVR is enabled, but doesn't seem to be necesssary otherwise.
	 * (Logging might be slightly odd up on the edges without it, though.)
	 * 
	 * @param appInstance
	 * @param baseUrl
	 * @param queryString
	 * @return Returns the newly created edge sessionId
	 */
	private Integer getEdgeSessionId(IApplicationInstance appInstance, String baseUrl, String queryString) {
		Pattern mediaPattern = Pattern.compile(".*<media [^>]+ url=\"[^>]+>.*");
		Pattern chunklistOld = Pattern.compile("^chunklist((-[^\\.]+)?\\.m3u8(.*))");
		Pattern chunklistNew = Pattern.compile("^chunklist((_w[^\\.]+)?\\.m3u8(.*))");
		getLogger().debug(logPrefix("getEdgeSessionId", appInstance) + ": Trying to generate and fetch sessionId from '" + baseUrl + queryString + "'");
		try {
			URL url = new URL(baseUrl + queryString);
			InputStreamReader inputStreamReader = new InputStreamReader(url.openStream());
			BufferedReader in = new BufferedReader(inputStreamReader);
			String line;
			while ((line = in.readLine()) != null) {
			    //System.out.println(inputLine);
				//getLogger().error(logPrefix("onHTTPSessionCreate", appInstance) + ": debug - " + line);
				try {
					if (mediaPattern.matcher(line).matches()) {
						getLogger().debug(logPrefix("getEdgeSessionId", appInstance) + ": Found media. Parsing line '" + line + "'");
						Integer sessionId = Integer.parseInt(line.replaceAll(".*<media[^>]+ url=\"[^>]+_w([0-9]+)(_[^>]+)?\\.abst\\/\">.*", "$1"));
						getLogger().info(logPrefix("getEdgeSessionId", appInstance) + ": Found sessionId '" + sessionId + "' on line '" + line + "'");
						return sessionId;
					}
					if (chunklistOld.matcher(line).matches()) {
						getLogger().debug(logPrefix("getEdgeSessionId", appInstance) + ": Found Wowza < 3.6.x style chunklist. Parsing line '" + line + "'");
						Integer sessionId = Integer.parseInt(line.replaceAll(".*[\\?&]wowzasessionid=([0-9]+).*", "$1"));
						getLogger().info(logPrefix("getEdgeSessionId", appInstance) + ": Found sessionId '" + sessionId + "' on line '" + line + "'");
						return sessionId;
					}
					if (chunklistNew.matcher(line).matches()) {
						getLogger().debug(logPrefix("getEdgeSessionId", appInstance) + ": Found Wowza >= 3.6.x style chunklist. Parsing line '" + line + "'");
						Integer sessionId = Integer.parseInt(line.replaceAll("chunklist_w([0-9]+).*", "$1"));
						getLogger().info(logPrefix("getEdgeSessionId", appInstance) + ": Found sessionId '" + sessionId + "' on line '" + line + "'");
						return sessionId;
					}
				} catch (NumberFormatException e) {
					// No sessionId found on the current line. Proceed to the next.
				}
			}
			in.close();
			inputStreamReader.close();
		} catch (java.io.FileNotFoundException e) {
			// Expected IOException. Just print a simple error message and return 0.
			getLogger().error(logPrefix("getEdgeSessionId", appInstance) + ": Could not fetch sessionId - got 404 / File Not Found exception from '" + baseUrl + queryString + "'. Check if stream is valid.");
			return 0;
		}
		catch (IOException e) {
			// Unexpected IOException. Print the whole stacktrace.
			e.printStackTrace();
		}
		getLogger().error(logPrefix("getEdgeSessionId", appInstance) + ": Could not fetch sessionId from '" + baseUrl + queryString + "'");
		return 0;
	}
}
