/**
 * HTTPStreamerAdapterSanJoseRedirector.java
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

package com.availo.wms.httpstreamer;

import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.mina.common.ByteBuffer;

import com.availo.wms.plugin.vhostloadbalancer.ConfigCache;
import com.availo.wms.plugin.vhostloadbalancer.LoadBalancerRedirectorBandwidth;
import com.availo.wms.plugin.vhostloadbalancer.ServerListenerLoadBalancerListener;
import com.availo.wms.plugin.vhostloadbalancer.ConfigCache.MissingPropertyException;
import com.wowza.util.FasterByteArrayOutputStream;
import com.wowza.wms.application.IApplicationInstance;
import com.wowza.wms.httpstreamer.sanjosestreaming.httpstreamer.HTTPStreamerAdapterSanJoseStreamer;
import com.wowza.wms.httpstreamer.model.IHTTPStreamerSession;
import com.wowza.wms.logging.WMSLogger;
import com.wowza.wms.logging.WMSLoggerFactory;
import com.wowza.wms.plugin.loadbalancer.LoadBalancerListener;
import com.wowza.wms.plugin.loadbalancer.LoadBalancerRedirect;
import com.wowza.wms.server.Server;
import com.wowza.wms.vhost.IVHost;

/**
 * Wowza HTTPStreamerAdapter for San Jose (HDS) with load balancing features
 * 
 * This module should replace the "sanjosestreaming" HTTPStreamer in HTTPStreamers.xml:
 * 
 * <!--<BaseClass>com.wowza.wms.httpstreamer.sanjosestreaming.httpstreamer.HTTPStreamerAdapterSanJoseStreamer</BaseClass>-->
 * <BaseClass>com.availo.wms.httpstreamer.HTTPStreamerAdapterSanJoseRedirector</BaseClass>
 * 
 * @author Brynjar Eide <brynjar@availo.no>
 * @version 1.1, 2012-12-05
 *
 */
public class HTTPStreamerAdapterSanJoseRedirector extends HTTPStreamerAdapterSanJoseStreamer {

	/**
	 * LoadBalancerListener is where we'll get our LoadBalancerRedirectorBandwidth from
	 */
	private LoadBalancerListener listener = null;

	/**
	 * LoadBalancerRedirectorBandwidth knows the IP address (or hostname) of the server that is deemed to be most suitable for the next connection
	 */
	private LoadBalancerRedirectorBandwidth redirector = null;

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
	 * Keep track of whether all properties have been loaded for this instance
	 */
	private boolean initialized = false;
	
	/**
	 * Used for logging purposes
	 */
	private static String className = "HTTPStreamerAdapterCupertinoRedirector";

	/**
	 * Rewrite all manifest.f4m files to contain absolute URLs
	 * FIXME Is there a more proper way to manipulate manifests, other than brutally rewriting the responses in serviceMsg()?
	 * (I'd rather use a hack on the loadbalancer listeners than on all the edges/senders.) 
	 */
	public void serviceMsg(long timestamp, org.apache.mina.common.IoSession ioSession, com.wowza.wms.server.RtmpRequestMessage req, com.wowza.wms.server.RtmpResponseMessage resp) {
		super.serviceMsg(timestamp, ioSession, req, resp);
		// Don't waste time and CPU cycles on parsing requests for anything other than manifest.f4m, optionally with parameters of some sort
		if (!req.getPath().matches("(?i).*\\/manifest\\.f4m(\\?[^\\/]*)?$")) {
			getLogger().debug(String.format("%s: Received a non-manifest request. (%s)", req.getSessionInfo().getVHost().getName(), req.getPath()));
			return;
		}
		if (!resp.getHeaders().containsKey("X-LoadBalancer-Target") || resp.getHeaders().get("X-LoadBalancer-Target") == null) {
			getLogger().debug(String.format("%s: Request with sessionId '%s' not flagged for loadbalancing. Ignoring.",
						logPrefix("serviceMsg", req.getSessionInfo().getVHost().getName()),
						req.getHTTPPendingRequestSessionId()
			));
			return;
		}

		IApplicationInstance appInstance = null;
		IHTTPStreamerSession httpSession = null;
		// Grab the session, so we can tell what application we're working with
		httpSession = grabSession(req);
		if (httpSession == null) {
			getLogger().warn(String.format("%s: Missing session data for request with sessionId '%s'.",
					logPrefix("serviceMsg", req.getSessionInfo().getVHost().getName()), req.getHTTPPendingRequestSessionId(), req.getPath()
			));
			return;
		}

		// Seems like we found everything we need to start manipulating the manifest with absolute URLs
		appInstance = httpSession.getAppInstance();
		getLogger().debug(String.format("%s: query string: %s", logPrefix("serviceMsg", appInstance), httpSession.getQueryStr()));
		// Load the ConfigCache and all relevant properties
		if (!initialized) {
			init(appInstance);
		}

		getLogger().debug(String.format("%s: Received a manifest request to '%s'", logPrefix("serviceMsg", appInstance), req.getPath()));

		// This header is set in ModuleLoadBalancerRedirector.onHTTPSessionCreate(). Ignore any request without the header.
		if (!resp.getHeaders().containsKey("X-LoadBalancer-Target")) {
			return;
		}

		// We have no data to manipulate.
		/*if (resp.getBodyList().isEmpty()) {
			return;
		}*/
		LoadBalancerRedirect redirect = redirector.getRedirect(appInstance.getVHost().getName());
		String loadbalancerTargetProtocol = "http://";
		String loadbalancerTarget = redirect.getHost();

		// Only add redirect to a specific port if we're using a non-standard port. (Ignore -1, which is the default value.)
		if (redirectPort > 0 && redirectPort != 80 && redirectPort != 443) {
			loadbalancerTarget += ":" + redirectPort;
		}
		if (redirectPort == 443) {
			loadbalancerTargetProtocol = "https://"; // FIXME This is an ugly way of figuring out whether to use https or not
		}

		// Wowza only accepts manifest.f4m. Get the path from the requested URL by removing /manifest.f4m, followed by optional HTTP GET arguments. (?wowzasessionid=xyz)
		getLogger().debug(String.format("%s: Attempting to remove manifest.f4m extension from '%s'", logPrefix("serviceMsg", appInstance), req.getPath()));
		String loadbalancerTargetPath = req.getPath().replaceFirst("(?i)/manifest\\.f4m?(\\?[^\\/]*)?$", "");

		// Check if we're redirecting to a different application, and rewrite if this is the case
		if (redirectAppName != null && redirectAppName != appInstance.getApplication().getName()) {
			String origName = appInstance.getApplication().getName();
			getLogger().debug(String.format("%s: redirectAppName '%s' differs from the current appName '%s'. Trying to rewrite.", logPrefix("serviceMsg", appInstance), redirectAppName, origName));
			String searchAppName = "^" + appInstance.getApplication().getName();
			loadbalancerTargetPath = loadbalancerTargetPath.replaceFirst(searchAppName, redirectAppName);
		}
		if (loadbalancerTargetPath != null) {
			loadbalancerTargetPath = loadbalancerTargetPath.startsWith("/") ? loadbalancerTargetPath : "/" + loadbalancerTargetPath;
		}

		getLogger().debug(String.format("%s: New path:  '%s'", logPrefix("serviceMsg", appInstance), loadbalancerTargetPath));
		
		String baseUrl = loadbalancerTargetProtocol + loadbalancerTarget + loadbalancerTargetPath;
		rewriteHTML(appInstance, resp, baseUrl);
	}
	 
	/**
	 * Rewrite the HTML manifests with absolute URLs
	 * @param appInstance
	 * @param resp The response that will be sent to the client
	 * @param baseUrl The absolute URL that will be used as a prefix to every relevant line in the manifest
	 * @return
	 */
	private boolean rewriteHTML(IApplicationInstance appInstance, com.wowza.wms.server.RtmpResponseMessage resp, String baseUrl) {
		boolean rewritten = false;
		try {
			StringBuffer absoluteData = new StringBuffer();

			Pattern mediaPattern = Pattern.compile(".*<media [^>]+ url=\"[^>]+>.*");
			Pattern mediaPatternAbsolute = Pattern.compile(".*<media [^>]+ url=\"http://[^>]+>.*");
			Pattern bootstrapPattern = Pattern.compile(".*<bootstrapInfo [^>]+ url=\"[^>]+>.*");
			Pattern bootstrapPatternAbsolute = Pattern.compile(".*<bootstrapInfo [^>]+ url=\"http://[^>]+>.*");

			List<ByteBuffer> bufferlist = resp.getBodyList();
			Charset charset = Charset.forName("UTF-8");
			CharsetDecoder decoder = charset.newDecoder();
			StringBuffer originalData = new StringBuffer();
			for (ByteBuffer buffer : bufferlist) {
				originalData.append(buffer.getString(decoder));
			}
			//getLogger().debug(String.format("%s: Working on data:\n%s", logPrefix("rewriteHTML", appInstance), originalData));
			String lines[] = originalData.toString().split("\\r?\\n|\\r");
			for (String line : lines) {
				//getLogger().debug(String.format("%s: Working on line '%s'", logPrefix("rewriteHTML", appInstance), line));
				// Rewrite media (stream) URL from relative to absolute <media width="640" height="480" url="media_b125000_w902486609.abst/">
				if (mediaPattern.matcher(line).matches() && !mediaPatternAbsolute.matcher(line).matches()) {
					line = line.replaceAll("(<media[^>]+) url=\"", "$1 url=\"" + baseUrl + "/");
					rewritten = true;
				}
				 // Rewrite playlist URL from relative to absolute <bootstrapInfo profile="named" url="playlist_b125000_w1903190415.abst"/> 
				if (bootstrapPattern.matcher(line).matches() && !bootstrapPatternAbsolute.matcher(line).matches()) {
					line = line.replaceAll("(<bootstrapInfo[^>]+) url=\"", "$1 url=\""  + baseUrl + "/");
					rewritten = true;
				}
				absoluteData.append(line + "\n");
			}

			if (rewritten) {
				// Get the outputstream that eventually will be sent to the user
				FasterByteArrayOutputStream outputStream = (FasterByteArrayOutputStream) resp.getOutputStream();
				outputStream.reset();
				PrintStream printStream = new PrintStream(outputStream);
				// Print the newly replaced data to it
				printStream.print(absoluteData.toString());
				printStream.close();
				getLogger().info(String.format("%s: manifest.f4m rewritten with absolute URLs (%s)", logPrefix("rewriteHTML", appInstance), baseUrl));
			}
			else {
				getLogger().debug(String.format("%s: manifest could not be rewritten with absolute URLs", logPrefix("rewriteHTML", appInstance)));
			}

		} catch (Exception e){
			getLogger().error(String.format("%s: %s", logPrefix("rewriteHTML", appInstance), e.getMessage()));
			e.printStackTrace();
		}
		return rewritten;
	}
	
	/**
	 * Initialise the ConfigCache and load all required properties
	 * @param appInstance
	 */
	private void init(IApplicationInstance appInstance) {
		if (config == null) {
			getLogger().info(String.format("%s: Loading ConfigCache and LoadBalancerRedirectorBandwidth.", logPrefix("init", appInstance)));
			config = ConfigCache.getInstance();
		}
		if (listener == null || redirector == null) {
			while (true) {
				listener = (LoadBalancerListener) Server.getInstance().getProperties().get(ServerListenerLoadBalancerListener.PROP_LOADBALANCERLISTENER);
				if (listener == null) {
					getLogger().warn(String.format("%s: LoadBalancerListener not found. All connections to this application will be refused.", logPrefix("init", appInstance)));
					break;
				}
	
				redirector = (LoadBalancerRedirectorBandwidth) listener.getRedirector();
	
				if (redirector == null) {
					getLogger().warn(String.format("%s: ILoadBalancerRedirector not found. All connections to this application will be refused.", logPrefix("init", appInstance)));
					break;
				}
				break;
			}
		}
		if (config.loadProperties(appInstance)) {
			try {
				redirectAppName = config.getRedirectAppName(appInstance);
				//redirectScheme = config.getRedirectScheme(appInstance); // Not in use with HTTP streaming
				redirectPort = config.getRedirectPort(appInstance);
				//redirectOnConnect = config.getRedirectOnConnect(appInstance); // redirectOnConnect is handled by *not* adding X-LoadBalancer-Target in ModuleLoadBalancerRedirect 
			} catch (MissingPropertyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		getLogger().debug(String.format("%s: Finished initializing.", logPrefix("init", appInstance)));
		initialized = true;
	}
	
	/**
	 * Check all known sessions for the sessionId from the HTTP request, and return it
	 * @param req The incoming HTTP request
	 * @return The session object that matches the incoming HTTP request
	 */
	private IHTTPStreamerSession grabSession(com.wowza.wms.server.RtmpRequestMessage req) {
		Iterator<IHTTPStreamerSession> sessions = req.getSessionInfo().getHTTPSession().iterator();
		IVHost vhost = req.getSessionInfo().getVHost();
		IHTTPStreamerSession httpSession = null;
		while (sessions.hasNext()) {
			httpSession = (IHTTPStreamerSession) sessions.next();
			if (req.getHTTPPendingRequestSessionId() == httpSession.getSessionId()) {
				getLogger().debug(
						String.format("%s: Found a match for request with sessionId '%s'",
						logPrefix("grabSession", httpSession.getAppInstance()), req.getHTTPPendingRequestSessionId())
				);
				// TODO Uncomment the next line once I am more familiar with Wowza's session handling
				//break;
			}
			else {
				// I haven't found any documentation on how Wowza handles sessions. Print a warning until I feel more confident about them.
				getLogger().warn(
						String.format("%s: Request with PendingRequestSessionId '%s' also has active sessionId '%s'",
								logPrefix("grabSession", httpSession.getAppInstance()), req.getHTTPPendingRequestSessionId(), httpSession.getSessionId())
				);
			}
		}
		if (httpSession == null) {
			// This is probably not an error, but log a warning for the time being.
			getLogger().warn(
					String.format("%s: Got a HTTP request with sessionId '%s', but couldn't find any relevant session.'",
					vhost.getName(), req.getHTTPPendingRequestSessionId())
			);
		}
		return httpSession;
	}

	/**
	 * Shortcut used for adding 'classname.functionname' to logs
	 * @param appInstance
	 * @return
	 */
	private String logPrefix(String vhostName, String functionName) {
		return String.format("%s.%s", className, functionName);
	}
	
	/**
	 * Shortcut used for adding 'classname.functionname[vhostname/applicationname/instance]' to logs
	 * @param appInstance
	 * @return
	 */
	private String logPrefix(String functionName, IApplicationInstance appInstance) {
		return String.format("%s.%s[%s/%s/%s]", className, functionName, appInstance.getVHost().getName(), appInstance.getApplication().getName(), appInstance.getName());
	}

	/**
	 * Shortcut to debug logging
	 * @return WMSLogger object used to log warnings or debug messages
	 */
	protected static WMSLogger getLogger() {
		return WMSLoggerFactory.getLogger(HTTPStreamerAdapterSanJoseRedirector.class);
	}
}
