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
import com.wowza.wms.application.IApplication;
import com.wowza.wms.application.IApplicationInstance;
import com.wowza.wms.httpstreamer.cupertinostreaming.httpstreamer.HTTPStreamerAdapterCupertinoStreamer;
import com.wowza.wms.httpstreamer.model.IHTTPStreamerSession;
import com.wowza.wms.logging.*;
import com.wowza.wms.plugin.loadbalancer.LoadBalancerListener;
import com.wowza.wms.plugin.loadbalancer.LoadBalancerRedirect;
import com.wowza.wms.server.Server;
import com.wowza.wms.vhost.IVHost;

/**
 * Wowza HTTPStreamerAdapter for Cupertino (HLS) with load balancing features
 * 
 * This module should replace the "cupertinostreaming" HTTPStreamer in HTTPStreamers.xml:
 * 
 * <!--<BaseClass>com.wowza.wms.httpstreamer.cupertinostreaming.httpstreamer.HTTPStreamerAdapterCupertinoStreamer</BaseClass>-->
 * <BaseClass>com.availo.wms.httpstreamer.HTTPStreamerAdapterCupertinoRedirector</BaseClass>
 * 
 * @author Brynjar Eide <brynjar@availo.no>
 * @version 1.0b, 2012-09-20
 *
 */
public class HTTPStreamerAdapterCupertinoRedirector extends HTTPStreamerAdapterCupertinoStreamer {

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
	 * Semi-optional property that decides whether a connection should be automatically redirected, or if it needs to ask for a redirect. (RTMP-only) 
	 */
	private boolean redirectOnConnect;

	private boolean initialized = false;

	/**
	 * Call the parent class and log a debug statement 
	 */
	public HTTPStreamerAdapterCupertinoRedirector() {
		super();
		getLogger().debug("HTTPStreamerAdapterCupertinoRedirector: Starting HTTPStreamerAdapterCupertinoRedirector");
	}

	/**
	 * Rewrite all Playlist.m3u8 files to contain absolute URLs
	 * FIXME Is there a more proper way to manipulate playlists, other than brutally rewriting the responses in serviceMsg()?
	 * (I'd rather use a hack on the loadbalancer listeners than on all the edges/senders.) 
	 */
	public void serviceMsg(long timestamp, org.apache.mina.common.IoSession ioSession, com.wowza.wms.server.RtmpRequestMessage req, com.wowza.wms.server.RtmpResponseMessage resp) {
		super.serviceMsg(timestamp, ioSession, req, resp);
		// Don't waste time and CPU cycles on parsing requests for anything other than .m3u8 (or .m3u) playlists, optionally with parameters of some sort
		//if (!req.getPath().matches("(?i).*\\/([^\\/]*)\\.m3u8?$")) {
		if (!req.getPath().matches("(?i).*\\/([^\\/]*)\\.m3u8?(\\?[^\\/]*)?$")) {
			getLogger().debug(String.format("HTTPStreamerAdapterCupertinoRedirector.serviceMsg[%s]: Received a non-Playlist request. (%s)", req.getSessionInfo().getVHost().getName(), req.getPath()));
			return;
		}
		if (!resp.getHeaders().containsKey("X-LoadBalancer-Target") || resp.getHeaders().get("X-LoadBalancer-Target") == null) {
			getLogger().debug(String.format("HTTPStreamerAdapterCupertinoRedirector.serviceMsg[%s]: Request with sessionId '%s' not flagged for loadbalancing (%s). Ignoring.", req.getSessionInfo().getVHost(), req.getHTTPPendingRequestSessionId(), req.getPath()));
			return;
		}

		IApplicationInstance appInstance = null;
		IHTTPStreamerSession httpSession = null;
		// Grab the session, so we can tell what application we're working with
		httpSession = grabSession(req);
		if (httpSession == null) {
			getLogger().warn(String.format("HTTPStreamerAdapterCupertinoRedirector.serviceMsg[%s]: Missing session data for request with sessionId '%s' (%s)", req.getSessionInfo().getVHost(), req.getHTTPPendingRequestSessionId(), req.getPath()));
			return;
		}

		// Seems like we found everything we need to start manipulating the playlist with absolute URLs
		appInstance = httpSession.getAppInstance();

		// Load the ConfigCache and all relevant properties
		if (!initialized) {
			init(appInstance);
		}

		getLogger().debug(String.format("HTTPStreamerAdapterCupertinoRedirector.serviceMsg[%s]: Received a playlist/medialist request to '%s'", getFullName(appInstance), req.getPath()));

		// This header is set in ModuleLoadBalancerRedirector.onHTTPSessionCreate(). Ignore any request without the header.
		if (!resp.getHeaders().containsKey("X-LoadBalancer-Target")) {
			return;
		}
		
		// We have no data to manipulate.
		if (resp.getBodyList().isEmpty()) {
			return;
		}
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

		// Wowza accepts *.m3u and *.m3u8. Get the path from the requested URL by removing /<whatever>.m3u8, followed by optional HTTP GET arguments. (?wowzasessionid=xyz)
		getLogger().debug(String.format("HTTPStreamerAdapterCupertinoRedirector.serviceMsg[%s]: Attempting to remove playlist extension from '%s'", getFullName(appInstance), req.getPath()));
		String loadbalancerTargetPath = req.getPath().replaceFirst("(?i)/[^\\/]*\\.m3u8?(\\?[^\\/]*)?$", "");

		// Check if we're redirecting to a different application, and rewrite if this is the case
		if (redirectAppName != null && redirectAppName != appInstance.getApplication().getName()) {
			String origName = appInstance.getApplication().getName();
			getLogger().debug(String.format("HTTPStreamerAdapterCupertinoRedirector.serviceMsg[%s]: redirectAppName '%s' differs from the current appName '%s'. Trying to rewrite.", getFullName(appInstance), redirectAppName, origName));
			String searchAppName = "^" + appInstance.getApplication().getName();
			loadbalancerTargetPath = loadbalancerTargetPath.replaceFirst(searchAppName, redirectAppName);
		}

		getLogger().debug(String.format("HTTPStreamerAdapterCupertinoRedirector.serviceMsg[%s]: New path:  '%s'", getFullName(appInstance), loadbalancerTargetPath));
		
		String baseUrl = loadbalancerTargetProtocol + loadbalancerTarget + "/" + loadbalancerTargetPath;
		rewriteHTML(appInstance, resp, baseUrl);

	}
	
	/**
	 * Rewrite the HTML playlists with absolute URLs
	 * @param appInstance
	 * @param resp The response that will be sent to the client
	 * @param baseUrl The absolute URL that will be used as a prefix to every relevant line in the playlist
	 * @return
	 */
	private boolean rewriteHTML(IApplicationInstance appInstance, com.wowza.wms.server.RtmpResponseMessage resp, String baseUrl) {
		boolean rewritten = false;
		List<ByteBuffer> bufferlist = resp.getBodyList();
		Charset charset = Charset.forName("UTF-8");
		CharsetDecoder decoder = charset.newDecoder();
		try {
			StringBuffer absoluteData = new StringBuffer();

			// Rewrite chunklist URL from relative to absolute chunklist.m3u8?wowzasessionid=1482042183
			Pattern chunklist = Pattern.compile("^chunklist((-[^\\.]+)?\\.m3u8(.*))");

			// Rewrite media URLs from relative to absolute: media-b3500000_145.ts?wowzasessionid=1482042183 - (this shouldn't happen if the chunklist redirects as intended.)
			Pattern medialist = Pattern.compile("^media((-[^\\.]+)?\\.ts(.*))");
			StringBuffer originalData = new StringBuffer();
			for (ByteBuffer buffer : bufferlist) {
				 originalData.append(buffer.getString(decoder));
			}
			getLogger().debug(String.format("HTTPStreamerAdapterCupertinoRedirector.rewriteHTML[%s]: Working on data:\n%s", getFullName(appInstance), originalData));
			String lines[] = originalData.toString().split("\\r?\\n|\\r");
			for (String line : lines) {
				getLogger().debug(String.format("HTTPStreamerAdapterCupertinoRedirector.rewriteHTML[%s]: Working on line '%s'", getFullName(appInstance), line));
				if (chunklist.matcher(line).matches()) {
					line = chunklist.matcher(line).replaceAll(baseUrl + "/chunklist$1");
					rewritten = true;
				}
				if (medialist.matcher(line).matches()) {
					line = medialist.matcher(line).replaceAll(baseUrl + "/media$1");
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
				getLogger().info(String.format("HTTPStreamerAdapterCupertinoRedirector.rewriteHTML[%s]: Playlist.m3u8 rewritten with absolute URLs (%s)", getFullName(appInstance), baseUrl));
			}
			else {
				getLogger().debug(String.format("HTTPStreamerAdapterCupertinoRedirector.rewriteHTML[%s]: Playlist.m3u8 could not be rewritten with absolute URLs", getFullName(appInstance)));
			}

		} catch (Exception e){
			getLogger().error(String.format("HTTPStreamerAdapterCupertinoRedirector.rewriteHTML[%s]: %s", getFullName(appInstance), e.getMessage()));
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
			getLogger().info("HTTPStreamerAdapterCupertinoRedirector.init[" + getFullName(appInstance) + "]: Loading ConfigCache and LoadBalancerRedirectorBandwidth.");
			config = ConfigCache.getInstance();
		}
		if (listener == null || redirector == null) {
			while (true) {
				listener = (LoadBalancerListener) Server.getInstance().getProperties().get(ServerListenerLoadBalancerListener.PROP_LOADBALANCERLISTENER);
				if (listener == null) {
					getLogger().warn("HTTPStreamerAdapterCupertinoRedirector.init[" + getFullName(appInstance) + "]: LoadBalancerListener not found. All connections to this application will be refused.");
					break;
				}
	
				redirector = (LoadBalancerRedirectorBandwidth) listener.getRedirector();
	
				if (redirector == null) {
					getLogger().warn("HTTPStreamerAdapterCupertinoRedirector.init[" + getFullName(appInstance) + "]: ILoadBalancerRedirector not found. All connections to this application will be refused.");
					break;
				}
				break;
			}
		}
		if (config.loadProperties(appInstance)) {
			try {
				redirectAppName = config.getRedirectAppName(appInstance);
				// redirectScheme = config.getRedirectScheme(appInstance); // Not in use with HTTP streaming
				redirectPort = config.getRedirectPort(appInstance);
				redirectOnConnect = config.getRedirectOnConnect(appInstance);
			} catch (MissingPropertyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		getLogger().debug("HTTPStreamerAdapterCupertinoRedirector.init[" + getFullName(appInstance) + "]: Finished initializing.");
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
						String.format("HTTPStreamerAdapterCupertinoRedirector.grabSession[%s]: Found a match for request with sessionId '%s'",
						getFullName(httpSession.getAppInstance()), req.getHTTPPendingRequestSessionId())
				);
				// TODO Uncomment the next line once I am more familiar with Wowza's session handling
				//break;
			}
			else {
				// I haven't found any documentation on how Wowza handles sessions. Print a warning until I feel more confident about them.
				getLogger().warn(
						String.format("HTTPStreamerAdapterCupertinoRedirector.grabSession[%s]: Request with PendingRequestSessionId '%s' also has active sessionId '%s'",
						getFullName(httpSession.getAppInstance()), req.getHTTPPendingRequestSessionId(), httpSession.getSessionId())
				);
			}
		}
		if (httpSession == null) {
			// This is probably not an error, but log a warning for the time being.
			getLogger().warn(
					String.format("HTTPStreamerAdapterCupertinoRedirector.grabSession[%s]: Got a HTTP request with sessionId '%s', but couldn't find any relevant session.'",
					vhost.getName(), req.getHTTPPendingRequestSessionId())
			);
		}
		return httpSession;
	}
	
	/**
	 * Shortcut used for adding 'vhostname/applicationname/instance' to logs
	 * @param appInstance
	 * @return
	 */
	private String getFullName(IApplicationInstance appInstance) {
		return appInstance.getVHost().getName() + "/" + appInstance.getApplication().getName() + "/" + appInstance.getName();
	}

	/**
	 * Shortcut to debug logging
	 * @return WMSLogger object used to log warnings or debug messages
	 */
	protected static WMSLogger getLogger() {
		return WMSLoggerFactory.getLogger(HTTPStreamerAdapterCupertinoRedirector.class);
	}
}
