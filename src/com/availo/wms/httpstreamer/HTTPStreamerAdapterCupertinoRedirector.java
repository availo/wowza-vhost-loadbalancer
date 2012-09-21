package com.availo.wms.httpstreamer;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.regex.Pattern;
import org.apache.mina.common.ByteBuffer;
import com.wowza.wms.httpstreamer.cupertinostreaming.httpstreamer.HTTPStreamerAdapterCupertinoStreamer;
import com.wowza.wms.logging.*;

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
	 * Rewrite all Playlist.m3u8 files to contain absolute URLs
	 * FIXME This is really just a hack to avoid using 30X HTTP redirects, which do not work without absolute URLs on the edges.
	 * I'd rather use a hack on the loadbalancer listeners than on all the edges/senders. 
	 */
	public void serviceMsg(long timestamp, org.apache.mina.common.IoSession ioSession, com.wowza.wms.server.RtmpRequestMessage req, com.wowza.wms.server.RtmpResponseMessage resp) {
		super.serviceMsg(timestamp, ioSession, req, resp);
		// Do not attempt to rewrite *anything* that isn't Playlist.m3u8.
		// TODO Add a redirect if we have requests for playlists or streams by this point, to avoid cases where players start streaming from our loadbalancer
		if (!req.getPath().matches(".*/Playlist\\.m3u8$")) {
			getLogger().debug("HTTPStreamerAdapterCupertinoRedirector.serviceMsg received a non-Playlist request: '" + req.getPath() + "'");
		}
		else {
			getLogger().debug("HTTPStreamerAdapterCupertinoRedirector.serviceMsg received a Playlist request: req.getPath(): '" + req.getPath() + "'");
			ByteBuffer buffer = resp.getBody();
			Charset charset = Charset.forName("UTF-8");
			CharsetDecoder decoder = charset.newDecoder();
		
			if (buffer != null && resp.getHeaders().containsKey("X-LoadBalancer-Target")) {
				// This header is set in ModuleLoadBalancerRedirector.onHTTPSessionCreate()
				String loadbalancerTarget = resp.getHeaders().get("X-LoadBalancer-Target");
				// Get the path from the requested URL by removing /Playlist.m3u8
				String loadbalancerTargetPath = req.getPath().replaceFirst("/Playlist\\.m3u8$", "");
				String loadbalancerTargetProtocol = "http://";
				if (loadbalancerTarget.matches(":443$")) {
					loadbalancerTargetProtocol = "https://"; // FIXME This is an ugly way of figuring out whether to use https or not
				}
				try {
					int oldPosition = buffer.position();
					String absoluteData = buffer.getString(decoder);
	
					/* 
					 * Rewrite chunklist URL from relative to absolute
					 * chunklist.m3u8?wowzasessionid=1982042183
					 */
					Pattern pattern = Pattern.compile("(.*)^chunklist\\.m3u8(.*)", Pattern.MULTILINE + Pattern.DOTALL);
					if (pattern.matcher(absoluteData).matches()) {
						//absoluteData = absoluteData.replaceFirst("^chunklist\\.m3u8", loadbalancerTargetProtocol + loadbalancerTarget + "/" + loadbalancerTargetPath + "/chunklist.m3u8");
						absoluteData = pattern.matcher(absoluteData).replaceFirst("$1" + loadbalancerTargetProtocol + loadbalancerTarget + "/" + loadbalancerTargetPath + "/chunklist.m3u8$2");
						resp.clearBody();
						resp.appendBody(absoluteData);
						getLogger().debug("HTTPStreamerAdapterCupertinoRedirector.serviceMsg Playlist.m3u8 rewritten with absolute URLs (" + loadbalancerTarget + ")");
					}
					else {
						getLogger().debug("HTTPStreamerAdapterCupertinoRedirector.serviceMsg Playlist.m3u8 could not be rewritten with absolute URLs");
					}

					// Reset buffer's position to its original
					buffer.position(oldPosition);
				} catch (Exception e){
					getLogger().error("HTTPStreamerAdapterCupertinoRedirector.serviceMsg: " + e.getMessage());
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * Shortcut to debug logging
	 * @return WMSLogger object used to log warnings or debug messages
	 */
	protected static WMSLogger getLogger() {
		return WMSLoggerFactory.getLogger(HTTPStreamerAdapterCupertinoRedirector.class);
	}
}
