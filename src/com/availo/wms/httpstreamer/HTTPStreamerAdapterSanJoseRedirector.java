package com.availo.wms.httpstreamer;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.regex.Pattern;
import org.apache.mina.common.ByteBuffer;
import com.wowza.wms.httpstreamer.sanjosestreaming.httpstreamer.HTTPStreamerAdapterSanJoseStreamer;
import com.wowza.wms.logging.*;

/**
 * Wowza HTTPStreamerAdapter for San Jose (HDS) with load balancing features
 * 
 * This module should replace the "sanjosestreaming" HTTPStreamer in HTTPStreamers.xml:
 * 
 * <!--<BaseClass>com.wowza.wms.httpstreamer.sanjosestreaming.httpstreamer.HTTPStreamerAdapterSanJoseStreamer</BaseClass>-->
 * <BaseClass>com.availo.wms.httpstreamer.HTTPStreamerAdapterSanJoseRedirector</BaseClass>
 * 
 * @author Brynjar Eide <brynjar@availo.no>
 * @version 1.0b, 2012-09-20
 *
 */
public class HTTPStreamerAdapterSanJoseRedirector extends HTTPStreamerAdapterSanJoseStreamer {

	/**
	 * Rewrite all manifest.f4m files to contain absolute URLs
	 * FIXME This is really just a hack to avoid using 30X HTTP redirects, which do not work without absolute URLs on the edges.
	 * I'd rather use a hack on the loadbalancer listeners than on all the edges/senders. 
	 */
	public void serviceMsg(long timestamp, org.apache.mina.common.IoSession ioSession, com.wowza.wms.server.RtmpRequestMessage req, com.wowza.wms.server.RtmpResponseMessage resp) {
		super.serviceMsg(timestamp, ioSession, req, resp);
		// Do not attempt to rewrite *anything* that isn't manifest.f4m.
		// TODO Add a redirect if we have requests for playlists or streams by this point, to avoid cases where players start streaming from our loadbalancer
		if (!req.getPath().matches(".*/manifest\\.f4m$")) {
			getLogger().debug("HTTPStreamerAdapterSanJoseRedirector.serviceMsg received a non-manifest request: '" + req.getPath() + "'");
		}
		else {
			getLogger().debug("HTTPStreamerAdapterSanJoseRedirector.serviceMsg received a manifest request: req.getPath(): '" + req.getPath() + "'");
			ByteBuffer buffer = resp.getBody();
			Charset charset = Charset.forName("UTF-8");
			CharsetDecoder decoder = charset.newDecoder();
		
			if (buffer != null && resp.getHeaders().containsKey("X-LoadBalancer-Target")) {
				// This header is set in ModuleLoadBalancerRedirector.onHTTPSessionCreate()
				String loadbalancerTarget = resp.getHeaders().get("X-LoadBalancer-Target");
				// Get the path from the requested URL by removing /manifest.f4m
				String loadbalancerTargetPath = req.getPath().replaceFirst("/manifest\\.f4m$", "");
				String loadbalancerTargetProtocol = "http://";
				if (loadbalancerTarget.matches(".*:443$")) {
					loadbalancerTargetProtocol = "https://"; // FIXME This is an ugly way of figuring out whether to use https or not
				}
				
				try {
					int oldPosition = buffer.position();
					String absoluteData = buffer.getString(decoder);
					boolean rewritten = false;
					
					// Hopefully these regexes should match one specific line each in the multiline XML-data
					Pattern mediaPattern = Pattern.compile(".*<media [^\\r\\n]+ url=\"[^\\r\\n]+>.*", Pattern.MULTILINE | Pattern.DOTALL);
					Pattern mediaPatternAbsolute = Pattern.compile(".*<media [^\\r\\n]+ url=\"http://[^\\r\\n]+>.*", Pattern.MULTILINE | Pattern.DOTALL);
					Pattern bootstrapPattern = Pattern.compile(".*<bootstrapInfo [^\\r\\n]+ url=\"[^\\r\\n]+>.*", Pattern.MULTILINE | Pattern.DOTALL);
					Pattern bootstrapPatternAbsolute = Pattern.compile(".*<bootstrapInfo [^\\r\\n]+ url=\"http://[^\\r\\n]+>.*", Pattern.MULTILINE | Pattern.DOTALL);
		
					/* 
					 * Rewrite media (stream) URL from relative to absolute
					 * <media width="640" height="480" url="media_b125000_w902486609.abst/">
					 */
					if (mediaPattern.matcher(absoluteData).matches() && !mediaPatternAbsolute.matcher(absoluteData).matches()) {
						absoluteData = absoluteData.replaceFirst("(<media[^>]+) url=\"", "$1 url=\"" + loadbalancerTargetProtocol + loadbalancerTarget + "/" + loadbalancerTargetPath + "/");
						rewritten = true;
					}

					/*
					 * Rewrite playlist URL from relative to absolute
					 * <bootstrapInfo profile="named" url="playlist_b125000_w1903190415.abst"/> 
					 */
					if (bootstrapPattern.matcher(absoluteData).matches() && !bootstrapPatternAbsolute.matcher(absoluteData).matches()) {
						absoluteData = absoluteData.replaceFirst("(<bootstrapInfo[^>]+) url=\"", "$1 url=\""  + loadbalancerTargetProtocol  + loadbalancerTarget + "/" + loadbalancerTargetPath + "/");
						rewritten = true;
					}

					if (rewritten) {
						resp.clearBody();
						resp.appendBody(absoluteData);
						getLogger().debug("HTTPStreamerAdapterSanJoseRedirector.serviceMsg manifest.f4m rewritten with absolute URLs (" + loadbalancerTarget + ")");
					}

					// Reset buffer's position to its original
					buffer.position(oldPosition);
				} catch (Exception e){
					getLogger().error("HTTPStreamerAdapterSanJoseRedirector.serviceMsg: " + e.getMessage());
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
		return WMSLoggerFactory.getLogger(HTTPStreamerAdapterSanJoseRedirector.class);
	}
}
