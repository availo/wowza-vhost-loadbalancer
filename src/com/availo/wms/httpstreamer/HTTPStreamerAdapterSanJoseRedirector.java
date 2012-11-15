package com.availo.wms.httpstreamer;

import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.mina.common.ByteBuffer;

import com.wowza.util.FasterByteArrayOutputStream;
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
		if (!req.getPath().matches("(?i).*/manifest\\.f4m$")) {
			getLogger().debug("HTTPStreamerAdapterSanJoseRedirector.serviceMsg: received a non-manifest request: '" + req.getPath() + "'");
		}
		else {
			getLogger().debug("HTTPStreamerAdapterSanJoseRedirector.serviceMsg: received a manifest request: req.getPath(): '" + req.getPath() + "'");
			List<ByteBuffer> bufferlist = resp.getBodyList();
			Charset charset = Charset.forName("UTF-8");
			CharsetDecoder decoder = charset.newDecoder();
		
			if (!bufferlist.isEmpty() && resp.getHeaders().containsKey("X-LoadBalancer-Target")) {
				// This header is set in ModuleLoadBalancerRedirector.onHTTPSessionCreate()
				String loadbalancerTarget = resp.getHeaders().get("X-LoadBalancer-Target");
				// Get the path from the requested URL by removing /manifest.f4m
				String loadbalancerTargetPath = req.getPath().replaceFirst("(?i)/manifest\\.f4m$", "");
				String loadbalancerTargetProtocol = "http://";
				if (loadbalancerTarget.matches(".*:443$")) {
					loadbalancerTargetProtocol = "https://"; // FIXME This is an ugly way of figuring out whether to use https or not
				}
				
				try {
					StringBuffer absoluteData = new StringBuffer();
					
					Pattern mediaPattern = Pattern.compile(".*<media [^>]+ url=\"[^>]+>.*");
					Pattern mediaPatternAbsolute = Pattern.compile(".*<media [^>]+ url=\"http://[^>]+>.*");
					Pattern bootstrapPattern = Pattern.compile(".*<bootstrapInfo [^>]+ url=\"[^>]+>.*");
					Pattern bootstrapPatternAbsolute = Pattern.compile(".*<bootstrapInfo [^>]+ url=\"http://[^>]+>.*");
					boolean rewritten = false;

					for (ByteBuffer buffer : bufferlist) {
						String originalData = buffer.getString(decoder);
						String lines[] = originalData.split("\\r?\\n|\\r");
						for (String line : lines) {
							getLogger().debug("HTTPStreamerAdapterSanJoseRedirector.serviceMsg: Checking line '" + line + "' for relative URLs");
	
							// Rewrite media (stream) URL from relative to absolute <media width="640" height="480" url="media_b125000_w902486609.abst/">
							if (mediaPattern.matcher(line).matches() && !mediaPatternAbsolute.matcher(line).matches()) {
								line = line.replaceAll("(<media[^>]+) url=\"", "$1 url=\"" + loadbalancerTargetProtocol + loadbalancerTarget + "/" + loadbalancerTargetPath + "/");
								getLogger().debug("HTTPStreamerAdapterSanJoseRedirector.serviceMsg: line rewritten to '" + line + "'");
								rewritten = true;
							}
	
							 // Rewrite playlist URL from relative to absolute <bootstrapInfo profile="named" url="playlist_b125000_w1903190415.abst"/> 
							if (bootstrapPattern.matcher(line).matches() && !bootstrapPatternAbsolute.matcher(line).matches()) {
								line = line.replaceAll("(<bootstrapInfo[^>]+) url=\"", "$1 url=\""  + loadbalancerTargetProtocol  + loadbalancerTarget + "/" + loadbalancerTargetPath + "/");
								getLogger().debug("HTTPStreamerAdapterSanJoseRedirector.serviceMsg: line rewritten to '" + line + "'");
								rewritten = true;
							}
							
							if (!rewritten) {
								getLogger().debug("HTTPStreamerAdapterSanJoseRedirector.serviceMsg: No relative URLs found");
							}
							absoluteData.append(line + "\n");
						}

					}
					if (rewritten) {
						// Get the outputstream that eventually will be sent to the user
						FasterByteArrayOutputStream outputStream = (FasterByteArrayOutputStream) resp.getOutputStream();
						outputStream.reset();
						PrintStream printStream = new PrintStream(outputStream);
						// Print the newly replaced data to it
						printStream.print(absoluteData.toString());
						printStream.close();

						getLogger().info("HTTPStreamerAdapterSanJoseRedirector.serviceMsg: manifest.f4m rewritten with absolute URLs (" + loadbalancerTarget + ")");
					}
					else {
						getLogger().debug("HTTPStreamerAdapterSanJoseRedirector.serviceMsg: manifest.f4m could not be rewritten with absolute URLs");
					}

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
