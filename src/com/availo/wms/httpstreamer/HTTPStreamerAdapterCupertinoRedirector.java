package com.availo.wms.httpstreamer;

import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.mina.common.ByteBuffer;

import com.wowza.util.FasterByteArrayOutputStream;
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
		if (!req.getPath().matches("(?i).*\\/Playlist\\.m3u8$")) {
			getLogger().debug("HTTPStreamerAdapterCupertinoRedirector.serviceMsg: received a non-Playlist request: '" + req.getPath() + "'");
		}
		else {
			getLogger().debug("HTTPStreamerAdapterCupertinoRedirector.serviceMsg: received a Playlist request: req.getPath(): '" + req.getPath() + "'");
			List<ByteBuffer> bufferlist = resp.getBodyList();
			Charset charset = Charset.forName("UTF-8");
			CharsetDecoder decoder = charset.newDecoder();

			if (!bufferlist.isEmpty() && resp.getHeaders().containsKey("X-LoadBalancer-Target")) {
				// This header is set in ModuleLoadBalancerRedirector.onHTTPSessionCreate()
				String loadbalancerTarget = resp.getHeaders().get("X-LoadBalancer-Target");
				// Get the path from the requested URL by removing /Playlist.m3u8
				String loadbalancerTargetPath = req.getPath().replaceFirst("(?i)/Playlist\\.m3u8$", "");
				String loadbalancerTargetProtocol = "http://";

				if (loadbalancerTarget.matches(":443$")) {
					loadbalancerTargetProtocol = "https://"; // FIXME This is an ugly way of figuring out whether to use https or not
				}

				try {
					StringBuffer absoluteData = new StringBuffer();
					
					//Rewrite chunklist URL from relative to absolute chunklist.m3u8?wowzasessionid=1982042183
					Pattern pattern = Pattern.compile("(.*)^chunklist((-[^\\.]+)?\\.m3u8(.*))");
					boolean rewritten = false;

					for (ByteBuffer buffer : bufferlist) {
						String originalData = buffer.getString(decoder);
						String lines[] = originalData.split("\\r?\\n|\\r");
						for (String line : lines) {
							getLogger().debug("HTTPStreamerAdapterCupertinoRedirector.serviceMsg: working on line '" + line + "'");
							if (pattern.matcher(line).matches()) {
								//absoluteData = absoluteData.replaceFirst("^chunklist\\.m3u8", loadbalancerTargetProtocol + loadbalancerTarget + "/" + loadbalancerTargetPath + "/chunklist.m3u8");
								line = pattern.matcher(line).replaceAll("$1" + loadbalancerTargetProtocol + loadbalancerTarget + "/" + loadbalancerTargetPath + "/chunklist$2");
								absoluteData.append(line + "\n");
								rewritten = true;
							}
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
						getLogger().info("HTTPStreamerAdapterCupertinoRedirector.serviceMsg: Playlist.m3u8 rewritten with absolute URLs (" + loadbalancerTarget + ")");
					}
					else {
						getLogger().debug("HTTPStreamerAdapterCupertinoRedirector.serviceMsg: Playlist.m3u8 could not be rewritten with absolute URLs");
					}

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
