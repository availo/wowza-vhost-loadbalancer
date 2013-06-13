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

import java.io.*;
import java.util.*;

//import com.wowza.util.*;
import com.wowza.wms.application.*;
import com.wowza.wms.http.*;
import com.wowza.wms.logging.*;
import com.wowza.wms.server.*;
import com.wowza.wms.vhost.*;
import com.wowza.wms.plugin.loadbalancer.*;
import org.json.simple.JSONObject;

/**
 * Redirector class with optional JSON-output.
 * 
 * Based on http://www.wowza.com/forums/content.php?108
 * 
 * @author Brynjar Eide <brynjar@availo.no>
 * @version 2.0b, 2013-06-13
 *
 */
public class HTTPLoadBalancerRedirector extends HTTProvider2Base {
//	private IVHost vhost = null;
	private LoadBalancerListener listener = null;
	private LoadBalancerRedirectorBandwidth redirector = null;
	private boolean enableServerInfo = false;

	public void onBind(IVHost vhost, HostPort hostPort) {
		super.onBind(vhost, hostPort);
//		this.vhost = vhost;
	}

	public void setProperties(WMSProperties properties) {
		super.setProperties(properties);
		if (this.properties != null) {
			enableServerInfo = this.properties.getPropertyBoolean("enableServerInfo", enableServerInfo);
			if (!enableServerInfo) {
				// Allow using the original name for this property, in case of old config files.
				enableServerInfo = this.properties.getPropertyBoolean("enableServerInfoXML", enableServerInfo);
			}
		}
	}

	private LoadBalancerRedirectorBandwidth getRedirector() {
		if (redirector == null) {
			while (true) {
				this.listener = (LoadBalancerListener) Server.getInstance().getProperties().get(ServerListenerLoadBalancerListener.PROP_LOADBALANCERLISTENER);
				if (this.listener == null) {
					WMSLoggerFactory.getLogger(HTTPLoadBalancerRedirector.class).warn("HTTPLoadBalancerRedirector.constructor: LoadBalancerListener not found.");
				}

				this.redirector = (LoadBalancerRedirectorBandwidth) this.listener.getRedirector();
				if (this.redirector == null) {
					WMSLoggerFactory.getLogger(HTTPLoadBalancerRedirector.class).warn("HTTPLoadBalancerRedirector.constructor: ILoadBalancerRedirector not found.");
					break;
				}
				break;
			}
		}

		return redirector;
	}

	public void onHTTPRequest(IVHost vhost, IHTTPRequest req, IHTTPResponse resp) {
		if (!doHTTPAuthentication(vhost, req, resp)) {
			return;
		}
		
		String vhostName = vhost.getName();
		getRedirector();

		String retStr = null;

		String queryStr = req.getQueryString();
		boolean isServerInfoXML = queryStr == null || !enableServerInfo ? false : queryStr.indexOf("serverInfoXML") >= 0;
		boolean isServerInfoJSON = queryStr == null || !enableServerInfo ? false : queryStr.indexOf("serverInfoJSON") >= 0;
		boolean isServerInfo = queryStr == null || !enableServerInfo ? false : queryStr.indexOf("serverInfo") >= 0;

		if (isServerInfoXML) {
			List<Map<String, Object>> info = this.redirector == null ? null : this.redirector.getInfo(vhostName);
			retStr = LoadBalancerUtils.serverInfoToXMLStr(info);
			resp.setHeader("Content-Type", "text/xml");
		}
		else if (isServerInfoJSON) {
			List<Map<String, Object>> info = this.redirector == null ? null : this.redirector.getInfo(vhostName);
			JSONObject jsonOutput = new JSONObject();
			jsonOutput.put("LoadBalancerServerInfo", info);
			retStr = jsonOutput.toString();
			resp.setHeader("Content-Type", "application/json");
		}
		else if (isServerInfo) {
			/** @TODO Currently, the only difference between this and the JSON output, is the content-type. */
			List<Map<String, Object>> info = this.redirector == null ? null : this.redirector.getInfo(vhostName);
			JSONObject jsonOutput = new JSONObject();
			jsonOutput.put("LoadBalancerServerInfo", info);
			retStr = jsonOutput.toString();
			resp.setHeader("Content-Type", "text/plain");
		}
		else {
			LoadBalancerRedirect redirect = this.redirector == null ? null : this.redirector.getRedirect(vhostName);
			retStr = "redirect=" + (redirect == null ? "unknown" : redirect.getHost());
		}

		try {
			OutputStream out = resp.getOutputStream();
			byte[] outBytes = retStr.getBytes();
			out.write(outBytes);
		} catch (Exception e) {
			WMSLoggerFactory.getLogger(HTTPLoadBalancerRedirector.class).error("HTTPLoadBalancerRedirector: " + e.toString());
		}

	}
}
