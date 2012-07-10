package com.availo.wms.plugin.vhostloadbalancer;

import java.util.*;

import com.wowza.wms.plugin.loadbalancer.*;

import com.wowza.wms.logging.*;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * Load Balancer Redirector with support for weighted servers and vhosts.
 * Based on http://www.wowza.com/forums/content.php?108
 * 
 * Uses bandwidth instead of connections when comparing servers.
 * @author Brynjar Eide <brynjar@availo.no>
 * @version 1.0b, 2012-07-02
 *
 */
public class LoadBalancerRedirectorBandwidth implements ILoadBalancerRedirector {
	public static final String PROP_LOADBALANCERREDIRECTOR = "LoadBalancerRedirectorBandwidth";

	class ServerHolder implements Comparable<ServerHolder> {
		int redirectCount = 0;
		int connectCount = 0;
		int status = LoadBalancerServer.STATUS_UNKNOWN;
		String serverId = null;
		String redirect = null;
		int inRate = 0;
		int outRate = 0;
		int weight = 1;
		
		Map<String, Object> vhosts = null;

		public ServerHolder(String serverId) {
			this.serverId = serverId;
		}

		/*
		 * We need this to conform with the interface
		 */
		public String getRedirectAddress() {
			return getRedirectAddress(null);
		}

		
		public String getRedirectAddress(String vhostName) {
			String redirectAddress = null;

			if (vhostName != null && vhostName != "" && vhosts != null) {
				if (vhosts.containsKey(vhostName)) {
					Map<String, Object> vhostProperties = (Map<String, Object>)vhosts.get(vhostName);
					if (vhostProperties.containsKey("redirectAddress") && vhostProperties.get("redirectAddress") != null) {
						redirectAddress = vhostProperties.get("redirectAddress").toString();
					}
					else {
						// This is probably either a configuration error, or a VHost that isn't intended to be used for load balancing
						WMSLoggerFactory.getLogger(LoadBalancerRedirectorBandwidth.class).error("LoadBalancerRedirectorConcurrentConnects.getRedirect: Got a redirect request from vhost '" + vhostName + "', but we couldn't find any redirect address for this vhost.");
					}
				}
				else {
					// This could mean that none of the load balancer senders are updated to use the 
					WMSLoggerFactory.getLogger(LoadBalancerRedirectorBandwidth.class).error("LoadBalancerRedirectorConcurrentConnects.getRedirect: Got a redirect request from vhost '" + vhostName + "', but we couldn't find any properties for this vhost.");
				}
			}
			// Use this to return null if the requested vhost is unknown
//			 else {
			// Use this to default to the IP address defined in Server.xml
			if (redirectAddress == null) {
				 redirectAddress = this.redirect;
			}
//			System.out.println("VHost '" + vhostName + "' = " + redirectAddress);
			return redirectAddress;
		}

		
		/**
		 * compareTo-function with support for weighted servers, using outRate instead of connects 
		 */
		public int compareTo(ServerHolder o) {
			// If the numbers are identical, redirect based on the serverId
			if ((this.redirectCount + (this.outRate / this.weight)) == (o.redirectCount + (o.outRate / o.weight))) {
				return this.serverId.compareTo(o.serverId);
			}
			// If not, redirect based on the server with the least current traffic.
			return (this.redirectCount + (this.outRate / this.weight)) > (o.redirectCount + (o.outRate / o.weight)) ? 1 : -1;
		}
		
		// Original implementation
		public int compareConnections(ServerHolder o) {
			if ((this.redirectCount + this.connectCount) == (o.redirectCount + o.connectCount)) {
				return this.serverId.compareTo(o.serverId);
			}

			return (this.redirectCount + this.connectCount) > (o.redirectCount + o.connectCount) ? 1 : -1;
		}

		public boolean equals(Object other) {
			if (!(other instanceof ServerHolder))
				return false;

			return this.serverId.equals(((ServerHolder) other).serverId);
		}

	}

	private SortedSet<ServerHolder> servers = new TreeSet<ServerHolder>();
	private Map<String, ServerHolder> serverMap = new HashMap<String, ServerHolder>();
	private Object lock = new Object();
	private long redirectCount = 0;
	private LoadBalancerListener listener = null;

	public LoadBalancerRedirectorBandwidth() {
	}

	public void init(LoadBalancerListener listener) {
		this.listener = listener;
	}

	public long getRedirectCount() {
		synchronized (lock) {
			return this.redirectCount;
		}
	}

	
	public List<Map<String, Object>> getInfo() {
		return getInfo(null);
	}
	
	public List<Map<String, Object>> getInfo(String vhostName) {
		List<Map<String, Object>> ret = new ArrayList<Map<String, Object>>();

		synchronized (lock) {
//			Iterator<ServerHolder> iter = serverMap.values().iterator();

			/*
			 *  Hopefully, this change doesn't break anything. This was done to make it easier to debug the full output in ServerInfo*,
			 *  by using a sorted set of servers instead of the default.
			 *  
			 *  Should be safe, I guess, since "servers", as opposed to "serverMap", is used by getRedirect().
			 */
			Iterator<ServerHolder> iter = servers.iterator();
			while (iter.hasNext()) {
				ServerHolder serverHolder = iter.next();

				Map<String, Object> map = new HashMap<String, Object>();

				map.put("serverId", serverHolder.serverId);
				map.put("status", LoadBalancerUtils.statusToString(serverHolder.status));
				String redirectAddress = null;
				if (vhostName != null) {
					if (serverHolder != null && serverHolder.vhosts != null) {
						if (serverHolder.vhosts.containsKey(vhostName)) {
							Map<String, Object> vhostProperties = (Map<String, Object>)serverHolder.vhosts.get(vhostName);
							if (vhostProperties.containsKey("redirectAddress")) {
								redirectAddress = (String)vhostProperties.get("redirectAddress");
								map.put("redirect", redirectAddress);
							}
							else {
								// This is probably either a configuration error, or a VHost that isn't intended to be used for load balancing
								WMSLoggerFactory.getLogger(LoadBalancerRedirectorBandwidth.class).error("LoadBalancerRedirectorConcurrentConnects.getRedirect: Got a redirect request from vhost '" + vhostName + "', but we couldn't find any redirect address for this vhost. Check the relevant VHost.xml config on all LoadBalancerSenders.");
							}
						}
						else {
							// This could mean that none of the load balancer senders are updated to use the 
							WMSLoggerFactory.getLogger(LoadBalancerRedirectorBandwidth.class).error("LoadBalancerRedirectorConcurrentConnects.getRedirect: Got a redirect request from vhost '" + vhostName + "', but we couldn't find any properties for this vhost.");
						}
					}
					else {
						// This means that *no* vhosts are known at all.
						WMSLoggerFactory.getLogger(LoadBalancerRedirectorBandwidth.class).error("LoadBalancerRedirectorConcurrentConnects.getRedirect: Got a redirect request from vhost '" + vhostName + "', but we have no information about any VHost at all. Check the modules and configs on all LoadBalancerSenders.");
					}
				}

				/*
				 *  You might want to change this to *only* count if there are no vhosts at all, to avoid potential errorenous redirects.
				 *  Currently, this will use the default redirectAddress from Server.xml, for legacy servers. Possibly not a good idea in all use-cases.
				 */
				if (redirectAddress == null) {
					map.put("redirect", serverHolder.redirect);
				}

				/*
				 * Uncomment the following three lines if you want to output information about all available vhosts.
				 * It doesn't make any sense from a practical point of view, but could be helpful while debugging.
				 * Note: The LoadBalancerUtils is closed source, so LoadBalancerUtils.serverInfoToXMLStr() is not VHosts-aware, and
				 * will just output the information in a single XML tag. The JSON output works better in this regard. 
				 */
				/*if (serverHolder.vhosts != null && serverHolder.vhosts.size() > 0) {
					map.put("VHosts", serverHolder.vhosts);
				}*/

				map.put("weight", new Integer(serverHolder.weight));
				map.put("inRate", new Integer(serverHolder.inRate));
				map.put("outRate", new Integer(serverHolder.outRate));
				map.put("connectCount", new Integer(serverHolder.connectCount));
				map.put("redirectCount", new Integer(serverHolder.redirectCount));
				map.put("debug", new Float(serverHolder.outRate / serverHolder.weight));

				while (true) {
					if (this.listener == null) {
						break;
					}

					LoadBalancerServer loadBalancerServer = this.listener.getServer(serverHolder.serverId);
					if (loadBalancerServer == null) {
						break;
					}
					map.put("lastMessage", loadBalancerServer.getLastMessageReceiveTimeStr());
					break;
				}

				ret.add(map);
			}
		}
		return ret;
	}

	public long getTotalConnections() {
		long ret = 0;
		synchronized (lock) {
			Iterator<ServerHolder> iter = serverMap.values().iterator();
			while (iter.hasNext()) {
				ServerHolder serverHolder = iter.next();
				ret += serverHolder.connectCount;
				ret += serverHolder.redirectCount;
			}
		}

		return ret;
	}

	public LoadBalancerRedirect getRedirect() {
		return getRedirect(null);
	}
	
	public LoadBalancerRedirect getRedirect(String vhostName) {
		LoadBalancerRedirect ret = null;

		synchronized (lock) {
			while (true) {

				if (servers.size() <= 0) {
					break;
				}

				
				ServerHolder first = servers.first();
				if (first == null) {
					WMSLoggerFactory.getLogger(LoadBalancerRedirectorBandwidth.class).debug("LoadBalancerRedirectorConcurrentConnects.getRedirect: No servers.");
					break;
				}

				servers.remove(first);
				first.redirectCount++;
				servers.add(first);

				ret = new LoadBalancerRedirect(first.getRedirectAddress(vhostName));
				redirectCount++;
				break;
			}
		}

		return ret;
	}

	public void onMessage(LoadBalancerServer loadBalancerServer, LoadBalancerMessage message) {

//		WMSLoggerFactory.getLogger(LoadBalancerRedirectorBandwidth.class).info("LoadBalancerMessage.toString() =  " + message.toString());
		loadBalancerServer.handleMessage(message);

		Map<String, String> values = message.getValues();
		synchronized (lock) {
			String connectCountStr = values.get(LoadBalancerMonitorDefault.MSGFIELDS_CONNECTCOUNT);
			int connectCount = -1;
			if (connectCountStr != null) {
				try {
					connectCount = Integer.parseInt(connectCountStr);
				} catch (Exception e) {
				}
			}
			
			String inRateStr = values.get(LoadBalancerMonitorDefault.MSGFIELDS_INRATE);
			int inRate = -1;
			if (inRateStr != null) {
				try {
					inRate = Integer.parseInt(inRateStr);
				} catch (Exception e) {
				}
			}
			
			String outRateStr = values.get(LoadBalancerMonitorDefault.MSGFIELDS_OUTRATE);
			int outRate = -1;
			if (outRateStr != null) {
				try {
					outRate = Integer.parseInt(outRateStr);
				} catch (Exception e) {
				}
			}

			String serverId = loadBalancerServer.getServerId();
			String redirect = loadBalancerServer.getRedirect();
			int status = loadBalancerServer.getStatus();
			int checkWeight = 1;
			
			
			ServerHolder serverHolder = serverMap.get(serverId);
			if (serverHolder == null) {
				serverHolder = new ServerHolder(serverId);
				serverMap.put(serverId, serverHolder);
			}

			servers.remove(serverHolder);
			
			if (values.get("customProperties") != null) {
				String customProperties =  values.get("customProperties");
//				System.out.println("customProperties:\t" + customProperties + "\n");
//				WMSLoggerFactory.getLogger(LoadBalancerRedirectorBandwidth.class).info("LoadBalancerMessage=>customProperties:	" + customProperties);
				Object json = JSONValue.parse(customProperties);
				JSONObject jsonServer = (JSONObject)json;
				JSONObject vhosts = (JSONObject)jsonServer.get("vhosts");
				checkWeight = Integer.parseInt((String)jsonServer.get("weight"));
				serverHolder.vhosts = (Map<String, Object>)vhosts;
			}

			serverHolder.status = status;
			serverHolder.connectCount = connectCount;
			serverHolder.redirectCount = 0;
			serverHolder.redirect = redirect;
			serverHolder.inRate = inRate;
			serverHolder.outRate = outRate;
			
			if (checkWeight > 0) {
				serverHolder.weight = checkWeight;
			}

			if (status == LoadBalancerServer.STATUS_RUNNING) {
				servers.add(serverHolder);
			}
		}
	}

	public void onIdle(LoadBalancerListener listener) {

		List<String> serverIds = listener.getServerIds();
		Iterator<String> iter = serverIds.iterator();
		while (iter.hasNext()) {
			String serverId = iter.next();
			LoadBalancerServer loadBalancerServer = listener.getServer(serverId);
			if (loadBalancerServer == null)
				continue;

			if (loadBalancerServer.isMessageLate()) {
				WMSLoggerFactory.getLogger(LoadBalancerRedirectorBandwidth.class).info("VHostLoadBalancerRedirectorConcurrentConnects.onIdle: Server message timeout: " + serverId);
				int status = LoadBalancerServer.STATUS_MSGTIMEOUT;
				loadBalancerServer.setStatus(status);
				synchronized (lock) {
					ServerHolder serverHolder = serverMap.get(serverId);
					if (serverHolder == null) {
						serverHolder = new ServerHolder(serverId);
						serverMap.put(serverId, serverHolder);
					}

					servers.remove(serverHolder);

					serverHolder.status = status;
				}
			}
		}
	}
}
