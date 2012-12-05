/**
 * ConfigCache.java
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

import java.util.HashMap;
import java.util.Map;
import com.availo.wms.httpstreamer.HTTPStreamerAdapterCupertinoRedirector;
import com.availo.wms.plugin.vhostloadbalancer.ConfigCache.VHost.Application;
import com.wowza.wms.application.IApplicationInstance;
import com.wowza.wms.logging.WMSLogger;
import com.wowza.wms.logging.WMSLoggerFactory;
import com.wowza.wms.module.ModuleBase;

/**
 * Class that is used by HTTPStreamerAdapter-redirectors, to avoid re-reading the config file for every request.
 * 
 * @author Brynjar Eide <brynjar@availo.no>
 * @version 1.1, 2012-12-05
 */
public class ConfigCache extends ModuleBase {

	/**
	 * Thrown if a requested property couldn't be found 
	 */
	public class MissingPropertyException extends Exception {
		// We'll probably never serialize this. Add a default serial version uid 
		private static final long serialVersionUID = 1L;
		public MissingPropertyException(String errorMessage) {
			super(errorMessage);
		}
	}

	/**
	 * Class that contains the vhosts available to the ConfigCache 
	 */
	class VHost {
		
		/**
		 * Class that contains the Applications used for loadbalancing in the available VHosts
		 */
		class Application {
			
			/**
			 * Contains all known properties
			 */
			private Map<String, Object> configOptions;
			
			/**
			 * Whether properties for this application currently is cached or not
			 */
			private boolean isCached = false;
			
			/**
			 * The name of this application
			 */
			private String appName = null;
			
			/**
			 * Keep track of the application name
			 * @param appName
			 */
			Application(String appName) {
				this.appName = appName;
				configOptions = new HashMap<String, Object>();
			}
			
			/**
			 * Keep track of the application name
			 * @param appInstance
			 */
			Application(IApplicationInstance appInstance) {
				appName = appInstance.getApplication().getName();
				configOptions = new HashMap<String, Object>();
			}
			
			/**
			 * Get the name of this application
			 * @return
			 */
			String getAppName() {
				return appName;
			}
			
			/**
			 * Get the value for a particular property
			 * @param propertyName
			 * @return The value of the requested property
			 * @throws MissingPropertyException
			 */
			Object getProperty(String propertyName) throws MissingPropertyException {
				if (configOptions != null && configOptions.containsKey(propertyName)) {
					return configOptions.get(propertyName);
				}
				throw new MissingPropertyException("Property '" + propertyName + "' not found");
			}
			
			/**
			 * Set a new property
			 * @param propertyName
			 * @param propertyValue
			 * @return True if the property was correctly set
			 */
			boolean setProperty(String propertyName, Object propertyValue) {
				if (configOptions != null) {
					getLogger().debug(String.format("ConfigCache.setProperty[%s/%s]: Setting property '%s' to '%s'.", getVHostName(), getAppName(), propertyName, propertyValue));
					configOptions.put(propertyName,  propertyValue);
					return true;
				}
				return false;
			}
			
			/**
			 * Set the cache status for this application
			 * @param value True if the properties are currently cached
			 */
			void setIsCached(boolean isCached) {
				this.isCached = isCached;
			}
			/**
			 * Get the cache status for this application
			 * @return
			 */
			boolean isCached() {
				return isCached;
			}
			
			/**
			 * Remove all properties from this application's cache and mark it as no longer cached
			 * @return True if the properties could be removed
			 */
			boolean removeProperties() {
				if (configOptions != null) {
					configOptions.clear();
					setIsCached(false);
					return true;
				}
				return false;
			}
		}
		
		/**
		 * The name for this VHost
		 */
		String vhostName;
		
		/**
		 * All known and cached applications for this VHost
		 */
		Map<String, Application> applications;
		
		/**
		 * Keep track of the vhost name
		 * @param vhostName
		 */
		public VHost(String vhostName) {
			this.vhostName = vhostName;
			applications = new HashMap<String, Application>();
		}

		/**
		 * Keep track of the vhost name
		 * @param appInstance
		 */
		public VHost(IApplicationInstance appInstance) {
			vhostName = appInstance.getVHost().getName();
			applications = new HashMap<String, Application>();
		}

		/**
		 * Get the vhost name
		 * @return
		 */
		String getVHostName() {
			return vhostName;
		}
		
		/**
		 * Get an application inside this vhost from an application name 
		 * @param loadbalancerAppName
		 * @return
		 */
		Application getApplication(String loadbalancerAppName) {
			if (applications != null && applications.containsKey(loadbalancerAppName)) {
				return applications.get(loadbalancerAppName);
			}
			return null;
		}

		/**
		 * Insert a new application to the Map of applications
		 * @param app The application instance to insert
		 * @return True if the application could be added
		 */
		boolean addApplication(Application app) {
			if (applications != null) {
				applications.put(app.getAppName(), app);
				return true;
			}
			return false;
		}
		
		/**
		 * Remove an application from our Map of applications
		 * @param app
		 * @return True if the application could be removed
		 */
		boolean removeApplication(Application app) {
			if (applications != null && applications.containsKey(app.getAppName())) {
				applications.remove(app.getAppName());
				return true;
			}
			return false;
		}
		
	}
	
	/**
	 * Map that contains all currently available VHost objects
	 * The VHost objects contains all currently available Application objects for that VHost 
	 */
	private Map<String, VHost> vhosts;
	
	/**
	 * Default redirectAppName, in case nothing is defined in Application.xml
	 * 
	 * This should not be set to anything other than null, unless there is a very good reason for it
	 */
	private String defaultRedirectAppName = null;
	
	/**
	 * Default redirectScheme, in case nothing is defined in Application.xml
	 * @deprecated Should not be used, but is kept here to support legacy redirectScheme properties. It will *only* work with RTMP/RTMPT, and it is completely untested!
	 */
	private String defaultRedirectScheme = null;
	
	/**
	 * Default redirectPort, in case nothing is defined in Application.xml
	 * 
	 * This could, for example, be defined as 80, if all traffic should be sent to port 80 on the loadbalancer-senders.
	 * A default value of -1 means that the redirector should try to use the port from the incoming connection, which makes sense in most cases. 
	 */
	private int defaultRedirectPort = -1;
	
	/**
	 * Whether to redirect immidiately on an incoming connection
	 * 
	 * Should default to true if HTTP redirects are being used, as RTMP is the only protocol that currently allows running a call to getLoadBalancerRedirect().
	 */
	private boolean defaultRedirectOnConnect = true;

	/**
	 * Where we keep the ConfigCache object used by all other classes
	 */
	private static ConfigCache cache;
	
	/**
	 * Get a vhost from a vhost name
	 * @param vhostName
	 * @return
	 */
	private VHost getVHost(String vhostName) {
		if (vhosts != null && vhosts.containsKey(vhostName)) {
			return vhosts.get(vhostName);
		}
		return null;
	}
	
	/**
	 * Get a VHost from an application instance
	 * @param appInstance
	 * @return
	 */
	private VHost getVHost(IApplicationInstance appInstance) {
		return getVHost(appInstance.getVHost().getName());
	}
	
	/**
	 * Get an Application from a vhost name and an application name
	 * @param vhostName
	 * @param loadbalancerAppName
	 * @return
	 */
	private Application getApplication(String vhostName, String loadbalancerAppName) {
		if (getVHost(vhostName) != null) {
			return getVHost(vhostName).getApplication(loadbalancerAppName);
		}
		return null;
	}
	
	/**
	 * Get an Application from an application instance
	 * @param appInstance
	 * @return
	 */
	private Application getApplication(IApplicationInstance appInstance) {
		return getApplication(appInstance.getVHost().getName(), appInstance.getApplication().getName());
	}
	
	/**
	 * Check if the properties for an application inside a specific vhost is loaded
	 * @param vhostName
	 * @param loadbalancerAppName
	 * @param configKey
	 * @return true if the properties have been loaded
	 */
	private boolean isValid(String vhostName, String loadbalancerAppName, String configKey) {
		if (getApplication(vhostName, loadbalancerAppName) != null) {
			if (getApplication(vhostName, loadbalancerAppName).isCached()) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Check if a specific application is loaded in the specified vhost
	 * @param vhostName
	 * @param loadbalancerAppName
	 * @return true if we found the application inside a specific vhost
	 */
	private boolean isValid(String vhostName, String loadbalancerAppName) {
		if (getApplication(vhostName, loadbalancerAppName) != null) {
			return true;
		}
		return false;
	}
	
	/**
	 * Check if the vhost is currently loaded
	 * @param vhostName
	 * @return true if we found the vhost
	 */
	private boolean isValid(String vhostName) {
		if (getVHost(vhostName) != null) {
			return true;
		}
		return false;
	}
	
	/**
	 * Handle all exception messages and error logging
	 * @param vhostName
	 * @param loadbalancerAppName
	 * @param propertyName
	 * @return The requested property value from Application.xml
	 * @throws MissingPropertyException If the property wasn't found for any reason
	 */
	private Object getProperty(String vhostName, String loadbalancerAppName, String propertyName) throws MissingPropertyException {
		if (isValid(vhostName, loadbalancerAppName, propertyName)) {
			return getVHost(vhostName).getApplication(loadbalancerAppName).getProperty(propertyName);
		}
		if (!isValid(vhostName)) {
			throw new MissingPropertyException("VHost '" + vhostName + "' is not cached.");
		}
		if (!isValid(vhostName, loadbalancerAppName)) {
			throw new MissingPropertyException("Application config for '" + loadbalancerAppName + "' in vhost '" + vhostName + "' is not cached.");
		}
		throw new MissingPropertyException("Application '" + loadbalancerAppName + "' in vhost '" + vhostName + "' seems to exists, but no properties are loaded.");
	}
	
	/**
	 * Fetch an application if the vhost and application is already loaded. Create them otherwise.
	 * @param appInstance
	 * @return The application that was found/created
	 */
	private Application loadApplication(IApplicationInstance appInstance) {
		Application app = getApplication(appInstance);
		if (app == null) {
			String vhostName = appInstance.getVHost().getName();
			String appName = appInstance.getApplication().getName();
			VHost vhost = getVHost(vhostName);
			if (vhost == null) {
				vhost = new VHost(vhostName);
				addVHost(vhost);
			}
			app = vhost.new Application(appName);
			vhost.addApplication(app);
		}
		return app;
	}
	
	ConfigCache() {
		getLogger().info("ConfigCache: Creating new ConfigCache for LoadBalancer applications");
		vhosts = new HashMap<String, VHost>();
	}
	
	/**
	 * Add a VHost to our Map of vhosts
	 * @param vhost
	 * @return True if the VHost could be added
	 */
	boolean addVHost(VHost vhost) {
		if (vhosts != null) {
			vhosts.put(vhost.getVHostName(), vhost);
			return true;
			
		}
		return false;
		
	}
	
	/**
	 * Remove a VHost from our Map of vhosts
	 * @param vhost
	 * @return True if the VHost could be removed
	 */
	boolean removeVHost(VHost vhost) {
		if (vhosts != null && vhosts.containsKey(vhost.getVHostName())) {
			vhosts.remove(vhost.getVHostName());
			return true;
		}
		return false;
	}

	/**
	 * Get cached redirectAppName setting
	 * @param vhostName
	 * @param loadbalancerAppName The application name used on the LoadBalancerListener
	 * @return The application name we should use instead
	 * @throws MissingPropertyException
	 */
	public String getRedirectAppName(String vhostName, String loadbalancerAppName) throws MissingPropertyException {
		String propertyName = "redirectAppName";
		if (isValid(vhostName, loadbalancerAppName, propertyName)) {  
			return (String) getProperty(vhostName, loadbalancerAppName, propertyName);
		}
		getLogger().debug(String.format("ConfigCache.getRedirectAppName[%s/%s]: Could not find property '%s'. Returning default value.", vhostName, loadbalancerAppName, propertyName));
		return defaultRedirectAppName;
	}
	
	/**
	 * Get cached redirectAppName setting
	 * @param appInstance
	 * @return
	 * @throws MissingPropertyException
	 */
	public String getRedirectAppName(IApplicationInstance appInstance) throws MissingPropertyException {
		return getRedirectAppName(appInstance.getVHost().getName(), appInstance.getApplication().getName());
	}
	
	/**
	 * Get cached redirectScheme setting
	 * @param vhostName
	 * @param loadbalancerAppName The application name used on the LoadBalancerListener
	 * @return The application name we should use instead
	 * @deprecated Should not be used, but is kept here to support legacy redirectScheme properties. It will *only* work with RTMP/RTMPT, and it is completely untested!
	 * @throws MissingPropertyException
	 */
	public String getRedirectScheme(String vhostName, String loadbalancerAppName) {
		String propertyName = "redirectScheme";
		try {
			if (isValid(vhostName, loadbalancerAppName, propertyName)) {
				return (String) getProperty(vhostName, loadbalancerAppName, propertyName);
			} 
		} catch (MissingPropertyException e) {
			// Ignore missing redirectScheme settings, as it has been deprecated.
		}
		// This can be uncommented if people actually use the redirectScheme
		//getLogger().debug(String.format("ConfigCache.getRedirectScheme: Could not find property '%s' for application '%s' in vhost '%s'. Returning default value.", "redirectScheme", loadbalancerAppName, vhostName));
		return defaultRedirectScheme;
	}
	
	/**
	 * Get cached redirectScheme setting
	 * @param appInstance
	 * @return
	 * @deprecated Should not be used, but is kept here to support legacy redirectScheme properties. It will *only* work with RTMP/RTMPT, and it is completely untested!
	 * @throws MissingPropertyException
	 */
	public String getRedirectScheme(IApplicationInstance appInstance) throws MissingPropertyException{
		return getRedirectScheme(appInstance.getVHost().getName(), appInstance.getApplication().getName());
	}
	
	/**
	 * Get cached redirectPort setting
	 * @param vhostName
	 * @param loadbalancerAppName
	 * @return
	 * @throws MissingPropertyException
	 */
	public int getRedirectPort(String vhostName, String loadbalancerAppName) throws MissingPropertyException {
		String propertyName = "redirectPort";
		if (isValid(vhostName, loadbalancerAppName, propertyName)) {
			return (Integer) getProperty(vhostName, loadbalancerAppName, propertyName);
		}
		getLogger().debug(String.format("ConfigCache.getRedirectPort[%s/%s]: Could not find property '%s'. Returning default value.", vhostName, loadbalancerAppName, propertyName));
		return defaultRedirectPort;
	}
	
	/**
	 * Get cached redirectPort setting
	 * @param appInstance
	 * @return
	 * @throws MissingPropertyException
	 */
	public int getRedirectPort(IApplicationInstance appInstance) throws MissingPropertyException{
		return getRedirectPort(appInstance.getVHost().getName(), appInstance.getApplication().getName());
	}
	
	/**
	 * Get cached redirectOnConnect setting
	 * @param vhostName
	 * @param loadbalancerAppName
	 * @return
	 * @throws MissingPropertyException
	 */
	public boolean getRedirectOnConnect(String vhostName, String loadbalancerAppName) throws MissingPropertyException {
		String propertyName = "redirectOnConnect";
		if (isValid(vhostName, loadbalancerAppName, propertyName)) {
			return (Boolean) getProperty(vhostName, loadbalancerAppName, propertyName);
		}
		getLogger().debug(String.format("ConfigCache.getRedirectOnConnect[%s/%s]: Could not find property '%s'. Returning default value.", vhostName, loadbalancerAppName, propertyName)); 
		return defaultRedirectOnConnect;
	}
	
	/**
	 * Get cached redirectOnConnect setting
	 * @param appInstance
	 * @return
	 * @throws MissingPropertyException
	 */
	public boolean getRedirectOnConnect(IApplicationInstance appInstance) throws MissingPropertyException{
		return getRedirectOnConnect(appInstance.getVHost().getName(), appInstance.getApplication().getName());
	}

	/**
	 * Load and cache all known properties for the specified application 
	 * @param appInstance
	 * @return True if the properties was successfully loaded
	 */
	public boolean loadProperties(IApplicationInstance appInstance) {
		Application app = loadApplication(appInstance);
		if (app.isCached()) {
			getLogger().debug(String.format("ConfigCache.loadProperties[%s/%s]: Properties are already cached.", appInstance.getVHost().getName(), appInstance.getApplication().getName()));
		}
		else {
			getLogger().debug(String.format("ConfigCache.loadProperties[%s/%s]: Loading properties", appInstance.getVHost().getName(), appInstance.getApplication().getName()));
			app.setProperty("redirectAppName", appInstance.getProperties().getPropertyStr("redirectAppName", defaultRedirectAppName));
			app.setProperty("redirectPort", appInstance.getProperties().getPropertyInt("redirectPort", defaultRedirectPort));
			app.setProperty("redirectOnConnect", appInstance.getProperties().getPropertyBoolean("redirectOnConnect", defaultRedirectOnConnect));
			app.setIsCached(true);
		}
		return app.isCached();
	}

	/**
	 * Remove all properties from a cached application, and remove the (now useless) application from the vhost
	 * @param appInstance
	 */
	public void expireProperties(IApplicationInstance appInstance) {
		getLogger().debug(String.format("ConfigCache.expireProperties[%s/%s]: Application has stopped. Expiring all properties and cleaning up the application.", appInstance.getVHost().getName(), appInstance.getApplication().getName()));
		Application app = getApplication(appInstance);
		VHost vhost = getVHost(appInstance);
		if (app != null) {
			app.removeProperties();
		}
		if (vhost != null) {
			vhost.removeApplication(app);
		}
	}
	
	/**
	 * Shortcut to debug logging
	 * @return WMSLogger object used to log warnings or debug messages
	 */
	protected static WMSLogger getLogger() {
		return WMSLoggerFactory.getLogger(HTTPStreamerAdapterCupertinoRedirector.class);
	}
	
	/**
	 * Create the ConfigCache if necessary, and return it to the requesting class
	 * @return The global ConfigCache shared by all applications on all vhosts
	 */
	public static ConfigCache getInstance() {
		if (ConfigCache.cache == null) {
			ConfigCache cache = new ConfigCache();
			ConfigCache.cache = cache;
		}
		return ConfigCache.cache;
	}


}