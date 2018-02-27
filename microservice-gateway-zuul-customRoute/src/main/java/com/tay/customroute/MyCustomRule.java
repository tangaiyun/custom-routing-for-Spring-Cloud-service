package com.tay.customroute;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.client.config.IClientConfig;
import com.netflix.loadbalancer.AbstractLoadBalancerRule;
import com.netflix.loadbalancer.ILoadBalancer;
import com.netflix.loadbalancer.Server;
import com.netflix.niws.loadbalancer.DiscoveryEnabledServer;
import com.netflix.zuul.context.RequestContext;

public class MyCustomRule extends AbstractLoadBalancerRule {

	private ConcurrentHashMap<String, AtomicInteger> nextServerCyclicCounterMap;

	private static Logger log = LoggerFactory.getLogger(MyCustomRule.class);

	private static final String ROUTE_HEAD_COOKIE_NAME = "ROUTECODE";
	private static final String ZK_CONN_STR = "192.168.0.106:2181";
	private static final String ROUTE_CONFIG_ZK_PATH = "/config/zuul/route";
	private static final String GROUP_PREFIX = "group_";
	private static final String ALL_AS_ONE_GROUP = "onegroup";
	private static final String DEFAULT_ROUTE_KEY = "default_route";
	private static final String ENABLE_KEY = "enable";
	private volatile boolean isAcmInited = false;
	private Properties routeConfigProperties = new Properties();
	private boolean isCustomRouteEnable = false;
	private ConcurrentHashMap<String, ArrayList<String>> groupMemberMap;
	private String defaultRoute;

	public MyCustomRule() {
		nextServerCyclicCounterMap = new ConcurrentHashMap<String, AtomicInteger>();
		nextServerCyclicCounterMap.put(ALL_AS_ONE_GROUP, new AtomicInteger(0));
		groupMemberMap = new ConcurrentHashMap<String, ArrayList<String>>();
	}

	public MyCustomRule(ILoadBalancer lb) {
		this();
		setLoadBalancer(lb);
	}

	private synchronized void configZKInit() {

		System.out.println(" loading route config from ZK*****************************************************");
		CuratorFramework client = CuratorFrameworkFactory.newClient(ZK_CONN_STR, new ExponentialBackoffRetry(1000, 3));
		client.start();
		try {
			final CountDownLatch cdl = new CountDownLatch(1);
			@SuppressWarnings("resource")
			final NodeCache cache = new NodeCache(client, ROUTE_CONFIG_ZK_PATH);
			NodeCacheListener listener = () -> {
				ChildData data = cache.getCurrentData();
				if (null != data) {
					String strRouteConfig = new String(cache.getCurrentData().getData());
					System.out.println("节点数据：" + strRouteConfig);
					loadConfig(strRouteConfig);

				} else {
					System.out.println("节点被删除!");
				}
				cdl.countDown();
			};
			cache.getListenable().addListener(listener);

			cache.start();
			isAcmInited = true;
			cdl.await();
		} catch (Exception e) {
			log.error("路由配置节点监控器启动错误", e);
		}
	}

	private synchronized void loadConfig(String content) {
		try {
			routeConfigProperties.load(new ByteArrayInputStream(content.getBytes()));
			isCustomRouteEnable = Boolean.parseBoolean(routeConfigProperties.getProperty(ENABLE_KEY).trim());
			Set<Object> keySet = routeConfigProperties.keySet();
			for(Object key : keySet) {
				String strKey = (String)key;
				if(strKey.startsWith(GROUP_PREFIX)) {
					ArrayList<String> groupMemberList = new ArrayList<String>();
					groupMemberList.addAll(Arrays.asList(routeConfigProperties.getProperty(strKey).split(",")));
					groupMemberMap.put(strKey, groupMemberList);
					nextServerCyclicCounterMap.put(strKey, new AtomicInteger(0));
				}
			}
			defaultRoute = routeConfigProperties.getProperty(DEFAULT_ROUTE_KEY);
		} catch (IOException e) {
			log.error("配置读入到Properties对象失败", e);
		}
	}

	public Server choose(ILoadBalancer lb, Object key) {
		if (lb == null) {
			log.warn("no load balancer");
			return null;
		}

		if (!isAcmInited) {
			configZKInit();
		}

		Server server = null;
		if (!isCustomRouteEnable) {
			int count = 0;
			while (server == null && count++ < 10) {
				List<Server> reachableServers = lb.getReachableServers();
				List<Server> allServers = lb.getAllServers();
				int upCount = reachableServers.size();
				int serverCount = allServers.size();

				if ((upCount == 0) || (serverCount == 0)) {
					log.warn("No up servers available from load balancer: " + lb);
					return null;
				}

				int nextServerIndex = incrementAndGetModulo(ALL_AS_ONE_GROUP, serverCount);
				List<Server> sortedAllServers = allServers.stream().sorted((s1, s2) -> s1.getId().compareTo(s2.getId()))
						.collect(Collectors.toList());
				System.out.println("all servers info:");
				printServersInfo(sortedAllServers);
				server = sortedAllServers.get(nextServerIndex);

				if (server == null) {
					/* Transient. */
					Thread.yield();
					continue;
				}

				if (server.isAlive() && (server.isReadyToServe())) {
					System.out.println("this request will be served by the following server:");
					printServerInfo(server);
					return (server);
				}
				// Next.
				server = null;
			}

			if (count >= 10) {
				log.warn("No available alive servers after 10 tries from load balancer: " + lb);
			}
		} else {
			int count = 0;
			while (server == null && count++ < 10) {
				List<Server> reachableServers = lb.getReachableServers();
				List<Server> allServers = lb.getAllServers();
				int upCount = reachableServers.size();
				int serverCount = allServers.size();

				if ((upCount == 0) || (serverCount == 0)) {
					log.warn("No up servers available from load balancer: " + lb);
					return null;
				}
				String routeKey = getRouteKey();
				String routeGroup = getGroupbyRouteKey(routeKey);
				if(routeGroup == null) {
					routeGroup = defaultRoute;
				}
				final String appFilter = routeGroup;
				List<Server> serverCandidates = allServers.stream()
						.filter(s -> appFilter
								.equalsIgnoreCase(((DiscoveryEnabledServer) s).getInstanceInfo().getAppGroupName()))
						.sorted((s1, s2) -> s1.getId().compareTo(s2.getId())).collect(Collectors.toList());
				
				int nextServerIndex = incrementAndGetModulo(routeGroup,serverCandidates.size());
				server = serverCandidates.get(nextServerIndex);
			
				
				if (server == null) {
					/* Transient. */
					Thread.yield();
					continue;
				}
				if (server.isAlive() && (server.isReadyToServe())) {
					System.out.println("this request will be served by the following server:");
					printServerInfo(server);
					return (server);
				}
				// Next.
				server = null;
			}
			if (count >= 10) {
				log.warn("No available alive servers after 10 tries from load balancer: " + lb);
			}
		}
		return server;
	}

	private void printServersInfo(Collection<Server> servers) {
		for (Server s : servers) {
			printServerInfo(s);
		}
	}

	private void printServerInfo(Server server) {
		System.out.print("appName: " + ((DiscoveryEnabledServer) server).getInstanceInfo().getAppName() + " ");
		System.out.print("appGroup: " + ((DiscoveryEnabledServer) server).getInstanceInfo().getAppGroupName() + " ");
		System.out.print("id: " + server.getId() + " isAlive: " + server.isAlive() + " ");
		System.out.print("id: " + server.getId() + " isReadyToServe: " + server.isReadyToServe() + " ");
		System.out.println();
	}

	/**
	 * first find
	 * 
	 * @return
	 */
	private String getRouteKey() {
		RequestContext ctx = RequestContext.getCurrentContext();
		HttpServletRequest request = ctx.getRequest();
		String orgCode = "";
		Cookie[] cookies = request.getCookies();
		if (cookies != null) {
			for (Cookie cookie : cookies) {
				if (ROUTE_HEAD_COOKIE_NAME.equals(cookie.getName())) {
					orgCode = cookie.getValue();
					break;
				}
			}
		}
		if ("".equals(orgCode)) {
			orgCode = request.getHeader(ROUTE_HEAD_COOKIE_NAME);
		}

		return orgCode;
	}
	
	private String getGroupbyRouteKey(String routeKey) {
		String group = null;
		Set<Entry<String, ArrayList<String>>> entrySet = groupMemberMap.entrySet();
		for(Entry<String, ArrayList<String>> entry : entrySet) {
			List<String> memberList = entry.getValue();
			if(memberList.contains(routeKey)) {
				group = entry.getKey();
				break;
			}
		}
		return group;
	}
 
	/**
	 * Inspired by the implementation of {@link AtomicInteger#incrementAndGet()}.
	 *
	 * @param modulo
	 *            The modulo to bound the value of the counter.
	 * @return The next value.
	 */
	private int incrementAndGetModulo(String group, int modulo) {
		AtomicInteger groupCurrent = nextServerCyclicCounterMap.get(group);
		for (;;) {
			int current = groupCurrent.get();
			int next = (current + 1) % modulo;
			if (groupCurrent.compareAndSet(current, next))
				return next;
		}
	}

	@Override
	public Server choose(Object key) {
		return choose(getLoadBalancer(), key);
	}

	@Override
	public void initWithNiwsConfig(IClientConfig clientConfig) {
	}
}
