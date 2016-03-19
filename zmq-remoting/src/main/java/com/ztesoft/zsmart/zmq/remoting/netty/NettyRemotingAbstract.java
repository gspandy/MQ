package com.ztesoft.zsmart.zmq.remoting.netty;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.remoting.ChannelEventListener;
import com.ztesoft.zsmart.zmq.remoting.InvokeCallback;
import com.ztesoft.zsmart.zmq.remoting.RPCHook;
import com.ztesoft.zsmart.zmq.remoting.common.Pair;
import com.ztesoft.zsmart.zmq.remoting.common.RemotingHelper;
import com.ztesoft.zsmart.zmq.remoting.common.SemaphoreReleaseOnlyOnce;
import com.ztesoft.zsmart.zmq.remoting.common.ServiceThread;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingSendRequestException;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingTimeoutException;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingTooMuchRequestException;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingSysResponseCode;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;

/**
 * Server 与 Client 公用抽象类
 * 
 * @author J.Wang
 *
 */
public abstract class NettyRemotingAbstract {
	private static final Logger plog = LoggerFactory.getLogger(RemotingHelper.RemotingLogName);

	// 信号量 OneWay 情况会使用 防止地本netty缓存请求过多
	protected final Semaphore semaphoreOneway;

	// 信号量 异步情况会使用 防止本地Netty缓存请求过多
	protected final Semaphore semaphoreAsync;

	// 缓存所有对外请求
	protected final ConcurrentHashMap<Integer, ResponseFuture> responseTable = new ConcurrentHashMap<Integer, ResponseFuture>(
			256);

	// 默认请求代码处理器
	protected Pair<NettyRequestProcessor, ExecutorService> defaultRequestProcessor;

	// 注册的各个 RPC处理器
	protected final HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>> processorTable = new HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>>(
			64);

	protected final NettyEventExecuter nettyEventExecuter = new NettyEventExecuter();

	public abstract ChannelEventListener getChannelEventListener();

	public abstract RPCHook getRPCHook();

	public void putNettyEvent(final NettyEvent event) {
		this.nettyEventExecuter.putNettyEvent(event);
	}

	class NettyEventExecuter extends ServiceThread {
		private final LinkedBlockingDeque<NettyEvent> eventQueue = new LinkedBlockingDeque<NettyEvent>();
		private final int MaxSize = 10000;

		public void putNettyEvent(NettyEvent event) {
			if (this.eventQueue.size() <= MaxSize) {
				this.eventQueue.add(event);
			} else {
				plog.error("event queue size [{}] enough so drop this event {}", this.eventQueue.size(),
						event.toString());
			}
		}

		@Override
		public void run() {
			plog.info(this.getServiceName() + " server started");

		}

		@Override
		public String getServiceName() {
			return nettyEventExecuter.getClass().getSimpleName();
		}
	}

	public NettyRemotingAbstract(final int permitsOneWay, final int permitsAsync) {
		this.semaphoreOneway = new Semaphore(permitsOneWay);
		this.semaphoreAsync = new Semaphore(permitsAsync);
	}

	public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
		final Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());
		final Pair<NettyRequestProcessor, ExecutorService> pair = null == matched ? this.defaultRequestProcessor
				: matched;

		if (pair != null) {
			Runnable run = new Runnable() {

				public void run() {
					try {
						RPCHook rpcHook = NettyRemotingAbstract.this.getRPCHook();
						if (rpcHook != null) {
							rpcHook.doBeforeRequest(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), cmd);
						}

						final RemotingCommand response = pair.getObject1().processRequest(ctx, cmd);

						if (rpcHook != null) {
							rpcHook.doAfterResponse(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), cmd,
									response);
						}

						if (!cmd.isOnewayRPC()) {
							if (response != null) {
								response.setOpaque(cmd.getOpaque());
								response.markResponseType();
								try {
									ctx.writeAndFlush(response);
								} catch (Exception e) {
									plog.error("process request over ,but respose failed ", e);
									plog.error(cmd.toString());
									plog.error(response.toString());
								}
							} else {
								// 收到请滶 但是返回没有应答
							}
						}

					} catch (Exception e) {
						plog.error("process request exception", e);
						plog.error(cmd.toString());

						if (!cmd.isOnewayRPC()) {
							final RemotingCommand response = RemotingCommand.createResponseCommand(
									RemotingSysResponseCode.SYSTEM_ERROR, //
									RemotingHelper.exceptionSimpleDesc(e));
							response.setOpaque(cmd.getOpaque());
							ctx.writeAndFlush(response);
						}
					}
				}
			};

			try {
				pair.getObject2().submit(run);
			} catch (RejectedExecutionException e) {
				// 每个线程10s打印一次
				if ((System.currentTimeMillis() % 10000) == 0) {
					plog.warn(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) //
							+ ", too many requests and system thread pool busy, RejectedExecutionException " //
							+ pair.getObject2().toString() //
							+ " request code: " + cmd.getCode());
				}

				if (!cmd.isOnewayRPC()) {
					final RemotingCommand response = RemotingCommand.createResponseCommand(
							RemotingSysResponseCode.SYSTEM_BUSY,
							"too many requests and system thread pool busy, please try another server");
					response.setOpaque(cmd.getOpaque());
					ctx.writeAndFlush(response);
				}
			}
		} else {
			String error = "request type " + cmd.getCode() + " not supported";
			final RemotingCommand response = RemotingCommand
					.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
			response.setOpaque(cmd.getOpaque());
			ctx.write(response);
			plog.error(RemotingHelper.parseChannelRemoteName(ctx.channel()));
		}
	}

	public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
		final ResponseFuture responseFuture = responseTable.get(cmd.getOpaque());

		if (responseFuture != null) {
			responseFuture.setResponseCommand(cmd);
			responseFuture.release();
			responseTable.remove(cmd.getOpaque());

			if (responseFuture.getInvokeCallback() != null) {
				boolean runInThisThread = false;
				ExecutorService executor = this.getCallbackExecutor();
				if (executor != null) {
					try {
						executor.submit(new Runnable() {
							public void run() {
								try {
									responseFuture.executeInvokeCallback();
								} catch (Throwable e) {
									plog.warn("excute callback in executor exception, and callback throw", e);
								}
							}
						});
					} catch (Exception e) {
						runInThisThread = true;
						plog.warn("excute callback in executor exception, maybe executor busy", e);
					}
				} else {
					runInThisThread = true;
				}

				if (runInThisThread) {
					try {
						responseFuture.executeInvokeCallback();
					} catch (Throwable e) {
						plog.warn("executeInvokeCallback Exception", e);
					}
				}
			} else {
				responseFuture.putResponse(cmd);
			}
		} else {
			plog.warn("receive response, but not matched any request, "
					+ RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
			plog.warn(cmd.toString());
		}
	}

	abstract public ExecutorService getCallbackExecutor();

	public void processMessageReceive(ChannelHandlerContext ctx, RemotingCommand msg) {
		final RemotingCommand cmd = msg;

		if (cmd != null) {
			switch (cmd.getType()) {
			case REQUEST_COMMAND:
				processRequestCommand(ctx, cmd);
				break;
			case RESPONSE_COMMAND:
				processResponseCommand(ctx, cmd);
				break;
			default:
				break;
			}
		}
	}

	public void scanResponseType() {
		Iterator<Entry<Integer, ResponseFuture>> it = this.responseTable.entrySet().iterator();

		while (it.hasNext()) {
			Entry<Integer, ResponseFuture> next = it.next();
			ResponseFuture rep = next.getValue();

			if ((rep.getBeginTimestamp() + rep.getTimeoutMillis() + 1000) <= System.currentTimeMillis()) {
				it.remove();
				try {
					rep.executeInvokeCallback();
				} catch (Throwable e) {
					plog.warn("scanResponseTable, operationComplete Exception", e);
				} finally {
					rep.release();
				}

				plog.warn("remove timeout request, " + rep);
			}
		}
	}

	public RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request,
			final long timeoutMillis)
					throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
		try {
			final ResponseFuture responseFuture = new ResponseFuture(request.getOpaque(), timeoutMillis, null, null);

			this.responseTable.put(request.getOpaque(), responseFuture);
			channel.writeAndFlush(request).addListener(new ChannelFutureListener() {

				@Override
				public void operationComplete(ChannelFuture f) throws Exception {
					if (f.isSuccess()) {
						responseFuture.setSendRequestOK(true);
						return;
					} else {
						responseFuture.setSendRequestOK(false);
					}

					responseTable.remove(request.getOpaque());
					responseFuture.setCause(f.cause());
					responseFuture.putResponse(null);
					plog.warn("send a request command to channel <" + channel.remoteAddress() + "> failed.");
					plog.warn(request.toString());

				}
			});

			RemotingCommand responseCommand = responseFuture.waitResponse(timeoutMillis);
			if (null == responseCommand) {
				if (responseFuture.isSendRequestOK()) {
					throw new RemotingTimeoutException(RemotingHelper.parseChannelRemoteAddr(channel), timeoutMillis,
							responseFuture.getCause());
				} else {
					throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel),
							responseFuture.getCause());
				}
			}

			return responseCommand;
		} finally {
			this.responseTable.remove(request.getOpaque());
		}
	}

	public void invokeAsyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis,
			final InvokeCallback invokeCallback) throws InterruptedException, RemotingTooMuchRequestException,
					RemotingTimeoutException, RemotingSendRequestException {
		boolean acquired = this.semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
		if (acquired) {
			final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreAsync);

			final ResponseFuture responseFuture = new ResponseFuture(request.getOpaque(), timeoutMillis, invokeCallback,
					once);
			this.responseTable.put(request.getOpaque(), responseFuture);
			try {
				channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
					@Override
					public void operationComplete(ChannelFuture f) throws Exception {
						if (f.isSuccess()) {
							responseFuture.setSendRequestOK(true);
							return;
						} else {
							responseFuture.setSendRequestOK(false);
						}

						responseFuture.putResponse(null);
						responseTable.remove(request.getOpaque());
						try {
							responseFuture.executeInvokeCallback();
						} catch (Throwable e) {
							plog.warn("excute callback in writeAndFlush addListener, and callback throw", e);
						} finally {
							responseFuture.release();
						}

						plog.warn("send a request command to channel <{}> failed.",
								RemotingHelper.parseChannelRemoteAddr(channel));
						plog.warn(request.toString());
					}
				});
			} catch (Exception e) {
				responseFuture.release();
				plog.warn("send a request command to channel <" + RemotingHelper.parseChannelRemoteAddr(channel)
						+ "> Exception", e);
				throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
			}
		} else {
			if (timeoutMillis <= 0) {
				throw new RemotingTooMuchRequestException("invokeAsyncImpl invoke too fast");
			} else {
				String info = String.format(
						"invokeAsyncImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d", //
						timeoutMillis, //
						this.semaphoreAsync.getQueueLength(), //
						this.semaphoreAsync.availablePermits()//
				);
				plog.warn(info);
				plog.warn(request.toString());
				throw new RemotingTimeoutException(info);
			}
		}
	}

	public void invokeOnewayImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis)
			throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException,
			RemotingSendRequestException {
		request.markOnewayRPC();
		boolean acquired = this.semaphoreOneway.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
		if (acquired) {
			final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreOneway);
			try {
				channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
					@Override
					public void operationComplete(ChannelFuture f) throws Exception {
						once.release();
						if (!f.isSuccess()) {
							plog.warn("send a request command to channel <" + channel.remoteAddress() + "> failed.");
							plog.warn(request.toString());
						}
					}
				});
			} catch (Exception e) {
				once.release();
				plog.warn("write send a request command to channel <" + channel.remoteAddress() + "> failed.");
				throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
			}
		} else {
			if (timeoutMillis <= 0) {
				throw new RemotingTooMuchRequestException("invokeOnewayImpl invoke too fast");
			} else {
				String info = String.format(
						"invokeOnewayImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d", //
						timeoutMillis, //
						this.semaphoreAsync.getQueueLength(), //
						this.semaphoreAsync.availablePermits()//
				);
				plog.warn(info);
				plog.warn(request.toString());
				throw new RemotingTimeoutException(info);
			}
		}
	}
}
