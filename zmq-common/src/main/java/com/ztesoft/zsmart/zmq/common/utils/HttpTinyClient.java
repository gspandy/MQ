package com.ztesoft.zsmart.zmq.common.utils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.List;

import com.ztesoft.zsmart.zmq.common.MQVersion;

/**
 * http 简易客户端
 * 
 * @author J.Wang
 *
 */
public class HttpTinyClient {

	/**
	 * 发送get 请求
	 * 
	 * @param url
	 * @param headers
	 * @param paramValues
	 * @param encoding
	 * @param readTimeoutMs
	 * @return
	 * @throws IOException
	 */
	public static HttpResult httpGet(String url, List<String> headers, List<String> paramValues, String encoding,
			long readTimeoutMs) throws IOException {
		String encodedContent = encodingParams(paramValues, encoding);

		url += (null == encodedContent) ? "" : ("?" + encodedContent);

		HttpURLConnection conn = null;

		try {
			conn = (HttpURLConnection) new URL(url).openConnection();
			conn.setRequestMethod("GET");
			conn.setConnectTimeout((int) readTimeoutMs);
			conn.setReadTimeout((int) readTimeoutMs);
			setHeaders(conn, headers, encoding);
			conn.connect();
			int respCode = conn.getResponseCode();
			String resp = null;

			if (HttpURLConnection.HTTP_OK == respCode) {
				resp = IOTinyUtils.toString(conn.getInputStream(), encoding);
			} else {
				resp = IOTinyUtils.toString(conn.getErrorStream(), encoding);
			}

			return new HttpResult(respCode, resp);

		} finally {
			if (conn != null) {
				conn.disconnect();
			}
		}
	}

	public static HttpResult httpPost(String url, List<String> headers, List<String> paramValues, String encoding,
			long readTimeoutMs) throws IOException{
		String encodedContent = encodingParams(paramValues, encoding);

		HttpURLConnection conn = null;
		try {
			conn = (HttpURLConnection) new URL(url).openConnection();
			conn.setRequestMethod("POST");
			conn.setConnectTimeout(3000);
			conn.setReadTimeout((int) readTimeoutMs);
			conn.setDoOutput(true);
			conn.setDoInput(true);
			setHeaders(conn, headers, encoding);
			conn.getOutputStream().write(encodedContent.getBytes());

			int respCode = conn.getResponseCode();
			String resp = null;

			if (HttpURLConnection.HTTP_OK == respCode) {
				resp = IOTinyUtils.toString(conn.getInputStream(), encoding);
			} else {
				resp = IOTinyUtils.toString(conn.getErrorStream(), encoding);
			}

			return new HttpResult(respCode, resp);
		} finally {
			if (conn != null) {
				conn.disconnect();
			}
		}
	}

	/**
	 * 设置http header
	 * 
	 * @param conn
	 * @param headers
	 * @param encoding
	 */
	private static void setHeaders(HttpURLConnection conn, List<String> headers, String encoding) {
		if (null != headers) {
			for (Iterator<String> iter = headers.iterator(); iter.hasNext();) {
				conn.addRequestProperty(iter.next(), iter.next());
			}

			conn.addRequestProperty("Client-Version", MQVersion.getVersionDesc(MQVersion.CurrentVersion));
			conn.addRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=" + encoding);

			// 其它
			String ts = String.valueOf(System.currentTimeMillis());
			conn.addRequestProperty("Metaq-Client-RequestTs", ts);
		}
	}

	/**
	 * 设置参数编码
	 * 
	 * @param paramValues
	 * @param encoding
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	public static String encodingParams(List<String> paramValues, String encoding) throws UnsupportedEncodingException {
		StringBuilder sb = new StringBuilder();

		if (paramValues == null) {
			return null;
		}

		for (Iterator<String> iter = paramValues.iterator(); iter.hasNext();) {
			sb.append(iter.next()).append("=");
			sb.append(URLEncoder.encode(iter.next(), encoding));
			if (iter.hasNext()) {
				sb.append("&");
			}
		}

		return sb.toString();
	}

	public static class HttpResult {
		final public int code;
		final public String content;

		public HttpResult(int code, String content) {
			super();
			this.code = code;
			this.content = content;
		}
	}
}
