package org.autojs.autojs.pluginclient;

import android.annotation.SuppressLint;
import android.content.Context;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;
import android.os.Build;
import android.os.Handler;
import android.os.Looper;
import android.util.Log;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.stardust.app.GlobalAppContext;
import com.stardust.autojs.core.http.IWsManager;
import com.stardust.util.MapBuilder;

import org.autojs.autojs.BuildConfig;
import org.autojs.autojs.ui.main.drawer.DrawerFragment;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.reactivex.android.schedulers.AndroidSchedulers;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okio.ByteString;

@SuppressWarnings("ALL")
public class WSBiz implements IWsManager {
    public static WSBiz BIZ_WS = null;
    private static final String TYPE_HELLO = "hello";
    private static final String HEART_CHECK = "heartcheck";
    private static final String TYPE_BYTES_COMMAND = "bytes_command";
    private static final int CLIENT_VERSION = 2;
    private final static int RECONNECT_INTERVAL = 10 * 1000;    //重连自增步长
    private final static long RECONNECT_MAX_TIME = 120 * 1000;   //最大重连间隔
    private DevPluginResponseHandler mResponseHandler;
    private final HashMap<String, JsonWebSocket.Bytes> mBytes = new HashMap<>();
    private final HashMap<String, JsonObject> mRequiredBytesCommands = new HashMap<>();
    private static final ExecutorService executors = Executors.newFixedThreadPool(2);
    public static Context mContext;
    private String wsUrl;
    private WebSocket mWebSocket;
    private OkHttpClient mOkHttpClient;
    private static Request mRequest;
    private int mCurrentStatus = WsStatus.DISCONNECTED;     //websocket连接状态
    private boolean isNeedReconnect;          //是否需要断线自动重连
    public static boolean isManualClose = false;         //是否为手动关闭websocket连接
    private Lock mLock;
    private Handler wsMainHandler = new Handler(Looper.getMainLooper());
    private int reconnectCount = 0;   //重连次数

    public WSBiz() {
        File cache = new File(GlobalAppContext.get().getCacheDir(), "remote_project");
        mResponseHandler = new DevPluginResponseHandler(cache);
    }

    private Runnable reconnectRunnable = new Runnable() {
        @Override
        public void run() {
            Log.e("websocket", "服务器重连接中...");
            buildConnect();
        }
    };

    private WebSocketListener mWebSocketListener = new WebSocketListener() {

        @Override
        public void onOpen(WebSocket webSocket, final Response response) {
            mWebSocket = webSocket;
            setCurrentStatus(WsStatus.CONNECTED);
            connected();
            if (Looper.myLooper() != Looper.getMainLooper()) {
                Map<String, Object> msg = new MapBuilder<String, Object>()
                        .put("type",TYPE_HELLO)
                        .put("device_name", Build.BRAND + " " + Build.MODEL)
                        .put("client_version", CLIENT_VERSION)
                        .put("app_version", BuildConfig.VERSION_NAME)
                        .put("app_version_code", BuildConfig.VERSION_CODE)
                        .put("app_serial",Build.ID)
                        .build();
                sendMessage(msg.toString());
            } else {
                Log.e("WSBiz websocket", "WSBiz服务器连接失败");
            }
        }

        @Override
        public void onMessage(WebSocket webSocket, final ByteString bytes) {
            if (Looper.myLooper() != Looper.getMainLooper()) {
                wsMainHandler.post(new Runnable() {
                    @Override
                    public void run() {
                        Log.e("WSBiz websocket", "WsManager-----onMessage");

                    }
                });
            } else {
                Log.e("websocket", "WSBiz Manager-----onMessage");
            }
        }

        @Override
        public void onMessage(WebSocket webSocket, final String text) {
            if (Looper.myLooper() != Looper.getMainLooper()) {
                wsMainHandler.post(new Runnable() {
                    @Override
                    public void run() {
                        Log.e("WSBiz websocket", "WsManager-----onMessage:"+text);
                        try {
                            Gson gson = new Gson();
                            JsonObject obj = gson.fromJson(text,JsonObject.class);
                            JsonElement typeElement = obj.get("type");
                            JsonElement cmdElement = obj.get("cmd");
                            String cmd = null;
                            if(cmdElement!= null){
                                cmd = cmdElement.getAsString();
                            }
                            if(typeElement == null && HEART_CHECK.equals(cmd)){
                                onServerHello();
                                return;
                            }
                            if (typeElement == null || !typeElement.isJsonPrimitive()) {
                                return;
                            }
                            String type = typeElement.getAsString();
                            if (TYPE_HELLO.equals(type)) {
                                onServerHello();
                                return;
                            }
                            if (TYPE_BYTES_COMMAND.equals(type)) {
                                String md5 = obj.get("md5").getAsString();
                                JsonWebSocket.Bytes bytes = mBytes.remove(md5);
                                if (bytes != null) {
                                    handleBytes(obj, bytes);
                                } else {
                                    mRequiredBytesCommands.put(md5, obj);
                                }
                                return;
                            }
                            mResponseHandler.handle(obj);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                });
            } else {
                Log.e("websocket", "WSBiz WsManager-----onMessage");
            }
        }

        @SuppressLint("CheckResult")
        private void handleBytes(JsonObject obj, JsonWebSocket.Bytes bytes) {
            mResponseHandler.handleBytes(obj, bytes)
                    .observeOn(AndroidSchedulers.mainThread())
                    .subscribe(dir -> {
                        obj.get("data").getAsJsonObject().add("dir", new JsonPrimitive(dir.getPath()));
                        mResponseHandler.handle(obj);
                    });
        }

        @Override
        public void onClosing(WebSocket webSocket, final int code, final String reason) {
            if (Looper.myLooper() != Looper.getMainLooper()) {
                wsMainHandler.post(new Runnable() {
                    @Override
                    public void run() {
                        Log.e("websocket", "WSBiz服务器连接关闭中");
                    }
                });
            } else {
                Log.e("websocket", "WSBiz服务器连接关闭中");
            }
        }

        @Override
        public void onClosed(WebSocket webSocket, final int code, final String reason) {

            if (Looper.myLooper() != Looper.getMainLooper()) {
                wsMainHandler.post(new Runnable() {
                    @Override
                    public void run() {
                        Log.e("websocket", "WSBiz服务器连接已关闭");
                    }
                });
            } else {
                Log.e("websocket", "WSBiz服务器连接已关闭");
            }
        }

        @Override
        public void onFailure(WebSocket webSocket, final Throwable t, final Response response) {
            try {
                tryReconnect();
                Log.e("onFailure", "WSBiz[走的链接失败这里！！！！！！！！！！！！！！！！]");
                if (Looper.myLooper() != Looper.getMainLooper()) {
                    wsMainHandler.post(new Runnable() {
                        @Override
                        public void run() {
                            Log.e("websocket", "WSBiz服务器连接失败");
                        }
                    });
                } else {
                    Log.e("websocket", "WSBiz服务器连接失败");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    };

    private void onServerHello() {

        executors.submit(new Runnable() {
            @Override
            public void run() {
                try{//异步延缓3S 回应服务器 防止掉线
                    Thread.sleep(5000);
                    Map<String, Object> msg = new MapBuilder<String, Object>()
                            .put("type",TYPE_HELLO)
                            .put("device_name", Build.BRAND + " " + Build.MODEL)
                            .put("client_version", CLIENT_VERSION)
                            .put("app_version", BuildConfig.VERSION_NAME)
                            .put("app_version_code", BuildConfig.VERSION_CODE)
                            .put("app_serial",Build.ID)
                            .build();
                    sendMessage(msg.toString());
                }catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });




    }

    public WSBiz(Builder builder) {
        mContext = builder.mContext;
        wsUrl = builder.wsUrl;
        isNeedReconnect = builder.needReconnect;
        mOkHttpClient = builder.mOkHttpClient;
        this.mLock = new ReentrantLock();
        File cache = new File(GlobalAppContext.get().getCacheDir(), "remote_project");
        mResponseHandler = new DevPluginResponseHandler(cache);
    }

    private void initWebSocket() {
        if (mOkHttpClient == null) {
            mOkHttpClient = new OkHttpClient.Builder()
                    .retryOnConnectionFailure(true)
                    .build();
        }
        if (mRequest == null) {
            mRequest = new Request.Builder()
                    .url(wsUrl)
                    .build();
        }
        mOkHttpClient.dispatcher().cancelAll();
        try {
            mLock.lockInterruptibly();
            try {
                mOkHttpClient.newWebSocket(mRequest, mWebSocketListener);
            } finally {
                mLock.unlock();
            }
        } catch (InterruptedException e) {
        }
    }

    @Override
    public WebSocket getWebSocket() {
        return mWebSocket;
    }


    @Override
    public synchronized boolean isWsConnected() {
        return mCurrentStatus == WsStatus.CONNECTED;
    }

    @Override
    public synchronized int getCurrentStatus() {
        return mCurrentStatus;
    }

    @Override
    public synchronized void setCurrentStatus(int currentStatus) {
        this.mCurrentStatus = currentStatus;
    }

    @Override
    public void startConnect() {
        isManualClose = false;
        buildConnect();
    }

    @Override
    public void stopConnect() {
        isManualClose = true;
        disconnect();
    }

    private void tryReconnect() {
        if (!isNeedReconnect | isManualClose | !DrawerFragment.WS_CHECKED) {
            return;
        }
        Log.e("tryReconnect", "WSBizreconnectCount2222222[" + reconnectCount + "]");
        if (!isNetworkConnected(mContext)) {
            setCurrentStatus(WsStatus.DISCONNECTED);
            Log.e("tryReconnect", "WSBiz[请您检查网络，未连接]");
//            return;
        }
        setCurrentStatus(WsStatus.RECONNECT);
        Log.e("tryReconnect", "WSBizreconnectCount11111111[" + reconnectCount + "]");
        long delay = reconnectCount * RECONNECT_INTERVAL;
//        wsMainHandler.postDelayed(reconnectRunnable, delay > RECONNECT_MAX_TIME ? RECONNECT_MAX_TIME : delay);
        wsMainHandler.postDelayed(reconnectRunnable, 10000);
        Log.e("tryReconnect", "WSBizreconnectCount[" + reconnectCount + "]");
        reconnectCount++;

    }

    private void cancelReconnect() {
        wsMainHandler.removeCallbacks(reconnectRunnable);
        reconnectCount = 0;
    }

    private void connected() {
        cancelReconnect();
    }

    private void disconnect() {
        if (mCurrentStatus == WsStatus.DISCONNECTED) {
            return;
        }
        cancelReconnect();
        if (mOkHttpClient != null) {
            mOkHttpClient.dispatcher().cancelAll();
        }
        if (mWebSocket != null) {
            boolean isClosed = mWebSocket.close(WsStatus.CODE.NORMAL_CLOSE, WsStatus.TIP.NORMAL_CLOSE);
            //非正常关闭连接
            if (!isClosed) {
                Log.e("websocket", "WSBiz服务器连接失败");
            }
        }
        setCurrentStatus(WsStatus.DISCONNECTED);
    }

    private synchronized void buildConnect() {
        if (!isNetworkConnected(mContext)) {
            setCurrentStatus(WsStatus.DISCONNECTED);
//            return;
        }
        switch (getCurrentStatus()) {
            case WsStatus.CONNECTED:
            case WsStatus.CONNECTING:
                break;
            default:
                setCurrentStatus(WsStatus.CONNECTING);
                initWebSocket();
        }
    }

    //发送消息
    @Override
    public boolean sendMessage(String msg) {
        return send(msg);
    }

    @Override
    public boolean sendMessage(ByteString byteString) {
        return send(byteString);
    }

    private boolean send(Object msg) {
        boolean isSend = false;
        if (mWebSocket != null && mCurrentStatus == WsStatus.CONNECTED) {
            if (msg instanceof String) {
                isSend = mWebSocket.send((String) msg);
            } else if (msg instanceof ByteString) {
                isSend = mWebSocket.send((ByteString) msg);
            }
            //发送消息失败，尝试重连
            if (!isSend) {
                tryReconnect();
            }
        }
        return isSend;
    }

    //检查网络是否连接
    private boolean isNetworkConnected(Context context) {
        if (context != null) {
            ConnectivityManager mConnectivityManager = (ConnectivityManager) context
                    .getSystemService(Context.CONNECTIVITY_SERVICE);
            @SuppressLint("MissingPermission") NetworkInfo mNetworkInfo = mConnectivityManager.getActiveNetworkInfo();
            if (mNetworkInfo != null) {
                return mNetworkInfo.isAvailable();
            }
        }
        return false;
    }

    public static final class Builder {

        private Context mContext;
        private String wsUrl;
        private boolean needReconnect = true;
        private OkHttpClient mOkHttpClient;

        public Builder(Context val) {
            mContext = val;
        }

        public Builder wsUrl(String val) {
            wsUrl = val;
            return this;
        }

        public Builder client(OkHttpClient val) {
            mOkHttpClient = val;
            return this;
        }

        public Builder needReconnect(boolean val) {
            needReconnect = val;
            return this;
        }

        public WSBiz build() {
            return new WSBiz(this);
        }

    }
}
