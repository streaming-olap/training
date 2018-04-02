package com.xiaoxiang.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;

/**
 * Created by yuhailin on 2018/3/1.
 */
public class SocketSource implements SourceFunction<String> {

    private volatile boolean running = true;



    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (running) {
            try {
                String response = SocketSource.sendGet("http://127.0.0.1:8080/getconfig", "");
                sourceContext.collect(response);
            } catch (Exception e) {
                //TODO
            }
            Thread.sleep(60000);
        }

    }

    @Override
    public void cancel() {

        running = false;
    }

    public static String sendGet(String url, String param) throws Exception {
        String result = "";
        BufferedReader in = null;

        try {
            String e = url + param;
            URL realUrl = new URL(e);
            URLConnection connection = realUrl.openConnection();
            connection.setConnectTimeout(3000);
            connection.connect();

            String line;
            for(in = new BufferedReader(new InputStreamReader(connection.getInputStream())); (line = in.readLine()) != null; result = result + line) {
                ;
            }
        } catch (Exception var15) {
            var15.printStackTrace();
            throw new Exception("发送GET请求出现异常！" + var15);
        } finally {
            try {
                if(in != null) {
                    in.close();
                }
            } catch (Exception var14) {
                var14.printStackTrace();
                throw new Exception("关闭网络请求异常！" + var14);
            }

        }

        return result;
    }


}
