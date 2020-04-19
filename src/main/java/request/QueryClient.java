package request;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import entities.DetectedEvent;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class QueryClient implements Serializable {
    // one instance, reuse
    private static final CloseableHttpClient httpClient = HttpClients.createDefault();
    private static String host;
    private static String endpoint;

    public QueryClient(String host, String endpoint){
        this.host = host;
        this.endpoint = endpoint;
    }

    public void close() throws IOException {
        httpClient.close();
    }

    public String getBatch() {
        String url = "http://" + host + endpoint;
        HttpGet request = new HttpGet(url);

        try (CloseableHttpResponse response = httpClient.execute(request)) {
            // Get HttpResponse Status
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 404 || statusCode == 500){
                return null;
            }

            HttpEntity entity = response.getEntity();

            if (entity != null) {
                // return it as a String
                String result = EntityUtils.toString(entity);
                return result;
            }

        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public static void post(DetectedEvent playload) throws Exception {
        String url = "http://" + host + endpoint;
        HttpPost request = new HttpPost(url);
        request.addHeader("Content-type", "application/json");
        // add request parameter, form parameters
        request.setEntity(new StringEntity(playload.toJson()));
        CloseableHttpResponse response = httpClient.execute(request);;
        try {
            EntityUtils.consume(response.getEntity());
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            response.close();
        }
//        try {
//            httpClient.execute(request);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }finally {
//            httpClient.close();
//        }
    }

    public static void finalGet() throws Exception{
        String url = "http://" + host + endpoint;
        HttpGet request = new HttpGet(url);
        try {
            CloseableHttpResponse response = httpClient.execute(request);
            response.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
