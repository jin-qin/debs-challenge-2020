package request;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import entities.DetectedEvent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Query1Client {
    // one instance, reuse
    private final CloseableHttpClient httpClient = HttpClients.createDefault();
    private String host;
    private String endpoint;

    public Query1Client(String host, String endpoint){
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

        } catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public void post(DetectedEvent playload) throws Exception {
        String url = "http://" + host + endpoint;
        HttpPost request = new HttpPost(url);
        request.addHeader("Content-type", "application/json");

        // add request parameter, form parameters
        List<NameValuePair> urlParameters = new ArrayList<>();
        urlParameters.add(new BasicNameValuePair("json", playload.toString()));

        request.setEntity(new UrlEncodedFormEntity(urlParameters));

        try {
            httpClient.execute(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
