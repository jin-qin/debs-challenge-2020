package request;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class Query1Client {
    // one instance, reuse
    private final CloseableHttpClient httpClient = HttpClients.createDefault();
    private String host;
    private String endpoint;

    public Query1Client(String host, String endpoint){
        this.host = host;
        this.endpoint = endpoint;
    }

    private void close() throws IOException {
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
//    private void sendPost() throws Exception {
//
//        HttpPost post = new HttpPost("https://httpbin.org/post");
//
//        // add request parameter, form parameters
//        List<NameValuePair> urlParameters = new ArrayList<>();
//        urlParameters.add(new BasicNameValuePair("username", "abc"));
//        urlParameters.add(new BasicNameValuePair("password", "123"));
//        urlParameters.add(new BasicNameValuePair("custom", "secret"));
//
//        post.setEntity(new UrlEncodedFormEntity(urlParameters));
//
//        try (CloseableHttpClient httpClient = HttpClients.createDefault();
//             CloseableHttpResponse response = httpClient.execute(post)) {
//
//            System.out.println(EntityUtils.toString(response.getEntity()));
//        }
//
//    }
}
