import java.io.FilterInputStream;
import java.io.InputStream;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class S3ObjectInputStreamWrapper extends FilterInputStream {
 
    @SuppressWarnings("unused")
    private AmazonS3 client;
 
    // don't call this
    protected S3ObjectInputStreamWrapper(InputStream inputStream) {
        super(inputStream);
    }
    
    public S3ObjectInputStreamWrapper(S3ObjectInputStream inputStream, AmazonS3 client) {
        super(inputStream);
        this.client = client;
    }
    
    // S3ObjectInputStream also implements abort() and getHttpRequest(). Override and delegate if needed.
}