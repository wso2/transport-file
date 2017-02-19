package org.wso2.carbon.transport.file.connector.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.messaging.CarbonCallback;
import org.wso2.carbon.messaging.CarbonMessage;
import org.wso2.carbon.messaging.ServerConnectorErrorHandler;
import org.wso2.carbon.transport.file.connector.server.util.Constants;

/**
 * Error handler for the file server connector.
 */
public class FileServerConnectorErrorHandler implements ServerConnectorErrorHandler {
    private Logger logger = LoggerFactory.getLogger(FileServerConnectorErrorHandler.class);

    @Override
    public void handleError(Exception e, CarbonMessage carbonMessage, CarbonCallback carbonCallback)
            throws Exception {
        /*
         * When there is an error in the application side, it is required to inform to the transport, since it will be
         * waiting for the callback. After acknowledging, error should be thrown, to the application level.
         */
        carbonCallback.done(carbonMessage);
        logger.error("Error occurred while trying to process the file content", e);
    }

    @Override
    public String getProtocol() {
        return Constants.PROTOCOL_NAME;
    }
}
