package org.apache.rocketmq.mqtt.cs.config;

import java.util.EnumSet;

public class CoapConf {
    /**
     * Coap Version
     */
    public static final int VERSION = 1;

    /**
     * Coap Division Marker for Payload
     */
    public static final int PAYLOAD_MARKER = 0xFF;

    /**
     * Coap Token Length must be 0~8
     */
    public static final int MAX_TOKEN_LENGTH = 8;

    /**
     * Coap Type,
     * Four types: CON, NON, ACK, RST
     */
    public enum TYPE {
        CON(0),
        NON(1),
        ACK(2),
        RST(3);

        private final int value;

        TYPE(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    /**
     * Coap Request Code for 0.xx,
     * Four types: GET, POST, PUT, DELETE
     */
    public enum REQUEST_CODE {
        GET(1),
        POST(2),
        PUT(3),
        DELETE(4);

        private final int value;

        REQUEST_CODE(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    /**
     * Coap Response Code for 2.xx Success:
     * 2.01: Created, 2.02: Deleted, 2.03: Valid, 2.04: Changed, 2.05: Content
     */
    public enum RESPONSE_CODE_SUCCESS {
        CREATED(65),
        DELETED(66),
        VALID(67),
        CHANGED(68),
        CONTENT(69);

        private final int value;

        RESPONSE_CODE_SUCCESS(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    /**
     * Coap Response Code for 4.xx Client Error:
     * 4.00: Bad Request, 4.01: Unauthorized, 4.02: Bad Option, 4.03: Forbidden, 4.04: Not Found, 4.05: Method Not Allowed, 4.06: Not Acceptable, 4.12: Precondition Failed, 4.13: Request Entity Too Large, 4.15: Unsupported Content-Format
     */
    public enum RESPONSE_CODE_CLIENT_ERROR {
        BAD_REQUEST(128),
        UNAUTHORIZED(129),
        BAD_OPTION(130),
        FORBIDDEN(131),
        NOT_FOUND(132),
        METHOD_NOT_ALLOWED(133),
        NOT_ACCEPTABLE(134),
        PRECONDITION_FAILED(140),
        REQUEST_ENTITY_TOO_LARGE(141),
        UNSUPPORTED_CONTENT_FORMAT(143);

        private final int value;

        RESPONSE_CODE_CLIENT_ERROR(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    /**
     * Coap Response Code for 5.xx Server Error:
     * 5.00: Internal Server Error, 5.01: Not Implemented, 5.02: Bad Gateway, 5.03: Service Unavailable, 5.04: Gateway Timeout, 5.05: Proxying Not Supported
     */
    public enum RESPONSE_CODE_SERVER_ERROR {
        INTERNAL_SERVER_ERROR(160),
        NOT_IMPLEMENTED(161),
        BAD_GATEWAY(162),
        SERVICE_UNAVAILABLE(163),
        GATEWAY_TIMEOUT(164),
        PROXYING_NOT_SUPPORTED(165);

        private final int value;

        RESPONSE_CODE_SERVER_ERROR(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    /**
     * Coap Option Number
     */
    public enum OPTION_NUMBER {
        IF_MATCH(1),
        URI_HOST(3),
        ETAG(4),
        IF_NONE_MATCH(5),
        OBSERVE(6),
        URI_PORT(7),
        LOCATION_PATH(8),
        URI_PATH(11),
        CONTENT_FORMAT(12),
        MAX_AGE(14),
        URI_QUERY(15),
        ACCEPT(17),
        LOCATION_QUERY(20),
        BLOCK_2(23),
        BLOCK_1(27),
        SIZE_2(28),
        PROXY_URI(35),
        PROXY_SCHEME(39),
        SIZE_1(60);

        private final int value;

        OPTION_NUMBER(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public static boolean isValid(int value) {
            return EnumSet.allOf(OPTION_NUMBER.class).stream().anyMatch(option -> option.getValue() == value);
        }
    }



}
