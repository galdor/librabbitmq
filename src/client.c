/*
 * Copyright (c) 2015 Nicolas Martyanoff
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "internal.h"

/* ---------------------------------------------------------------------------
 *  Consumer
 * ------------------------------------------------------------------------ */
struct rmq_consumer *
rmq_consumer_new(const char *queue, char *tag) {
    struct rmq_consumer *consumer;

    consumer = c_malloc0(sizeof(struct rmq_consumer));

    consumer->queue = c_strdup(queue);
    consumer->tag = tag;

    return consumer;
}

void
rmq_consumer_delete(struct rmq_consumer *consumer) {
    if (!consumer)
        return;

    c_free(consumer->queue);
    c_free(consumer->tag);

    c_free0(consumer, sizeof(struct rmq_consumer));
}

/* ---------------------------------------------------------------------------
 *  Client
 * ------------------------------------------------------------------------ */
static void rmq_client_signal_event(struct rmq_client *,
                                    enum rmq_client_event, void *);
static void rmq_client_trace(struct rmq_client *, const char *, ...)
    __attribute__ ((format(printf, 2, 3)));
static void rmq_client_error(struct rmq_client *, const char *, ...)
    __attribute__ ((format(printf, 2, 3)));
static void rmq_client_fatal(struct rmq_client *, const char *, ...)
    __attribute__ ((format(printf, 2, 3)));

static void rmq_client_on_tcp_event(struct io_tcp_client *,
                                    enum io_tcp_client_event,
                                    void *);

static void rmq_client_on_conn_closed(struct rmq_client *);
static void rmq_client_on_conn_established(struct rmq_client *);
static void rmq_client_on_data(struct rmq_client *);

static int rmq_client_on_frame(struct rmq_client *, const struct rmq_frame *);
static int rmq_client_on_method(struct rmq_client *,
                                const struct rmq_method_frame *);

struct rmq_client *
rmq_client_new(struct io_base *io_base) {
    struct rmq_client *client;

    client = c_malloc0(sizeof(struct rmq_client));

    client->io_base = io_base;

    client->tcp_client = io_tcp_client_new(io_base,
                                           rmq_client_on_tcp_event, client);

    client->login = c_strdup("guest");
    client->password = c_strdup("guest");
    client->vhost = c_strdup("/");

    client->consumers = c_hash_table_new(c_hash_string, c_equal_string);

    return client;
}

void
rmq_client_delete(struct rmq_client *client) {
    struct c_hash_table_iterator *it;
    struct rmq_consumer *consumer;

    if (!client)
        return;

    io_tcp_client_delete(client->tcp_client);

    c_free(client->login);
    c_free(client->password);
    c_free(client->vhost);

    it = c_hash_table_iterate(client->consumers);
    while (c_hash_table_iterator_next(it, NULL, (void **)&consumer) == 1)
        rmq_consumer_delete(consumer);
    c_hash_table_iterator_delete(it);
    c_hash_table_delete(client->consumers);

    c_free0(client, sizeof(struct rmq_client));
}

void
rmq_client_set_event_cb(struct rmq_client *client,
                        rmq_client_event_cb cb, void *arg) {
    client->event_cb = cb;
    client->event_cb_arg = arg;
}

void
rmq_client_set_credentials(struct rmq_client *client,
                           const char *login, const char *password) {
    c_free(client->login);
    if (login) {
        client->login = c_strdup(login);
    } else {
        client->login = NULL;
    }

    c_free(client->password);
    if (password) {
        client->password = c_strdup(password);
    } else {
        client->password = NULL;
    }
}

void
rmq_client_set_vhost(struct rmq_client *client, const char *vhost) {
    c_free(client->vhost);
    client->vhost = c_strdup(vhost);
}

int
rmq_client_connect(struct rmq_client *client,
                   const char *host, uint16_t port) {
    if (!client->login) {
        c_set_error("missing login");
        return -1;
    }

    if (!client->password) {
        c_set_error("missing password");
        return -1;
    }

    return io_tcp_client_connect(client->tcp_client, host, port);
}

void
rmq_client_disconnect(struct rmq_client *client) {
    if (io_tcp_client_is_connected(client->tcp_client)) {
        rmq_client_connection_close(client, RMQ_REPLY_CODE_SUCCESS, "goodbye");

        /* TODO Timeout in case the server never sends Connection.Close-Ok and
         * does not close the connection */
    } else {
        io_tcp_client_disconnect(client->tcp_client);
    }
}

void
rmq_client_send_frame(struct rmq_client *client, enum rmq_frame_type type,
                      uint16_t channel, const void *data, size_t size) {
    struct rmq_frame frame;
    struct c_buffer *wbuf;

    assert(size <= UINT32_MAX);

    rmq_frame_init(&frame);

    frame.type = type;
    frame.channel = channel;
    frame.size = size;
    frame.payload = data;
    frame.end = RMQ_FRAME_END;

    wbuf = io_tcp_client_wbuf(client->tcp_client);
    rmq_frame_write(&frame, wbuf);
    io_tcp_client_signal_data_written(client->tcp_client);
}

void
rmq_client_send_method(struct rmq_client *client, enum rmq_method method, ...) {
    struct rmq_method_frame method_frame;
    struct c_buffer *args_buf, *buf;
    va_list ap;

    /* Args */
    args_buf = c_buffer_new();

    va_start(ap, method);
    rmq_fields_vwrite(args_buf, ap);
    va_end(ap);

    /* Method frame */
    rmq_method_frame_init(&method_frame);

    method_frame.class_id = method >> 16;
    method_frame.method_id = method & 0x0000ffff;
    method_frame.args = c_buffer_data(args_buf);
    method_frame.args_sz = c_buffer_length(args_buf);

    /* Frame */
    buf = c_buffer_new();
    rmq_method_frame_write(&method_frame, buf);

    rmq_client_send_frame(client, RMQ_FRAME_TYPE_METHOD, client->channel,
                          c_buffer_data(buf), c_buffer_length(buf));

    c_buffer_delete(args_buf);
    c_buffer_delete(buf);
}

void
rmq_client_send_header(struct rmq_client *client, uint16_t class_id,
                       uint64_t body_size,
                       const struct rmq_properties *properties) {
    struct rmq_header_frame header_frame;
    struct c_buffer *buf;

    /* Header frame */
    rmq_header_frame_init(&header_frame);

    header_frame.class_id = class_id;
    header_frame.body_size = body_size;
    header_frame.properties = properties;

    /* Frame */
    buf = c_buffer_new();
    rmq_header_frame_write(&header_frame, buf);

    rmq_client_send_frame(client, RMQ_FRAME_TYPE_HEADER, client->channel,
                          c_buffer_data(buf), c_buffer_length(buf));

    c_buffer_delete(buf);
}

void
rmq_client_send_body(struct rmq_client *client,
                     const void *data, size_t size) {
    rmq_client_send_frame(client, RMQ_FRAME_TYPE_BODY, client->channel,
                          data, size);
}

void
rmq_client_connection_close(struct rmq_client *client,
                            enum rmq_reply_code code, const char *fmt, ...) {
    char text[256];
    va_list ap;

    assert(client->state != RMQ_CLIENT_STATE_DISCONNECTED);

    va_start(ap, fmt);
    vsnprintf(text, sizeof(text), fmt, ap);
    va_end(ap);

    rmq_client_send_method(client, RMQ_METHOD_CONNECTION_CLOSE,
                           RMQ_FIELD_SHORT_UINT, code,
                           RMQ_FIELD_SHORT_STRING, text,
                           RMQ_FIELD_SHORT_UINT, 0, /* class id */
                           RMQ_FIELD_SHORT_UINT, 0, /* method id */
                           RMQ_FIELD_END);

    client->state = RMQ_CLIENT_STATE_CLOSING;
}

void
rmq_client_publish(struct rmq_client *client, struct rmq_msg *msg,
                   const char *exchange,
                   const char *routing_key, uint32_t options) {
    rmq_client_send_method(client, RMQ_METHOD_BASIC_PUBLISH,
                           RMQ_FIELD_SHORT_UINT, 0, /* reserved */
                           RMQ_FIELD_SHORT_STRING, exchange,
                           RMQ_FIELD_SHORT_STRING, routing_key,
                           RMQ_FIELD_SHORT_SHORT_UINT, (uint8_t)options,
                           RMQ_FIELD_END);

    rmq_client_send_header(client, RMQ_CLASS_BASIC, msg->data_sz,
                           &msg->properties);

    rmq_client_send_body(client, msg->data, msg->data_sz);

    rmq_msg_delete(msg);
}

void
rmq_client_subscribe(struct rmq_client *client, const char *queue,
                     uint8_t options) {
    struct rmq_field_table *arguments;
    struct rmq_consumer *consumer;
    char *tag;

    assert(c_hash_table_get(client->consumers, queue, (void **)&consumer) == 0);

    c_asprintf(&tag, "consumer-%d", ++client->consumer_tag_id);
    consumer = rmq_consumer_new(queue, tag);

    c_hash_table_insert(client->consumers, consumer->queue, consumer);

    arguments = rmq_field_table_new();

    rmq_client_send_method(client, RMQ_METHOD_BASIC_CONSUME,
                           RMQ_FIELD_SHORT_UINT, 0, /* reserved */
                           RMQ_FIELD_SHORT_STRING, consumer->queue,
                           RMQ_FIELD_SHORT_STRING, consumer->tag,
                           RMQ_FIELD_SHORT_SHORT_UINT, options,
                           RMQ_FIELD_TABLE, arguments,
                           RMQ_FIELD_END);

    rmq_field_table_delete(arguments);
}

void
rmq_client_unsubscribe(struct rmq_client *client, const char *queue,
                       uint8_t options) {
    struct rmq_consumer *consumer;

    assert(c_hash_table_get(client->consumers, queue, (void **)&consumer) == 1);

    c_hash_table_remove(client->consumers, queue);
    rmq_consumer_delete(consumer);

    rmq_client_send_method(client, RMQ_METHOD_BASIC_CANCEL,
                           RMQ_FIELD_SHORT_STRING, consumer->tag,
                           RMQ_FIELD_SHORT_SHORT_UINT, options,
                           RMQ_FIELD_END);
}

static void
rmq_client_signal_event(struct rmq_client *client,
                        enum rmq_client_event event, void *arg) {
    if (!client->event_cb)
        return;

    client->event_cb(client, event, arg, client->event_cb_arg);
}

static void
rmq_client_trace(struct rmq_client *client, const char *fmt, ...) {
    char buf[C_ERROR_BUFSZ];
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(buf, C_ERROR_BUFSZ, fmt, ap);
    va_end(ap);

    rmq_client_signal_event(client, RMQ_CLIENT_EVENT_TRACE, buf);
}

static void
rmq_client_error(struct rmq_client *client, const char *fmt, ...) {
    char buf[C_ERROR_BUFSZ];
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(buf, C_ERROR_BUFSZ, fmt, ap);
    va_end(ap);

    rmq_client_signal_event(client, RMQ_CLIENT_EVENT_ERROR, buf);
}

static void
rmq_client_fatal(struct rmq_client *client, const char *fmt, ...) {
    char buf[C_ERROR_BUFSZ];
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(buf, C_ERROR_BUFSZ, fmt, ap);
    va_end(ap);

    rmq_client_signal_event(client, RMQ_CLIENT_EVENT_ERROR, buf);
    io_tcp_client_disconnect(client->tcp_client);
}

static void
rmq_client_on_tcp_event(struct io_tcp_client *tcp_client,
                        enum io_tcp_client_event event,
                        void *arg) {
    struct rmq_client *client;

    client = arg;

    switch (event) {
    case IO_TCP_CLIENT_EVENT_CONN_ESTABLISHED:
        rmq_client_on_conn_established(client);
        break;

    case IO_TCP_CLIENT_EVENT_CONN_FAILED:
        rmq_client_signal_event(client, RMQ_CLIENT_EVENT_CONN_FAILED, NULL);
        break;

    case IO_TCP_CLIENT_EVENT_CONN_CLOSED:
        rmq_client_on_conn_closed(client);
        break;

    case IO_TCP_CLIENT_EVENT_ERROR:
        rmq_client_error(client, "%s", c_get_error());
        break;

    case IO_TCP_CLIENT_EVENT_DATA_READ:
        rmq_client_on_data(client);
        break;
    }
}

static void
rmq_client_on_conn_closed(struct rmq_client *client) {
    struct c_hash_table_iterator *it;
    struct rmq_consumer *consumer;

    client->state = RMQ_CLIENT_STATE_DISCONNECTED;

    it = c_hash_table_iterate(client->consumers);
    while (c_hash_table_iterator_next(it, NULL, (void **)&consumer) == 1)
        rmq_consumer_delete(consumer);
    c_hash_table_iterator_delete(it);
    c_hash_table_clear(client->consumers);

    rmq_client_signal_event(client, RMQ_CLIENT_EVENT_CONN_CLOSED, NULL);
}

static void
rmq_client_on_conn_established(struct rmq_client *client) {
    client->state = RMQ_CLIENT_STATE_CONNECTED;
    rmq_client_signal_event(client, RMQ_CLIENT_EVENT_CONN_ESTABLISHED, NULL);

    /* Protocol header */
    io_tcp_client_write(client->tcp_client, "AMQP\x00\x00\x09\x01", 8);
}

static void
rmq_client_on_data(struct rmq_client *client) {
    struct c_buffer *rbuf;

    rbuf = io_tcp_client_rbuf(client->tcp_client);

    while (c_buffer_length(rbuf) > 0) {
        struct rmq_frame frame;
        int ret;
        const void *data;
        size_t size, frame_size;

        data = c_buffer_data(rbuf);
        size = c_buffer_length(rbuf);

        ret = rmq_frame_read(&frame, data, size, &frame_size);
        if (ret == -1) {
            rmq_client_fatal(client, "cannot read frame: %s", c_get_error());
            return;
        } else if (ret == 0) {
            break;
        }

        if (rmq_client_on_frame(client, &frame) == -1) {
            rmq_client_fatal(client, "cannot process frame: %s", c_get_error());
            return;
        }

        c_buffer_skip(rbuf, frame_size);
    }
}

static int
rmq_client_on_frame(struct rmq_client *client, const struct rmq_frame *frame) {
    struct rmq_method_frame method;

    if (frame->end != RMQ_FRAME_END) {
        c_set_error("invalid frame end 0x%02x", frame->end);
        return -1;
    }

    switch (frame->type) {
    case RMQ_FRAME_TYPE_METHOD:
        if (rmq_method_frame_read(&method, frame) == -1) {
            c_set_error("cannot read method frame: %s", c_get_error());
            return -1;
        }

        if (rmq_client_on_method(client, &method) == -1) {
            c_set_error("cannot process method frame: %s", c_get_error());
            return -1;
        }
        break;

    case RMQ_FRAME_TYPE_HEADER:
        rmq_client_trace(client, "header frame channel %u", frame->channel);
        break;

    case RMQ_FRAME_TYPE_BODY:
        rmq_client_trace(client, "body frame channel %u", frame->channel);
        break;

    case RMQ_FRAME_TYPE_HEARTBEAT:
        if (frame->channel != 0) {
            /* TODO error 503 */
        }

        rmq_client_trace(client, "heartbeat frame");
        break;

    default:
        c_set_error("unknown frame type %d", frame->type);
        return -1;
    }

    return 0;
}

/* ---------------------------------------------------------------------------
 *  Method handlers
 * ------------------------------------------------------------------------ */
#define RMQ_METHOD_HANDLER(name_)                               \
    static int                                                  \
    rmq_client_on_method_##name_(struct rmq_client *client,     \
                                 const void *data, size_t size)

RMQ_METHOD_HANDLER(connection_start) {
    uint8_t version_major, version_minor;
    struct rmq_field_table *server_properties, *client_properties;
    struct rmq_long_string mechanisms, locales;
    struct rmq_long_string response;
    size_t login_sz, password_sz;
    const char *mechanism, *locale;

    if (client->state != RMQ_CLIENT_STATE_CONNECTED) {
        c_set_error("unexpected method");
        return -1;
    }

    if (rmq_fields_read(data, size, NULL,
                        RMQ_FIELD_SHORT_SHORT_UINT, &version_major,
                        RMQ_FIELD_SHORT_SHORT_UINT, &version_minor,
                        RMQ_FIELD_TABLE, &server_properties,
                        RMQ_FIELD_LONG_STRING, &mechanisms,
                        RMQ_FIELD_LONG_STRING, &locales,
                        RMQ_FIELD_END) == -1) {
        /* TODO error 505 */
        c_set_error("invalid arguments: %s", c_get_error());
        return -1;
    }

    rmq_long_string_free(&mechanisms);
    rmq_long_string_free(&locales);
    rmq_field_table_delete(server_properties);

    /* Response */
    client_properties = rmq_field_table_new();

    mechanism = "PLAIN"; /* TODO */

    login_sz = strlen(client->login);
    password_sz = strlen(client->password);

    rmq_long_string_init(&response);
    response.len = 1 + login_sz + 1 + password_sz;
    response.ptr = c_malloc(response.len);
    response.ptr[0] = '\0';
    memcpy(response.ptr + 1, client->login, login_sz);
    response.ptr[1 + login_sz] = '\0';
    memcpy(response.ptr + 1 + login_sz + 1, client->password, password_sz);

    locale = "en_US"; /* TODO */

    rmq_client_send_method(client, RMQ_METHOD_CONNECTION_START_OK,
                           RMQ_FIELD_TABLE, client_properties,
                           RMQ_FIELD_SHORT_STRING, mechanism,
                           RMQ_FIELD_LONG_STRING, &response,
                           RMQ_FIELD_SHORT_STRING, locale,
                           RMQ_FIELD_END);

    rmq_field_table_delete(client_properties);
    rmq_long_string_free(&response);

    client->state = RMQ_CLIENT_STATE_START_RECEIVED;
    return 0;
}

RMQ_METHOD_HANDLER(connection_tune) {
    uint16_t channel_max, heartbeat;
    uint32_t frame_max;

    if (client->state != RMQ_CLIENT_STATE_START_RECEIVED) {
        c_set_error("unexpected method");
        return -1;
    }

    if (rmq_fields_read(data, size, NULL,
                        RMQ_FIELD_SHORT_UINT, &channel_max,
                        RMQ_FIELD_LONG_UINT, &frame_max,
                        RMQ_FIELD_SHORT_UINT, &heartbeat,
                        RMQ_FIELD_END) == -1) {
        /* TODO error 505 */
        c_set_error("invalid arguments: %s", c_get_error());
        return -1;
    }

    /* Response */
    channel_max = 1; /* We do not support multiplexing for the moment */

    rmq_client_send_method(client, RMQ_METHOD_CONNECTION_TUNE_OK,
                           RMQ_FIELD_SHORT_UINT, channel_max,
                           RMQ_FIELD_LONG_UINT, frame_max,
                           RMQ_FIELD_SHORT_UINT, heartbeat,
                           RMQ_FIELD_END);

    client->state = RMQ_CLIENT_STATE_TUNE_RECEIVED;

    /* Select a vhost */
    rmq_client_send_method(client, RMQ_METHOD_CONNECTION_OPEN,
                           RMQ_FIELD_SHORT_STRING, client->vhost,
                           RMQ_FIELD_SHORT_STRING, "",    /* deprecated */
                           RMQ_FIELD_SHORT_SHORT_UINT, 0, /* deprecated */
                           RMQ_FIELD_END);
    return 0;
}

RMQ_METHOD_HANDLER(connection_open_ok) {
    client->state = RMQ_CLIENT_STATE_CONNECTION_OPEN;

    /* Open a channel */
    client->channel = 1;

    rmq_client_send_method(client, RMQ_METHOD_CHANNEL_OPEN,
                           RMQ_FIELD_SHORT_STRING, "", /* deprecated */
                           RMQ_FIELD_END);
    return 0;
}

RMQ_METHOD_HANDLER(connection_close) {
    rmq_client_send_method(client, RMQ_METHOD_CONNECTION_CLOSE_OK,
                           RMQ_FIELD_END);

    io_tcp_client_disconnect(client->tcp_client);
    return 0;
}

RMQ_METHOD_HANDLER(connection_close_ok) {

    if (client->state != RMQ_CLIENT_STATE_CLOSING) {
        c_set_error("unexpected method");
        return -1;
    }

    io_tcp_client_disconnect(client->tcp_client);
    return 0;
}

RMQ_METHOD_HANDLER(channel_open_ok) {
    if (client->state != RMQ_CLIENT_STATE_CONNECTION_OPEN) {
        c_set_error("unexpected method");
        return -1;
    }

    client->state = RMQ_CLIENT_STATE_READY;
    rmq_client_signal_event(client, RMQ_CLIENT_EVENT_READY, NULL);

    return 0;
}

RMQ_METHOD_HANDLER(basic_consume_ok) {
    return 0;
}

#undef RMQ_METHOD_HANDLER

/* ---------------------------------------------------------------------------
 *  Generic method handler
 * ------------------------------------------------------------------------ */
static int
rmq_client_on_method(struct rmq_client *client,
                     const struct rmq_method_frame *frame) {
    const char *method_string;
    enum rmq_method method;

    method = RMQ_METHOD(frame->class_id, frame->method_id);
    method_string = rmq_method_to_string(method);

    rmq_client_trace(client, "method %u.%u %s",
                     frame->class_id, frame->method_id,
                     method_string ? method_string : "unknown");

    if (client->state == RMQ_CLIENT_STATE_CLOSING
     && method != RMQ_METHOD_CONNECTION_CLOSE
     && method != RMQ_METHOD_CONNECTION_CLOSE_OK) {
        rmq_client_trace(client, "ignoring method %u.%u %s since "
                         "connection is being closed",
                         frame->class_id, frame->method_id,
                         method_string ? method_string : "unknown");
        return 0;
    }

    switch (method) {
#define RMQ_HANDLER(method_, function_)                               \
    case RMQ_METHOD_##method_:                                        \
        if (rmq_client_on_method_##function_(client,                  \
                                             frame->args,             \
                                             frame->args_sz) == -1) { \
            goto error;                                               \
        }                                                             \
        break;

    RMQ_HANDLER(CONNECTION_START, connection_start);
    RMQ_HANDLER(CONNECTION_TUNE, connection_tune);
    RMQ_HANDLER(CONNECTION_OPEN_OK, connection_open_ok);
    RMQ_HANDLER(CONNECTION_CLOSE, connection_close);
    RMQ_HANDLER(CONNECTION_CLOSE_OK, connection_close_ok);

    RMQ_HANDLER(CHANNEL_OPEN_OK, channel_open_ok);

    RMQ_HANDLER(BASIC_CONSUME_OK, basic_consume_ok);

#undef RMQ_HANDLER

    default:
        c_set_error("unhandled method");
        goto error;
    }

    return 0;

error:
    if (method_string) {
        c_set_error("%s: %s", method_string, c_get_error());
    } else {
        c_set_error("%u.%u: %s", frame->class_id, frame->method_id,
                    c_get_error());
    }

    return -1;
}
