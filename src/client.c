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
static int rmq_client_on_method_connection_start(struct rmq_client *,
                                                 const void *, size_t);

struct rmq_client *
rmq_client_new(struct io_base *io_base) {
    struct rmq_client *client;

    client = c_malloc0(sizeof(struct rmq_client));

    client->io_base = io_base;

    client->tcp_client = io_tcp_client_new(io_base,
                                           rmq_client_on_tcp_event, client);

    return client;
}

void
rmq_client_delete(struct rmq_client *client) {
    if (!client)
        return;

    io_tcp_client_delete(client->tcp_client);

    c_free0(client, sizeof(struct rmq_client));
}

void
rmq_client_set_event_cb(struct rmq_client *client,
                        rmq_client_event_cb cb, void *arg) {
    client->event_cb = cb;
    client->event_cb_arg = arg;
}

int
rmq_client_connect(struct rmq_client *client,
                   const char *host, uint16_t port) {
    return io_tcp_client_connect(client->tcp_client, host, port);
}

void
rmq_client_disconnect(struct rmq_client *client) {
    return io_tcp_client_disconnect(client->tcp_client);
}

void
rmq_client_send_frame(struct rmq_client *client, enum rmq_frame_type type,
                      uint16_t channel, const void *data, size_t size) {
    struct rmq_frame frame;
    struct c_buffer *wbuf;

    assert(size <= UINT32_MAX);

    rmq_frame_init(&frame);

    frame.type = RMQ_FRAME_TYPE_METHOD;
    frame.channel = channel;
    frame.size = size;
    frame.payload = data;
    frame.end = RMQ_FRAME_END;

    wbuf = io_tcp_client_wbuf(client->tcp_client);
    rmq_frame_write(&frame, wbuf);
    io_tcp_client_signal_data_written(client->tcp_client);
}

void
rmq_client_send_method(struct rmq_client *client, enum rmq_method method) {
    struct rmq_method_frame method_frame;
    uint16_t channel;
    const uint8_t *data;
    size_t size;

    rmq_method_frame_init(&method_frame);

    method_frame.class_id = method >> 16;
    method_frame.method_id = method & 0x0000ffff;

    /* TODO arguments */

    channel = 0; /* TODO */
    data = NULL; /* TODO */
    size = 0; /* TODO */

    rmq_client_send_frame(client, RMQ_FRAME_TYPE_METHOD, channel, data, size);
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
    rmq_client_disconnect(client);
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
    client->state = RMQ_CLIENT_STATE_DISCONNECTED;
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

static int
rmq_client_on_method_connection_start(struct rmq_client *client,
                                      const void *data, size_t size) {
    uint8_t version_major, version_minor;
    struct rmq_field_table *server_properties;
    char *mechanisms, *locales;

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

    c_free(mechanisms);
    c_free(locales);
    rmq_field_table_delete(server_properties);

    /* TODO send Connection.Start-Ok method */

    client->state = RMQ_CLIENT_STATE_START_RECEIVED;
    return 0;
}
