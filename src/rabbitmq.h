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

#ifndef LIBRABBITMQ_RABBITMQ_H
#define LIBRABBITMQ_RABBITMQ_H

#include <core.h>
#include <io.h>

/* ---------------------------------------------------------------------------
 *  Message
 * ------------------------------------------------------------------------ */
enum rmq_property {
    RMQ_PROPERTY_CONTENT_TYPE      = 0x8000,
    RMQ_PROPERTY_CONTENT_ENCODING  = 0x4000,
    RMQ_PROPERTY_HEADERS           = 0x2000,
    RMQ_PROPERTY_DELIVERY_MODE     = 0x1000,
    RMQ_PROPERTY_PRIORITY          = 0x0800,
    RMQ_PROPERTY_CORRELATION_ID    = 0x0400,
    RMQ_PROPERTY_REPLY_TO          = 0x0200,
    RMQ_PROPERTY_EXPIRATION        = 0x0100,
    RMQ_PROPERTY_MESSAGE_ID        = 0x0080,
    RMQ_PROPERTY_TIMESTAMP         = 0x0040,
    RMQ_PROPERTY_TYPE              = 0x0020,
    RMQ_PROPERTY_USER_ID           = 0x0010,
    RMQ_PROPERTY_APP_ID            = 0x0008,
};

enum rmq_delivery_mode {
    RMQ_MSG_DELIVERY_NON_PERSISTENT = 1,
    RMQ_MSG_DELIVERY_PERSISTENT     = 2,
};

struct rmq_msg *rmq_msg_new(void);
void rmq_msg_delete(struct rmq_msg *);

void rmq_msg_set_content_type(struct rmq_msg *, const char *);
void rmq_msg_set_content_encoding(struct rmq_msg *, const char *);
// TODO
//void rmq_msg_append_header_nocopy(struct rmq_msg *,
//                                  const char *, struct rmq_field *);
void rmq_msg_set_delivery_mode(struct rmq_msg *, enum rmq_delivery_mode);
void rmq_msg_set_priority(struct rmq_msg *, uint8_t);
void rmq_msg_set_correlation_id(struct rmq_msg *, const char *);
void rmq_msg_set_reply_to(struct rmq_msg *, const char *);
void rmq_msg_set_expiration(struct rmq_msg *, const char *);
void rmq_msg_set_message_id(struct rmq_msg *, const char *);
void rmq_msg_set_timestamp(struct rmq_msg *, uint64_t);
void rmq_msg_set_type(struct rmq_msg *, const char *);
void rmq_msg_set_user_id(struct rmq_msg *, const char *);
void rmq_msg_set_app_id(struct rmq_msg *, const char *);

void rmq_msg_set_data_nocopy(struct rmq_msg *, void *, size_t);
void rmq_msg_set_data(struct rmq_msg *, const void *, size_t);

/* ---------------------------------------------------------------------------
 *  Client
 * ------------------------------------------------------------------------ */
struct rmq_client;

enum rmq_client_event {
    RMQ_CLIENT_EVENT_CONN_ESTABLISHED,
    RMQ_CLIENT_EVENT_CONN_FAILED,
    RMQ_CLIENT_EVENT_CONN_CLOSED,
    RMQ_CLIENT_EVENT_READY,

    RMQ_CLIENT_EVENT_ERROR,
    RMQ_CLIENT_EVENT_TRACE,
};

typedef void (*rmq_client_event_cb)(struct rmq_client *, enum rmq_client_event,
                                    void *, void *);

struct rmq_client *rmq_client_new(struct io_base *);
void rmq_client_delete(struct rmq_client *);

void rmq_client_set_event_cb(struct rmq_client *, rmq_client_event_cb, void *);
void rmq_client_set_credentials(struct rmq_client *,
                                const char *, const char *);
void rmq_client_set_vhost(struct rmq_client *, const char *);

int rmq_client_connect(struct rmq_client *, const char *, uint16_t);
void rmq_client_disconnect(struct rmq_client *);

enum rmq_publish_option {
    RMQ_PUBLISH_DEFAULT   = 0x00,
    RMQ_PUBLISH_MANDATORY = 0x01,
    RMQ_PUBLISH_IMMEDIATE = 0x02,
};

void rmq_client_publish(struct rmq_client *, struct rmq_msg *, const char *,
                        const char *, uint32_t);

#endif
