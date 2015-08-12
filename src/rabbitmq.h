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
 *  Table
 * ------------------------------------------------------------------------ */
struct rmq_field_table *rmq_field_table_new(void);
void rmq_field_table_delete(struct rmq_field_table *);

struct rmq_field *rmq_field_table_get(const struct rmq_field_table *,
                                      const char* );
void rmq_field_table_add_nocopy(struct rmq_field_table *,
                                char *, struct rmq_field *);

/* ---------------------------------------------------------------------------
 *  Field
 * ------------------------------------------------------------------------ */
enum rmq_field_type {
    RMQ_FIELD_BOOLEAN,
    RMQ_FIELD_SHORT_SHORT_INT,
    RMQ_FIELD_SHORT_SHORT_UINT,
    RMQ_FIELD_SHORT_INT,
    RMQ_FIELD_SHORT_UINT,
    RMQ_FIELD_LONG_INT,
    RMQ_FIELD_LONG_UINT,
    RMQ_FIELD_LONG_LONG_INT,
    RMQ_FIELD_LONG_LONG_UINT,
    RMQ_FIELD_FLOAT,
    RMQ_FIELD_DOUBLE,
    RMQ_FIELD_DECIMAL,
    RMQ_FIELD_SHORT_STRING,
    RMQ_FIELD_LONG_STRING,
    RMQ_FIELD_ARRAY,
    RMQ_FIELD_TIMESTAMP,
    RMQ_FIELD_TABLE,
    RMQ_FIELD_NO_VALUE,
};

const char *rmq_field_type_to_string(enum rmq_field_type);

struct rmq_field *rmq_field_new(enum rmq_field_type);
void rmq_field_delete(struct rmq_field *);

struct rmq_field *rmq_field_new_boolean(bool);
struct rmq_field *rmq_field_new_short_short_int(int8_t);
struct rmq_field *rmq_field_new_short_short_uint(uint8_t);
struct rmq_field *rmq_field_new_short_int(int16_t);
struct rmq_field *rmq_field_new_short_uint(uint16_t);
struct rmq_field *rmq_field_new_long_int(int32_t);
struct rmq_field *rmq_field_new_long_uint(uint32_t);
struct rmq_field *rmq_field_new_long_long_int(int64_t);
struct rmq_field *rmq_field_new_long_long_uint(uint64_t);
struct rmq_field *rmq_field_new_float(float);
struct rmq_field *rmq_field_new_double(double);
struct rmq_field *rmq_field_new_short_string(const char *);
struct rmq_field *rmq_field_new_long_string(const void *, size_t);
struct rmq_field *rmq_field_new_array(void);
struct rmq_field *rmq_field_new_timestamp(uint64_t);
struct rmq_field *rmq_field_new_table(void);
struct rmq_field *rmq_field_new_no_value(void);

/* ---------------------------------------------------------------------------
 *  Protocol
 * ------------------------------------------------------------------------ */
/* Reply codes */
enum rmq_reply_code {
    RMQ_REPLY_CODE_SUCCESS             = 200,
    RMQ_REPLY_CODE_CONTENT_TOO_LARGE   = 311,
    RMQ_REPLY_CODE_NO_CONSUMERS        = 313,
    RMQ_REPLY_CODE_CONNECTION_FORCED   = 320,
    RMQ_REPLY_CODE_INVALID_PATH        = 402,
    RMQ_REPLY_CODE_ACCESS_REFUSED      = 403,
    RMQ_REPLY_CODE_NOT_FOUND           = 404,
    RMQ_REPLY_CODE_RESOURCE_LOCKED     = 405,
    RMQ_REPLY_CODE_PRECONDITION_FAILED = 406,
    RMQ_REPLY_CODE_FRAME_ERROR         = 501,
    RMQ_REPLY_CODE_SYNTAX_ERROR        = 502,
    RMQ_REPLY_CODE_COMMAND_INVALID     = 503,
    RMQ_REPLY_CODE_CHANNEL_ERROR       = 504,
    RMQ_REPLY_CODE_UNEXPECTED_FRAME    = 505,
    RMQ_REPLY_CODE_RESOURCE_ERROR      = 506,
    RMQ_REPLY_CODE_NOT_ALLOWED         = 530,
    RMQ_REPLY_CODE_NOT_IMPLEMENTED     = 540,
    RMQ_REPLY_CODE_INTERNAL_ERROR      = 541,
};

/* ---------------------------------------------------------------------------
 *  Delivery
 * ------------------------------------------------------------------------ */
struct rmq_delivery;

uint64_t rmq_delivery_tag(const struct rmq_delivery *);

const char *rmq_delivery_exchange(const struct rmq_delivery *);
const char *rmq_delivery_routing_key(const struct rmq_delivery *);

bool rmq_delivery_is_redelivered(const struct rmq_delivery *);

enum rmq_reply_code
rmq_delivery_undeliverable_reply_code(const struct rmq_delivery *);
const char *rmq_delivery_undeliverable_reply_text(const struct rmq_delivery *);

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

const char *rmq_msg_content_type(const struct rmq_msg *);
const char *rmq_msg_content_encoding(const struct rmq_msg *);
struct rmq_field *rmq_msg_content_header(const struct rmq_msg *, const char *);
enum rmq_delivery_mode rmq_msg_delivery_mode(const struct rmq_msg *);
uint8_t rmq_msg_priority(const struct rmq_msg *);
const char *rmq_msg_correlation_id(const struct rmq_msg *);
const char *rmq_msg_reply_to(const struct rmq_msg *);
const char *rmq_msg_expiration(const struct rmq_msg *);
const char *rmq_msg_message_id(const struct rmq_msg *);
uint64_t rmq_msg_timestamp(const struct rmq_msg *);
const char *rmq_msg_type(const struct rmq_msg *);
const char *rmq_msg_user_id(const struct rmq_msg *);
const char *rmq_msg_app_id(const struct rmq_msg *);

void rmq_msg_set_content_type(struct rmq_msg *, const char *);
void rmq_msg_set_content_encoding(struct rmq_msg *, const char *);
void rmq_msg_add_header_nocopy(struct rmq_msg *,
                               const char *, struct rmq_field *);
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

const void *rmq_msg_data(const struct rmq_msg *, size_t *);
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

enum rmq_msg_action {
    RMQ_MSG_ACTION_NONE,
    RMQ_MSG_ACTION_ACK,
    RMQ_MSG_ACTION_REJECT,
    RMQ_MSG_ACTION_REQUEUE,
};

typedef enum rmq_msg_action (*rmq_msg_cb)(struct rmq_client *,
                                          const struct rmq_delivery *,
                                          const struct rmq_msg *, void *);
typedef void (*rmq_undeliverable_msg_cb)(struct rmq_client *,
                                         const struct rmq_delivery *,
                                         const struct rmq_msg *, void *);

struct rmq_client *rmq_client_new(struct io_base *);
void rmq_client_delete(struct rmq_client *);

void rmq_client_set_event_cb(struct rmq_client *, rmq_client_event_cb, void *);
void rmq_client_set_undeliverable_msg_cb(struct rmq_client *,
                                         rmq_undeliverable_msg_cb, void *);
void rmq_client_set_credentials(struct rmq_client *,
                                const char *, const char *);
void rmq_client_set_vhost(struct rmq_client *, const char *);

int rmq_client_connect(struct rmq_client *, const char *, uint16_t);
void rmq_client_disconnect(struct rmq_client *);
int rmq_client_reconnect(struct rmq_client *);

bool rmq_client_is_ready(const struct rmq_client *);

/* Base */
enum rmq_publish_option {
    RMQ_PUBLISH_DEFAULT   = 0x00,
    RMQ_PUBLISH_MANDATORY = 0x01,
    RMQ_PUBLISH_IMMEDIATE = 0x02,
};

void rmq_client_publish(struct rmq_client *, struct rmq_msg *, const char *,
                        const char *, uint32_t);

enum rmq_subscribe_option {
    RMQ_SUBSCRIBE_DEFAULT   = 0x00,
    RMQ_SUBSCRIBE_NO_LOCAL  = 0x01,
    RMQ_SUBSCRIBE_NO_ACK    = 0x02,
    RMQ_SUBSCRIBE_EXCLUSIVE = 0x04,
};

void rmq_client_subscribe(struct rmq_client *, const char *, uint8_t,
                          rmq_msg_cb, void *);

void rmq_client_unsubscribe(struct rmq_client *, const char *);

/* Message handling */
void rmq_client_ack(struct rmq_client *, uint64_t);
void rmq_client_reject(struct rmq_client *, uint64_t);
void rmq_client_requeue(struct rmq_client *, uint64_t);

/* Queues */
enum rmq_queue_option {
    RMQ_QUEUE_DEFAULT     = 0x00,
    RMQ_QUEUE_DURABLE     = 0x02,
    RMQ_QUEUE_EXCLUSIVE   = 0x04,
    RMQ_QUEUE_AUTO_DELETE = 0x08,
};

void rmq_client_declare_queue(struct rmq_client *, const char *, uint8_t,
                              const struct rmq_field_table *);

enum rmq_queue_delete_option {
    RMQ_QUEUE_DELETE_DEFAULT   = 0x00,
    RMQ_QUEUE_DELETE_IF_UNUSED = 0x01,
    RMQ_QUEUE_DELETE_IF_EMPTY  = 0x02,
};

void rmq_client_delete_queue(struct rmq_client *, const char *, uint8_t);

#endif
