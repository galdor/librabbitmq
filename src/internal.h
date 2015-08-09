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

#ifndef LIBRABBITMQ_INTERNAL_H
#define LIBRABBITMQ_INTERNAL_H

#include <assert.h>
#include <ctype.h>
#include <inttypes.h>
#include <string.h>

#include "rabbitmq.h"

/* ---------------------------------------------------------------------------
 *  Protocol
 * ------------------------------------------------------------------------ */
/* Decimal */
struct rmq_decimal {
    uint8_t scale;
    uint32_t value;
};

/* Long string */
struct rmq_long_string {
    char *ptr;
    size_t len;
};

void rmq_long_string_init(struct rmq_long_string *);
void rmq_long_string_free(struct rmq_long_string *);

/* Field values */
#define RMQ_FIELD_END 0xffff

struct rmq_field {
    enum rmq_field_type type;

    union {
        bool boolean;
        int8_t short_short_int;
        uint8_t short_short_uint;
        int16_t short_int;
        uint16_t short_uint;
        int32_t long_int;
        uint32_t long_uint;
        int64_t long_long_int;
        uint64_t long_long_uint;
        float float_value;
        double double_value;
        struct rmq_decimal decimal;
        char *short_string;
        struct rmq_long_string long_string;
        struct c_ptr_vector *array;
        uint64_t timestamp;
        struct rmq_field_table *table;
    } u;
};

int rmq_field_read_boolean(const void *, size_t, bool *, size_t *);
int rmq_field_read_short_short_int(const void *, size_t, int8_t *, size_t *);
int rmq_field_read_short_short_uint(const void *, size_t, uint8_t *, size_t *);
int rmq_field_read_short_int(const void *, size_t, int16_t *, size_t *);
int rmq_field_read_short_uint(const void *, size_t, uint16_t *, size_t *);
int rmq_field_read_long_int(const void *, size_t, int32_t *, size_t *);
int rmq_field_read_long_uint(const void *, size_t, uint32_t *, size_t *);
int rmq_field_read_long_long_int(const void *, size_t, int64_t *, size_t *);
int rmq_field_read_long_long_uint(const void *, size_t, uint64_t *, size_t *);
int rmq_field_read_float(const void *, size_t, float *, size_t *);
int rmq_field_read_double(const void *, size_t, double *, size_t *);
int rmq_field_read_decimal(const void *, size_t,
                           struct rmq_decimal *, size_t *);
int rmq_field_read_short_string(const void *, size_t, char **, size_t *);
int rmq_field_read_long_string(const void *, size_t,
                               struct rmq_long_string *, size_t *);
int rmq_field_read_array(const void *, size_t,
                         struct c_ptr_vector **, size_t *);
int rmq_field_read_timestamp(const void *, size_t, uint64_t *, size_t *);
int rmq_field_read_table(const void *, size_t,
                         struct rmq_field_table **, size_t *);
int rmq_field_read_no_value(const void *, size_t, size_t *);

void rmq_field_write_boolean(bool, struct c_buffer *);
void rmq_field_write_short_short_int(int8_t, struct c_buffer *);
void rmq_field_write_short_short_uint(uint8_t, struct c_buffer *);
void rmq_field_write_short_int(int16_t, struct c_buffer *);
void rmq_field_write_short_uint(uint16_t, struct c_buffer *);
void rmq_field_write_long_int(int32_t, struct c_buffer *);
void rmq_field_write_long_uint(uint32_t, struct c_buffer *);
void rmq_field_write_long_long_int(int64_t, struct c_buffer *);
void rmq_field_write_long_long_uint(uint64_t, struct c_buffer *);
void rmq_field_write_float(float, struct c_buffer *);
void rmq_field_write_double(double, struct c_buffer *);
void rmq_field_write_decimal(const struct rmq_decimal *, struct c_buffer *);
void rmq_field_write_short_string(const char *, struct c_buffer *);
void rmq_field_write_long_string(const struct rmq_long_string *,
                                 struct c_buffer *);
void rmq_field_write_array(const struct c_ptr_vector *, struct c_buffer *);
void rmq_field_write_timestamp(uint64_t, struct c_buffer *);
void rmq_field_write_table(const struct rmq_field_table *, struct c_buffer *);
void rmq_field_write_no_value(struct c_buffer *);

struct rmq_field *rmq_field_read(const void *, size_t,
                                 enum rmq_field_type, size_t *);
void rmq_field_write(const struct rmq_field *, struct c_buffer *);

struct rmq_field *rmq_field_read_tagged(const void *, size_t, size_t *);
void rmq_field_write_tagged(const struct rmq_field *, struct c_buffer *);

int rmq_fields_read(const void *, size_t, size_t *, ...);
void rmq_fields_vwrite(struct c_buffer *, va_list);
void rmq_fields_write(struct c_buffer *, ...);

/* Field table */
struct rmq_field_pair {
    char *name;
    struct rmq_field *value;
};

void rmq_field_pair_init(struct rmq_field_pair *);
void rmq_field_pair_free(struct rmq_field_pair *);

struct rmq_field_table {
    struct c_vector *pairs;
};

/* Message properties */
struct rmq_properties {
    uint16_t mask; /* enum rmq_msg_property */

    char *content_type;
    char *content_encoding;
    struct rmq_field_table *headers;
    enum rmq_delivery_mode delivery_mode;
    uint8_t priority;
    char *correlation_id;
    char *reply_to;
    char *expiration;
    char *message_id;
    uint64_t timestamp;
    char *type;
    char *user_id;
    char *app_id;
};

void rmq_properties_init(struct rmq_properties *);
void rmq_properties_free(struct rmq_properties *);

void rmq_properties_set_content_type(struct rmq_properties *, const char *);
void rmq_properties_set_content_encoding(struct rmq_properties *, const char *);
void rmq_properties_add_header_nocopy(struct rmq_properties *,
                                      const char *, struct rmq_field *);
void rmq_properties_set_delivery_mode(struct rmq_properties *,
                                      enum rmq_delivery_mode);
void rmq_properties_set_priority(struct rmq_properties *, uint8_t);
void rmq_properties_set_correlation_id(struct rmq_properties *, const char *);
void rmq_properties_set_reply_to(struct rmq_properties *, const char *);
void rmq_properties_set_expiration(struct rmq_properties *, const char *);
void rmq_properties_set_message_id(struct rmq_properties *, const char *);
void rmq_properties_set_timestamp(struct rmq_properties *, uint64_t);
void rmq_properties_set_type(struct rmq_properties *, const char *);
void rmq_properties_set_user_id(struct rmq_properties *, const char *);
void rmq_properties_set_app_id(struct rmq_properties *, const char *);

/* Frame */
#define RMQ_FRAME_END ((uint8_t)0xce)

enum rmq_frame_type {
    RMQ_FRAME_TYPE_METHOD     = 1,
    RMQ_FRAME_TYPE_HEADER     = 2,
    RMQ_FRAME_TYPE_BODY       = 3,
    RMQ_FRAME_TYPE_HEARTBEAT  = 8,
};

struct rmq_frame {
    uint8_t type; /* enum rmq_frame_type */
    uint16_t channel;
    uint32_t size;

    const void *payload;

    uint8_t end;
};

void rmq_frame_init(struct rmq_frame *);

int rmq_frame_read(struct rmq_frame *, const void *, size_t, size_t *);
void rmq_frame_write(const struct rmq_frame *, struct c_buffer *);

/* Method */
#define RMQ_METHOD(class_, id_) (unsigned int)(((class_) << 16) | (id_))

enum rmq_class {
    RMQ_CLASS_CONNECTION = 10,
    RMQ_CLASS_CHANNEL    = 20,
    RMQ_CLASS_EXCHANGE   = 40,
    RMQ_CLASS_QUEUE      = 50,
    RMQ_CLASS_BASIC      = 60,
    RMQ_CLASS_TX         = 90,
};

enum rmq_method {
    RMQ_METHOD_CONNECTION_START     = RMQ_METHOD(RMQ_CLASS_CONNECTION,  10),
    RMQ_METHOD_CONNECTION_START_OK  = RMQ_METHOD(RMQ_CLASS_CONNECTION,  11),
    RMQ_METHOD_CONNECTION_SECURE    = RMQ_METHOD(RMQ_CLASS_CONNECTION,  20),
    RMQ_METHOD_CONNECTION_SECURE_OK = RMQ_METHOD(RMQ_CLASS_CONNECTION,  21),
    RMQ_METHOD_CONNECTION_TUNE      = RMQ_METHOD(RMQ_CLASS_CONNECTION,  30),
    RMQ_METHOD_CONNECTION_TUNE_OK   = RMQ_METHOD(RMQ_CLASS_CONNECTION,  31),
    RMQ_METHOD_CONNECTION_OPEN      = RMQ_METHOD(RMQ_CLASS_CONNECTION,  40),
    RMQ_METHOD_CONNECTION_OPEN_OK   = RMQ_METHOD(RMQ_CLASS_CONNECTION,  41),
    RMQ_METHOD_CONNECTION_CLOSE     = RMQ_METHOD(RMQ_CLASS_CONNECTION,  50),
    RMQ_METHOD_CONNECTION_CLOSE_OK  = RMQ_METHOD(RMQ_CLASS_CONNECTION,  51),

    RMQ_METHOD_CHANNEL_OPEN         = RMQ_METHOD(RMQ_CLASS_CHANNEL,  10),
    RMQ_METHOD_CHANNEL_OPEN_OK      = RMQ_METHOD(RMQ_CLASS_CHANNEL,  11),
    RMQ_METHOD_CHANNEL_FLOW         = RMQ_METHOD(RMQ_CLASS_CHANNEL,  20),
    RMQ_METHOD_CHANNEL_FLOW_OK      = RMQ_METHOD(RMQ_CLASS_CHANNEL,  21),
    RMQ_METHOD_CHANNEL_CLOSE        = RMQ_METHOD(RMQ_CLASS_CHANNEL,  40),
    RMQ_METHOD_CHANNEL_CLOSE_OK     = RMQ_METHOD(RMQ_CLASS_CHANNEL,  41),

    RMQ_METHOD_QUEUE_DECLARE        = RMQ_METHOD(RMQ_CLASS_QUEUE,  10),
    RMQ_METHOD_QUEUE_DECLARE_OK     = RMQ_METHOD(RMQ_CLASS_QUEUE,  11),
    RMQ_METHOD_QUEUE_BIND           = RMQ_METHOD(RMQ_CLASS_QUEUE,  20),
    RMQ_METHOD_QUEUE_BIND_OK        = RMQ_METHOD(RMQ_CLASS_QUEUE,  21),
    RMQ_METHOD_QUEUE_PURGE          = RMQ_METHOD(RMQ_CLASS_QUEUE,  30),
    RMQ_METHOD_QUEUE_PURGE_OK       = RMQ_METHOD(RMQ_CLASS_QUEUE,  31),
    RMQ_METHOD_QUEUE_DELETE         = RMQ_METHOD(RMQ_CLASS_QUEUE,  40),
    RMQ_METHOD_QUEUE_DELETE_OK      = RMQ_METHOD(RMQ_CLASS_QUEUE,  41),
    RMQ_METHOD_QUEUE_UNBIND         = RMQ_METHOD(RMQ_CLASS_QUEUE,  50),
    RMQ_METHOD_QUEUE_UNBIND_OK      = RMQ_METHOD(RMQ_CLASS_QUEUE,  51),

    RMQ_METHOD_BASIC_QOS            = RMQ_METHOD(RMQ_CLASS_BASIC,  10),
    RMQ_METHOD_BASIC_QOS_OK         = RMQ_METHOD(RMQ_CLASS_BASIC,  11),
    RMQ_METHOD_BASIC_CONSUME        = RMQ_METHOD(RMQ_CLASS_BASIC,  20),
    RMQ_METHOD_BASIC_CONSUME_OK     = RMQ_METHOD(RMQ_CLASS_BASIC,  21),
    RMQ_METHOD_BASIC_CANCEL         = RMQ_METHOD(RMQ_CLASS_BASIC,  30),
    RMQ_METHOD_BASIC_CANCEL_OK      = RMQ_METHOD(RMQ_CLASS_BASIC,  31),
    RMQ_METHOD_BASIC_PUBLISH        = RMQ_METHOD(RMQ_CLASS_BASIC,  40),
    RMQ_METHOD_BASIC_RETURN         = RMQ_METHOD(RMQ_CLASS_BASIC,  50),
    RMQ_METHOD_BASIC_DELIVER        = RMQ_METHOD(RMQ_CLASS_BASIC,  60),
    RMQ_METHOD_BASIC_GET            = RMQ_METHOD(RMQ_CLASS_BASIC,  70),
    RMQ_METHOD_BASIC_GET_OK         = RMQ_METHOD(RMQ_CLASS_BASIC,  71),
    RMQ_METHOD_BASIC_GET_EMPTY      = RMQ_METHOD(RMQ_CLASS_BASIC,  72),
    RMQ_METHOD_BASIC_ACK            = RMQ_METHOD(RMQ_CLASS_BASIC,  80),
    RMQ_METHOD_BASIC_REJECT         = RMQ_METHOD(RMQ_CLASS_BASIC,  90),
    RMQ_METHOD_BASIC_RECOVER_ASYNC  = RMQ_METHOD(RMQ_CLASS_BASIC, 100),
    RMQ_METHOD_BASIC_RECOVER        = RMQ_METHOD(RMQ_CLASS_BASIC, 110),
    RMQ_METHOD_BASIC_RECOVER_OK     = RMQ_METHOD(RMQ_CLASS_BASIC, 111),
};

const char *rmq_method_to_string(enum rmq_method);

/* Method frame */
struct rmq_method_frame {
    uint16_t class_id;
    uint16_t method_id;

    const void *args;
    size_t args_sz;
};

void rmq_method_frame_init(struct rmq_method_frame *);

int rmq_method_frame_read(struct rmq_method_frame *, const struct rmq_frame *);
void rmq_method_frame_write(const struct rmq_method_frame *, struct c_buffer *);

/* Header frame */
struct rmq_header_frame {
    uint16_t class_id;
    uint64_t body_size;

    const struct rmq_properties *properties;
};

void rmq_header_frame_init(struct rmq_header_frame *);

int rmq_header_frame_read(struct rmq_header_frame *, struct rmq_properties *,
                          const struct rmq_frame *);
void rmq_header_frame_write(const struct rmq_header_frame *, struct c_buffer *);

/* Misc */
enum rmq_unsubscribe_option {
    RMQ_UNSUBSCRIBE_DEFAULT   = 0x00,
    RMQ_UNSUBSCRIBE_NO_WAIT   = 0x01,
};

/* ---------------------------------------------------------------------------
 *  Message
 * ------------------------------------------------------------------------ */
struct rmq_msg {
    struct rmq_properties properties;

    void *data;
    size_t data_sz;
    bool data_owned;
};

/* ---------------------------------------------------------------------------
 *  Delivery
 * ------------------------------------------------------------------------ */
enum rmq_delivery_type {
    RMQ_DELIVERY_TYPE_BASIC_DELIVER,
    RMQ_DELIVERY_TYPE_BASIC_RETURN,
};

enum rmq_delivery_state {
    RMQ_DELIVERY_STATE_METHOD_RECEIVED,
    RMQ_DELIVERY_STATE_HEADER_RECEIVED,
};

struct rmq_delivery {
    enum rmq_delivery_type type;
    enum rmq_delivery_state state;

    union {
        struct {
            uint64_t tag;
            struct rmq_consumer *consumer;
            bool redelivered;
        } basic_deliver;
        struct {
            enum rmq_reply_code reply_code;
            char *reply_text;
        } basic_return;
    } u;

    char *exchange;
    char *routing_key;

    struct rmq_msg *msg;
    size_t data_size;
};

void rmq_delivery_init(struct rmq_delivery *);
void rmq_delivery_free(struct rmq_delivery *);

/* ---------------------------------------------------------------------------
 *  Consumer
 * ------------------------------------------------------------------------ */
struct rmq_consumer {
    char *queue;
    char *tag;

    rmq_msg_cb msg_cb;
    void *msg_cb_arg;

    /* Current delivery */
    bool has_delivery;
    struct rmq_delivery delivery;
};

struct rmq_consumer *rmq_consumer_new(const char *, char *);
void rmq_consumer_delete(struct rmq_consumer *);

/* ---------------------------------------------------------------------------
 *  Client
 * ------------------------------------------------------------------------ */
enum rmq_client_state {
    RMQ_CLIENT_STATE_DISCONNECTED,
    RMQ_CLIENT_STATE_CONNECTED,
    RMQ_CLIENT_STATE_START_RECEIVED,
    RMQ_CLIENT_STATE_TUNE_RECEIVED,
    RMQ_CLIENT_STATE_CONNECTION_OPEN,
    RMQ_CLIENT_STATE_READY,
    RMQ_CLIENT_STATE_CLOSING,
};

struct rmq_client {
    struct io_base *io_base;
    struct io_tcp_client *tcp_client;

    enum rmq_client_state state;

    rmq_client_event_cb event_cb;
    void *event_cb_arg;

    rmq_undeliverable_msg_cb undeliverable_msg_cb;
    void *undeliverable_msg_cb_arg;

    char *login;
    char *password;
    char *vhost;

    uint16_t channel;

    struct c_hash_table *consumers_by_tag;
    struct c_hash_table *consumers_by_queue;
    int consumer_tag_id;

    bool has_current_delivery;
    struct rmq_delivery current_delivery;
};

void rmq_client_send_frame(struct rmq_client *, enum rmq_frame_type,
                           uint16_t, const void *, size_t);
void rmq_client_send_method(struct rmq_client *, enum rmq_method, ...);
void rmq_client_send_header(struct rmq_client *, uint16_t, uint64_t,
                            const struct rmq_properties *);
void rmq_client_send_body(struct rmq_client *, const void *, size_t);

void rmq_client_connection_close(struct rmq_client *,
                                 enum rmq_reply_code, const char *, ...)
    __attribute__ ((format(printf, 3, 4)));

#endif
