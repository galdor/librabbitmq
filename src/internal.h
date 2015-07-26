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

/* Field values */
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

    RMQ_FIELD_END,
};

const char *rmq_field_type_to_string(enum rmq_field_type);

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
        char *long_string;
        struct c_ptr_vector *array;
        uint64_t timestamp;
        struct rmq_field_table *table;
    } u;
};

struct rmq_field *rmq_field_new(enum rmq_field_type);
void rmq_field_delete(struct rmq_field *);

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
int rmq_field_read_long_string(const void *, size_t, char **, size_t *);
int rmq_field_read_array(const void *, size_t,
                         struct c_ptr_vector **, size_t *);
int rmq_field_read_timestamp(const void *, size_t, uint64_t *, size_t *);
int rmq_field_read_table(const void *, size_t,
                         struct rmq_field_table **, size_t *);
int rmq_field_read_no_value(const void *, size_t, size_t *);

struct rmq_field *rmq_field_read(const void *, size_t,
                                 enum rmq_field_type, size_t *);

struct rmq_field *rmq_field_read_tagged_value(const void *, size_t, size_t *);

int rmq_fields_read(const void *, size_t, size_t *, ...);
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

struct rmq_field_table *rmq_field_table_new(void);
void rmq_field_table_delete(struct rmq_field_table *);

void rmq_field_table_append_nocopy(struct rmq_field_table *,
                                   char *, struct rmq_field *);

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
void rmq_frame_write(struct rmq_frame *, struct c_buffer *);

/* Method */
#define RMQ_METHOD(class_, id_) (unsigned int)(((class_) << 16) | (id_))

enum rmq_method {
    RMQ_METHOD_CONNECTION_START     = RMQ_METHOD(10, 10),
    RMQ_METHOD_CONNECTION_START_OK  = RMQ_METHOD(10, 11),
    RMQ_METHOD_CONNECTION_SECURE    = RMQ_METHOD(10, 20),
    RMQ_METHOD_CONNECTION_SECURE_OK = RMQ_METHOD(10, 21),
    RMQ_METHOD_CONNECTION_TUNE      = RMQ_METHOD(10, 30),
    RMQ_METHOD_CONNECTION_TUNE_OK   = RMQ_METHOD(10, 31),
    RMQ_METHOD_CONNECTION_OPEN      = RMQ_METHOD(10, 40),
    RMQ_METHOD_CONNECTION_OPEN_OK   = RMQ_METHOD(10, 41),
    RMQ_METHOD_CONNECTION_CLOSE     = RMQ_METHOD(10, 50),
    RMQ_METHOD_CONNECTION_CLOSE_OK  = RMQ_METHOD(10, 51),
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

/* ---------------------------------------------------------------------------
 *  Client
 * ------------------------------------------------------------------------ */
enum rmq_client_state {
    RMQ_CLIENT_STATE_DISCONNECTED,
    RMQ_CLIENT_STATE_CONNECTED,
    RMQ_CLIENT_STATE_START_RECEIVED,
    RMQ_CLIENT_STATE_CHALLENGE_RECEIVED,
    RMQ_CLIENT_STATE_TUNE_RECEIVED,
    RMQ_CLIENT_STATE_READY,
    RMQ_CLIENT_STATE_VHOST_OPEN,
};

struct rmq_client {
    struct io_base *io_base;
    struct io_tcp_client *tcp_client;

    enum rmq_client_state state;

    rmq_client_event_cb event_cb;
    void *event_cb_arg;
};

void rmq_client_send_frame(struct rmq_client *, enum rmq_frame_type,
                           uint16_t, const void *, size_t);
void rmq_client_send_method(struct rmq_client *, enum rmq_method);

#endif
