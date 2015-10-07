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
 *  Message properties
 * ------------------------------------------------------------------------ */
void
rmq_properties_init(struct rmq_properties *properties) {
    memset(properties, 0, sizeof(struct rmq_properties));
}

void
rmq_properties_free(struct rmq_properties *properties) {
    if (!properties)
        return;

    c_free(properties->content_type);
    c_free(properties->content_encoding);
    rmq_field_table_delete(properties->headers);
    c_free(properties->correlation_id);
    c_free(properties->reply_to);
    c_free(properties->expiration);
    c_free(properties->message_id);
    c_free(properties->type);
    c_free(properties->user_id);
    c_free(properties->app_id);

    memset(properties, 0, sizeof(struct rmq_properties));
}

void
rmq_properties_set_content_type(struct rmq_properties *properties, const char *value) {
    properties->mask |= RMQ_PROPERTY_CONTENT_TYPE;

    c_free(properties->content_type);
    properties->content_type = c_strdup(value);
}

void
rmq_properties_set_content_encoding(struct rmq_properties *properties,
                                    const char *value) {
    properties->mask |= RMQ_PROPERTY_CONTENT_ENCODING;

    c_free(properties->content_encoding);
    properties->content_encoding = c_strdup(value);
}

void
rmq_properties_add_header_nocopy(struct rmq_properties *properties,
                                 const char *name, struct rmq_field *value) {
    properties->mask |= RMQ_PROPERTY_HEADERS;

    if (!properties->headers)
        properties->headers = rmq_field_table_new();

    rmq_field_table_add_nocopy(properties->headers, c_strdup(name), value);
}

void
rmq_properties_set_delivery_mode(struct rmq_properties *properties,
                                 enum rmq_delivery_mode value) {
    properties->mask |= RMQ_PROPERTY_DELIVERY_MODE;

    properties->delivery_mode = value;
}

void
rmq_properties_set_priority(struct rmq_properties *properties, uint8_t value) {
    assert(value <= 9);

    properties->mask |= RMQ_PROPERTY_PRIORITY;

    properties->priority = value;
}

void
rmq_properties_set_correlation_id(struct rmq_properties *properties,
                                  const char *value) {
    properties->mask |= RMQ_PROPERTY_CORRELATION_ID;

    c_free(properties->correlation_id);
    properties->correlation_id = c_strdup(value);
}

void
rmq_properties_set_reply_to(struct rmq_properties *properties,
                            const char *value) {
    properties->mask |= RMQ_PROPERTY_REPLY_TO;

    c_free(properties->reply_to);
    properties->reply_to = c_strdup(value);
}

void
rmq_properties_set_expiration(struct rmq_properties *properties,
                              const char *value) {
    properties->mask |= RMQ_PROPERTY_EXPIRATION;

    c_free(properties->expiration);
    properties->expiration = c_strdup(value);
}

void
rmq_properties_set_message_id(struct rmq_properties *properties,
                              const char *value) {
    properties->mask |= RMQ_PROPERTY_MESSAGE_ID;

    c_free(properties->message_id);
    properties->message_id = c_strdup(value);
}

void
rmq_properties_set_timestamp(struct rmq_properties *properties,
                             uint64_t value) {
    properties->mask |= RMQ_PROPERTY_TIMESTAMP;

    properties->timestamp = value;
}

void
rmq_properties_set_type(struct rmq_properties *properties,
                        const char *value) {
    properties->mask |= RMQ_PROPERTY_TYPE;

    c_free(properties->type);
    properties->type = c_strdup(value);
}

void
rmq_properties_set_user_id(struct rmq_properties *properties,
                           const char *value) {
    properties->mask |= RMQ_PROPERTY_USER_ID;

    c_free(properties->user_id);
    properties->user_id = c_strdup(value);
}

void
rmq_properties_set_app_id(struct rmq_properties *properties,
                          const char *value) {
    properties->mask |= RMQ_PROPERTY_APP_ID;

    c_free(properties->app_id);
    properties->app_id = c_strdup(value);
}

/* ---------------------------------------------------------------------------
 *  Message
 * ------------------------------------------------------------------------ */
struct rmq_msg *
rmq_msg_new(void) {
    struct rmq_msg *msg;

    msg = c_malloc0(sizeof(struct rmq_msg));

    rmq_properties_init(&msg->properties);

    return msg;
}

void
rmq_msg_delete(struct rmq_msg *msg) {
    if (!msg)
        return;

    rmq_properties_free(&msg->properties);

    if (msg->data_owned)
        c_free(msg->data);

    c_free0(msg, sizeof(struct rmq_msg));
}

const char *
rmq_msg_content_type(const struct rmq_msg *msg) {
    return msg->properties.content_type;
}

const char *
rmq_msg_content_encoding(const struct rmq_msg *msg) {
    return msg->properties.content_encoding;
}

struct rmq_field *
rmq_msg_header(const struct rmq_msg *msg, const char *name) {
    if (!msg->properties.headers)
        return NULL;

    return rmq_field_table_get(msg->properties.headers, name);
}

enum rmq_delivery_mode
rmq_msg_delivery_mode(const struct rmq_msg *msg) {
    return msg->properties.delivery_mode;
}

uint8_t
rmq_msg_priority(const struct rmq_msg *msg) {
    return msg->properties.priority;
}

const char *
rmq_msg_correlation_id(const struct rmq_msg *msg) {
    return msg->properties.correlation_id;
}

const char *
rmq_msg_reply_to(const struct rmq_msg *msg) {
    return msg->properties.reply_to;
}

const char *
rmq_msg_expiration(const struct rmq_msg *msg) {
    return msg->properties.expiration;
}

const char *
rmq_msg_message_id(const struct rmq_msg *msg) {
    return msg->properties.message_id;
}

uint64_t
rmq_msg_timestamp(const struct rmq_msg *msg) {
    return msg->properties.timestamp;
}

const char *
rmq_msg_type(const struct rmq_msg *msg) {
    return msg->properties.type;
}

const char *
rmq_msg_user_id(const struct rmq_msg *msg) {
    return msg->properties.user_id;
}

const char *
rmq_msg_app_id(const struct rmq_msg *msg) {
    return msg->properties.app_id;
}

void
rmq_msg_set_content_type(struct rmq_msg *msg, const char *value) {
    rmq_properties_set_content_type(&msg->properties, value);
}

void
rmq_msg_set_content_encoding(struct rmq_msg *msg, const char *value) {
    rmq_properties_set_content_encoding(&msg->properties, value);
}

void
rmq_msg_add_header_nocopy(struct rmq_msg *msg,
                             const char *name, struct rmq_field *value) {
    rmq_properties_add_header_nocopy(&msg->properties, name, value);
}

void
rmq_msg_set_delivery_mode(struct rmq_msg *msg, enum rmq_delivery_mode value) {
    rmq_properties_set_delivery_mode(&msg->properties, value);
}

void
rmq_msg_set_priority(struct rmq_msg *msg, uint8_t value) {
    rmq_properties_set_priority(&msg->properties, value);
}

void
rmq_msg_set_correlation_id(struct rmq_msg *msg, const char *value) {
    rmq_properties_set_correlation_id(&msg->properties, value);
}

void
rmq_msg_set_reply_to(struct rmq_msg *msg, const char *value) {
    rmq_properties_set_reply_to(&msg->properties, value);
}

void
rmq_msg_set_expiration(struct rmq_msg *msg, const char *value) {
    rmq_properties_set_expiration(&msg->properties, value);
}

void
rmq_msg_set_message_id(struct rmq_msg *msg, const char *value) {
    rmq_properties_set_message_id(&msg->properties, value);
}

void
rmq_msg_set_timestamp(struct rmq_msg *msg, uint64_t value) {
    rmq_properties_set_timestamp(&msg->properties, value);
}

void
rmq_msg_set_type(struct rmq_msg *msg, const char *value) {
    rmq_properties_set_type(&msg->properties, value);
}

void
rmq_msg_set_user_id(struct rmq_msg *msg, const char *value) {
    rmq_properties_set_user_id(&msg->properties, value);
}

void
rmq_msg_set_app_id(struct rmq_msg *msg, const char *value) {
    rmq_properties_set_app_id(&msg->properties, value);
}

const void *
rmq_msg_data(const struct rmq_msg *msg, size_t *psz) {
    *psz = msg->data_sz;
    return msg->data;
}

void
rmq_msg_set_data_nocopy(struct rmq_msg *msg, void *data, size_t size) {
    if (msg->data_owned)
        c_free(msg->data);

    msg->data = data;
    msg->data_sz = size;
    msg->data_owned = true;
}

void
rmq_msg_set_data(struct rmq_msg *msg, const void *data, size_t size) {
    if (msg->data_owned)
        c_free(msg->data);

    msg->data = c_memdup(data, size);
    msg->data_sz = size;
    msg->data_owned = true;
}
