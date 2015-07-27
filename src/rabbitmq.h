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

struct rmq_client;

enum rmq_client_event {
    RMQ_CLIENT_EVENT_CONN_ESTABLISHED,
    RMQ_CLIENT_EVENT_CONN_FAILED,
    RMQ_CLIENT_EVENT_CONN_CLOSED,

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

int rmq_client_connect(struct rmq_client *, const char *, uint16_t);
void rmq_client_disconnect(struct rmq_client *);

#endif
