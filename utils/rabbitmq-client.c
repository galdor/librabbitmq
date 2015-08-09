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

#include <signal.h>
#include <string.h>

#include <core.h>
#include <io.h>

#include "rabbitmq.h"

struct rmqu {
    struct io_base *io_base;
    struct rmq_client *client;

    bool do_exit;
};

static struct rmqu rmqu;

static void rmqu_die(const char *, ...)
    __attribute__ ((format(printf, 1, 2), noreturn));

static void rmqu_on_signal(int, void *);

static void rmqu_on_client_event(struct rmq_client *, enum rmq_client_event,
                                 void *, void *);
static void rmqu_on_client_ready(void);

static enum rmq_msg_action rmqu_on_msg(struct rmq_client *,
                                       const struct rmq_delivery *,
                                       const struct rmq_msg *, void *);
static void rmqu_on_undeliverable_msg(struct rmq_client *,
                                      const struct rmq_delivery *,
                                      const struct rmq_msg *, void *);

int
main(int argc, char **argv) {
    struct c_command_line *cmdline;
    const char *host, *port_string;
    const char *user, *password, *vhost;
    uint16_t port;

    /* Command line */
    cmdline = c_command_line_new();

    c_command_line_add_option(cmdline, "p", "port",
                              "the port to connect to", "port", "5672");
    c_command_line_add_option(cmdline, "u", "user",
                              "the user name", "name", "guest");
    c_command_line_add_option(cmdline, "w", "password",
                              "the password", "string", "guest");
    c_command_line_add_option(cmdline, "v", "vhost",
                              "the virtual host", "vhost", "/");

    c_command_line_add_argument(cmdline, "the host to connect to", "host");

    if (c_command_line_parse(cmdline, argc, argv) == -1)
        rmqu_die("%s", c_get_error());

    host = c_command_line_argument_value(cmdline, 0);
    port_string = c_command_line_option_value(cmdline, "port");
    if (c_parse_u16(port_string, &port, NULL) == -1)
        rmqu_die("invalid port: %s", c_get_error());

    user = c_command_line_option_value(cmdline, "user");
    password = c_command_line_option_value(cmdline, "password");
    vhost = c_command_line_option_value(cmdline, "vhost");

    /* IO base */
    rmqu.io_base = io_base_new();

    if (io_base_watch_signal(rmqu.io_base, SIGINT, rmqu_on_signal, NULL) == -1)
        rmqu_die("cannot watch signal: %s", c_get_error());
    if (io_base_watch_signal(rmqu.io_base, SIGTERM, rmqu_on_signal, NULL) == -1)
        rmqu_die("cannot watch signal: %s", c_get_error());

    /* Client */
    rmqu.client = rmq_client_new(rmqu.io_base);

    rmq_client_set_event_cb(rmqu.client, rmqu_on_client_event, NULL);
    rmq_client_set_undeliverable_msg_cb(rmqu.client,
                                        rmqu_on_undeliverable_msg, NULL);
    rmq_client_set_credentials(rmqu.client, user, password);
    rmq_client_set_vhost(rmqu.client, vhost);

    if (rmq_client_connect(rmqu.client, host, port) == -1) {
        rmqu_die("cannot connect to %s:%d: %s",
                host, port, c_get_error());
    }

    /* Main loop */
    while (!rmqu.do_exit) {
        if (io_base_read_events(rmqu.io_base) == -1)
            rmqu_die("cannot read events: %s", c_get_error());
    }

    /* Shutdown */
    rmq_client_disconnect(rmqu.client);

    io_base_unwatch_signal(rmqu.io_base, SIGINT);
    io_base_unwatch_signal(rmqu.io_base, SIGTERM);

    while (io_base_has_watchers(rmqu.io_base)) {
        if (io_base_read_events(rmqu.io_base) == -1)
            rmqu_die("cannot read events: %s", c_get_error());
    }

    /* Cleaning */
    rmq_client_delete(rmqu.client);
    io_base_delete(rmqu.io_base);

    c_command_line_delete(cmdline);
    return 0;
}

void
rmqu_die(const char *fmt, ...) {
    va_list ap;

    fprintf(stderr, "fatal error: ");

    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);

    putc('\n', stderr);
    exit(1);
}

static void
rmqu_on_signal(int signo, void *arg) {
    printf("signal %d received\n", signo);

    switch (signo) {
    case SIGINT:
    case SIGTERM:
        rmqu.do_exit = true;
        break;
    }
}

static void
rmqu_on_client_event(struct rmq_client *client, enum rmq_client_event event,
                     void *data, void *arg) {
    switch (event) {
    case RMQ_CLIENT_EVENT_CONN_ESTABLISHED:
        printf("connection established\n");
        break;

    case RMQ_CLIENT_EVENT_CONN_FAILED:
        printf("connection failed\n");
        rmqu.do_exit = true;
        break;

    case RMQ_CLIENT_EVENT_CONN_CLOSED:
        printf("connection closed\n");
        rmqu.do_exit = true;
        break;

    case RMQ_CLIENT_EVENT_READY:
        printf("ready\n");
        rmqu_on_client_ready();
        break;

    case RMQ_CLIENT_EVENT_ERROR:
        fprintf(stderr, "error: %s\n", (const char *)data);
        break;

    case RMQ_CLIENT_EVENT_TRACE:
        fprintf(stderr, "%s\n", (const char *)data);
        break;
    }
}

static void
rmqu_on_client_ready(void) {
#if 0
    struct rmq_msg *msg;
    const char *string;

    string = "hello world";

    msg = rmq_msg_new();
    rmq_msg_set_content_type(msg, "text/plain");
    rmq_msg_add_header_nocopy(msg, "foo", rmq_field_new_long_int(42));
    rmq_msg_set_data_nocopy(msg, (void *)string, strlen(string));

    rmq_client_publish(rmqu.client, msg, "messages", "", RMQ_PUBLISH_MANDATORY);
#endif

#if 0
    rmq_client_subscribe(rmqu.client, "messages", RMQ_SUBSCRIBE_DEFAULT,
                         rmqu_on_msg, NULL);
#endif

#if 0
    rmq_client_declare_queue(rmqu.client, "foo", RMQ_QUEUE_DEFAULT, NULL);
#endif

#if 1
    rmq_client_delete_queue(rmqu.client, "foo", RMQ_QUEUE_DELETE_DEFAULT);
#endif
}

static enum rmq_msg_action
rmqu_on_msg(struct rmq_client *client,
            const struct rmq_delivery *delivery,
            const struct rmq_msg *msg, void *arg) {
    const uint8_t *data;
    size_t size;

    data = rmq_msg_data(msg, &size);

    printf("message received (%zu bytes)\n", size);

    return RMQ_MSG_ACTION_ACK;
}

static void
rmqu_on_undeliverable_msg(struct rmq_client *client,
                          const struct rmq_delivery *delivery,
                          const struct rmq_msg *msg, void *arg) {
    const uint8_t *data;
    const char *text;
    size_t size;

    data = rmq_msg_data(msg, &size);
    text = rmq_delivery_undeliverable_reply_text(delivery);

    printf("message cannot be delivered: %s\n", text);
}
