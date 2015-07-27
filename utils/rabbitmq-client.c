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

int
main(int argc, char **argv) {
    struct c_command_line *cmdline;
    const char *host, *port_string;
    const char *user, *password;
    uint16_t port;

    /* Command line */
    cmdline = c_command_line_new();

    c_command_line_add_option(cmdline, "p", "port",
                              "the port to connect to", "port", "5672");
    c_command_line_add_option(cmdline, "u", "user",
                              "the user name", "name", NULL);
    c_command_line_add_option(cmdline, "w", "password",
                              "the password", "string", NULL);

    c_command_line_add_argument(cmdline, "the host to connect to", "host");

    if (c_command_line_parse(cmdline, argc, argv) == -1)
        rmqu_die("%s", c_get_error());

    host = c_command_line_argument_value(cmdline, 0);
    port_string = c_command_line_option_value(cmdline, "port");
    if (c_parse_u16(port_string, &port, NULL) == -1)
        rmqu_die("invalid port: %s", c_get_error());

    user = c_command_line_option_value(cmdline, "user");
    password = c_command_line_option_value(cmdline, "password");

    /* IO base */
    rmqu.io_base = io_base_new();

    if (io_base_watch_signal(rmqu.io_base, SIGINT, rmqu_on_signal, NULL) == -1)
        rmqu_die("cannot watch signal: %s", c_get_error());
    if (io_base_watch_signal(rmqu.io_base, SIGTERM, rmqu_on_signal, NULL) == -1)
        rmqu_die("cannot watch signal: %s", c_get_error());

    /* IMAP client */
    rmqu.client = rmq_client_new(rmqu.io_base);

    rmq_client_set_event_cb(rmqu.client, rmqu_on_client_event, NULL);
    rmq_client_set_credentials(rmqu.client, user, password);

    if (rmq_client_connect(rmqu.client, host, port) == -1) {
        rmqu_die("cannot connect to %s:%d: %s",
                host, port, c_get_error());
    }

    /* Main loop */
    while (!rmqu.do_exit) {
        if (io_base_read_events(rmqu.io_base) == -1)
            rmqu_die("cannot read events: %s", c_get_error());
    }

    /* Cleaning */
    rmq_client_disconnect(rmqu.client);

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

    case RMQ_CLIENT_EVENT_ERROR:
        fprintf(stderr, "error: %s\n", (const char *)data);
        break;

    case RMQ_CLIENT_EVENT_TRACE:
        fprintf(stderr, "%s\n", (const char *)data);
        break;
    }
}
