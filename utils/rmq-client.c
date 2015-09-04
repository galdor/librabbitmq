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

#include "../src/rabbitmq.h"

struct rmqu {
    struct io_base *io_base;
    struct rmq_client *client;

    bool do_exit;
    bool verbose;

    const char *cmd_name;
    void (*cmd_exec)(int, char **);
    int argc;
    char **argv;
};

static struct rmqu rmqu;

static void rmqu_trace(const char *, ...)
    __attribute__ ((format(printf, 1, 2)));
static void rmqu_error(const char *, ...)
    __attribute__ ((format(printf, 1, 2)));
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

struct rmqu_cmd {
    const char *name;
    void (*exec)(int, char **);
};

static void rmqu_cmd_declare_exchange(int, char **);
static void rmqu_cmd_delete_exchange(int, char **);
static void rmqu_cmd_declare_queue(int, char **);
static void rmqu_cmd_delete_queue(int, char **);
static void rmqu_cmd_bind_queue(int, char **);
static void rmqu_cmd_unbind_queue(int, char **);

static struct rmqu_cmd rmqu_cmds[] = {
    {"declare-exchange", rmqu_cmd_declare_exchange},
    {"delete-exchange", rmqu_cmd_delete_exchange},
    {"declare-queue", rmqu_cmd_declare_queue},
    {"delete-queue", rmqu_cmd_delete_queue},
    {"bind-queue", rmqu_cmd_bind_queue},
    {"unbind-queue", rmqu_cmd_unbind_queue},
};
size_t rmqu_nb_cmds = sizeof(rmqu_cmds) / sizeof(rmqu_cmds[0]);

int
main(int argc, char **argv) {
    struct c_command_line *cmdline;
    const char *host, *port_string;
    const char *user, *password, *vhost;
    uint16_t port;
    int ret;

    /* Command line */
    cmdline = c_command_line_new();

    c_command_line_set_trailing_text(cmdline, "COMMANDS\n\n"
                                     "help                   "
                                     "  display help\n"
                                     "declare-exchange       "
                                     "  create an exchange\n"
                                     "delete-exchange        "
                                     "  delete an exchange\n"
                                     "declare-queue          "
                                     "  create a queue\n"
                                     "delete-queue           "
                                     "  delete a queue\n"
                                     "bind-queue             "
                                     "  bind a queue to an exchange\n"
                                     "unbind-queue           "
                                     "  unbind a queue from an exchange\n"
                                    );

    c_command_line_add_option(cmdline, "s", "host",
                              "the host to connect to", "host", "localhost");
    c_command_line_add_option(cmdline, "p", "port",
                              "the port to connect to", "port", "5672");
    c_command_line_add_option(cmdline, "u", "user",
                              "the user name", "name", "guest");
    c_command_line_add_option(cmdline, "w", "password",
                              "the password", "string", "guest");
    c_command_line_add_option(cmdline, "i", "vhost",
                              "the virtual host", "vhost", "/");
    c_command_line_add_flag(cmdline, "v", "verbose", "enable verbose mode");

    c_command_line_add_argument(cmdline, "the command to execute", "command");

    ret = c_command_line_parse(cmdline, argc, argv);
    if (ret == -1)
        rmqu_die("%s", c_get_error());

    argc -= ret - 1; /* keep the command name */
    argv += ret - 1;

    host = c_command_line_option_value(cmdline, "host");
    port_string = c_command_line_option_value(cmdline, "port");
    if (c_parse_u16(port_string, &port, NULL) == -1)
        rmqu_die("invalid port: %s", c_get_error());

    user = c_command_line_option_value(cmdline, "user");
    password = c_command_line_option_value(cmdline, "password");
    vhost = c_command_line_option_value(cmdline, "vhost");

    rmqu.verbose = c_command_line_is_option_set(cmdline, "verbose");

    /* Main */
    rmqu.cmd_name = c_command_line_argument_value(cmdline, 0);
    if (strcmp(rmqu.cmd_name, "help") == 0) {
        c_command_line_usage_print(cmdline, stdout);
        return 0;
    }

    for (size_t i = 0; i < rmqu_nb_cmds; i++) {
        if (strcmp(rmqu_cmds[i].name, rmqu.cmd_name) == 0) {
            rmqu.cmd_exec = rmqu_cmds[i].exec;
            break;
        }
    }

    if (!rmqu.cmd_exec)
        rmqu_die("unknown command '%s'", rmqu.cmd_name);

    rmqu.argc = argc;
    rmqu.argv = argv;

    if (argc >= 2
     && (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0)) {
        rmqu.cmd_exec(rmqu.argc, rmqu.argv);
    }

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
rmqu_trace(const char *fmt, ...) {
    va_list ap;

    if (!rmqu.verbose)
        return;

    va_start(ap, fmt);
    vfprintf(stdout, fmt, ap);
    va_end(ap);

    putchar('\n');
}

void
rmqu_error(const char *fmt, ...) {
    va_list ap;

    fprintf(stderr, "error: ");

    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);

    putc('\n', stderr);
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
    rmqu_trace("signal %d received\n", signo);

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
        rmqu_trace("connection established");
        break;

    case RMQ_CLIENT_EVENT_CONN_FAILED:
        rmqu_trace("connection failed");
        rmqu.do_exit = true;
        break;

    case RMQ_CLIENT_EVENT_CONN_CLOSED:
        rmqu_trace("connection closed");
        rmqu.do_exit = true;
        break;

    case RMQ_CLIENT_EVENT_READY:
        rmqu_trace("ready");
        rmqu_on_client_ready();
        break;

    case RMQ_CLIENT_EVENT_ERROR:
        rmqu_error("%s", (const char *)data);
        break;

    case RMQ_CLIENT_EVENT_TRACE:
        rmqu_trace("%s", (const char *)data);
        break;
    }
}

static void
rmqu_on_client_ready(void) {
    rmqu.cmd_exec(rmqu.argc, rmqu.argv);
}

static enum rmq_msg_action
rmqu_on_msg(struct rmq_client *client,
            const struct rmq_delivery *delivery,
            const struct rmq_msg *msg, void *arg) {
    const uint8_t *data;
    size_t size;

    data = rmq_msg_data(msg, &size);

    rmqu_trace("message received (%zu bytes)", size);

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

    rmqu_error("message cannot be delivered: %s", text);
}

static void
rmqu_cmd_declare_exchange(int argc, char **argv) {
    struct c_command_line *cmdline;
    enum rmq_exchange_type type;
    const char *type_string, *name;
    bool passive, durable, auto_delete, internal;
    uint8_t options;

    /* Command line */
    cmdline = c_command_line_new();

    c_command_line_add_flag(cmdline, "p", "passive",
                            "create a passive exchange");
    c_command_line_add_flag(cmdline, "d", "durable",
                            "create a durable exchange");
    c_command_line_add_flag(cmdline, "a", "auto-delete",
                            "automatically delete the exchange when all "
                            "queues have finished using it");
    c_command_line_add_flag(cmdline, "i", "internal",
                            "create an internal exchange");

    c_command_line_add_argument(cmdline, "the name of the exchange", "name");
    c_command_line_add_argument(cmdline, "the type of the exchange", "type");

    if (c_command_line_parse(cmdline, argc, argv) == -1)
        rmqu_die("%s", c_get_error());

    passive = c_command_line_is_option_set(cmdline, "passive");
    durable = c_command_line_is_option_set(cmdline, "durable");
    auto_delete = c_command_line_is_option_set(cmdline, "auto-delete");
    internal = c_command_line_is_option_set(cmdline, "internal");

    name = c_command_line_argument_value(cmdline, 0);
    type_string = c_command_line_argument_value(cmdline, 1);

    if (rmq_exchange_type_parse(type_string, &type) == -1)
        rmqu_die("unknown exchange type");

    /* Main */
    options = RMQ_EXCHANGE_DEFAULT;

    if (passive)
        options |= RMQ_EXCHANGE_PASSIVE;
    if (durable)
        options |= RMQ_EXCHANGE_DURABLE;
    if (auto_delete)
        options |= RMQ_EXCHANGE_AUTO_DELETE;
    if (internal)
        options |= RMQ_EXCHANGE_INTERNAL;

    rmq_client_declare_exchange(rmqu.client, name, type, options, NULL);
    rmq_client_disconnect(rmqu.client);

    c_command_line_delete(cmdline);
}

static void
rmqu_cmd_delete_exchange(int argc, char **argv) {
    struct c_command_line *cmdline;
    bool if_unused;
    const char *name;
    uint8_t options;

    /* Command line */
    cmdline = c_command_line_new();

    c_command_line_add_flag(cmdline, "u", "if-unused",
                            "only delete the exchange if it has no queue "
                            "bindings");

    c_command_line_add_argument(cmdline, "the name of the exchange", "name");

    if (c_command_line_parse(cmdline, argc, argv) == -1)
        rmqu_die("%s", c_get_error());

    if_unused = c_command_line_is_option_set(cmdline, "if-unused");

    name = c_command_line_argument_value(cmdline, 0);

    /* Main */
    options = RMQ_EXCHANGE_DELETE_DEFAULT;

    if (if_unused)
        options |= RMQ_EXCHANGE_DELETE_IF_UNUSED;

    rmq_client_delete_exchange(rmqu.client, name, options);
    rmq_client_disconnect(rmqu.client);

    c_command_line_delete(cmdline);
}

static void
rmqu_cmd_declare_queue(int argc, char **argv) {
    struct c_command_line *cmdline;
    bool durable, exclusive, auto_delete;
    const char *name;
    uint8_t options;

    /* Command line */
    cmdline = c_command_line_new();

    c_command_line_add_flag(cmdline, "d", "durable",
                            "create a durable queue");
    c_command_line_add_flag(cmdline, "e", "exclusive",
                            "create an exclusive queue");
    c_command_line_add_flag(cmdline, "a", "auto-delete",
                            "automatically delete the queue when it has "
                            "no consumer");

    c_command_line_add_argument(cmdline, "the name of the queue", "name");

    if (c_command_line_parse(cmdline, argc, argv) == -1)
        rmqu_die("%s", c_get_error());

    durable = c_command_line_is_option_set(cmdline, "durable");
    exclusive = c_command_line_is_option_set(cmdline, "exclusive");
    auto_delete = c_command_line_is_option_set(cmdline, "auto-delete");

    name = c_command_line_argument_value(cmdline, 0);

    /* Main */
    options = RMQ_QUEUE_DEFAULT;

    if (durable)
        options |= RMQ_QUEUE_DURABLE;
    if (exclusive)
        options |= RMQ_QUEUE_EXCLUSIVE;
    if (auto_delete)
        options |= RMQ_QUEUE_AUTO_DELETE;

    rmq_client_declare_queue(rmqu.client, name, options, NULL);
    rmq_client_disconnect(rmqu.client);

    c_command_line_delete(cmdline);
}

static void
rmqu_cmd_delete_queue(int argc, char **argv) {
    struct c_command_line *cmdline;
    bool if_unused, if_empty;
    const char *name;
    uint8_t options;

    /* Command line */
    cmdline = c_command_line_new();

    c_command_line_add_flag(cmdline, "u", "if-unused",
                            "only delete the queue if it has no consumer");
    c_command_line_add_flag(cmdline, "e", "if-empty",
                            "only delete the queue if it is empty");

    c_command_line_add_argument(cmdline, "the name of the queue", "name");

    if (c_command_line_parse(cmdline, argc, argv) == -1)
        rmqu_die("%s", c_get_error());

    if_unused = c_command_line_is_option_set(cmdline, "if-unused");
    if_empty = c_command_line_is_option_set(cmdline, "if-empty");

    name = c_command_line_argument_value(cmdline, 0);

    /* Main */
    options = RMQ_QUEUE_DELETE_DEFAULT;

    if (if_unused)
        options |= RMQ_QUEUE_DELETE_IF_UNUSED;
    if (if_empty)
        options |= RMQ_QUEUE_DELETE_IF_EMPTY;

    rmq_client_delete_queue(rmqu.client, name, options);
    rmq_client_disconnect(rmqu.client);

    c_command_line_delete(cmdline);
}

static void
rmqu_cmd_bind_queue(int argc, char **argv) {
    struct c_command_line *cmdline;
    const char *queue, *exchange, *routing_key;

    /* Command line */
    cmdline = c_command_line_new();

    c_command_line_add_option(cmdline, "k", "routing-key",
                              "the routing key", "key", "");

    c_command_line_add_argument(cmdline, "the name of the queue", "queue");
    c_command_line_add_argument(cmdline, "the name of the exchange",
                                "exchange");

    if (c_command_line_parse(cmdline, argc, argv) == -1)
        rmqu_die("%s", c_get_error());

    queue = c_command_line_argument_value(cmdline, 0);
    exchange = c_command_line_argument_value(cmdline, 1);
    routing_key = c_command_line_option_value(cmdline, "routing-key");

    /* Main */
    rmq_client_bind_queue(rmqu.client, queue, exchange, routing_key, NULL);
    rmq_client_disconnect(rmqu.client);

    c_command_line_delete(cmdline);
}

static void
rmqu_cmd_unbind_queue(int argc, char **argv) {
    struct c_command_line *cmdline;
    const char *queue, *exchange, *routing_key;

    /* Command line */
    cmdline = c_command_line_new();

    c_command_line_add_option(cmdline, "k", "routing-key",
                              "the routing key", "key", "");

    c_command_line_add_argument(cmdline, "the name of the queue", "queue");
    c_command_line_add_argument(cmdline, "the name of the exchange",
                                "exchange");

    if (c_command_line_parse(cmdline, argc, argv) == -1)
        rmqu_die("%s", c_get_error());

    queue = c_command_line_argument_value(cmdline, 0);
    exchange = c_command_line_argument_value(cmdline, 1);
    routing_key = c_command_line_option_value(cmdline, "routing-key");

    /* Main */
    rmq_client_unbind_queue(rmqu.client, queue, exchange, routing_key, NULL);
    rmq_client_disconnect(rmqu.client);

    c_command_line_delete(cmdline);
}
