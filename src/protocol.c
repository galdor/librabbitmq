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

static uint8_t rmq_read_u8(const uint8_t *);
static uint16_t rmq_read_u16(const uint8_t *);
static uint32_t rmq_read_u32(const uint8_t *);
static uint64_t rmq_read_u64(const uint8_t *);

static void rmq_write_u8(uint8_t, uint8_t *);
static void rmq_write_u16(uint16_t, uint8_t *);
static void rmq_write_u32(uint32_t, uint8_t *);
static void rmq_write_u64(uint64_t, uint8_t *);

/* ---------------------------------------------------------------------------
 *  Field values
 * ------------------------------------------------------------------------ */
const char *
rmq_field_type_to_string(enum rmq_field_type type) {
    const char *strings[] = {
        [RMQ_FIELD_BOOLEAN]          = "boolean",
        [RMQ_FIELD_SHORT_SHORT_INT]  = "short short int",
        [RMQ_FIELD_SHORT_SHORT_UINT] = "short short uint",
        [RMQ_FIELD_SHORT_INT]        = "short int",
        [RMQ_FIELD_SHORT_UINT]       = "short uint",
        [RMQ_FIELD_LONG_INT]         = "long int",
        [RMQ_FIELD_LONG_UINT]        = "long uint",
        [RMQ_FIELD_LONG_LONG_INT]    = "long long int",
        [RMQ_FIELD_LONG_LONG_UINT]   = "long long uint",
        [RMQ_FIELD_FLOAT]            = "float",
        [RMQ_FIELD_DOUBLE]           = "double",
        [RMQ_FIELD_DECIMAL]          = "decimal",
        [RMQ_FIELD_SHORT_STRING]     = "short string",
        [RMQ_FIELD_LONG_STRING]      = "long string",
        [RMQ_FIELD_ARRAY]            = "array",
        [RMQ_FIELD_TIMESTAMP]        = "timestamp",
        [RMQ_FIELD_TABLE]            = "table",
    };
    size_t nb_strings = sizeof(strings) / sizeof(strings[0]);

    if (type >= nb_strings)
        return NULL;

    return strings[type];
}

struct rmq_field *
rmq_field_new(enum rmq_field_type type) {
    struct rmq_field *field;

    field = c_malloc0(sizeof(struct rmq_field));

    field->type = type;

    return field;
}

void
rmq_field_delete(struct rmq_field *field) {
    if (!field)
        return;

    switch (field->type) {
    case RMQ_FIELD_BOOLEAN:
    case RMQ_FIELD_SHORT_SHORT_INT:
    case RMQ_FIELD_SHORT_SHORT_UINT:
    case RMQ_FIELD_SHORT_INT:
    case RMQ_FIELD_SHORT_UINT:
    case RMQ_FIELD_LONG_INT:
    case RMQ_FIELD_LONG_UINT:
    case RMQ_FIELD_LONG_LONG_INT:
    case RMQ_FIELD_LONG_LONG_UINT:
    case RMQ_FIELD_FLOAT:
    case RMQ_FIELD_DOUBLE:
    case RMQ_FIELD_DECIMAL:
        break;

    case RMQ_FIELD_SHORT_STRING:
        c_free(field->u.short_string);
        break;

    case RMQ_FIELD_LONG_STRING:
        c_free(field->u.long_string);
        break;

    case RMQ_FIELD_ARRAY:
        for (size_t i = 0; i < c_ptr_vector_length(field->u.array); i++)
            rmq_field_delete(c_ptr_vector_entry(field->u.array, i));
        c_ptr_vector_delete(field->u.array);
        break;

    case RMQ_FIELD_TIMESTAMP:
        break;

    case RMQ_FIELD_TABLE:
        rmq_field_table_delete(field->u.table);
        break;

    case RMQ_FIELD_NO_VALUE:
    case RMQ_FIELD_END:
        break;
    }

    c_free0(field, sizeof(struct rmq_field));
}

int
rmq_field_read_boolean(const void *data, size_t size,
                       bool *pvalue, size_t *psz) {
    const uint8_t *ptr;
    size_t len;

    ptr = data;
    len = size;

    if (len < 1) {
        c_set_error("truncated boolean");
        return -1;
    }

    *pvalue = ptr[0];

    ptr += 1;
    len -= 1;

    *psz = size - len;
    return 0;
}

int
rmq_field_read_short_short_int(const void *data, size_t size,
                               int8_t *pvalue, size_t *psz) {
    const uint8_t *ptr;
    size_t len;

    ptr = data;
    len = size;

    if (len < 1) {
        c_set_error("truncated short short int");
        return -1;
    }

    *pvalue = (int8_t)ptr[0];

    ptr += 1;
    len -= 1;

    *psz = size - len;
    return 0;
}

int
rmq_field_read_short_short_uint(const void *data, size_t size,
                                uint8_t *pvalue, size_t *psz) {
    const uint8_t *ptr;
    size_t len;

    ptr = data;
    len = size;

    if (len < 1) {
        c_set_error("truncated short short uint");
        return -1;
    }

    *pvalue = ptr[0];

    ptr += 1;
    len -= 1;

    *psz = size - len;
    return 0;
}

int
rmq_field_read_short_int(const void *data, size_t size,
                         int16_t *pvalue, size_t *psz) {
    const uint8_t *ptr;
    size_t len;

    ptr = data;
    len = size;

    if (len < 1) {
        c_set_error("truncated short int");
        return -1;
    }

    *pvalue = (int16_t)rmq_read_u16(ptr);

    ptr += 2;
    len -= 2;

    *psz = size - len;
    return 0;
}

int
rmq_field_read_short_uint(const void *data, size_t size,
                          uint16_t *pvalue, size_t *psz) {
    const uint8_t *ptr;
    size_t len;

    ptr = data;
    len = size;

    if (len < 1) {
        c_set_error("truncated short uint");
        return -1;
    }

    *pvalue = rmq_read_u16(ptr);

    ptr += 2;
    len -= 2;

    *psz = size - len;
    return 0;
}

int
rmq_field_read_long_int(const void *data, size_t size,
                        int32_t *pvalue, size_t *psz) {
    const uint8_t *ptr;
    size_t len;

    ptr = data;
    len = size;

    if (len < 1) {
        c_set_error("truncated long int");
        return -1;
    }

    *pvalue = (int32_t)rmq_read_u32(ptr);

    ptr += 4;
    len -= 4;

    *psz = size - len;
    return 0;
}

int
rmq_field_read_long_uint(const void *data, size_t size,
                         uint32_t *pvalue, size_t *psz) {
    const uint8_t *ptr;
    size_t len;

    ptr = data;
    len = size;

    if (len < 1) {
        c_set_error("truncated long uint");
        return -1;
    }

    *pvalue = rmq_read_u32(ptr);

    ptr += 4;
    len -= 4;

    *psz = size - len;
    return 0;
}

int
rmq_field_read_long_long_int(const void *data, size_t size,
                             int64_t *pvalue, size_t *psz) {
    const uint8_t *ptr;
    size_t len;

    ptr = data;
    len = size;

    if (len < 1) {
        c_set_error("truncated long long int");
        return -1;
    }

    *pvalue = (int64_t)rmq_read_u64(ptr);

    ptr += 8;
    len -= 8;

    *psz = size - len;
    return 0;
}

int
rmq_field_read_long_long_uint(const void *data, size_t size,
                              uint64_t *pvalue, size_t *psz) {
    const uint8_t *ptr;
    size_t len;

    ptr = data;
    len = size;

    if (len < 1) {
        c_set_error("truncated long long uint");
        return -1;
    }

    *pvalue = rmq_read_u64(ptr);

    ptr += 8;
    len -= 8;

    *psz = size - len;
    return 0;
}

int
rmq_field_read_float(const void *data, size_t size,
                     float *pvalue, size_t *psz) {
    const uint8_t *ptr;
    size_t len;

    ptr = data;
    len = size;

    if (len < 4) {
        c_set_error("truncated float");
        return -1;
    }

    memcpy(pvalue, ptr, 4);

    ptr += 4;
    len -= 4;

    *psz = size - len;
    return 0;
}

int
rmq_field_read_double(const void *data, size_t size,
                      double *pvalue, size_t *psz) {
    const uint8_t *ptr;
    size_t len;

    ptr = data;
    len = size;

    if (len < 8) {
        c_set_error("truncated double");
        return -1;
    }

    memcpy(pvalue, ptr, 8);

    ptr += 8;
    len -= 8;

    *psz = size - len;
    return 0;
}

int
rmq_field_read_decimal(const void *data, size_t size,
                       struct rmq_decimal *pvalue, size_t *psz) {
    /* TODO */
    c_set_error("decimal not supported");
    return -1;
}

int
rmq_field_read_short_string(const void *data, size_t size,
                            char **pvalue, size_t *psz) {
    const uint8_t *ptr;
    size_t len;
    uint8_t string_length;

    ptr = data;
    len = size;

    if (len < 1) {
        c_set_error("truncated short string length");
        return -1;
    }

    string_length = ptr[0];

    ptr += 1;
    len -= 1;

    if (len < string_length) {
        c_set_error("truncated short string");
        return -1;
    }

    *pvalue = c_strndup((const char *)ptr, string_length);

    ptr += string_length;
    len -= string_length;

    *psz = size - len;
    return 0;
}

int
rmq_field_read_long_string(const void *data, size_t size,
                           char **pvalue, size_t *psz) {
    const uint8_t *ptr;
    size_t len;
    uint32_t string_length;

    ptr = data;
    len = size;

    if (len < 4) {
        c_set_error("truncated long string length");
        return -1;
    }

    string_length = rmq_read_u32(ptr);

    ptr += 4;
    len -= 4;

    if (len < string_length) {
        c_set_error("truncated long string");
        return -1;
    }

    *pvalue = c_strndup((const char *)ptr, string_length);

    ptr += string_length;
    len -= string_length;

    *psz = size - len;
    return 0;
}

int
rmq_field_read_array(const void *data, size_t size,
                     struct c_ptr_vector **pfields, size_t *psz) {
    const uint8_t *ptr;
    size_t len;
    struct c_ptr_vector *fields;
    size_t content_size, rest;

    ptr = data;
    len = size;

    if (len < 4) {
        c_set_error("truncated array size");
        return -1;
    }

    content_size = rmq_read_u32(ptr);

    ptr += 4;
    len -= 4;

    fields = c_ptr_vector_new();

    rest = content_size;
    while (rest > 0) {
        struct rmq_field *field;
        size_t value_size;

        field = rmq_field_read_tagged_value(ptr, len, &value_size);
        if (!field) {
            for (size_t i = 0; i < c_ptr_vector_length(fields); i++)
                rmq_field_delete(c_ptr_vector_entry(fields, i));
            c_ptr_vector_delete(fields);
            return -1;
        }

        c_ptr_vector_append(fields, field);

        ptr += value_size;
        rest -= value_size;
    }

    len -= content_size;

    *pfields = fields;

    *psz = size - len;
    return 0;
}

int
rmq_field_read_timestamp(const void *data, size_t size,
                         uint64_t *pvalue, size_t *psz) {
    const uint8_t *ptr;
    size_t len;

    ptr = data;
    len = size;

    if (len < 1) {
        c_set_error("truncated timestamp");
        return -1;
    }

    *pvalue = rmq_read_u64(ptr);

    ptr += 8;
    len -= 8;

    *psz = size - len;
    return 0;
}

int
rmq_field_read_table(const void *data, size_t size,
                     struct rmq_field_table **pvalue, size_t *psz) {
    const uint8_t *ptr;
    size_t len;
    struct rmq_field_table *table;
    size_t content_size, rest;

    ptr = data;
    len = size;

    if (len < 4) {
        c_set_error("missing table size");
        return -1;
    }

    content_size = rmq_read_u32(ptr);

    ptr += 4;
    len -= 4;

    table = rmq_field_table_new();

    rest = content_size;
    while (rest > 0) {
        struct rmq_field *value;
        size_t value_size;
        char *name;

        /* Name */
        if (rmq_field_read_short_string(ptr, rest, &name, &value_size) == -1) {
            rmq_field_table_delete(table);
            return -1;
        }

        ptr += value_size;
        rest -= value_size;

        /* Value */
        value = rmq_field_read_tagged_value(ptr, rest, &value_size);
        if (!value) {
            c_free(name);
            rmq_field_table_delete(table);
            return -1;
        }

        ptr += value_size;
        rest -= value_size;

        /* Append the pair to the table */
        rmq_field_table_append_nocopy(table, name, value);
    }

    len -= content_size;

    *pvalue = table;

    *psz = size - len;
    return 0;
}

int
rmq_field_read_no_value(const void *data, size_t size, size_t *psz) {
    *psz = 0;
    return 0;
}

struct rmq_field *
rmq_field_read(const void *data, size_t size,
               enum rmq_field_type type, size_t *psz) {
    struct rmq_field *field;
    int ret;

    field = rmq_field_new(type);

    switch (type) {
    case RMQ_FIELD_BOOLEAN:
        ret = rmq_field_read_boolean(data, size, &field->u.boolean, psz);
        break;

    case RMQ_FIELD_SHORT_SHORT_INT:
        ret = rmq_field_read_short_short_int(data, size,
                                             &field->u.short_short_int, psz);
        break;

    case RMQ_FIELD_SHORT_SHORT_UINT:
        ret = rmq_field_read_short_short_uint(data, size,
                                              &field->u.short_short_uint, psz);
        break;

    case RMQ_FIELD_SHORT_INT:
        ret = rmq_field_read_short_int(data, size, &field->u.short_int, psz);
        break;

    case RMQ_FIELD_SHORT_UINT:
        ret = rmq_field_read_short_uint(data, size, &field->u.short_uint, psz);
        break;

    case RMQ_FIELD_LONG_INT:
        ret = rmq_field_read_long_int(data, size, &field->u.long_int, psz);
        break;

    case RMQ_FIELD_LONG_UINT:
        ret = rmq_field_read_long_uint(data, size, &field->u.long_uint, psz);
        break;

    case RMQ_FIELD_LONG_LONG_INT:
        ret = rmq_field_read_long_long_int(data, size,
                                           &field->u.long_long_int, psz);
        break;

    case RMQ_FIELD_LONG_LONG_UINT:
        ret = rmq_field_read_long_long_uint(data, size,
                                            &field->u.long_long_uint, psz);
        break;

    case RMQ_FIELD_FLOAT:
        ret = rmq_field_read_float(data, size, &field->u.float_value, psz);
        break;

    case RMQ_FIELD_DOUBLE:
        ret = rmq_field_read_double(data, size, &field->u.double_value, psz);
        break;

    case RMQ_FIELD_DECIMAL:
        ret = rmq_field_read_decimal(data, size, &field->u.decimal, psz);
        break;

    case RMQ_FIELD_SHORT_STRING:
        ret = rmq_field_read_short_string(data, size,
                                          &field->u.short_string, psz);
        break;

    case RMQ_FIELD_LONG_STRING:
        ret = rmq_field_read_long_string(data, size,
                                         &field->u.long_string, psz);
        break;

    case RMQ_FIELD_ARRAY:
        ret = rmq_field_read_array(data, size, &field->u.array, psz);
        break;

    case RMQ_FIELD_TIMESTAMP:
        ret = rmq_field_read_timestamp(data, size, &field->u.timestamp, psz);
        break;

    case RMQ_FIELD_TABLE:
        ret = rmq_field_read_table(data, size, &field->u.table, psz);
        break;

    case RMQ_FIELD_NO_VALUE:
        ret = rmq_field_read_no_value(data, size, psz);
        break;

    case RMQ_FIELD_END:
        assert(false);
    }

    if (ret == -1) {
        rmq_field_delete(field);
        return NULL;
    }

    return field;
}

struct rmq_field *
rmq_field_read_tagged_value(const void *data, size_t size, size_t *psz) {
    struct rmq_field *field;
    const uint8_t *ptr;
    size_t len;
    char tag;
    size_t value_size;

    ptr = data;
    len = size;

    if (len < 1) {
        c_set_error("missing field type tag");
        return NULL;
    }

    tag = (char)ptr[0];

    ptr += 1;
    len -= 1;

    switch (tag) {
    case 't':
        /* Boolean */
        field = rmq_field_read(ptr, len, RMQ_FIELD_BOOLEAN, &value_size);
        break;

    case 'b':
        /* Short short int*/
        field = rmq_field_read(ptr, len, RMQ_FIELD_SHORT_SHORT_INT,
                               &value_size);
        break;

    case 'B':
        /* Short short uint*/
        field = rmq_field_read(ptr, len, RMQ_FIELD_SHORT_SHORT_UINT,
                               &value_size);
        break;

    case 'U':
        /* Short int*/
        field = rmq_field_read(ptr, len, RMQ_FIELD_SHORT_INT,
                               &value_size);
        break;

    case 'u':
        /* Short uint*/
        field = rmq_field_read(ptr, len, RMQ_FIELD_SHORT_UINT,
                               &value_size);
        break;

    case 'I':
        /* Long int */
        field = rmq_field_read(ptr, len, RMQ_FIELD_LONG_INT,
                               &value_size);
        break;

    case 'i':
        /* Long uint */
        field = rmq_field_read(ptr, len, RMQ_FIELD_LONG_UINT,
                               &value_size);
        break;

    case 'L':
        /* Long long int */
        field = rmq_field_read(ptr, len, RMQ_FIELD_LONG_LONG_INT,
                               &value_size);
        break;

    case 'l':
        /* Long long uint */
        field = rmq_field_read(ptr, len, RMQ_FIELD_LONG_LONG_UINT,
                               &value_size);
        break;

    case 'f':
        /* Float */
        field = rmq_field_read(ptr, len, RMQ_FIELD_FLOAT, &value_size);
        break;

    case 'd':
        /* Double */
        field = rmq_field_read(ptr, len, RMQ_FIELD_DOUBLE, &value_size);
        break;

    case 'D':
        /* Decimal */
        field = rmq_field_read(ptr, len, RMQ_FIELD_DECIMAL, &value_size);
        break;

    case 's':
        /* Short string */
        field = rmq_field_read(ptr, len, RMQ_FIELD_SHORT_STRING, &value_size);
        break;

    case 'S':
        /* Long string */
        field = rmq_field_read(ptr, len, RMQ_FIELD_LONG_STRING, &value_size);
        break;

    case 'A':
        /* Array */
        field = rmq_field_read(ptr, len, RMQ_FIELD_ARRAY, &value_size);
        break;

    case 'T':
        /* Timestamp */
        field = rmq_field_read(ptr, len, RMQ_FIELD_TIMESTAMP, &value_size);
        break;

    case 'F':
        /* Table */
        field = rmq_field_read(ptr, len, RMQ_FIELD_TABLE, &value_size);
        break;

    case 'V':
        /* No value */
        field = rmq_field_read(ptr, len, RMQ_FIELD_NO_VALUE, &value_size);
        break;

    default:
        if (isprint((unsigned char)tag)) {
            c_set_error("unknown field tag '%c'", tag);
        } else {
            c_set_error("unknown field tag 0x%02x", tag);
        }
        return NULL;
    }

    ptr += value_size;
    len -= value_size;

    *psz = size - len;
    return field;
}

int
rmq_fields_read(const void *data, size_t size, size_t *psz, ...) {
    const uint8_t *ptr;
    size_t len;
    va_list ap;
    size_t nb_fields;

    ptr = data;
    len = size;

    va_start(ap, psz);

    nb_fields = 0;

    for(;;) {
        enum rmq_field_type type;
        size_t field_size;
        int ret;

        type = va_arg(ap, enum rmq_field_type);
        if (type == RMQ_FIELD_END)
            break;

        switch (type) {
        case RMQ_FIELD_BOOLEAN:
            ret = rmq_field_read_boolean(ptr, len, va_arg(ap, bool *),
                                         &field_size);
            break;

        case RMQ_FIELD_SHORT_SHORT_INT:
            ret = rmq_field_read_short_short_int(ptr, len,
                                                 va_arg(ap, int8_t *),
                                                 &field_size);
            break;

        case RMQ_FIELD_SHORT_SHORT_UINT:
            ret = rmq_field_read_short_short_uint(ptr, len,
                                                  va_arg(ap, uint8_t *),
                                                  &field_size);
            break;

        case RMQ_FIELD_SHORT_INT:
            ret = rmq_field_read_short_int(ptr, len, va_arg(ap, int16_t *),
                                           &field_size);
            break;

        case RMQ_FIELD_SHORT_UINT:
            ret = rmq_field_read_short_uint(ptr, len, va_arg(ap, uint16_t *),
                                            &field_size);
            break;

        case RMQ_FIELD_LONG_INT:
            ret = rmq_field_read_long_int(ptr, len, va_arg(ap, int32_t *),
                                          &field_size);
            break;

        case RMQ_FIELD_LONG_UINT:
            ret = rmq_field_read_long_uint(ptr, len, va_arg(ap, uint32_t *),
                                           &field_size);
            break;

        case RMQ_FIELD_LONG_LONG_INT:
            ret = rmq_field_read_long_long_int(ptr, len,
                                               va_arg(ap, int64_t *),
                                               &field_size);
            break;

        case RMQ_FIELD_LONG_LONG_UINT:
            ret = rmq_field_read_long_long_uint(ptr, len,
                                                va_arg(ap, uint64_t *),
                                                &field_size);
            break;

        case RMQ_FIELD_FLOAT:
            ret = rmq_field_read_float(ptr, len, va_arg(ap, float *),
                                       &field_size);
            break;

        case RMQ_FIELD_DOUBLE:
            ret = rmq_field_read_double(ptr, len, va_arg(ap, double *),
                                        &field_size);
            break;

        case RMQ_FIELD_DECIMAL:
            ret = rmq_field_read_decimal(ptr, len,
                                         va_arg(ap, struct rmq_decimal *),
                                         &field_size);
            break;

        case RMQ_FIELD_SHORT_STRING:
            ret = rmq_field_read_short_string(ptr, len,
                                              va_arg(ap, char **),
                                              &field_size);
            break;

        case RMQ_FIELD_LONG_STRING:
            ret = rmq_field_read_long_string(ptr, len,
                                             va_arg(ap, char **),
                                             &field_size);
            break;

        case RMQ_FIELD_ARRAY:
            ret = rmq_field_read_array(ptr, len,
                                       va_arg(ap, struct c_ptr_vector **),
                                       &field_size);
            break;

        case RMQ_FIELD_TIMESTAMP:
            ret = rmq_field_read_timestamp(ptr, len, va_arg(ap, uint64_t *),
                                           &field_size);
            break;

        case RMQ_FIELD_TABLE:
            ret = rmq_field_read_table(ptr, len,
                                       va_arg(ap, struct rmq_field_table **),
                                       &field_size);
            break;

        case RMQ_FIELD_NO_VALUE:
            ret = rmq_field_read_no_value(ptr, len, &field_size);
            break;

        case RMQ_FIELD_END:
            assert(false);
            break;
        }

        if (ret == -1)
            goto error;

        ptr += field_size;
        len -= field_size;

        nb_fields++;
    }

    va_end(ap);

    if (psz)
        *psz = size - len;
    return 0;

error:
    va_start(ap, psz);
    for(size_t i = 0; i < nb_fields; i++) {
        enum rmq_field_type type;
        struct rmq_field_table **ptable;
        struct c_ptr_vector **pfields;
        char **pstring;

        type = va_arg(ap, enum rmq_field_type);
        if (type == RMQ_FIELD_END)
            break;

        switch (type) {
        case RMQ_FIELD_BOOLEAN:
        case RMQ_FIELD_SHORT_SHORT_INT:
        case RMQ_FIELD_SHORT_SHORT_UINT:
        case RMQ_FIELD_SHORT_INT:
        case RMQ_FIELD_SHORT_UINT:
        case RMQ_FIELD_LONG_INT:
        case RMQ_FIELD_LONG_UINT:
        case RMQ_FIELD_LONG_LONG_INT:
        case RMQ_FIELD_LONG_LONG_UINT:
        case RMQ_FIELD_FLOAT:
        case RMQ_FIELD_DOUBLE:
        case RMQ_FIELD_DECIMAL:
            break;

        case RMQ_FIELD_SHORT_STRING:
        case RMQ_FIELD_LONG_STRING:
            pstring = va_arg(ap, char **);
            c_free(*pstring);
            *pstring = NULL;
            break;

        case RMQ_FIELD_ARRAY:
            pfields = va_arg(ap, struct c_ptr_vector **);

            for (size_t j = 0; j < c_ptr_vector_length(*pfields); j++)
                rmq_field_delete(c_ptr_vector_entry(*pfields, j));
            c_ptr_vector_delete(*pfields);

            *pfields = NULL;
            break;

        case RMQ_FIELD_TIMESTAMP:
            break;

        case RMQ_FIELD_TABLE:
            ptable = va_arg(ap, struct rmq_field_table **);
            rmq_field_table_delete(*ptable);
            *ptable = NULL;
            break;

        case RMQ_FIELD_NO_VALUE:
            break;

        case RMQ_FIELD_END:
            assert(false);
            break;
        }
    }
    va_end(ap);

    return -1;
}

void
rmq_fields_write(struct c_buffer *buf, ...) {
    /* TODO */
}

/* ---------------------------------------------------------------------------
 *  Field table
 * ------------------------------------------------------------------------ */
void
rmq_field_pair_init(struct rmq_field_pair *pair) {
    memset(pair, 0, sizeof(struct rmq_field_pair));
}

void
rmq_field_pair_free(struct rmq_field_pair *pair) {
    if (!pair)
        return;

    c_free(pair->name);
    rmq_field_delete(pair->value);

    memset(pair, 0, sizeof(struct rmq_field_pair));
}

struct rmq_field_table *
rmq_field_table_new(void) {
    struct rmq_field_table *table;

    table = c_malloc0(sizeof(struct rmq_field_table));

    table->pairs = c_vector_new(sizeof(struct rmq_field_pair));

    return table;
}

void
rmq_field_table_delete(struct rmq_field_table *table) {
    if (!table)
        return;

    for (size_t i = 0; i < c_vector_length(table->pairs); i++)
        rmq_field_pair_free(c_vector_entry(table->pairs, i));
    c_vector_delete(table->pairs);

    c_free0(table, sizeof(struct rmq_field_table));
}

void
rmq_field_table_append_nocopy(struct rmq_field_table *table,
                              char *name, struct rmq_field *value) {
    struct rmq_field_pair pair;

    rmq_field_pair_init(&pair);
    pair.name = name;
    pair.value = value;

    c_vector_append(table->pairs, &pair);
}

/* ---------------------------------------------------------------------------
 *  Frame
 * ------------------------------------------------------------------------ */
void
rmq_frame_init(struct rmq_frame *frame) {
    memset(frame, 0, sizeof(struct rmq_frame));
}

int
rmq_frame_read(struct rmq_frame *frame, const void *data, size_t size,
               size_t *psz) {
    const uint8_t *ptr;
    size_t len;

    ptr = data;
    len = size;

    if (len < 7)
        return 0;

    rmq_frame_init(frame);

    /* Header */
    frame->type = rmq_read_u8(ptr);
    frame->channel = rmq_read_u16(ptr + 1);
    frame->size = rmq_read_u32(ptr + 3);

    ptr += 7;
    len -= 7;

    /* Payload */
    if (len < frame->size)
        return 0;

    frame->payload = ptr;

    ptr += frame->size;
    len -= frame->size;

    /* Frame end */
    if (len < 1)
        return 0;

    frame->end = ptr[0];

    ptr += 1;
    len -= 1;

    *psz = size - len;
    return 1;
}

void
rmq_frame_write(struct rmq_frame *frame, struct c_buffer *buf) {
    uint8_t *ptr;

    ptr = c_buffer_reserve(buf, 7 + frame->size + 1);

    rmq_write_u8(frame->type, ptr);
    rmq_write_u16(frame->channel, ptr + 1);
    rmq_write_u32(frame->size, ptr + 3);

    if (frame->payload)
        memcpy(ptr + 7, frame->payload, frame->size);

    ptr[7 + frame->size] = RMQ_FRAME_END;

    c_buffer_increase_length(buf, 7 + frame->size + 1);
}

/* ---------------------------------------------------------------------------
 *  Method
 * ------------------------------------------------------------------------ */
const char *
rmq_method_to_string(enum rmq_method method) {
    static const char *strings[] = {
        [RMQ_METHOD_CONNECTION_START]     = "Connection.Start",
        [RMQ_METHOD_CONNECTION_START_OK]  = "Connection.Start-Ok",
        [RMQ_METHOD_CONNECTION_SECURE]    = "Connection.Secure",
        [RMQ_METHOD_CONNECTION_SECURE_OK] = "Connection.Secure-Ok",
        [RMQ_METHOD_CONNECTION_TUNE]      = "Connection.Tune",
        [RMQ_METHOD_CONNECTION_TUNE_OK]   = "Connection.Tune-Ok",
        [RMQ_METHOD_CONNECTION_OPEN]      = "Connection.Open",
        [RMQ_METHOD_CONNECTION_OPEN_OK]   = "Connection.Open-Ok",
        [RMQ_METHOD_CONNECTION_CLOSE]     = "Connection.Close",
        [RMQ_METHOD_CONNECTION_CLOSE_OK]  = "Connection.Close-Ok",
    };
    static size_t nb_strings = sizeof(strings) / sizeof(strings[0]);

    if (method >= nb_strings)
        return NULL;

    return strings[method];
}

void
rmq_method_frame_init(struct rmq_method_frame *method) {
    memset(method, 0, sizeof(struct rmq_method_frame));
}

int
rmq_method_frame_read(struct rmq_method_frame *method,
                      const struct rmq_frame *frame) {
    const uint8_t *ptr;
    size_t len;

    ptr = frame->payload;
    len = frame->size;

    if (len < 4) {
        c_set_error("truncated payload");
        return -1;
    }

    rmq_method_frame_init(method);

    method->class_id = rmq_read_u16(ptr);
    method->method_id = rmq_read_u16(ptr + 2);

    ptr += 4;
    len -= 4;

    method->args = ptr;
    method->args_sz = len;

    return 0;
}

/* ---------------------------------------------------------------------------
 *  Internals
 * ------------------------------------------------------------------------ */
static uint8_t
rmq_read_u8(const uint8_t *ptr) {
    return ptr[0];
}

static uint16_t
rmq_read_u16(const uint8_t *ptr) {
    return (ptr[0] << 8) | ptr[1];
}

static uint32_t
rmq_read_u32(const uint8_t *ptr) {
    return ((uint32_t)ptr[0] << 24)
         | ((uint32_t)ptr[1] << 16)
         | ((uint32_t)ptr[2] <<  8)
         |  (uint32_t)ptr[3];
}

static uint64_t
rmq_read_u64(const uint8_t *ptr) {
    return  ((uint64_t)ptr[0] << 56)
          | ((uint64_t)ptr[1] << 48)
          | ((uint64_t)ptr[2] << 40)
          | ((uint64_t)ptr[3] << 32)
          | ((uint64_t)ptr[4] << 24)
          | ((uint64_t)ptr[5] << 16)
          | ((uint64_t)ptr[6] <<  8)
          |  (uint64_t)ptr[7];
}

static void
rmq_write_u8(uint8_t value, uint8_t *ptr) {
    ptr[0] = value;
}

static void
rmq_write_u16(uint16_t value, uint8_t *ptr) {
    ptr[0] = (value & 0xff00) >> 8;
    ptr[1] =  value & 0x00ff;
}

static void
rmq_write_u32(uint32_t value, uint8_t *ptr) {
    ptr[0] = (value & 0xff000000) >> 24;
    ptr[1] = (value & 0x00ff0000) >> 16;
    ptr[2] = (value & 0x0000ff00) >>  8;
    ptr[3] =  value & 0x000000ff;
}

static void
rmq_write_u64(uint64_t value, uint8_t *ptr) {
    ptr[0] = (value & 0xff00000000000000) >> 56;
    ptr[1] = (value & 0x00ff000000000000) >> 48;
    ptr[2] = (value & 0x0000ff0000000000) >> 40;
    ptr[3] = (value & 0x000000ff00000000) >> 32;
    ptr[4] = (value & 0x00000000ff000000) >> 24;
    ptr[5] = (value & 0x0000000000ff0000) >> 16;
    ptr[6] = (value & 0x000000000000ff00) >>  8;
    ptr[7] =  value & 0x00000000000000ff;
}
