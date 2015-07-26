# Common
prefix= /usr/local
libdir= $(prefix)/lib
incdir= $(prefix)/include
bindir= $(prefix)/bin

CC= clang

CFLAGS+= -std=c99
CFLAGS+= -Wall -Wextra -Werror -Wsign-conversion
CFLAGS+= -Wno-unused-parameter -Wno-unused-function

LDFLAGS=

LDLIBS= -lm -lpcre

PANDOC_OPTS= -s --toc --email-obfuscation=none

# Platform specific
platform= $(shell uname -s)

ifeq ($(platform), Linux)
	CFLAGS+= -DRMQ_PLATFORM_LINUX
	CFLAGS+= -D_POSIX_C_SOURCE=200809L
endif

# Debug
debug=0
ifeq ($(debug), 1)
	CFLAGS+= -g -ggdb
else
	CFLAGS+= -O2
endif

# Coverage
coverage?= 0
ifeq ($(coverage), 1)
	CC= gcc
	CFLAGS+= -fprofile-arcs -ftest-coverage
	LDFLAGS+= --coverage
endif

# Target: librabbitmq
librabbitmq_LIB= librabbitmq.a
librabbitmq_SRC= $(wildcard src/*.c)
librabbitmq_INC= $(wildcard src/*.h)
librabbitmq_PUBINC= src/rabbitmq.h
librabbitmq_OBJ= $(subst .c,.o,$(librabbitmq_SRC))

$(librabbitmq_LIB): CFLAGS+=

# Target: tests
tests_SRC= $(wildcard tests/*.c)
tests_OBJ= $(subst .c,.o,$(tests_SRC))
tests_BIN= $(subst .o,,$(tests_OBJ))

$(tests_BIN): CFLAGS+= -Isrc
$(tests_BIN): LDFLAGS+= -L.
$(tests_BIN): LDLIBS+= -lrabbitmq -lio -lcore -lutest -lssl -lcrypto

# Target: utils
utils_SRC= $(wildcard utils/*.c)
utils_OBJ= $(subst .c,.o,$(utils_SRC))
utils_BIN= $(subst .o,,$(utils_OBJ))

$(utils_BIN): CFLAGS+= -Isrc
$(utils_BIN): LDFLAGS+= -L.
$(utils_BIN): LDLIBS+= -lrabbitmq -lio -lcore -lssl -lcrypto

# Target: doc
doc_SRC= $(wildcard doc/*.mkd)
doc_HTML= $(subst .mkd,.html,$(doc_SRC))

# Rules
all: lib tests utils doc

lib: $(librabbitmq_LIB)

tests: lib $(tests_BIN)

utils: lib $(utils_BIN)

doc: $(doc_HTML)

$(librabbitmq_LIB): $(librabbitmq_OBJ)
	$(AR) cr $@ $(librabbitmq_OBJ)

$(tests_OBJ): $(librabbitmq_LIB) $(librabbitmq_INC)
tests/%: tests/%.o
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

$(utils_OBJ): $(librabbitmq_LIB) $(librabbitmq_INC)
utils/%: utils/%.o
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

doc/%.html: doc/*.mkd
	pandoc $(PANDOC_OPTS) -t html5 -o $@ $<

clean:
	$(RM) $(librabbitmq_LIB) $(wildcard src/*.o)
	$(RM) $(tests_BIN) $(wildcard tests/*.o)
	$(RM) $(utils_BIN) $(wildcard utils/*.o)
	$(RM) $(wildcard **/*.gc??)
	$(RM) -r coverage
	$(RM) -r $(doc_HTML)

coverage:
	lcov -o /tmp/librabbitmq.info -c -d . -b .
	genhtml -o coverage -t librabbitmq /tmp/librabbitmq.info
	rm /tmp/librabbitmq.info

install: lib
	mkdir -p $(libdir) $(incdir) $(bindir)
	install -m 644 $(librabbitmq_LIB) $(libdir)
	install -m 644 $(librabbitmq_PUBINC) $(incdir)
	install -m 755 $(utils_BIN) $(bindir)

uninstall:
	$(RM) $(addprefix $(libdir)/,$(librabbitmq_LIB))
	$(RM) $(addprefix $(incdir)/,$(librabbitmq_PUBINC))
	$(RM) $(addprefix $(bindir)/,$(utils_BIN))

tags:
	ctags -o .tags -a $(wildcard src/*.[hc])

.PHONY: all lib tests doc clean coverage install uninstall tags
