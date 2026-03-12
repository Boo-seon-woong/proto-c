CC ?= cc
CFLAGS ?= -O2 -g
WARNFLAGS := -Wall -Wextra -Wpedantic -Werror
CPPFLAGS += -Iinclude
LDLIBS += $(shell pkg-config --libs openssl librdmacm)
PKG_CFLAGS := $(shell pkg-config --cflags openssl librdmacm)

SRC := \
	src/jansson.c \
	src/common.c \
	src/models.c \
	src/config.c \
	src/crypto.c \
	src/rpc.c \
	src/rdma_rpc.c \
	src/tdx_runtime.c \
	src/mn/node.c \
	src/cn/node.c \
	src/cn/cache_rpc.c \
	src/cn/commit.c \
	src/cn/insert.c \
	src/cn/write.c \
	src/cn/update.c \
	src/cn/delete.c \
	src/cn/read.c \
	src/cn/snapshot.c \
	src/main.c

OBJ := $(SRC:.c=.o)
TEST_SRC := tests/test_kvs.c
TEST_OBJ := $(TEST_SRC:.c=.o)

BIN_DIR := bin
APP := $(BIN_DIR)/kvs
TEST_APP := $(BIN_DIR)/kvs-test

.PHONY: all clean test

all: $(APP) $(TEST_APP)

$(APP): $(OBJ) | $(BIN_DIR)
	$(CC) $(CFLAGS) $(WARNFLAGS) $(PKG_CFLAGS) -o $@ $(OBJ) $(LDLIBS) -lpthread

$(TEST_APP): $(filter-out src/main.o,$(OBJ)) $(TEST_OBJ) | $(BIN_DIR)
	$(CC) $(CFLAGS) $(WARNFLAGS) $(PKG_CFLAGS) -o $@ $(filter-out src/main.o,$(OBJ)) $(TEST_OBJ) $(LDLIBS) -lpthread

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

%.o: %.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARNFLAGS) $(PKG_CFLAGS) -c $< -o $@

test: $(TEST_APP)
	./$(TEST_APP)

clean:
	rm -f $(OBJ) $(TEST_OBJ) $(APP) $(TEST_APP)
