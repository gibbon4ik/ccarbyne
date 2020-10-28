CC=gcc
CFLAGS=-O2 -Wall -Wextra -Wno-unused-parameter -pthread
#CFLAGS+=-ggdb -g
LDFLAGS=
SOURCES=carbyne.c net.c murmur_hash2.c palloc.c
OBJECTS=$(SOURCES:.c=.o)
EXECUTABLE=ccarbyne

all: $(EXECUTABLE)

$(EXECUTABLE): $(SOURCES)
	$(CC) $(CFLAGS) $(LDFLAGS) $(SOURCES) -o $@

clean:
	rm -rf $(EXECUTABLE) core
