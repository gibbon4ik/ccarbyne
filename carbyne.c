#define _GNU_SOURCE // for recvmmsg

#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>

#include "common.h"
#include "palloc.h"

#define MTU_SIZE (2048-64*2)
#define MAX_MSG 64
#define STQUEUE_MAX 64

#define PROGNAME "ccarbyne v0.2"
#define RCVBUF_SIZE (1024*1024)
#define SNDBUF_SIZE (256*1024)

struct state {
	int fd;
	struct mmsghdr messages[MAX_MSG];
	char buffers[MAX_MSG][MTU_SIZE];
	struct iovec iovecs[MAX_MSG];
};


struct point {
	double value;
	unsigned int count;
	unsigned long long tm;
	char key[];
};

#include "murmur_hash2.c"
#define mh_name _pt
#define mh_key_t char *
#define mh_val_t struct point *
#define mh_hash(h, key) ({ MurmurHash2((key), strlen(key), 13); })
#define mh_eq(h, a, b) ({ strcmp((a), (b)) == 0; })
#define MH_STATIC
#include "mhash.h"

struct st {
	struct mh_pt_t *h;
	struct palloc_pool *p;
	unsigned long long tm;
};


struct st *total;
struct st *stq[STQUEUE_MAX];
int stqlast = 0;
pthread_mutex_t plock;
pthread_mutex_t qlock;

struct net_addr listen_addr;
struct net_addr remote_addr;

int interval = 60;
uint8_t use_tcp = 0;

volatile uint64_t getmetrics = 0;
volatile uint64_t errmetrics = 0;
volatile uint64_t sendmetrics = 0;

struct st * st_create()
{
	struct st *new = malloc(sizeof(*new));
	new->h = mh_pt_init(NULL);
	pthread_mutex_lock(&plock);
	new->p = palloc_create_pool((struct palloc_config){.name = "st"});
	pthread_mutex_unlock(&plock);
	new->tm = time(NULL);
	return new;
}

void st_destroy(struct st *st)
{
	mh_pt_destroy(st->h);
	pthread_mutex_lock(&plock);
	palloc_destroy_pool(st->p);
	pthread_mutex_unlock(&plock);
	free(st);
}

int stq_push(struct st *st)
{
	int r = 0;
	pthread_mutex_lock(&qlock);
	if (stqlast < STQUEUE_MAX) {
		stq[stqlast++] = st;
		r = 1;
	}
	pthread_mutex_unlock(&qlock);
	return r;

}

struct st * stq_pop()
{
	struct st *p = NULL;
	pthread_mutex_lock(&qlock);
	if (stqlast > 0) {
		p = stq[--stqlast];
	}
	pthread_mutex_unlock(&qlock);
	return p;
}

/***************************************************************************/

void add_point(struct st *st, char *key, double value, unsigned long long tm, unsigned int count)
{
	if (st == NULL || st->p == NULL)
		return;

	struct point *pt;
	uint32_t k = mh_pt_get(st->h, key);
	if (k != mh_end(st->h)) {
		pt = mh_pt_value(st->h, k);
	} else {
		int len = strlen(key);
		if (len > 512)
			return;

		pthread_mutex_lock(&plock);
		pt = palloc(st->p, sizeof(struct point) + len + 1);
		pthread_mutex_unlock(&plock);
		memset(pt, 0, sizeof(*pt));
		strcpy(pt->key, key);

		mh_pt_put(st->h, pt->key, pt, NULL);
	}

	pt->value += value;
	pt->tm += tm;
	pt->count += count;
}

int sock()
{
	int fd;
	int socktype = use_tcp ? SOCK_STREAM : SOCK_DGRAM;

	if ((fd = socket(AF_INET, socktype, 0)) == -1) {
		PFATAL("socket");
	}

	/*
	int nonblock = 1;
	if (ioctl(fd, FIONBIO, &nonblock) < 0) {
		PFATAL("ioctl");
	}
	*/
	if (connect(fd, remote_addr.sockaddr, remote_addr.sockaddr_len) < 0) {
		if (errno != EINPROGRESS) {
			PFATAL("connect");
		}
	}
	net_set_buffer_size(fd, SNDBUF_SIZE, 1);
	return fd;
}

/* ether mtu - ip header - udp header */
#define CARBON_LEN (1500 - 20 - 8)
#define OBUF_LEN (64 * 1024)
struct obuf {
	char buf[OBUF_LEN + 1];
	int len;
	int fd;
};

void flush(struct obuf *o)
{
	if (o->fd < 0)
		o->len = 0;
	if (o->len == 0)
		return;

	char *ptr = o->buf;
	do {
		int r = send(o->fd, ptr, o->len, 0);
		if (r < 0) {
			if (errno == EINTR || errno == EAGAIN) {
				usleep(1000);
				continue;
			}
			if (use_tcp) {
				ERRORF("error: tcp send [%d]\n", errno);
				close(o->fd);
				o->fd = -1;
			}
			o->len = 0;
			break;
		}
		o->len -= r;
		ptr += r;
	} while (o->len > 0);
}

void xmit(struct obuf *o, const char *metric, double value, unsigned long long tm)
{
	const int packet_len = use_tcp ? OBUF_LEN : CARBON_LEN;
	int r = 0;
again:
	r = snprintf(o->buf + o->len, packet_len - o->len + 1,
		     "%s %f %llu\n",
		     metric, value, tm);

	assert(r < CARBON_LEN);
	if (r > packet_len - o->len) {
		flush(o);
		goto again;
	}
	o->len += r;
}

char kbuf[520];
void dumper(struct st *st, struct obuf *o)
{
	struct st *all = st_create();
	struct mh_pt_t *h;
	if (o->fd < 0) {
		o->fd = sock();
		o->len = 0;
	}

	h = st->h;
	all->tm = 0;

	mh_foreach(_pt, h, i) {
		struct point *pt = mh_pt_value(h, i);

		pt->tm /= pt->count;

		if (strstr(pt->key, ".count.") != NULL) {
			xmit(o, pt->key, pt->value, pt->tm);
			__atomic_fetch_add(&sendmetrics, 1, 0);
			strcpy(kbuf, pt->key);
			char *rpoint = strrchr(kbuf, '.');
			if (rpoint) {
				strcpy(rpoint, ".all");
				add_point(all, kbuf, pt->value, pt->tm, 1);
			}
		}
		if (strstr(pt->key, ".avg.") != NULL) {
			xmit(o, pt->key, pt->value / pt->count, pt->tm);
			__atomic_fetch_add(&sendmetrics, 1, 0);
		}
	}

	h = all->h;
	mh_foreach(_pt, h, i) {
		struct point *pt = mh_pt_value(h, i);
		pt->tm /= pt->count;
		xmit(o, pt->key, pt->value, pt->tm);
		__atomic_fetch_add(&sendmetrics, 1, 0);
	}

	flush(o);
	st_destroy(st);
	st_destroy(all);
}


struct state *state_init(struct state *s)
{
	int i;
	for (i = 0; i < MAX_MSG; i++) {
		char *buf = &s->buffers[i][0];
		struct iovec *iovec = &s->iovecs[i];
		struct mmsghdr *msg = &s->messages[i];

		msg->msg_hdr.msg_iov = iovec;
		msg->msg_hdr.msg_iovlen = 1;

		iovec->iov_base = buf;
		iovec->iov_len = MTU_SIZE;
	}
	return s;
}

void aggregator_loop(void *userdata) {
	static __thread struct st *st;
	static __thread struct mh_pt_t *h;

	struct obuf *o = malloc(sizeof(*o));
	o->fd = - 1;
	o->len = 0;

	total = st_create();
	time_t nextdump = (time(NULL) / interval) * interval + interval;

	while (1) {
		st = stq_pop();
		if (st == NULL) {
			if (time(NULL) >= nextdump) {
				nextdump = time(NULL) + interval;
				dumper(total, o);
				total = st_create();
			}
			usleep(1000);
			continue;
		}

		h = st->h;
		mh_foreach(_pt, h, i) {
			struct point *pt = mh_pt_value(h, i);
			add_point(total, pt->key, pt->value, pt->tm, pt->count);
		}
		st_destroy(st);
	}
	close(o->fd);
	free(o);
}

void listener_loop(void *userdata)
{
	struct state *state = userdata;
	struct st *map = st_create();
	struct timespec timeout = { .tv_sec = 1, .tv_nsec = 0 };
	time_t nextsec = time(NULL) + 1;

	while (1) {
		if (time(NULL) >= nextsec) {
			nextsec = time(NULL) + 1;
			if (stq_push(map))
				map = st_create();
		}

		/* Blocking recv. */
		int r = recvmmsg(state->fd, &state->messages[0], MAX_MSG, MSG_WAITFORONE, &timeout);
		if (r <= 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
				continue;
			}
			PFATAL("recvmmsg()");
		}

		int i, bytes = 0;
		for (i = 0; i < r; i++) {
			struct mmsghdr *msg = &state->messages[i];
			int len = msg->msg_len;
			msg->msg_hdr.msg_flags = 0;
			msg->msg_len = 0;
			bytes += len;
			char *buf = msg->msg_hdr.msg_iov->iov_base;
			char *line, *save1, *save2;

			buf[len] = 0;
			for (save1 = NULL, line = strtok_r(buf, "\n", &save1); line; line = strtok_r(NULL, "\n", &save1)) {
				save2 = NULL;
				char *key = strtok_r(line, " ", &save2);
				char *value = strtok_r(NULL, " ", &save2);
				char *time = strtok_r(NULL, " ", &save2);

				if (!key || !value || !time) {
					__atomic_fetch_add(&errmetrics, 1, 0);
					continue;
				}
				__atomic_fetch_add(&getmetrics, 1, 0);


				double val = atof(value);
				unsigned long long tm = strtoull(time, NULL, 10);
				add_point(map, key, val, tm, 1);
			}
		}
	}
}

int main(int argc, char *argv[])
{
	const char *listen_addr_str = "127.0.0.1:2023";
	const char *remote_addr_str = "127.0.0.1:2003";
	const char *stat_addr_str = NULL;
	struct net_addr stat_addr;
	fd_set active_fd_set, read_fd_set;
	char buffer[1000];
	int stat_fd = -1;
	int thread_num = 1;
	char *progname = argv[0];
	int c, i;
	int errflg = 0;

	while ((c = getopt(argc, argv, "hl:r:s:w:i:t")) != -1) {
		switch(c) {
			case 'l':
				listen_addr_str = optarg;
				break;
			case 'r':
				remote_addr_str = optarg;
				break;
			case 's':
				stat_addr_str = optarg;
				break;
			case 'w':
				thread_num = atoi(optarg);
				if (thread_num < 1 || thread_num > 32) {
					ERRORF("workers number must between 1 and 32\n");
					errflg++;
				}
				break;
			case 'i':
				interval = atoi(optarg);
				if (interval < 1 || interval > 300) {
					ERRORF("interval must between 1 and 300\n");
					errflg++;
				}
				break;
			case 't':
				use_tcp = 1;
				break;
			case 'h':
				errflg++;
				break;
			case ':':
				ERRORF("Option -%c requires an operand\n", optopt);
				errflg++;
				break;
			case '?':
				ERRORF("Unrecognized option: '-%c'\n", optopt);
				errflg++;
		}
	}
	if (errflg) {
		fprintf(stderr, PROGNAME"\n\
usage: %s options\n\
  -i int\n\
    	Interval is seconds between aggregate data dump (default %d)\n\
  -l string\n\
    	Listen on udp host:port (default \"%s\")\n\
  -r string\n\
    	Send to host:port (default \"%s\")\n\
  -s string\n\
    	Listen for stat on tcp host:port\n\
  -w int\n\
    	Number of workers (default %d)\n\
  -t\n\
    	Use TCP outbound connection (default UDP)\n",
		progname, interval, listen_addr_str, remote_addr_str, thread_num);
		exit(EXIT_FAILURE);
	}

	parse_addr(&listen_addr, listen_addr_str);
	parse_addr(&remote_addr, remote_addr_str);

	struct state *array_of_states = calloc(thread_num, sizeof(struct state));

	if (pthread_mutex_init(&plock, NULL) != 0) {
		FATAL("plock mutex init failed");
	}

	if (pthread_mutex_init(&qlock, NULL) != 0) {
		FATAL("qlock mutex init failed");
	}

	ERRORF("Start %s\n", PROGNAME);
	thread_spawn(aggregator_loop, NULL);

	int t;
	for (t = 0; t < thread_num; t++) {
		struct state *state = &array_of_states[t];
		state_init(state);
		int fd = net_bind_udp(&listen_addr, 1);
		net_set_buffer_size(fd, RCVBUF_SIZE, 0);
		state->fd = fd;
		thread_spawn(listener_loop, state);
	}

	/* Initialize the set of active sockets. */
	FD_ZERO (&active_fd_set);
	if (stat_addr_str != NULL) {
		parse_addr(&stat_addr, stat_addr_str);
		stat_fd = net_bind_tcp(&stat_addr);
		c = listen(stat_fd, 10);
		if (c == -1) {
			PFATAL("listen()");
		}
		FD_SET (stat_fd, &active_fd_set);
	}

	struct timeval timeout =
		NSEC_TIMEVAL(MSEC_NSEC(1000UL));

	while (1) {
		read_fd_set = active_fd_set;

		int r = select(FD_SETSIZE, &read_fd_set, NULL, NULL, &timeout);
		if (r < 0) {
			PFATAL("select()");
		}
		if (r == 0) {
			continue;
		}

		/* Service all the sockets with input pending. */
		for (i = 0; i < FD_SETSIZE; ++i) {
			if (FD_ISSET (i, &read_fd_set)) {
				if (i == stat_fd) {
					/* Connection request on original socket. */
					int new;
					ssize_t r;
					struct sockaddr_in clientname;
					socklen_t size;

					size = sizeof (clientname);
					new = accept (stat_fd,
							(struct sockaddr *) &clientname,
							&size);
					if (new < 0) {
						perror ("accept");
						exit (EXIT_FAILURE);
					}
					FD_SET (new, &active_fd_set);
					int len = sprintf(buffer, "metrics\t%lu\nerrors\t%lu\nsend\t%lu\n",
							__atomic_load_n(&getmetrics, 0),
							__atomic_load_n(&errmetrics, 0),
							__atomic_load_n(&sendmetrics, 0));
					r = write(new, buffer, len);
					if (r < 0) {
						// ERRORF("write");
					}
					close (new);
					FD_CLR (new, &active_fd_set);
				}
			}
		}
	}

	/*	
	for (t = 0; t < thread_num; t++) {
		struct state *state = &array_of_states[t];
	}
	*/
	pthread_mutex_destroy(&plock);
	pthread_mutex_destroy(&qlock);
	return 0;
}
