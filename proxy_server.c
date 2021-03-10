#include <stdio.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <string.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>

//pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;

typedef struct MASTER{
    int fd;
    char* req_msg;
    struct sockaddr_in m_addr;
    int size;
}master;

typedef struct client{
    master *m;
    int fd;
    char* client_buf;
    struct sockaddr_in c_addr;
    int size;
}client;

typedef struct server{
    int fd;
	int client_fd;
    unsigned int sin_size;
    struct sockaddr_in s_addr, c_addr;
}server;

server s;

void masterFailover(client *c);

int createSock(struct server *s, char *port){

    int size;
    int opt=1;
    s->fd=socket(AF_INET, SOCK_STREAM, 0);
    setsockopt(s->fd, SOL_SOCKET, SO_REUSEADDR, (void *)&opt, 4);
    s->s_addr.sin_family = AF_INET;
    s->s_addr.sin_port = htons(atoi(port));
    s->s_addr.sin_addr.s_addr==htonl(INADDR_ANY);
    memset(&(s->s_addr.sin_zero), 0, 8);

    if(bind(s->fd, (struct sockaddr*)&(s->s_addr), sizeof(struct sockaddr)) < 0){
        perror("bind");
        return 1;
    }
    if(listen(s->fd, 5) < 0 ){
        perror("listen");
        return 1;
    }
    s->sin_size = sizeof(struct sockaddr_in);

    return 0;
}

void *createClient(client *c){

    char *port="3000";

    int size;
    int opt=1;
    c->m->fd=socket(AF_INET, SOCK_STREAM, 0);
    setsockopt(c->m->fd, SOL_SOCKET, SO_REUSEADDR, (void *)&opt, 4);
    c->m->m_addr.sin_family = AF_INET;
    c->m->m_addr.sin_port = htons(atoi(port));
    c->m->m_addr.sin_addr.s_addr==inet_addr("127.0.0.1");
    memset(&(c->m->m_addr.sin_zero), 0, 8);

    if(connect(c->m->fd, (struct sockaddr *)&(c->m->m_addr), sizeof(struct sockaddr_in)) < 0){
        perror("connect");
    }

    printf("Master Connected\n");
}


void revalMaster(client *c){
    char *port = "3000";

    int size;
    int opt=1;
    c->m->fd=socket(AF_INET, SOCK_STREAM, 0);
    setsockopt(c->m->fd, SOL_SOCKET, SO_REUSEADDR, (void *)&opt, 4);
    c->m->m_addr.sin_family = AF_INET;
    c->m->m_addr.sin_port = htons(atoi(port));
    c->m->m_addr.sin_addr.s_addr==inet_addr("127.0.0.1");
    memset(&(c->m->m_addr.sin_zero), 0, 8);

    if(connect(c->m->fd, (struct sockaddr *)&(c->m->m_addr), sizeof(struct sockaddr_in)) < 0){
        perror("connect");
      //  masterFailover(c);
    }

    printf("DRAM Master Connected\n");
}


void masterFailover(client *c){
    char *port = "3001";

    int size;
    int opt=1;
    c->m->fd=socket(AF_INET, SOCK_STREAM, 0);
    setsockopt(c->m->fd, SOL_SOCKET, SO_REUSEADDR, (void *)&opt, 4);
    c->m->m_addr.sin_family = AF_INET;
    c->m->m_addr.sin_port = htons(atoi(port));
    c->m->m_addr.sin_addr.s_addr==inet_addr("127.0.0.1");
    memset(&(c->m->m_addr.sin_zero), 0, 8);

    if(connect(c->m->fd, (struct sockaddr *)&(c->m->m_addr), sizeof(struct sockaddr_in)) < 0){
        perror("connect");
        revalMaster(c);
    }

    printf("NVM Master Connected\n");
}


void *Thread_main(void *arg){

    client *c;
    pthread_t m2c_th;

    c=malloc(sizeof(struct client));
    c->m=malloc(sizeof(struct MASTER));
    c->m->req_msg = malloc(17179869184);
    c->client_buf = malloc(17179869184);
	c->fd=*(int *)arg;

    free(arg);

    long long buf_len=0;
    long long master_buf_len=0;

    ssize_t failover_flag;

    createClient(c); //master구조체도 포함
    int count=0;

    while(1){
        c->size = recv(c->fd, c->client_buf+buf_len, 16*1024, 0);
        
		if(c->size ==0){
			continue;
		}
        
		if(c->size == -1){
			break;
		}

        failover_flag = send(c->m->fd, c->client_buf+buf_len, c->size, 0);
        if(failover_flag == -1){
            masterFailover(c);

            buf_len -=c->size;
            failover_flag=send(c->m->fd, c->client_buf+buf_len, c->size, 0);
            buf_len += c->size;
        }

        buf_len +=c->size;

		c->m->size = recv(c->m->fd, c->m->req_msg+master_buf_len, 16*1024, 0);
        if(c->m->size == -1 && count <=2){
            masterFailover(c);

            buf_len -=c->size;
            failover_flag=send(c->m->fd, c->client_buf+buf_len, c->size, 0);

            buf_len += c->size;
            c->m->size = recv(c->m->fd, c->m->req_msg+master_buf_len, 16*1024,0);
            count++;
        } 

        if(strcmp(c->m->req_msg+master_buf_len-c->m->size,"+OK\r\n")){
            if(count>=3){
                revalMaster(c);

                buf_len -= c->size;
                failover_flag=send(c->m->fd, c->client_buf+buf_len, c->size, 0);

                buf_len += c->size;
                c->m->size = recv(c->m->fd, c->m->req_msg+master_buf_len, 16*1024,0);
                count=0;
            }
        }


       // printf("c->m->size %d\n", c->m->size);

		send(c->fd, c->m->req_msg+master_buf_len, c->m->size, 0);
        master_buf_len+=c->m->size;
       // printf("insert data count %d\n", count);

    }
    free(c->m->req_msg);
    free(c->client_buf);
    free(c->m);
    free(c);
    return NULL;
}

int main(int argc, char *argv[]){
    int *arg;
    pthread_t t_id;


    if(createSock(&s, argv[1]) == 1){
        return 0;
    }

    while(1){
        s.client_fd = accept(s.fd, (struct sockaddr *)&(s.c_addr), &(s.sin_size));
		printf("%d\n", s.client_fd);
        if(s.client_fd < 0 ){
            perror("accept");
            return 0;
        }
        arg=(int *)malloc(sizeof(int));
        *(int *)arg=s.client_fd;
        pthread_create(&t_id, NULL, Thread_main, arg);
        pthread_detach(t_id);
    }
    close(s.fd);

    return 0;
}

