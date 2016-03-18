/* -*- Mode: C; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

#include "ngx_http_moz_ingest_module.h"

#include <luasandbox/util/heka_message.h>
#include <luasandbox/util/output_buffer.h>
#include <luasandbox/util/protobuf.h>
#include <ngx_http.h>
#include <stdarg.h>
#include <stdbool.h>
#include <time.h>

static void* ngx_http_moz_ingest_create_main_conf(ngx_conf_t *cf);
static void* ngx_http_moz_ingest_create_loc_conf(ngx_conf_t *cf);
static char* ngx_http_moz_ingest_merge_loc_conf(ngx_conf_t *cf,
                                                void *parent,
                                                void *child);
static ngx_int_t ngx_http_moz_ingest_handler(ngx_http_request_t *r);
static void  ngx_http_moz_ingest_exit_process(ngx_cycle_t *cycle);
static void  ngx_http_moz_ingest_exit_master(ngx_cycle_t *cycle);
static char* ngx_http_moz_ingest(ngx_conf_t *cf, ngx_command_t *cmd,
                                 void *conf);

static ngx_str_t ngx_http_text_plain_type = ngx_string("text/plain");
static ngx_command_t ngx_http_moz_ingest_cmds[] = {

  { ngx_string("moz_ingest"),
    NGX_HTTP_LOC_CONF | NGX_CONF_NOARGS,
    ngx_http_moz_ingest,
    0,
    0,
    NULL },

  { ngx_string("moz_ingest_max_content_size"),
    NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
    ngx_conf_set_size_slot,
    NGX_HTTP_LOC_CONF_OFFSET,
    offsetof(ngx_http_moz_ingest_loc_conf_t, max_content_size),
    NULL },

  { ngx_string("moz_ingest_max_unparsed_uri_size"),
    NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
    ngx_conf_set_size_slot,
    NGX_HTTP_LOC_CONF_OFFSET,
    offsetof(ngx_http_moz_ingest_loc_conf_t, max_unparsed_uri_size),
    NULL },

  { ngx_string("moz_ingest_client_ip"),
    NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG,
    ngx_conf_set_flag_slot,
    NGX_HTTP_LOC_CONF_OFFSET,
    offsetof(ngx_http_moz_ingest_loc_conf_t, client_ip),
    NULL },

  { ngx_string("moz_ingest_header"),
    NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
    ngx_conf_set_str_array_slot,
    NGX_HTTP_LOC_CONF_OFFSET,
    offsetof(ngx_http_moz_ingest_loc_conf_t, headers),
    NULL },

  { ngx_string("moz_ingest_kafka_max_buffer_size"),
    NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
    ngx_conf_set_size_slot,
    NGX_HTTP_LOC_CONF_OFFSET,
    offsetof(ngx_http_moz_ingest_loc_conf_t, max_buffer_size),
    NULL },

  { ngx_string("moz_ingest_kafka_max_buffer_ms"),
    NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
    ngx_conf_set_msec_slot,
    NGX_HTTP_LOC_CONF_OFFSET,
    offsetof(ngx_http_moz_ingest_loc_conf_t, max_buffer_ms),
    NULL },

  { ngx_string("moz_ingest_kafka_batch_size"),
    NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
    ngx_conf_set_size_slot,
    NGX_HTTP_LOC_CONF_OFFSET,
    offsetof(ngx_http_moz_ingest_loc_conf_t, batch_size),
    NULL },

  { ngx_string("moz_ingest_kafka_brokerlist"),
    NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
    ngx_conf_set_str_slot,
    NGX_HTTP_LOC_CONF_OFFSET,
    offsetof(ngx_http_moz_ingest_loc_conf_t, brokerlist),
    NULL },

  { ngx_string("moz_ingest_kafka_topic"),
    NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
    ngx_conf_set_str_slot,
    NGX_HTTP_LOC_CONF_OFFSET,
    offsetof(ngx_http_moz_ingest_loc_conf_t, topic),
    NULL },

  ngx_null_command
};


static ngx_http_module_t ngx_http_moz_ingest_module_ctx = {
  NULL,    /* preconfiguration */
  NULL,    /* postconfiguration */
  ngx_http_moz_ingest_create_main_conf,
  NULL,    /* merge_main_conf */
  NULL,    /* create_srv_conf */
  NULL,    /* merge_srv_conf */
  ngx_http_moz_ingest_create_loc_conf,
  ngx_http_moz_ingest_merge_loc_conf
};


ngx_module_t ngx_http_moz_ingest_module = {
  NGX_MODULE_V1,
  &ngx_http_moz_ingest_module_ctx,  /* module context */
  ngx_http_moz_ingest_cmds,         /* module directives */
  NGX_HTTP_MODULE,                  /* module type */
  NULL,    /* init master */
  NULL,    /* init module */
  NULL,    /* init process */
  NULL,    /* init thread */
  NULL,    /* exit thread */
  ngx_http_moz_ingest_exit_process,   /* clean up kafka producers/topics */
  ngx_http_moz_ingest_exit_master,    /* release the Kafka lib resources */
  NGX_MODULE_V1_PADDING
};


/* todo add delivery confirmation, this callback cannot be used to safely
   finalize the request so it will have to communicate the result back to nginx
   in a different fashion.

static void msg_delivered(rd_kafka_t *rk,
                          void *payload,
                          size_t len,
                          int error_code,
                          void *opaque,
                          void *msg_opaque)
{
  (void)rk;
  (void)payload;
  (void)len;
  (void)opaque;
  ngx_http_request_t *r = msg_opaque;
  if (!error_code) {
    ngx_http_complex_value_t cv;
    ngx_memzero(&cv, sizeof(ngx_http_complex_value_t));
    cv.value.len = 2;
    cv.value.data = (u_char *)"OK";
    ngx_int_t rc = ngx_http_send_response(r,
                                          NGX_HTTP_OK,
                                          &ngx_http_text_plain_type,
                                          &cv);
    ngx_http_finalize_request(r, rc);
  } else {
    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                  "kafka delivery failure: %d - %s", error_code,
                  rd_kafka_err2str(error_code));
    ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
  }
}
*/


static void ngx_http_moz_ingest_exit_process(ngx_cycle_t *cycle)
{
  ngx_http_moz_ingest_main_conf_t *mc =
      ngx_http_cycle_get_module_main_conf(cycle, ngx_http_moz_ingest_module);

  if (mc->topics) {
    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, cycle->log, 0, "freeing %zu topics\n",
                   mc->topics_size);
    for (size_t i = 0; i < mc->topics_size; ++i) {
      rd_kafka_topic_destroy(mc->topics[i]);
    }
    free(mc->topics);
    mc->topics = NULL;
  }

  if (mc->producers) {
    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, cycle->log, 0, "freeing %zu producers\n",
                   mc->producers_size);
    for (size_t i = 0; i < mc->producers_size; ++i) {
      rd_kafka_destroy(mc->producers[i]);
    }
    free(mc->producers);
    mc->producers = NULL;
  }
}


static void ngx_http_moz_ingest_exit_master(ngx_cycle_t *cycle)
{
  rd_kafka_wait_destroyed(1000);
}


static void* ngx_http_moz_ingest_create_main_conf(ngx_conf_t *cf)
{
  ngx_http_moz_ingest_main_conf_t *conf;

  conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_moz_ingest_main_conf_t));
  if (conf == NULL) {
    return NULL;
  }
  return conf;
}


static void* ngx_http_moz_ingest_create_loc_conf(ngx_conf_t *cf)
{
  ngx_http_moz_ingest_loc_conf_t *conf =
      ngx_pcalloc(cf->pool, sizeof(ngx_http_moz_ingest_loc_conf_t));
  if (conf == NULL) return NULL;

  conf->max_content_size      = NGX_CONF_UNSET_SIZE;
  conf->max_unparsed_uri_size = NGX_CONF_UNSET_SIZE;
  conf->client_ip             = NGX_CONF_UNSET;
  conf->headers               = NGX_CONF_UNSET_PTR;

  conf->max_buffer_size   = NGX_CONF_UNSET_SIZE;
  conf->batch_size        = NGX_CONF_UNSET_SIZE;
  conf->max_buffer_ms     = NGX_CONF_UNSET_MSEC;
  // everything else is properly initialized by pcalloc
}


static char*
ngx_http_moz_ingest_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child)
{
  ngx_http_moz_ingest_loc_conf_t *prev = parent;
  ngx_http_moz_ingest_loc_conf_t *conf = child;
  ngx_conf_merge_size_value(conf->max_content_size, prev->max_content_size,
                            1024 * 100);
  ngx_conf_merge_size_value(conf->max_unparsed_uri_size,
                            prev->max_unparsed_uri_size, 256);
  ngx_conf_merge_value(conf->client_ip, prev->client_ip, 1);
  ngx_conf_merge_ptr_value(conf->headers, prev->headers, NGX_CONF_UNSET_PTR);

  ngx_conf_merge_size_value(conf->max_buffer_size, prev->max_buffer_size,
                            NGX_CONF_UNSET_SIZE);
  ngx_conf_merge_msec_value(conf->max_buffer_ms, prev->max_buffer_ms,
                            NGX_CONF_UNSET_MSEC);
  ngx_conf_merge_size_value(conf->batch_size, prev->batch_size,
                            NGX_CONF_UNSET_SIZE);
  ngx_conf_merge_str_value(conf->brokerlist, prev->brokerlist, "");
  ngx_conf_merge_str_value(conf->topic, prev->topic, "");
  return NGX_CONF_OK;
}


static bool
ngx_http_moz_ingest_init_kafka(ngx_http_request_t *r,
                               ngx_http_moz_ingest_loc_conf_t *conf)
{
  ngx_http_moz_ingest_main_conf_t *mc =
      ngx_http_get_module_main_conf(r, ngx_http_moz_ingest_module);

  rd_kafka_conf_t *kconf = rd_kafka_conf_new();
  if (!kconf) {
    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                  "rd_kafka_conf_new failed");
    return false;
  }
  char errstr[512];
  char value[21];
  rd_kafka_conf_res_t rv;
  rv = rd_kafka_conf_set(kconf, "delivery.report.no.poll", "true", errstr,
                         sizeof errstr);
  if (rv) {
    rd_kafka_conf_destroy(kconf);
    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                  "rd_kafka_conf_set failed: %s", errstr);
    return false;
  }
  if (conf->max_buffer_size != NGX_CONF_UNSET_SIZE) {
    snprintf(value, sizeof value, "%zu", conf->max_buffer_size);
    rv = rd_kafka_conf_set(kconf, "queue.buffering.max.messages", value, errstr,
                           sizeof errstr);
    if (rv) {
      rd_kafka_conf_destroy(kconf);
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                    "rd_kafka_conf_set failed: %s", errstr);
      return false;
    }
  }
  if (conf->max_buffer_ms != NGX_CONF_UNSET_MSEC) {
    snprintf(value, sizeof value, "%zu", conf->max_buffer_ms / 1000);
    rv = rd_kafka_conf_set(kconf, "queue.buffering.max.ms", value, errstr,
                           sizeof errstr);
    if (rv) {
      rd_kafka_conf_destroy(kconf);
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                    "rd_kafka_conf_set failed: %s", errstr);
      return false;
    }
  }
  if (conf->batch_size != NGX_CONF_UNSET_SIZE) {
    snprintf(value, sizeof value, "%zu", conf->batch_size);
    rv = rd_kafka_conf_set(kconf, "batch.num.messages", value, errstr,
                           sizeof errstr);
    if (rv) {
      rd_kafka_conf_destroy(kconf);
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                    "rd_kafka_conf_set failed: %s", errstr);
      return false;
    }
  }
  // rd_kafka_conf_set_dr_msg_cb(kconf, msg_delivered);
  rd_kafka_conf_set_dr_msg_cb(kconf, NULL); // disable the callback for now
  rd_kafka_conf_set_log_cb(kconf, NULL); // disable logging

  conf->rk = rd_kafka_new(RD_KAFKA_PRODUCER, kconf, errstr, sizeof errstr);
  if (!conf->rk) {
    rd_kafka_conf_destroy(kconf); // the producer has not taken ownership
    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                  "Failed to create a Kafka producer: %s", errstr);
    return false;
  }

  char brokerlist[conf->brokerlist.len + 1];
  memcpy(brokerlist, conf->brokerlist.data, conf->brokerlist.len + 1);
  if (rd_kafka_brokers_add(conf->rk, conf->brokerlist.data) == 0) {
    rd_kafka_destroy(conf->rk);
    conf->rk = NULL;
    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                  "No valid brokers specified");
    return false;
  }

  char topic[conf->topic.len + 1];
  memcpy(topic, conf->topic.data, conf->topic.len + 1);
  rd_kafka_topic_conf_t *tconf = rd_kafka_topic_conf_new();
  if (!tconf) {
    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                  "rd_kafka_topic_conf_new failed");
    return false;
  }
  conf->rkt = rd_kafka_topic_new(conf->rk, topic, tconf);
  if (!conf->rkt) {
    rd_kafka_topic_conf_destroy(tconf);
    rd_kafka_destroy(conf->rk);
    conf->rk = NULL;
    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                  "Failed to create a Kafka topic: %s", errstr);
    return false;
  }
  rd_kafka_t **ptmp = realloc(mc->producers,
                              sizeof*ptmp * (mc->producers_size + 1));
  if (ptmp) {
    mc->producers = ptmp;
    mc->producers[mc->producers_size++] = conf->rk;
  } else {
    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                  "producers realloc failed");
    return false;
  }

  rd_kafka_topic_t **ttmp = realloc(mc->topics,
                                    sizeof*ttmp * (mc->topics_size + 1));
  if (ttmp) {
    mc->topics = ttmp;
    mc->topics[mc->topics_size++] = conf->rkt;
  } else {
    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                  "topics realloc failed");
    return false;
  }
  return true;
}


static ngx_table_elt_t*
search_headers_in(ngx_http_request_t *r, u_char *name, size_t len)
{
  ngx_list_part_t            *part;
  ngx_table_elt_t            *h;
  ngx_uint_t                  i;

  part = &r->headers_in.headers.part;
  h = part->elts;

  for (i = 0; true; i++) {
    if (i >= part->nelts) {
      if (part->next == NULL) {
        break;
      }
      part = part->next;
      h = part->elts;
      i = 0;
    }
    if (len != h[i].key.len || ngx_strcasecmp(name, h[i].key.data) != 0) {
      continue;
    }
    return &h[i];
  }
  return NULL;
}


static lsb_err_value write_str_field(lsb_output_buffer *ob,
                                     u_char *key, size_t klen,
                                     u_char *value, size_t vlen)
{
  lsb_pb_write_key(ob, LSB_PB_FIELDS, LSB_PB_WT_LENGTH);
  size_t len_pos = ob->pos;
  lsb_pb_write_varint(ob, 0);  // length tbd later
  lsb_pb_write_string(ob, LSB_PB_NAME, key, klen);
  lsb_pb_write_string(ob, LSB_PB_VALUE_STRING, value, vlen);
  return lsb_pb_update_field_length(ob, len_pos);
}


static lsb_err_value write_content_field(lsb_output_buffer *ob,
                                         ngx_http_request_t *r)
{
  lsb_err_value ev = NULL;
  char vint[LSB_MAX_VARINT_BYTES];
  unsigned long long clen = (unsigned long long)r->headers_in.content_length_n;
  int vlen = lsb_pb_output_varint(vint, clen);

  lsb_pb_write_key(ob, LSB_PB_FIELDS, LSB_PB_WT_LENGTH);
  lsb_pb_write_varint(ob, 9 + 2 + 1 + vlen + clen);
  lsb_pb_write_string(ob, LSB_PB_NAME, "content", 7);
  lsb_pb_write_key(ob, LSB_PB_VALUE_TYPE, LSB_PB_WT_VARINT);
  ev = lsb_pb_write_varint(ob, LSB_PB_BYTES);

  if (NULL == r->request_body->temp_file) {
    ngx_buf_t *buf;
    ngx_chain_t *cl;
    cl = r->request_body->bufs;
    for (bool first = true; cl && !ev; cl = cl->next) {
      if (first) {
        first = false;
        lsb_pb_write_key(ob, LSB_PB_VALUE_BYTES, LSB_PB_WT_LENGTH);
        lsb_outputs(ob, vint, vlen);
      }
      ev = lsb_outputs(ob, cl->buf->pos, cl->buf->last - cl->buf->pos);
    }
  } else {
    size_t ret;
    size_t offset = 0;
    unsigned char data[4096];
    while ((ret = ngx_read_file(&r->request_body->temp_file->file, data, 4096,
                                offset)) > 0 && !ev) {
      ev = lsb_outputs(ob, data, ret);
      offset = offset + ret;
    }
  }
  return ev;
}


static void
ngx_http_moz_ingest_body_handler(ngx_http_request_t *r)
{
  static ngx_str_t h_content_length = ngx_string("content-length");
  static ngx_str_t h_host = ngx_string("host");
  static ngx_str_t h_user_agent = ngx_string("user-agent");

  if (r->request_body == NULL) {
    ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
    return;
  }

  ngx_http_moz_ingest_loc_conf_t *conf =
      ngx_http_get_module_loc_conf(r, ngx_http_moz_ingest_module);

  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  unsigned long long ns = ts.tv_sec * 1000000000LL + ts.tv_nsec;
  lsb_output_buffer ob;
  lsb_err_value ev = lsb_init_output_buffer(&ob, conf->max_content_size  * 2);
  if (ev) {
    ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
    return;
  }
  lsb_write_heka_uuid(&ob, NULL, 0);

  lsb_pb_write_key(&ob, LSB_PB_TIMESTAMP, LSB_PB_WT_VARINT);
  lsb_pb_write_varint(&ob, ns);

  lsb_pb_write_string(&ob, LSB_PB_TYPE, "moz_ingest", 10);

  lsb_pb_write_string(&ob, LSB_PB_HOSTNAME, ngx_cycle->hostname.data,
                      ngx_cycle->hostname.len);

  if (conf->client_ip) {
    write_str_field(&ob, "remote_addr", 11, r->connection->addr_text.data,
                    r->connection->addr_text.len);
  }
  write_str_field(&ob, "uri", 3, r->uri.data, r->uri.len);
  if (r->args.len) {
    write_str_field(&ob, "args", 4, r->args.data, r->args.len);
  }
  ev = write_str_field(&ob, "protocol", 8, r->http_protocol.data,
                       r->http_protocol.len);

  if (conf->headers != NGX_CONF_UNSET_PTR) {
    ngx_str_t *hdr = conf->headers->elts;
    ngx_table_elt_t *e;
    for (int i = 0; i < conf->headers->nelts && !ev; ++i) {
      if (h_content_length.len == hdr[i].len &&
          ngx_strcasecmp(h_content_length.data, hdr[i].data) == 0) {
        e = r->headers_in.content_length;
      } else if ((h_host.len == hdr[i].len &&
                  ngx_strcasecmp(h_host.data, hdr[i].data) == 0)) {
        e = r->headers_in.host;
      } else if ((h_user_agent.len == hdr[i].len &&
                  ngx_strcasecmp(h_user_agent.data, hdr[i].data) == 0)) {
        e = r->headers_in.user_agent;
      } else {
        e = search_headers_in(r, hdr[i].data, hdr[i].len);
      }
      if (e) {
        ev = write_str_field(&ob, hdr[i].data, hdr[i].len, e->value.data,
                             e->value.len);
      }
    }
  }

  ev = write_content_field(&ob, r);
  if (ev) {
    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                  "Failed to allocate protobuf buffer: %s.", ev);
    ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
    return;
  }

  errno = 0;
  int ret = rd_kafka_produce(conf->rkt, RD_KAFKA_PARTITION_UA,
                             RD_KAFKA_MSG_F_FREE,
                             (void *)ob.buf, ob.pos,
                             NULL, 0, // optional key/len
                             r // opaque pointer
                            );
  if (ret == -1) {
    lsb_free_output_buffer(&ob);
    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                  "kafka produce failure: %d", errno);
    ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
    return;
  }
  ob.buf = NULL; // kafka has ownership, don't call lsb_free_output_buffer

  ngx_http_complex_value_t cv;
  ngx_memzero(&cv, sizeof(ngx_http_complex_value_t));
  cv.value.len = 2;
  cv.value.data = (u_char *)"OK";
  ngx_int_t rc = ngx_http_send_response(r,
                                        NGX_HTTP_OK,
                                        &ngx_http_text_plain_type,
                                        &cv);
  ngx_http_finalize_request(r, rc);
  return;
}


static ngx_int_t
ngx_http_moz_ingest_handler(ngx_http_request_t *r)
{
  if (!(r->method & (NGX_HTTP_POST | NGX_HTTP_PUT))) {
    return NGX_HTTP_NOT_ALLOWED;
  }

  if (!r->headers_in.content_length) {
    return NGX_HTTP_LENGTH_REQUIRED;
  }

  ngx_http_moz_ingest_loc_conf_t *conf =
      ngx_http_get_module_loc_conf(r, ngx_http_moz_ingest_module);

  if (r->unparsed_uri.len > conf->max_unparsed_uri_size) {
    return NGX_HTTP_REQUEST_URI_TOO_LARGE;
  }

  if (r->headers_in.content_length_n > (off_t)conf->max_content_size) {
    return NGX_HTTP_REQUEST_ENTITY_TOO_LARGE;
  }

  if (!conf->rk) {
    if (!ngx_http_moz_ingest_init_kafka(r, conf)) {
      return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
  }

  ngx_int_t rc =
      ngx_http_read_client_request_body(r, ngx_http_moz_ingest_body_handler);

  if (rc >= NGX_HTTP_SPECIAL_RESPONSE) {
    return rc;
  }
  return NGX_DONE;
}


static char*
ngx_http_moz_ingest(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
  ngx_http_core_loc_conf_t *clcf =
      ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
  clcf->handler = ngx_http_moz_ingest_handler;

  rd_kafka_conf_t *kconf = rd_kafka_conf_new();
  if (!kconf) {
    ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "rd_kafka_conf_new failed");
    return NGX_CONF_ERROR;
  }
  // test to make sure the no poll version of librdkafka is installed
  char errstr[512];
  rd_kafka_conf_res_t rv = rd_kafka_conf_set(kconf, "delivery.report.no.poll",
                                             "true", errstr, sizeof errstr);
  if (rv) {
    ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "rrd_kafka_conf_set failed: %s"
                       , errstr);
    return NGX_CONF_ERROR;
  }
  rd_kafka_conf_destroy(kconf);
  return NGX_CONF_OK;
}
