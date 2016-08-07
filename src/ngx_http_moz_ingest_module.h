/* -*- Mode: C; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

#ifndef NGX_HTTP_MOZ_INGEST_MODULE_H
#define NGX_HTTP_MOZ_INGEST_MODULE_H

#include <ngx_config.h>
#include <ngx_core.h>
#include <nginx.h>
#include <librdkafka/rdkafka.h>
#include <stdio.h>
#include <time.h>

extern ngx_module_t ngx_http_moz_ingest_module;

typedef struct {
  FILE    *fh;
  time_t  t;
  int     cnt;
} landfill_file;

typedef struct {
  rd_kafka_t        *rk;
  rd_kafka_topic_t  *rkt;

  landfill_file     lfmain;
  landfill_file     lfother;

  size_t      max_content_size;
  size_t      max_unparsed_uri_size;
  ngx_flag_t  client_ip;
  ngx_array_t *headers;

  // Kafka settings
  size_t      max_buffer_size;
  ngx_msec_t  max_buffer_ms;
  size_t      batch_size;
  ngx_str_t   brokerlist;
  ngx_str_t   topic;

  // Landfill settings (alternate S3 load mechanism)
  size_t      landfill_roll_size;
  ngx_str_t   landfill_dir;
  ngx_str_t   landfill_name;
} ngx_http_moz_ingest_loc_conf_t;

typedef struct {
  ngx_http_moz_ingest_loc_conf_t **confs;
  size_t confs_size;
} ngx_http_moz_ingest_main_conf_t;

#endif /* NGX_HTTP_MOZ_INGEST_MODULE_H */

