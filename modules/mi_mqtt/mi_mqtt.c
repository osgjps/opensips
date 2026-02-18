/*
 * This file is part of Open SIP Server (opensips).
 *
 * opensips is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * opensips is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
 *
 * History:
 * ---------
 *  2026-02-17 First Version (james@fivecats.org)
 */

#include <stdlib.h>

#include "../../globals.h"
#include "../../sr_module.h"
#include "../../str.h"
#include "../../ut.h"
#include "../../resolve.h"
#include "../../mem/mem.h"
#include "../../mem/shm_mem.h"
#include "../../mi/mi_trace.h"

#include "MQTTAsync.h"


/* module functions */
static int mod_init();
static void destroy(void);

/* formated JSON response printing, disabled by default */
int pretty_print;

MQTTAsync client;

#define QOS         1
#define TIMEOUT     10000L

//static str backend = str_init("json");
static str topic_base = str_init("commands/");
static str broadcast_topic = str_init("allopensips");
static str mqtt_clientid = str_init("OpensipsMQTT");
static char *command_topic_s;
static str mqtt_uri = str_init("tcp://test.mosquitto.org:1883");

char *mqtt_user_s;
char *mqtt_pass_s;
char hostname[MAXHOSTNAMELEN];

/* MQTT Callbacks */
static int msgarrvd(void *, char *, int, MQTTAsync_message *);
static void connlost(void *, char *);




/* module parameters */
static const param_export_t mi_params[] = {
        {"topic_base",      STR_PARAM, &topic_base.s },
        {"broadcast_topic", STR_PARAM, &broadcast_topic.s },
	{"command_topic",   STR_PARAM, &command_topic_s },
        {"mqtt_uri",        STR_PARAM, &mqtt_uri.s  },
	{"mqtt_clientid",   STR_PARAM, &mqtt_clientid.s },
	{"mqtt_user",       STR_PARAM, &mqtt_user_s },
	{"mqtt_password",   STR_PARAM, &mqtt_pass_s },
        {0,0,0}
};
/*
static const proc_export_t mi_procs[] = {
        {"MIMQTT",  0,  0, mqtt_proc, 1,
                PROC_FLAG_INITCHILD|PROC_FLAG_HAS_IPC|PROC_FLAG_NEEDS_SCRIPT },
        {NULL, 0, 0, NULL, 0, 0}
};
*/


/* module exports */
struct module_exports exports = {
        "mi_mqtt",                                      /* module name */
        MOD_TYPE_DEFAULT,                       /* class of this module */
        MODULE_VERSION,
        DEFAULT_DLFLAGS,                        /* dlopen flags */
        0,                                                      /* load function */
        NULL,                                          /* No module dependencies */
        NULL,                                           /* exported functions */
        NULL,                                           /* exported async functions */
        mi_params,                                      /* exported parameters */
        NULL,                                           /* exported statistics */
        NULL,                                           /* exported MI functions */
        NULL,                                           /* exported PV */
        NULL,                                           /* exported transformations */
        NULL,                                                      /* extra processes */
        0,                                                      /* module pre-initialization function */
        mod_init,                                       /* module initialization function */
        (response_function) 0,          /* response handling function */
        (destroy_function) destroy,     /* destroy function */
        NULL,                                           /* per-child init function */
        NULL                                            /* reload confirm function */
};


static int msgarrvd(void *context, char *topicName, int topicLen, MQTTAsync_message *message) {

  const char **parse_end = NULL;
  mi_request_t request;
  mi_response_t *response = NULL;
  struct mi_cmd *cmd = NULL;
  str outmsg;
  char *req_method = NULL;
  
  LM_DBG("MQTT Message received - Topic: %s   Message: %.*s\n",topicName,message->payloadlen, (char*)message->payload);

  memset(&request,0,sizeof(request));
  if (parse_mi_request((char*)message->payload, parse_end, &request) < 0) {
    LM_ERR("cannot parse command: %.*s\n", message->payloadlen, (char*)message->payload);
  } else {

    req_method = mi_get_req_method(&request);
    if (req_method)
      cmd = lookup_mi_cmd(req_method, strlen(req_method));
    
    if (cmd) { 
      response = handle_mi_request(&request, cmd, 0);
    }
    
    if (response == NULL) {
      LM_ERR("failed to build response!\n");
    } else {
      outmsg.s = pkg_malloc(256);
      outmsg.len = 256;
      print_mi_response(response, request.id, &outmsg, 1);
      LM_DBG("MQTT MI response: %.*s\n",outmsg.len,outmsg.s);
      free_mi_response(response);
      pkg_free(outmsg.s);
    }
    
  }
  
  MQTTAsync_freeMessage(&message);
  MQTTAsync_free(topicName);
  return 1;
    
}
 
static void connlost(void *context, char *cause) {

  LM_ERR("MQTT Connection Lost - Cause: %s\n",cause);

}

void onSubscribe(void* context, MQTTAsync_successData* response)
{
	LM_DBG("Subscribe succeeded\n");
}


static void onConnect(void* context, MQTTAsync_successData* response)
{
	MQTTAsync client = (MQTTAsync)context;
	MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
	int rc;
	char topic[128];
	char hostname[MAXHOSTNAMELEN];
	

	opts.onSuccess = onSubscribe;
	opts.context = client;
	// Subscribe to the broadcast channel
	sprintf(topic,"%s%s",topic_base.s,broadcast_topic.s);
	if ((rc = MQTTAsync_subscribe(client, topic, QOS, &opts)) != MQTTASYNC_SUCCESS) {
	  LM_ERR("Failed to start subscribe for %s, return code %d\n", topic, rc);
	} else {
	  LM_DBG("Subscribing to %s\n",topic);
	}

	// Subscribe to the instance specific command channel
	if ( !command_topic_s ) {
	  gethostname(hostname,sizeof(hostname));
	  LM_DBG("MQTT Instance Specific channel not specified.  Defaulting to hostname %s\n",hostname);
	  sprintf(topic,"%s%s",topic_base.s,hostname);
	} else {
	  sprintf(topic,"%s%s",topic_base.s,command_topic_s);
	}
	if ((rc = MQTTAsync_subscribe(client, topic, QOS, &opts)) != MQTTASYNC_SUCCESS) {
	  LM_ERR("Failed to start subscribe for %s, return code %d\n", topic, rc);
	} else {
	  LM_DBG("Subscribing to %s\n",topic);
	}

}

void onConnectFailure(void* context, MQTTAsync_failureData* response)
{
	LM_ERR("MQTT Connect failed, rc %d\n", response->code);
}

static int mod_init(void) {

  MQTTAsync_connectOptions conn_opts = MQTTAsync_connectOptions_initializer;
  int rc;


  LM_DBG("Calling mod_init\n");

  if ((rc = MQTTAsync_create(&client, mqtt_uri.s, mqtt_clientid.s,			      MQTTCLIENT_PERSISTENCE_NONE, NULL)) != MQTTASYNC_SUCCESS) {
    LM_ERR("Failed to create MQTT client, return code %d\n", rc);
    return -1;
  }

  if ((rc = MQTTAsync_setCallbacks(client, client, connlost, msgarrvd, NULL)) != MQTTASYNC_SUCCESS) {
    
    LM_ERR("Failed to set MQTT client callbacks, return code %d\n", rc);
    destroy();
    return -1;
  }

  conn_opts.keepAliveInterval = 20;
  conn_opts.cleansession = 1;
  conn_opts.onSuccess = onConnect;
  conn_opts.onFailure = onConnectFailure;
  conn_opts.context = client;
  
  if ((rc = MQTTAsync_connect(client, &conn_opts)) != MQTTASYNC_SUCCESS) {
    LM_ERR("Failed to connect to MQTT server, return code %d\n", rc);
    destroy();
    return -1;
  }
 
  return 0;

}


static void destroy(void)
{
  LM_DBG("calling destroy for mi_mqtt\n");
  MQTTAsync_destroy(&client);
}
