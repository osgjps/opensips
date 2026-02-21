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
#include <uuid/uuid.h>


/* module functions */
static int mod_init();
static void destroy(void);
static void mqtt_process(int);


/* formated JSON response printing, disabled by default */
int pretty_print;

MQTTAsync client;

#define QOS         1
#define TIMEOUT     10000L

//static str backend = str_init("json");
static str topic_base = str_init("commands/");
static str broadcast_topic = str_init("allopensips");
static char *command_topic_s;
static str mqtt_uri = str_init("tcp://test.mosquitto.org:1883");

static char *mqtt_clientid_s;
static char *mqtt_user_s;
static char *mqtt_pass_s;
char hostname[MAXHOSTNAMELEN];

/* MQTT Callbacks */
static int msgarrvd(void *, char *, int, MQTTAsync_message *);
static void connlost(void *, char *);
static void doSubscribe(void* , char*);

static int doshutdown = 0;




/* module parameters */
static const param_export_t mi_params[] = {
        {"topic_base",      STR_PARAM, &topic_base.s },
        {"broadcast_topic", STR_PARAM, &broadcast_topic.s },
	{"command_topic",   STR_PARAM, &command_topic_s },
        {"mqtt_uri",        STR_PARAM, &mqtt_uri.s  },
	{"mqtt_clientid",   STR_PARAM, &mqtt_clientid_s },
	{"mqtt_user",       STR_PARAM, &mqtt_user_s },
	{"mqtt_password",   STR_PARAM, &mqtt_pass_s },
        {0,0,0}
};

static const proc_export_t mi_procs[] = {
        {"MIMQTT",  0,  0, mqtt_process, 1,
                PROC_FLAG_INITCHILD|PROC_FLAG_HAS_IPC|PROC_FLAG_NEEDS_SCRIPT },
        {NULL, 0, 0, NULL, 0, 0}
};



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
        mi_procs,                                                      /* extra processes */
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
  struct mi_handler *async_hdl;
  struct mi_cmd *cmd = NULL;
  str outmsg;
  char *req_method = NULL;
  MQTTProperty *responseprop;
  MQTTProperty *correlationprop;
  int rc;
  
  LM_DBG("MQTT Message received - Topic: %s   Message: %.*s\n",topicName,message->payloadlen, (char*)message->payload);

  if (MQTTProperties_hasProperty(&message->properties, MQTTPROPERTY_CODE_RESPONSE_TOPIC))
    responseprop = MQTTProperties_getProperty(&message->properties, MQTTPROPERTY_CODE_RESPONSE_TOPIC);
  if (MQTTProperties_hasProperty(&message->properties, MQTTPROPERTY_CODE_CORRELATION_DATA))
    correlationprop = MQTTProperties_getProperty(&message->properties, MQTTPROPERTY_CODE_CORRELATION_DATA);

  
  memset(&request,0,sizeof(request));
  if (parse_mi_request((char*)message->payload, parse_end, &request) < 0) {
    LM_ERR("cannot parse command: %.*s\n", message->payloadlen, (char*)message->payload);
  } else {

    req_method = mi_get_req_method(&request);
    LM_DBG("got MI command=%s\n", req_method);
    if (req_method)
      cmd = lookup_mi_cmd(req_method, strlen(req_method));

    if (cmd && cmd->flags & MI_ASYNC_RPL_FLAG) {
      LM_DBG("command=%s is async\n", req_method);
    }

    
    if (cmd) { 
      response = handle_mi_request(&request, cmd, NULL);
    }

    if (response == MI_ASYNC_RPL) {
		LM_DBG("got an async reply\n");
		//		response = mi_json_wait_async_reply(async_hdl);
    }
    
    if (response == NULL) {
      LM_ERR("failed to build response!\n");
    } else {
      outmsg.s = pkg_malloc(256*256);
      outmsg.len = 256*256;
      rc = print_mi_response(response, request.id, &outmsg, 1);
      LM_DBG("print_mi_response returned %i\n",rc);
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

  int rc;
  LM_ERR("MQTT Connection Lost - Caggguse: %s   %p\n",cause, context);
  MQTTAsync client = (MQTTAsync)context;
  sleep(5);
  MQTTAsync_setConnected(client, client, doSubscribe); 
  rc = MQTTAsync_reconnect(client);
  if (rc != MQTTASYNC_SUCCESS) {
    LM_ERR("Reconnect failure\n");
  } else {
    LM_DBG("Reconnect success?\n");
  }

}

static void onSubscribe(void* context, MQTTAsync_successData5* response)
{
	LM_DBG("Subscribe succeeded\n");
}

static void onFailure(void* context, MQTTAsync_failureData5* response)
{
  LM_DBG("Subscribe failed. RC %i Message %s\n",response->reasonCode, response->message);
}


static void onConnect(void* context, MQTTAsync_successData5* response)
{

  doSubscribe(context, "connect");

}

static void doSubscribe(void* context, char* cause) {
	MQTTAsync client = (MQTTAsync)context;
	MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
	int rc;
	char topic[128];
	char hostname[128];

	LM_DBG("Connected to MQTT server\n");
	if (client == NULL) {
	  LM_ERR("SHITBALLS\n");
	}

	opts.onSuccess5 = onSubscribe;
	opts.onFailure5 = onFailure;
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
	  gethostname(hostname,128);
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

void onConnectFailure(void* context, MQTTAsync_failureData5* response)
{
  LM_ERR("MQTT Connect failed, rc %d: %s\n", response->code, response->message);
}


static int mod_init(void) {


  LM_DBG("Calling mod_init\n");

  if (mqtt_clientid_s == NULL) {
    uuid_t uuid_obj;  
    mqtt_clientid_s = pkg_malloc(37 * sizeof(char));
    uuid_generate_random(uuid_obj);
    uuid_unparse(uuid_obj, mqtt_clientid_s);
    LM_DBG("No client ID specified.  Generating a random UUID one: %s.\n", mqtt_clientid_s);
  } else {
    LM_DBG("Using client ID %s\n",mqtt_clientid_s);
  }

  return 0;

}

static void mqtt_process(int rank) {

  MQTTAsync_connectOptions conn_opts = MQTTAsync_connectOptions_initializer5;
  int rc;

  LM_DBG("new MQTT process with pid = %d created\n",getpid());

  if( init_mi_child()!=0) {
    LM_CRIT("failed to init the mi process\n");
    exit(-1);
  }



  MQTTAsync_createOptions crea_opts = MQTTAsync_createOptions_initializer5;
  
  
  if ((rc = MQTTAsync_createWithOptions(&client, mqtt_uri.s, mqtt_clientid_s, MQTTCLIENT_PERSISTENCE_NONE, NULL, &crea_opts)) != MQTTASYNC_SUCCESS) {
    LM_ERR("Failed to create MQTT client, return code %d\n", rc);
    exit(-1);
  }

  if ((rc = MQTTAsync_setCallbacks(client, client, connlost, msgarrvd, NULL)) != MQTTASYNC_SUCCESS) {
    
    LM_ERR("Failed to set MQTT client callbacks, return code %d\n", rc);
    destroy();
    exit(-1);

  }

  conn_opts.keepAliveInterval = 20;
  conn_opts.onSuccess5 = onConnect;
  conn_opts.onFailure5 = onConnectFailure;
  conn_opts.context = client;
  conn_opts.automaticReconnect = 1;
  conn_opts.cleansession = 0;


  if (mqtt_user_s) {
    conn_opts.username = mqtt_user_s;
    LM_DBG("MQTT Using a useraccount %s\n",conn_opts.username);

  }
  if (mqtt_pass_s) {
    conn_opts.password = mqtt_pass_s;
    LM_DBG("MQTT Using a password %s\n",conn_opts.password);

  }

  //MQTTAsync_setConnected(client, client, doSubscribe); 
  
  if ((rc = MQTTAsync_connect(client, &conn_opts)) != MQTTASYNC_SUCCESS) {
    LM_ERR("Failed to connect to MQTT server, return code %d\n", rc);
    destroy();
    exit(-1);
  } else {
    LM_DBG("Connecting to %s.....\n",mqtt_uri.s);
  }

  while(!doshutdown) {
    usleep(1000L);
  }
  LM_DBG("Shutting down mqtt thread\n");

  return ;

}


static void destroy(void)
{
  LM_DBG("calling destroy for mi_mqtt\n");
  MQTTAsync_disconnectOptions disc_opts = MQTTAsync_disconnectOptions_initializer;
  MQTTAsync_disconnect(client, &disc_opts);
  MQTTAsync_destroy(&client);
  doshutdown = 1;
}
