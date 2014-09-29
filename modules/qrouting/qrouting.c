/**
 *
 * qrouting module: qrouting.c
 *
 * Copyright (C) 2004-2005 FhG Fokus
 * Copyright (C) 2006-2010 Voice Sistem SRL
 * Copyright (C) 2014 OpenSIPS Foundation
 *
 * This file is part of opensips, a free SIP server.
 *
 * opensips is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version
 *
 * opensips is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * History
 * -------
 *  2014-08-28  initial version (Mihai Tiganus)
 */
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <unistd.h>

#include "../../sr_module.h"
#include "../../str.h"
#include "../../timer.h"

#include "qr_stats.h"
#include "qr_sort.h"
#include "qr_acc.h"
#include "qrouting.h"

#define T_PROC_LABEL "[qrouting]:sampling interval"
#define MAX_HISTORY 1000 /* TODO:*/

static int history = 30; /* the history span in minutes */
static int sampling_interval = 5; /* the sampling interval in seconds */

str avp_invite_time_name_pdd = str_init("$avp(qr_invite_time_pdd)");
str avp_invite_time_name_ast = str_init("$avp(qr_invite_time_ast)");



/* timer use for creating the statistics */
struct sr_timer_process t_proc;

static cmd_export_t cmds[] = {
	{"test_acc",  (cmd_function)test_acc,   0,  0, 0,
		REQUEST_ROUTE|FAILURE_ROUTE|LOCAL_ROUTE},
	{0, 0, 0, 0, 0, 0}
};
static param_export_t params[] = {
	{"history", INT_PARAM, &history},
	{"sampling_interval", INT_PARAM, &sampling_interval},
	{0, 0, 0}
};


#define HLP1 "Params: [ rule_name [ gw_name ] ]; List the QR statistics for gateways"
static mi_export_t mi_cmds[] = {
	{ "qr_status",         HLP1, qr_status_cmd,    0, 0,  0},
	{ 0, 0, 0, 0, 0, 0}
};

static int qr_init(void);
static int qr_child_init(int rank);
static int qr_exit(void);

static void timer_func(void);


struct module_exports exports = {
	"qrouting",
	MOD_TYPE_DEFAULT,/* class of this module */
	MODULE_VERSION,
	DEFAULT_DLFLAGS, /* dlopen flags */
	0,           /* OpenSIPS module dependencies */
	cmds,            /* Exported functions */
	params,          /* Exported parameters */
	0,               /* exported statistics */
	mi_cmds,         /* exported MI functions */
	0,               /* exported pseudo-variables */
	0,               /* additional processes */
	qr_init,         /* Module initialization function */
	(response_function) 0,
	(destroy_function) qr_exit,
	(child_init_function) qr_child_init /* per-child init function */
};

static int qr_init(void){
	int i;
	qr_rule_t *my_rule; /* FIXME: testing purpose */
	LM_INFO("QR module\n");
	LM_DBG("history = %d, sampling_interval = %d\n", history,
			sampling_interval);
	register_timer_process(T_PROC_LABEL, (void*)timer_func, NULL,
			sampling_interval, 0);
	qr_n = (history * 60)/sampling_interval; /* the number of sampling
												intervals in history */

	if(load_tm_api(&tmb) == -1) {
		LM_ERR("failed to load tm functions. Tm module loaded?\n");
		return -1;
	}
	if(load_dlg_api(&dlgcb) == -1) {
		LM_ERR("failed to load dlg functions. Dialog module loaded?\n");
		return -1;
	}
	if(load_dr_api(&drb) == -1) {
		LM_ERR("Failed to load dr functions. DR modules loaded\n");
		return -1;
	}

	qr_rules_start = (qr_rule_t **)shm_malloc(sizeof(qr_rule_t*));
	if(qr_rules_start == NULL) {
		LM_ERR("no more shm memory\n");
		return -1;
	}
	*qr_rules_start = NULL;


	if(drb.register_drcb(DRCB_REG_INIT_RULE, &qr_create_rule, NULL, NULL) < 0) {
		LM_ERR("[QR] failed to register DRCB_REG_INIT_RULE callback to DR\n");
		return -1;
	}
	if(drb.register_drcb(DRCB_REG_GW, &qr_dst_is_gw, NULL, NULL) < 0) {
		LM_ERR("[QR] failed to register DRCB_REG_REG_GW callback to DR\n");
		return -1;
	}
	if(drb.register_drcb(DRCB_REG_ADD_RULE, &qr_add_rule, NULL, NULL) < 0) {
		LM_ERR("[QR] failed to register DRCB_REG_ADD_RULE callback to DR\n");
		return -1;
	}
	LM_DBG("[QR] callbacks in DR were registered\n");



	return 0;
}

static int qr_child_init(int rank) {
	return 0;
}

static int qr_exit(void) {
	return 0;
}

static void timer_func(void) {
	qr_rule_t *it;
	int i;

	for(it = *qr_rules_start; it != NULL; it = it->next) {
		for(i = 0; i < it->n; i++) {
			if(it->dest[i].type == QR_DST_GW) {
				update_gw_stats(it->dest[i].dst.gw);
			} else {
				update_grp_stats(it->dest[i].dst.grp);
			}
		}
	}
}

/* searches for a given rule in the QR list */
static qr_rule_t * qr_search_rule(int r_id) {
	qr_rule_t * rule_it;

	for(rule_it = *qr_rules_start; rule_it != NULL; rule_it = rule_it->next) {
		if(rule_it->r_id == r_id) {
			return rule_it;
		}
	}
	return NULL;
}

static str * qr_get_dst_name(qr_dst_t * dst) {
	if(dst->type == QR_DST_GW) {
		return drb.get_gw_name(dst->dst.gw->dr_gw);
	} else {
		return &dst->dst.grp.name;
	}
}

/* searches for a given gw inside a rule */
static qr_dst_t * qr_search_dst(qr_rule_t * rule, str *dst_name) {
	int i;
	str *cur_dst_name;
	if(dst_name == NULL)
		return NULL;
	for(i = 0; i < rule->n; i++) {
		cur_dst_name = qr_get_dst_name(&rule->dest[i]);
		/* TODO: cur_dst_name != NULL because no dr_api */
		if(cur_dst_name != NULL && cur_dst_name->len == dst_name->len && memcmp(cur_dst_name->s,
					dst_name->s, dst_name->len) == 0) {
			return &rule->dest[i];
		}
	}
	return NULL;

}

static void qr_gw_attr(struct mi_node **node, qr_gw_t *gw) {
	struct mi_node *gw_node = NULL;
	struct mi_attr *attr = NULL;

	str tmp;
	str *p_tmp;
	tmp.s = (char*)shm_malloc(20*sizeof(char));
	memset(tmp.s, 0, 20*sizeof(char));

	p_tmp = drb.get_gw_name(gw->dr_gw);
	gw_node = add_mi_node_child(*node, 0, "Gw", 2,
			p_tmp->s, p_tmp->len);
	sprintf(tmp.s, "%lf", asr(gw));
	tmp.len = strlen(tmp.s);
	attr = add_mi_attr(gw_node, MI_DUP_VALUE, "ASR", 3,
			tmp.s, tmp.len);
	if(attr == NULL)
		goto error;
	memset(tmp.s, 0, 20*sizeof(char));
	sprintf(tmp.s, "%lf", ccr(gw));
	tmp.len = strlen(tmp.s);
	attr = add_mi_attr(gw_node, MI_DUP_VALUE, "CCR", 3,
			tmp.s, tmp.len);
	if(attr == NULL)
		goto error;
	memset(tmp.s, 0, 20*sizeof(char));
	sprintf(tmp.s, "%lf", pdd(gw));
	tmp.len = strlen(tmp.s);
	attr = add_mi_attr(gw_node, MI_DUP_VALUE, "PDD", 3,
			tmp.s, tmp.len);
	if(attr == NULL)
		goto error;
	memset(tmp.s, 0, 20*sizeof(char));
	sprintf(tmp.s, "%lf", ast(gw));
	tmp.len = strlen(tmp.s);
	attr = add_mi_attr(gw_node, MI_DUP_VALUE, "AST", 3,
			tmp.s, tmp.len);
	if(attr == NULL)
		goto error;
	memset(tmp.s, 0, 20*sizeof(char));
	sprintf(tmp.s, "%lf", acd(gw));
	tmp.len = strlen(tmp.s);
	attr = add_mi_attr(gw_node, MI_DUP_VALUE, "ACD", 3,
			tmp.s, tmp.len);
	if(attr == NULL)
		goto error;

	return ;
error:
	*node = NULL;
}

static void qr_grp_attr(struct mi_node **node, qr_grp_t * grp) {
	int i;
	struct mi_node *gw_node;
	gw_node = add_mi_node_child(*node, 0, "Group", 5, "group_name",
			strlen("group_name"));
	if(*node == NULL)
		goto error;
	for(i = 0; i<grp->n; i++) {
		qr_gw_attr(&gw_node, grp->gw[i]);
		if(gw_node == NULL) {
			goto error;
		}
	}

	return ;
error:
	node = NULL;
}

static void qr_dst_attr(struct mi_node ** node, qr_dst_t * dst) {
	if(dst->type == QR_DST_GW) {
		qr_gw_attr(node, dst->dst.gw);
	} else {
		qr_grp_attr(node, &dst->dst.grp);
	}
}


static struct mi_root* qr_status_cmd(struct mi_root *cmd_tree, void *param) {
	/* TODO protected from adding */
	int i;
	qr_rule_t *rule_it, *rule;
	qr_dst_t *dst;
	str rule_name, gw_name, error_str;
	unsigned int rule_id;
	struct mi_node * node = NULL, *rule_node = NULL;
	struct mi_root *rpl_tree = NULL, *error_tree = NULL;


	LM_INFO("qr_status command received\n");

	if(cmd_tree != NULL)
		node = cmd_tree->node.kids;

	rpl_tree = init_mi_tree(200, MI_OK_S, MI_OK_LEN);
	if(rpl_tree == NULL) {
		error_tree = init_mi_tree(500,"Failed to init reply tree",
				strlen("Failed to init reply tree"));
		goto error;
	}
	rpl_tree->node.flags |= MI_IS_ARRAY;

	if(node == NULL) { /* mi_tree with all the destinations (and rules) */
		for(rule_it = *qr_rules_start; rule_it != NULL; rule_it = rule_it->next) {
			if(rule_it->r_id != 0) { /* TODO: maybe -1 */
				rule_name.s = (char*)shm_malloc(10*sizeof(char));
				if(rule_name.s == NULL) {
					LM_ERR("no more shm memory\n");
					goto error;
				}
				memset(rule_name.s, 0, 10*sizeof(char));
				rule_name.len = snprintf(rule_name.s, 10,"%d", rule_it->r_id);
				rule_node = add_mi_node_child(&rpl_tree->node, 0,
						"Rule", 4, rule_name.s, rule_name.len);
			} else {
				rule_node = add_mi_node_child(&rpl_tree->node, 0,
						"Rule", 4, 0, 0);

			}
			for(i = 0;i < rule_it->n; i++) {
				qr_dst_attr(&rule_node, &rule_it->dest[i]);
			}

		}
	} else { /* mi_tree with a single destination (group/gateway) */
		rule_name = node->value;
		if(str2int(&node->value, &rule_id) < 0) {
			error_str.len = rule_name.len + 36;
			error_str.s = (char *)shm_malloc(error_str.len*sizeof(char));
			snprintf(error_str.s, error_str.len,
					"Failed to parse rule name '%.*s' to int", rule_name.len,
					rule_name.s);
			error_tree = init_mi_tree(500,error_str.s, error_str.len);
			goto error;
		}
		LM_DBG("searching for rule_id %d\n", rule_id);
		rule = qr_search_rule(rule_id);
		if(rule == NULL) {
			error_str.len = rule_name.len + 18;
			error_str.s = shm_malloc(error_str.len*sizeof(char));
			snprintf(error_str.s, error_str.len, "Rule '%.*s' not found",
					rule_name.len, rule_name.s);
			error_tree = init_mi_tree(400, error_str.s, error_str.len);
			goto error;
		}
		rule_node = add_mi_node_child(&rpl_tree->node, 0,
				"Rule", 4, rule_name.s, rule_name.len);

		if(node->next != NULL) {
			node = node->next;
			gw_name = node->value;
			dst = qr_search_dst(rule, &gw_name);
			if(dst == NULL) {
				error_str.len = gw_name.len+21;
				error_str.s = shm_malloc(error_str.len*sizeof(char));
				snprintf(error_str.s, error_str.len, "Gateway '%.*s' not found",
						gw_name.len, gw_name.s);
				error_tree = init_mi_tree(400, error_str.s, error_str.len);
				goto error;
			}
			qr_dst_attr(&rule_node, dst);

		} else { /* mi_tree with all the destinations for a rule */
			for(i = 0; i < rule->n; i++) {
				qr_dst_attr(&rule_node, &rule->dest[i]);
			}
		}
	}

	return rpl_tree;

error:
	return error_tree;

}

