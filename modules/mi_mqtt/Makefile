# WARNING: do not run this directly, it should be run by the master Makefile

include ../../Makefile.defs
auto_gen=
NAME=mi_mqtt.so

DEFS +=-I$(LOCALBASE)/include
LIBS +=-L$(LOCALBASE)/lib -lpaho-mqtt3as -luuid

include ../../Makefile.modules
