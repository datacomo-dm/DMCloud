PREFIX?=/usr/local
LIBDIR = $(PREFIX)/lib/
INCDIR = $(PREFIX)/include/

OSNAME = $(shell uname)

TARGET_A = libcfmdrv.a
TARGET_SO = libcfmdrv.so

TARGET = $(TARGET_A) $(TARGET_SO)

CC = gcc
CXX = g++
AR = ar
RANLIB = ranlib
RM = rm
MKDIR = mkdir
INSTALL = install
CPPFLAGS = -Wall -Werror -I .. -I $(INCDIR) -std=c++0x -g -fPIC
LDFLAGS = -fPIC -shared
EXT_LIBS = -lcr_class
ifeq ($(OSNAME), Linux)
LDFLAGS += -lrt
endif

SOURCES = $(wildcard *.cc)
OBJECTS = $(SOURCES:.cc=.o)

depend: .depend

.depend: $(SOURCES)
	$(RM) -f ./.depend
	$(CC) $(CPPFLAGS) -M $^ > ./.depend;

proto: $(SOURCES)
	protoc --cpp_out=. *.proto

include .depend

debug: CPPFLAGS += -DDEBUG -DDIAGNOSTIC -O0
debug: $(TARGET)

release: CPPFLAGS += -fno-strict-aliasing -DNDEBUG -O3
release: $(TARGET)

install: $(TARGET_A) $(TARGET_SO)
	$(RM) -f $(LIBDIR)$(TARGET_A)
	$(RM) -f $(LIBDIR)$(TARGET_SO)
	$(RM) -rf $(INCDIR)cfm_drv
	$(MKDIR) -p $(LIBDIR)
	$(MKDIR) -p $(INCDIR)cfm_drv
	$(INSTALL) $(TARGET_A) $(LIBDIR)
	$(INSTALL) $(TARGET_SO) $(LIBDIR)
	$(INSTALL) -m 444 *.h $(INCDIR)cfm_drv/

uninstall:
	$(RM) -f $(LIBDIR)$(TARGET_A)
	$(RM) -f $(LIBDIR)$(TARGET_SO)
	$(RM) -rf $(INCDIR)cfm_drv

$(TARGET_A): $(OBJECTS)
	${AR} rcs $(TARGET_A) $(OBJECTS)
	$(RANLIB) $(TARGET_A)

$(TARGET_SO): $(OBJECTS)
	${CC} -o $(TARGET_SO) $(LDFLAGS) $(OBJECTS) $(EXT_LIBS)

clean:
	$(RM) -f *.pb.cc *.pb.h *.o *.core *~ *.a *.so .depend
