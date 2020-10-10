PREFIX?=/usr/local
LIBDIR = $(PREFIX)/lib/
INCDIR = $(PREFIX)/include/

OSNAME = $(shell uname)

TARGET_A = libcr_class.a
TARGET_SO = libcr_class.so

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
EXT_LIBS = -pthread -lprotobuf -lpthread
EXT_LIBS += -lz -lsnappy -llz4
EXT_LIBS += `pkg-config --libs liblzma`
ifeq ($(OSNAME), Linux)
LDFLAGS += -lrt
endif

SOURCES = $(wildcard *.cc)
OBJECTS = $(SOURCES:.cc=.o)

depend: .depend

.depend: $(SOURCES)
	$(RM) -f ./.depend
	$(CXX) $(CPPFLAGS) -M $^ > ./.depend;

proto: $(SOURCES)
	protoc --cpp_out=. *.proto

include .depend

debug: CPPFLAGS += -DDEBUG -DDIAGNOSTIC -O0
debug: $(TARGET)

release: CPPFLAGS += -fno-strict-aliasing -O3
release: $(TARGET)

install: $(TARGET_A) $(TARGET_SO)
	$(RM) -f $(LIBDIR)$(TARGET_A)
	$(RM) -f $(LIBDIR)$(TARGET_SO)
	$(RM) -rf $(INCDIR)cr_class
	$(MKDIR) -p $(LIBDIR)
	$(MKDIR) -p $(INCDIR)cr_class
	$(INSTALL) $(TARGET_A) $(LIBDIR)
	$(INSTALL) $(TARGET_SO) $(LIBDIR)
	$(INSTALL) -m 444 *.h $(INCDIR)cr_class/

uninstall:
	$(RM) -f $(LIBDIR)$(TARGET_A)
	$(RM) -f $(LIBDIR)$(TARGET_SO)
	$(RM) -rf $(INCDIR)cr_class

$(TARGET_A): $(OBJECTS)
	${AR} rcs $(TARGET_A) $(OBJECTS)
	$(RANLIB) $(TARGET_A)

$(TARGET_SO): $(OBJECTS)
	${CXX} -o $(TARGET_SO) $(LDFLAGS) $(OBJECTS) $(EXT_LIBS)

clean:
	$(RM) -f *.pb.cc *.pb.h *.o *.core *~ *.a *.so .depend
