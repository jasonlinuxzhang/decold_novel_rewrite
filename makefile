
LINK    = @echo Linking $@ && g++ 
GCC     = @echo Compiling $@ && g++ 
GC      = @echo Compiling $@ && gcc 
AR      = @echo Generating Static Library $@ && ar crv
#FLAGS   = -g -DDEBUG -W -Wall -fPIC
FLAGS   = -g
GCCFLAGS =  `pkg-config --cflags --libs glib-2.0 `
DEFINES = 
HEADER  = -I ./
LIBS    = -lglib -lpthread  -lrt -lz  -ldl
LINKFLAGS =

#HEADER += -I./

#LIBS    += -lrt
#LIBS    += -lglib 


all : obtain_inter_data restore_identified_file restore_migriated_file get_total_data_size fid_to_path
.PHONY:all

BIN_PATH = ./

OBJECT := obtain_inter_data.o cal.o recipe.o common.o queue.o sync_queue.o containerstore.o serial.o
obtain_inter_data : $(OBJECT) 
	$(LINK) $(FLAGS) $(LINKFLAGS) -o $@ $^ $(LIBS)

OBJECT := restore_identified_file.o recipe.o common.o queue.o sync_queue.o containerstore.o serial.o
restore_identified_file : $(OBJECT) 
	$(LINK) $(FLAGS) $(LINKFLAGS) -o $@ $^ $(LIBS)

OBJECT := restore_migriated_file.o recipe.o common.o queue.o sync_queue.o containerstore.o serial.o
restore_migriated_file : $(OBJECT) 
	$(LINK) $(FLAGS) $(LINKFLAGS) -o $@ $^ $(LIBS)

OBJECT := get_total_data_size.o recipe.o common.o queue.o sync_queue.o containerstore.o serial.o
get_total_data_size : $(OBJECT) 
	$(LINK) $(FLAGS) $(LINKFLAGS) -o $@ $^ $(LIBS)

OBJECT := fid_to_path.o recipe.o common.o queue.o sync_queue.o containerstore.o serial.o
fid_to_path : $(OBJECT) 
	$(LINK) $(FLAGS) $(LINKFLAGS) -o $@ $^ $(LIBS)

.cpp.o:
	$(GCC) -c $(HEADER) $(FLAGS) $(GCCFLAGS) -o $@ $<

.c.o:
	$(GC) -c $(HEADER) $(FLAGS) -g -o $@ $<

install: $(TARGET)
	cp $(TARGET) $(BIN_PATH)

clean:
	rm -rf $(all) *.o *.so *.a


# ++++++++++++++++++++++++++++++++++++++
