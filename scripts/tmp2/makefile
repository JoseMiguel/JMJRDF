CC=gcc
CFLAGS=-I.
DEPS = struct_support.h
OBJ = struct.o struct_support.o 

struct.o: struct.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

struct_support.o: struct_support.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

structmake: $(OBJ)
	gcc -o $@ $^ $(CFLAGS)

clean:
	rm -rf *o structmake
